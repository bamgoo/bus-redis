package bus_redis

import (
	"context"
	"errors"
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/bamgoo/bamgoo"
	"github.com/bamgoo/bus"
	"github.com/redis/go-redis/v9"
)

var (
	errRedisInvalidConnection = errors.New("invalid redis connection")
	errRedisStopTimeout       = errors.New("redis bus stop timeout")
)

const (
	systemAnnounceTopic    = "announce"
	systemReplyTopic       = "reply"
	defaultAnnouncePeriod  = 10 * time.Second
	defaultAnnounceTimeout = 30 * time.Second
	defaultAnnounceJitter  = 2 * time.Second
	defaultReplyTTL        = 30 * time.Second
	defaultStopTimeout     = 8 * time.Second
	defaultQueueIdle       = 30 * time.Second
)

type (
	redisBusDriver struct{}

	redisBusConnection struct {
		mutex   sync.RWMutex
		running bool

		instance *bus.Instance
		setting  redisBusSetting
		client   *redis.Client

		subjects map[string]struct{}
		pubsubs  []*redis.PubSub

		identity bamgoo.NodeInfo
		cache    map[string]bamgoo.NodeInfo

		announceInterval time.Duration
		announceJitter   time.Duration
		announceTTL      time.Duration
		queueIdle        time.Duration
		queueGroup       string
		publishGroup     string
		queueConsumer    string
		done             chan struct{}
		wg               sync.WaitGroup

		stats map[string]*statsEntry
		rnd   *rand.Rand
	}

	redisBusSetting struct {
		Addr     string
		Username string
		Password string
		Database int
		Prefix   string
		Version  string

		AnnounceInterval time.Duration
		AnnounceJitter   time.Duration
		AnnounceTTL      time.Duration
		ReplyTTL         time.Duration
		StopTimeout      time.Duration
		QueueIdle        time.Duration
		QueueGroup       string
		PublishGroup     string
	}

	statsEntry struct {
		name         string
		numRequests  int
		numErrors    int
		totalLatency int64
	}

	requestMessage struct {
		ID    string `json:"id"`
		Reply string `json:"reply"`
		Data  []byte `json:"data"`
	}

	responseMessage struct {
		Data  []byte `json:"data"`
		Error string `json:"error,omitempty"`
	}

	announceMessage struct {
		Project  string   `json:"project"`
		Node     string   `json:"node"`
		Profile  string   `json:"profile"`
		Services []string `json:"services"`
		Updated  int64    `json:"updated"`
		Online   *bool    `json:"online,omitempty"`
	}
)

func init() {
	bamgoo.Register("redis", &redisBusDriver{})
}

func (d *redisBusDriver) Connect(inst *bus.Instance) (bus.Connection, error) {
	setting := redisBusSetting{
		Addr:     "127.0.0.1:6379",
		Database: 0,
		Prefix:   inst.Config.Prefix,
		Version:  "1.0.0",
	}

	cfg := inst.Config.Setting
	if v, ok := cfg["addr"].(string); ok && strings.TrimSpace(v) != "" {
		setting.Addr = strings.TrimSpace(v)
	}
	if v, ok := cfg["server"].(string); ok && strings.TrimSpace(v) != "" {
		setting.Addr = strings.TrimSpace(v)
	}
	if v, ok := cfg["host"].(string); ok && strings.TrimSpace(v) != "" {
		port := "6379"
		if p, ok := cfg["port"].(string); ok && strings.TrimSpace(p) != "" {
			port = strings.TrimSpace(p)
		}
		setting.Addr = strings.TrimSpace(v) + ":" + port
	}
	if v, ok := cfg["username"].(string); ok {
		setting.Username = v
	}
	if v, ok := cfg["password"].(string); ok {
		setting.Password = v
	}
	if v, ok := cfg["version"].(string); ok && strings.TrimSpace(v) != "" {
		setting.Version = strings.TrimSpace(v)
	}
	if n, ok := parseIntSetting(cfg["database"]); ok {
		setting.Database = n
	}

	setting.AnnounceInterval = parseDurationSetting(cfg["announce"])
	if setting.AnnounceInterval <= 0 {
		setting.AnnounceInterval = defaultAnnouncePeriod
	}
	setting.AnnounceTTL = parseDurationSetting(cfg["announce_ttl"])
	if setting.AnnounceTTL <= 0 {
		setting.AnnounceTTL = defaultAnnounceTimeout
	}
	setting.AnnounceJitter = parseDurationSetting(cfg["jitter"])
	if setting.AnnounceJitter <= 0 {
		// compatibility with older key
		setting.AnnounceJitter = parseDurationSetting(cfg["announce_jitter"])
	}
	if setting.AnnounceJitter <= 0 {
		setting.AnnounceJitter = defaultAnnounceJitter
	}
	setting.ReplyTTL = parseDurationSetting(cfg["reply_ttl"])
	if setting.ReplyTTL <= 0 {
		setting.ReplyTTL = defaultReplyTTL
	}
	setting.StopTimeout = parseDurationSetting(cfg["stop_timeout"])
	if setting.StopTimeout <= 0 {
		setting.StopTimeout = defaultStopTimeout
	}
	setting.QueueIdle = parseDurationSetting(cfg["queue_idle"])
	if setting.QueueIdle <= 0 {
		setting.QueueIdle = defaultQueueIdle
	}
	if v, ok := cfg["queue_group"].(string); ok {
		setting.QueueGroup = strings.TrimSpace(v)
	}
	if v := strings.TrimSpace(inst.Config.Group); v != "" {
		setting.PublishGroup = v
	}
	if v, ok := cfg["group"].(string); ok && strings.TrimSpace(v) != "" {
		setting.PublishGroup = strings.TrimSpace(v)
	}
	if v, ok := cfg["profile"].(string); ok && strings.TrimSpace(v) != "" {
		setting.PublishGroup = strings.TrimSpace(v)
	}

	project := strings.TrimSpace(bamgoo.Identity().Project)
	if project == "" {
		project = bamgoo.BAMGOO
	}
	identity := bamgoo.Identity()
	node := strings.TrimSpace(identity.Node)
	if node == "" {
		node = bamgoo.Generate("node")
	}
	profile := strings.TrimSpace(identity.Profile)
	if profile == "" {
		profile = bamgoo.BAMGOO
	}

	return &redisBusConnection{
		instance: inst,
		setting:  setting,
		client: redis.NewClient(&redis.Options{
			Addr:     setting.Addr,
			Username: setting.Username,
			Password: setting.Password,
			DB:       setting.Database,
		}),
		subjects:         make(map[string]struct{}, 0),
		pubsubs:          make([]*redis.PubSub, 0),
		identity:         bamgoo.NodeInfo{Project: project, Node: node, Profile: profile},
		cache:            make(map[string]bamgoo.NodeInfo, 0),
		announceInterval: setting.AnnounceInterval,
		announceJitter:   setting.AnnounceJitter,
		announceTTL:      setting.AnnounceTTL,
		queueIdle:        setting.QueueIdle,
		done:             make(chan struct{}),
		stats:            make(map[string]*statsEntry, 0),
		rnd:              rand.New(rand.NewSource(time.Now().UnixNano())),
	}, nil
}

func (c *redisBusConnection) Register(subject string) error {
	c.mutex.Lock()
	c.subjects[subject] = struct{}{}
	c.mutex.Unlock()
	return nil
}

func (c *redisBusConnection) Open() error {
	if c.client == nil {
		return errRedisInvalidConnection
	}
	return c.client.Ping(context.Background()).Err()
}

func (c *redisBusConnection) Close() error {
	if c.client == nil {
		return nil
	}
	return c.client.Close()
}

func (c *redisBusConnection) Start() error {
	c.mutex.Lock()
	if c.running {
		c.mutex.Unlock()
		return nil
	}
	if c.client == nil {
		c.mutex.Unlock()
		return errRedisInvalidConnection
	}

	for base := range c.subjects {
		callKey := "call." + base
		queueKey := "queue." + base
		groupKey := "publish." + base
		eventKey := "event." + base

		c.wg.Add(1)
		go c.consumeCall(base, callKey)

		if err := c.ensureQueueGroup(queueKey, c.resolveQueueGroup()); err != nil {
			c.mutex.Unlock()
			return err
		}
		if err := c.ensureQueueGroup(groupKey, c.resolvePublishGroup()); err != nil {
			c.mutex.Unlock()
			return err
		}
		c.wg.Add(1)
		go c.consumeQueue(base, queueKey, c.resolveQueueGroup())
		c.wg.Add(1)
		go c.consumeQueue(base, groupKey, c.resolvePublishGroup())

		ps := c.client.Subscribe(context.Background(), eventKey)
		c.pubsubs = append(c.pubsubs, ps)
		c.wg.Add(1)
		go c.consumeEvent(base, ps)
	}

	announceSub := c.client.Subscribe(context.Background(), c.announceSubject())
	c.pubsubs = append(c.pubsubs, announceSub)
	c.wg.Add(1)
	go c.consumeAnnounce(announceSub)

	c.running = true
	c.queueGroup = c.resolveQueueGroup()
	c.queueConsumer = c.resolveQueueConsumer()
	c.mutex.Unlock()

	c.publishAnnounce()

	c.wg.Add(2)
	go c.announceLoop()
	go c.gcLoop()

	return nil
}

func (c *redisBusConnection) Stop() error {
	c.mutex.Lock()
	if !c.running {
		c.mutex.Unlock()
		return nil
	}
	pubsubs := c.pubsubs
	c.pubsubs = nil
	done := c.done
	c.running = false
	c.mutex.Unlock()

	c.publishOffline()

	close(done)
	for _, ps := range pubsubs {
		_ = ps.Close()
	}
	waitDone := make(chan struct{})
	go func() {
		c.wg.Wait()
		close(waitDone)
	}()
	select {
	case <-waitDone:
		c.mutex.Lock()
		c.done = make(chan struct{})
		c.mutex.Unlock()
		return nil
	case <-time.After(c.setting.StopTimeout):
		return errRedisStopTimeout
	}
}

func (c *redisBusConnection) Request(subject string, data []byte, timeout time.Duration) ([]byte, error) {
	if c.client == nil {
		return nil, errRedisInvalidConnection
	}
	if timeout <= 0 {
		timeout = 5 * time.Second
	}

	replyKey := c.replyKey()
	req := requestMessage{ID: bamgoo.Generate("req"), Reply: replyKey, Data: data}
	body, err := bamgoo.Marshal(bamgoo.JSON, req)
	if err != nil {
		return nil, err
	}
	if err = c.client.LPush(context.Background(), subject, body).Err(); err != nil {
		return nil, err
	}

	values, err := c.client.BRPop(context.Background(), timeout, replyKey).Result()
	if err != nil {
		return nil, err
	}
	if len(values) < 2 {
		return nil, errors.New("invalid reply")
	}
	_ = c.client.Del(context.Background(), replyKey).Err()

	res := responseMessage{}
	if err := bamgoo.Unmarshal(bamgoo.JSON, []byte(values[1]), &res); err != nil {
		return nil, err
	}
	if strings.TrimSpace(res.Error) != "" {
		return nil, errors.New(res.Error)
	}
	return res.Data, nil
}

func (c *redisBusConnection) Publish(subject string, data []byte) error {
	if c.client == nil {
		return errRedisInvalidConnection
	}
	return c.client.Publish(context.Background(), subject, data).Err()
}

func (c *redisBusConnection) Enqueue(subject string, data []byte) error {
	if c.client == nil {
		return errRedisInvalidConnection
	}
	return c.client.XAdd(context.Background(), &redis.XAddArgs{
		Stream: subject,
		Values: map[string]any{
			"d": data,
		},
	}).Err()
}

func (c *redisBusConnection) Stats() []bamgoo.ServiceStats {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	all := make([]bamgoo.ServiceStats, 0, len(c.stats))
	for _, st := range c.stats {
		avg := int64(0)
		if st.numRequests > 0 {
			avg = st.totalLatency / int64(st.numRequests)
		}
		all = append(all, bamgoo.ServiceStats{
			Name:         st.name,
			Version:      c.setting.Version,
			NumRequests:  st.numRequests,
			NumErrors:    st.numErrors,
			TotalLatency: st.totalLatency,
			AvgLatency:   avg,
		})
	}
	return all
}

func (c *redisBusConnection) ListNodes() []bamgoo.NodeInfo {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	now := time.Now().UnixMilli()
	out := make([]bamgoo.NodeInfo, 0, len(c.cache))
	for _, item := range c.cache {
		if c.announceTTL > 0 && now-item.Updated > c.announceTTL.Milliseconds() {
			continue
		}
		item.Services = cloneStrings(item.Services)
		out = append(out, item)
	}

	sort.Slice(out, func(i, j int) bool {
		if out[i].Project == out[j].Project {
			if out[i].Profile == out[j].Profile {
				return out[i].Node < out[j].Node
			}
			return out[i].Profile < out[j].Profile
		}
		return out[i].Project < out[j].Project
	})
	return out
}

func (c *redisBusConnection) ListServices() []bamgoo.ServiceInfo {
	nodes := c.ListNodes()
	if len(nodes) == 0 {
		return nil
	}

	merged := make(map[string]*bamgoo.ServiceInfo)
	for _, node := range nodes {
		for _, svc := range node.Services {
			info, ok := merged[svc]
			if !ok {
				info = &bamgoo.ServiceInfo{Service: svc, Name: svc}
				merged[svc] = info
			}
			info.Nodes = append(info.Nodes, bamgoo.ServiceNode{Node: node.Node, Profile: node.Profile})
			if node.Updated > info.Updated {
				info.Updated = node.Updated
			}
		}
	}

	out := make([]bamgoo.ServiceInfo, 0, len(merged))
	for _, info := range merged {
		sort.Slice(info.Nodes, func(i, j int) bool {
			if info.Nodes[i].Profile == info.Nodes[j].Profile {
				return info.Nodes[i].Node < info.Nodes[j].Node
			}
			return info.Nodes[i].Profile < info.Nodes[j].Profile
		})
		info.Instances = len(info.Nodes)
		out = append(out, *info)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Service < out[j].Service })
	return out
}

func (c *redisBusConnection) consumeCall(name, key string) {
	defer c.wg.Done()
	for {
		select {
		case <-c.done:
			return
		default:
		}

		values, err := c.client.BRPop(context.Background(), time.Second, key).Result()
		if err != nil {
			if errors.Is(err, redis.Nil) {
				continue
			}
			time.Sleep(100 * time.Millisecond)
			continue
		}
		if len(values) < 2 {
			continue
		}

		req := requestMessage{}
		if err := bamgoo.Unmarshal(bamgoo.JSON, []byte(values[1]), &req); err != nil {
			continue
		}

		started := time.Now()
		respBytes, callErr := c.handleCall(req.Data)
		resp := responseMessage{}
		if callErr != nil {
			resp.Error = callErr.Error()
			c.recordStats(name, time.Since(started), callErr)
		} else {
			resp.Data = respBytes
			c.recordStats(name, time.Since(started), nil)
		}

		if strings.TrimSpace(req.Reply) == "" {
			continue
		}
		payload, err := bamgoo.Marshal(bamgoo.JSON, resp)
		if err != nil {
			continue
		}
		_ = c.client.LPush(context.Background(), req.Reply, payload).Err()
		_ = c.client.Expire(context.Background(), req.Reply, c.setting.ReplyTTL).Err()
	}
}

func (c *redisBusConnection) consumeQueue(name, key, group string) {
	defer c.wg.Done()
	consumer := c.resolveQueueConsumer()
	idle := c.queueIdle
	if idle <= 0 {
		idle = defaultQueueIdle
	}
	_ = c.ensureQueueGroup(key, group)

	for {
		select {
		case <-c.done:
			return
		default:
		}

		// reclaim stale pending entries first for at-least-once semantics.
		claimed, _, claimErr := c.client.XAutoClaim(context.Background(), &redis.XAutoClaimArgs{
			Stream:   key,
			Group:    group,
			Consumer: consumer,
			MinIdle:  idle,
			Start:    "0-0",
			Count:    1,
		}).Result()
		if claimErr != nil && !errors.Is(claimErr, redis.Nil) {
			time.Sleep(100 * time.Millisecond)
		}
		if len(claimed) > 0 {
			c.handleQueueStreams(name, key, group, claimed)
			continue
		}

		streams, err := c.client.XReadGroup(context.Background(), &redis.XReadGroupArgs{
			Group:    group,
			Consumer: consumer,
			Streams:  []string{key, ">"},
			Count:    1,
			Block:    time.Second,
		}).Result()
		if err != nil {
			if errors.Is(err, redis.Nil) {
				continue
			}
			time.Sleep(100 * time.Millisecond)
			continue
		}
		for _, stream := range streams {
			if len(stream.Messages) == 0 {
				continue
			}
			c.handleQueueStreams(name, key, group, stream.Messages)
		}
	}
}

func (c *redisBusConnection) handleQueueStreams(name, key, group string, messages []redis.XMessage) {
	for _, msg := range messages {
		raw, ok := msg.Values["d"]
		if !ok {
			_ = c.client.XAck(context.Background(), key, group, msg.ID).Err()
			_ = c.client.XDel(context.Background(), key, msg.ID).Err()
			continue
		}

		data, ok := bytesFromRedisValue(raw)
		if !ok {
			_ = c.client.XAck(context.Background(), key, group, msg.ID).Err()
			_ = c.client.XDel(context.Background(), key, msg.ID).Err()
			continue
		}

		started := time.Now()
		asyncErr := c.handleAsync(data)
		c.recordStats(name, time.Since(started), asyncErr)
		if asyncErr == nil {
			_ = c.client.XAck(context.Background(), key, group, msg.ID).Err()
			_ = c.client.XDel(context.Background(), key, msg.ID).Err()
		}
	}
}

func (c *redisBusConnection) ensureQueueGroup(stream, group string) error {
	err := c.client.XGroupCreateMkStream(context.Background(), stream, group, "0").Err()
	if err == nil {
		return nil
	}
	if strings.Contains(strings.ToUpper(err.Error()), "BUSYGROUP") {
		return nil
	}
	return err
}

func (c *redisBusConnection) resolveQueueGroup() string {
	if c.queueGroup != "" {
		return c.queueGroup
	}
	group := strings.TrimSpace(c.setting.QueueGroup)
	if group != "" {
		c.queueGroup = group
		return group
	}
	group = "_qg." + c.systemPrefixValue()
	c.queueGroup = group
	return group
}

func (c *redisBusConnection) resolvePublishGroup() string {
	if c.publishGroup != "" {
		return c.publishGroup
	}
	group := strings.TrimSpace(c.setting.PublishGroup)
	if group == "" {
		group = strings.TrimSpace(c.identity.Profile)
	}
	if group == "" {
		group = bamgoo.GLOBAL
	}
	c.publishGroup = "_pg." + group
	return c.publishGroup
}

func (c *redisBusConnection) resolveQueueConsumer() string {
	if c.queueConsumer != "" {
		return c.queueConsumer
	}
	name := strings.TrimSpace(c.identity.Node)
	if name == "" {
		name = bamgoo.Generate("node")
	}
	c.queueConsumer = "q." + name
	return c.queueConsumer
}

func (c *redisBusConnection) consumeEvent(name string, ps *redis.PubSub) {
	defer c.wg.Done()
	ch := ps.Channel()
	for {
		select {
		case msg, ok := <-ch:
			if !ok {
				return
			}
			started := time.Now()
			asyncErr := c.handleAsync([]byte(msg.Payload))
			c.recordStats(name, time.Since(started), asyncErr)
		case <-c.done:
			return
		}
	}
}

func (c *redisBusConnection) consumeAnnounce(ps *redis.PubSub) {
	defer c.wg.Done()
	ch := ps.Channel()
	for {
		select {
		case msg, ok := <-ch:
			if !ok {
				return
			}
			c.onAnnounce([]byte(msg.Payload))
		case <-c.done:
			return
		}
	}
}

func (c *redisBusConnection) announceLoop() {
	defer c.wg.Done()
	for {
		wait := c.nextAnnounceDelay()
		timer := time.NewTimer(wait)
		select {
		case <-timer.C:
			c.publishAnnounce()
		case <-c.done:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return
		}
	}
}

func (c *redisBusConnection) gcLoop() {
	defer c.wg.Done()
	interval := c.announceInterval
	if interval <= 0 {
		interval = defaultAnnouncePeriod
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			c.gcCache()
		case <-c.done:
			return
		}
	}
}

func (c *redisBusConnection) publishAnnounce() {
	c.publishAnnounceState(true)
}

func (c *redisBusConnection) publishOffline() {
	c.publishAnnounceState(false)
}

func (c *redisBusConnection) publishAnnounceState(online bool) {
	if c.client == nil {
		return
	}
	msg := announceMessage{
		Project: c.identity.Project,
		Node:    c.identity.Node,
		Profile: c.identity.Profile,
		Updated: time.Now().UnixMilli(),
	}
	if online {
		msg.Services = c.currentServices()
	}
	flag := online
	msg.Online = &flag

	data, err := bamgoo.Marshal(bamgoo.JSON, msg)
	if err != nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_ = c.client.Publish(ctx, c.announceSubject(), data).Err()
	c.onAnnounce(data)
}

func (c *redisBusConnection) onAnnounce(data []byte) {
	msg := announceMessage{}
	if err := bamgoo.Unmarshal(bamgoo.JSON, data, &msg); err != nil {
		return
	}
	if strings.TrimSpace(msg.Node) == "" {
		return
	}
	if strings.TrimSpace(msg.Project) == "" {
		msg.Project = bamgoo.BAMGOO
	}
	online := true
	if msg.Online != nil {
		online = *msg.Online
	}

	c.mutex.Lock()
	defer c.mutex.Unlock()

	key := msg.Project + "|" + msg.Node
	if !online {
		delete(c.cache, key)
		return
	}
	c.cache[key] = bamgoo.NodeInfo{
		Project:  msg.Project,
		Node:     msg.Node,
		Profile:  msg.Profile,
		Services: uniqueStrings(msg.Services),
		Updated:  msg.Updated,
	}
}

func (c *redisBusConnection) gcCache() {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if c.announceTTL <= 0 {
		return
	}
	now := time.Now().UnixMilli()
	for key, item := range c.cache {
		if now-item.Updated > c.announceTTL.Milliseconds() {
			delete(c.cache, key)
		}
	}
}

func (c *redisBusConnection) currentServices() []string {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	names := make([]string, 0, len(c.subjects))
	for name := range c.subjects {
		names = append(names, c.serviceName(name))
	}
	sort.Strings(names)
	return names
}

func (c *redisBusConnection) serviceName(subject string) string {
	if c.setting.Prefix == "" {
		return subject
	}
	return strings.TrimPrefix(subject, c.setting.Prefix)
}

func (c *redisBusConnection) announceSubject() string {
	return c.systemSubject(systemAnnounceTopic)
}

func (c *redisBusConnection) replyKey() string {
	id := bamgoo.Generate("reply")
	return c.systemSubject(systemReplyTopic + "." + id)
}

func (c *redisBusConnection) systemPrefixValue() string {
	if c.setting.Prefix != "" {
		return strings.TrimSuffix(c.setting.Prefix, ".")
	}
	project := strings.TrimSpace(c.identity.Project)
	if project == "" {
		project = strings.TrimSpace(bamgoo.Identity().Project)
	}
	if project == "" {
		project = bamgoo.BAMGOO
	}
	return project
}

func (c *redisBusConnection) systemSubject(msg string) string {
	return "_" + c.systemPrefixValue() + "." + msg
}

func (c *redisBusConnection) nextAnnounceDelay() time.Duration {
	base := c.announceInterval
	if base <= 0 {
		base = defaultAnnouncePeriod
	}
	jitter := c.announceJitter
	if jitter <= 0 || c.rnd == nil {
		return base
	}

	span := jitter.Milliseconds()
	if span <= 0 {
		return base
	}
	offsetMs := c.rnd.Int63n(span*2+1) - span
	delay := base + time.Duration(offsetMs)*time.Millisecond
	if delay < 100*time.Millisecond {
		delay = 100 * time.Millisecond
	}
	return delay
}

func (c *redisBusConnection) handleCall(data []byte) ([]byte, error) {
	if c.instance == nil {
		c.instance = &bus.Instance{}
	}
	return c.instance.HandleCall(data)
}

func (c *redisBusConnection) handleAsync(data []byte) error {
	if c.instance == nil {
		c.instance = &bus.Instance{}
	}
	return c.instance.HandleAsync(data)
}

func (c *redisBusConnection) recordStats(subject string, cost time.Duration, err error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	st, ok := c.stats[subject]
	if !ok {
		st = &statsEntry{name: subject}
		c.stats[subject] = st
	}
	st.numRequests++
	st.totalLatency += cost.Milliseconds()
	if err != nil {
		st.numErrors++
	}
}

func parseDurationSetting(v any) time.Duration {
	switch vv := v.(type) {
	case time.Duration:
		return vv
	case int:
		return time.Second * time.Duration(vv)
	case int64:
		return time.Second * time.Duration(vv)
	case float64:
		return time.Second * time.Duration(vv)
	case string:
		if d, err := time.ParseDuration(vv); err == nil {
			return d
		}
	}
	return 0
}

func bytesFromRedisValue(v any) ([]byte, bool) {
	switch vv := v.(type) {
	case []byte:
		return vv, true
	case string:
		return []byte(vv), true
	default:
		return nil, false
	}
}

func parseIntSetting(v any) (int, bool) {
	switch vv := v.(type) {
	case int:
		return vv, true
	case int64:
		return int(vv), true
	case float64:
		return int(vv), true
	case string:
		n, err := strconv.Atoi(strings.TrimSpace(vv))
		if err == nil {
			return n, true
		}
	}
	return 0, false
}

func uniqueStrings(in []string) []string {
	if len(in) == 0 {
		return []string{}
	}
	seen := make(map[string]struct{}, len(in))
	out := make([]string, 0, len(in))
	for _, item := range in {
		if _, ok := seen[item]; ok {
			continue
		}
		seen[item] = struct{}{}
		out = append(out, item)
	}
	sort.Strings(out)
	return out
}

func cloneStrings(in []string) []string {
	out := make([]string, len(in))
	copy(out, in)
	return out
}

var _ bus.Connection = (*redisBusConnection)(nil)
