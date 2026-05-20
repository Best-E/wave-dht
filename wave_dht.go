package main

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"math/rand"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"os/signal"
	"sort"
	"sync"
	"syscall"
	"time"

	"go.etcd.io/bbolt"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const (
	BucketSize = 256
	NumBuckets = 16
)

type ID [20]byte
func NewID(s string) ID { return sha256.Sum256([]byte(s)) }
func (id ID) Pot() float64 {
	var v uint64
	for i:=0;i<8;i++{v=(v<<8)|uint64(id[i])}
	return float64(v)/float64(^uint64(0))
}
func (id ID) Distance(other ID) float64 {
	return math.Abs(id.Pot() - other.Pot())
}
func (id ID) BucketIdx() int {
	return int(id.Pot() * float64(NumBuckets))
}

type Peer struct {
	ID ID `json:"id"`
	Addr string `json:"addr"`
	Pot float64 `json:"pot"`
	Ph float64 `json:"ph"`
	LastSeen int64 `json:"last_seen"`
	IsShortcut bool `json:"shortcut"`
}

type Entry struct {
	Val string `json:"val"`
	Exp int64 `json:"exp"`
	Ver int64 `json:"ver"`
	Hash [32]byte `json:"hash"`
}

type Config struct {
	Addr string
	Secret string
	DBPath string
	ReplicationFactor int
	TTL time.Duration
	MaxPeers int
	WaveTTL int
	WaveFanout int
	HTTPPort string
	NumShortcuts int
	SparseK int
}

type DHT struct {
	cfg Config
	me ID
	pot float64
	secret []byte
	
	db *bbolt.DB
	peers map[ID]Peer
	pmu sync.RWMutex
	
	gradientMap map[int]map[ID]float64
	gmu sync.RWMutex
	shortcuts []Peer
	
	conns sync.Pool
	rate map[string]float64
	rmu sync.Mutex
	
	ctx context.Context
	cancel context.CancelFunc
	
	lookupLatency prometheus.Histogram
	successCount prometheus.Counter
	peerCount prometheus.Gauge
	keyCount prometheus.Gauge
	msgsPerLookup prometheus.Histogram
	latencies []time.Duration
	lmu sync.Mutex
}

var (
	lookupLatency = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name: "wave_lookup_latency_ms", Buckets: prometheus.ExponentialBuckets(10, 2, 10),
	})
	successCount = prometheus.NewCounter(prometheus.CounterOpts{Name: "wave_lookup_success_total"})
	peerCount = prometheus.NewGauge(prometheus.GaugeOpts{Name: "wave_peer_count"})
	keyCount = prometheus.NewGauge(prometheus.GaugeOpts{Name: "wave_key_count"})
	msgsPerLookup = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name: "wave_msgs_per_lookup", Buckets: prometheus.LinearBuckets(1, 2, 15),
	})
)

func DefaultConfig() Config {
	return Config{
	Addr: "127.0.0.1:4000",
	Secret: "changeme",
		DBPath: "wave.db",
	ReplicationFactor: 3,
	TTL: 24*time.Hour,
		MaxPeers: 50,
	WaveTTL: 12,
	WaveFanout: 12,
	HTTPPort: "8081",
	NumShortcuts: 3,
		SparseK: 3,
	}
}

func NewDHT(cfg Config) (*DHT, error) {
	db, err := bbolt.Open(cfg.DBPath, 0600, &bbolt.Options{Timeout: 1*time.Second})
	if err!= nil { return nil, err }
	
	db.Update(func(tx *bbolt.Tx) error {
		tx.CreateBucketIfNotExists([]byte("store"))
		tx.CreateBucketIfNotExists([]byte("peers"))
		tx.CreateBucketIfNotExists([]byte("gradients"))
		return nil
	})
	
	id := NewID(cfg.Addr + time.Now().String())
	ctx, cancel := context.WithCancel(context.Background())
	
	d := &DHT{
		cfg: cfg, me: id, pot: id.Pot(), secret: []byte(cfg.Secret),
		db: db, peers: make(map[ID]Peer),
	gradientMap: make(map[int]map[ID]float64),
	rate: make(map[string]float64),
		ctx: ctx, cancel: cancel,
	lookupLatency: lookupLatency, successCount: successCount,
	peerCount: peerCount, keyCount: keyCount,
	msgsPerLookup: msgsPerLookup,
	latencies: make([]time.Duration, 0, 100),
	}
	
	d.loadPeers()
	d.loadGradients()
	d.buildShortcuts()
	
	go d.gossip()
	go d.decay()
	go d.expire()
	go d.refillRate()
	go d.antiEntropy()
	go d.updateMetrics()
	go d.maintainShortcuts()
	
	prometheus.MustRegister(lookupLatency, successCount, peerCount, keyCount, msgsPerLookup)
	return d, nil
}

func (d *DHT) Start() error {
	rpc.Register(d)
	l, err := net.Listen("tcp", d.cfg.Addr)
	if err!= nil { return err }
	
	go rpc.Accept(l)
	slog.Info("Neural Wave DHT started", "id", hex.EncodeToString(d.me[:4]), "addr", d.cfg.Addr)
	
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
	go func() {
	<-c
		slog.Info("Shutting down...")
		d.saveGradients()
		d.cancel()
		d.db.Close()
		os.Exit(0)
	}()
	return nil
}

func (d *DHT) Get(ctx context.Context, k string) (string, bool) {
	start := time.Now()
	defer func() {
		d.recordLatency(time.Since(start))
		d.lookupLatency.Observe(float64(time.Since(start).Milliseconds()))
	}()
	
	var entry Entry
	err := d.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte("store"))
		data := b.Get([]byte(k))
		if data == nil { return bbolt.ErrBucketNotFound }
		return json.Unmarshal(data, &entry)
	})
	if err == nil && entry.Exp > time.Now().Unix() {
		d.successCount.Inc()
		return entry.Val, true
	}
	
	target := NewID(k)
	candidates := d.selectNeighbors(target)
	
	type scoredPeer struct {
	peer Peer
		score float64
	}
	scored := make([]scoredPeer, len(candidates))
	for i, p := range candidates {
	gradient := d.getGradient(target.BucketIdx(), p.ID)
	pheromone := p.Ph
		random := rand.Float64()
		score := 0.6*gradient + 0.3*pheromone + 0.1*random
		scored[i] = scoredPeer{p, score}
	}
	
	sort.Slice(scored, func(i, j int) bool { return scored[i].score > scored[j].score })
	k := d.cfg.SparseK
	if k > len(scored) { k = len(scored) }
	
	type result struct {
		val string
		ok bool
	msgs int
	}
	ch := make(chan result, k)
	msgCount := 0
	
	for i := 0; i < k; i++ {
		p := scored[i].peer
		if p.ID == d.me { continue }
		msgCount++
		go func(peer Peer) {
			client, err := d.getConn(peer.Addr)
			if err!= nil { ch <- result{"", false, 0}; return }
			defer d.putConn(client)
			
			var res []string
			err = client.Call("DHT.Get", []string{k}, &res)
			if err == nil && len(res) > 0 {
				go d.Store(context.Background(), k, res[0])
				d.reinforce(peer.ID)
				d.updateGradient(target, peer.ID)
				ch <- result{res[0], true, 1}
			} else {
				ch <- result{"", false, 1}
			}
	}(p)
	}
	
	for i := 0; i < k; i++ {
		r := <-ch
		if r.ok {
			d.msgsPerLookup.Observe(float64(msgCount))
			d.successCount.Inc()
			return r.val, true
	}
	}
	
	d.msgsPerLookup.Observe(float64(msgCount))
	return "", false
}

func (d *DHT) selectNeighbors(target ID) []Peer {
	d.pmu.RLock()
	defer d.pmu.RUnlock()
	
	seen := make(map[ID]bool)
	candidates := make([]Peer, 0, 12)
	
	local := d.findClosest(target.Pot(), 3)
	candidates = append(candidates, local...)
	for _, p := range local { seen[p.ID] = true }
	
	for _, s := range d.shortcuts {
		if!seen[s.ID] && s.LastSeen > time.Now().Unix()-300 {
			candidates = append(candidates, s)
			seen[s.ID] = true
			if len(candidates) >= 6 { break }
	}
	}
	
	bucket := target.BucketIdx()
	d.gmu.RLock()
	if grads, ok := d.gradientMap[bucket]; ok {
		type gp struct{ id ID; score float64 }
		var gps []gp
		for id, score := range grads {
			if!seen[id] { gps = append(gps, gp{id, score}) }
	}
		sort.Slice(gps, func(i, j int) bool { return gps[i].score > gps[j].score })
		for i := 0; i < len(gps) && len(candidates) < 12; i++ {
			if p, ok := d.peers[gps[i].id]; ok {
				candidates = append(candidates, p)
			}
	}
	}
	d.gmu.RUnlock()
	
	return candidates
}

func (d *DHT) buildShortcuts() {
	d.pmu.RLock()
	defer d.pmu.RUnlock()
	
	d.shortcuts = []Peer{}
	if len(d.peers) < d.cfg.NumShortcuts { return }
	
	for i := 0; i < d.cfg.NumShortcuts; i++ {
		candidate := d.sampleKleinberg()
		if candidate.ID!= d.me {
			candidate.IsShortcut = true
			d.shortcuts = append(d.shortcuts, candidate)
	}
	}
}

func (d *DHT) sampleKleinberg() Peer {
	for {
		for _, p := range d.peers {
			dist := d.me.Distance(p.ID)
			if dist < 0.01 { continue }
			prob := 1.0 / (dist * dist)
			if rand.Float64() < prob*0.1 {
				return p
			}
	}
	}
}

func (d *DHT) maintainShortcuts() {
	t := time.NewTicker(60 * time.Second)
	for {
		select {
		case <-d.ctx.Done(): return
		case <-t.C:
			d.buildShortcuts()
	}
	}
}

func (d *DHT) getGradient(bucket int, peerID ID) float64 {
	d.gmu.RLock()
	defer d.gmu.RUnlock()
	if b, ok := d.gradientMap[bucket]; ok {
		if s, ok := b[peerID]; ok { return s }
	}
	return 0.1
}

func (d *DHT) updateGradient(target ID, peerID ID) {
	bucket := target.BucketIdx()
	d.gmu.Lock()
	defer d.gmu.Unlock()
	if d.gradientMap[bucket] == nil {
		d.gradientMap[bucket] = make(map[ID]float64)
	}
	d.gradientMap[bucket][peerID] = math.Min(d.gradientMap[bucket][peerID]+0.1, 1.0)
}

func (d *DHT) saveGradients() {
	d.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte("gradients"))
		b.DeleteBucket([]byte("data"))
		b, _ = b.CreateBucket([]byte("data"))
		for bucket, grads := range d.gradientMap {
			for peer, score := range grads {
				key := fmt.Sprintf("%d:%x", bucket, peer[:4])
				b.Put([]byte(key), []byte(fmt.Sprintf("%f", score)))
			}
	}
		return nil
	})
}

func (d *DHT) loadGradients() {
	d.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte("gradients"))
		if b == nil { return nil }
		b = b.Bucket([]byte("data"))
		if b == nil { return nil }
		c := b.Cursor()
		for k, v := c.First(); k!= nil; k, v = c.Next() {
			var bucket int
			var peerBytes []byte
			var score float64
			fmt.Sscanf(string(k), "%d:%x", &bucket, &peerBytes)
			fmt.Sscanf(string(v), "%f", &score)
			if d.gradientMap[bucket] == nil {
				d.gradientMap[bucket] = make(map[ID]float64)
			}
			var peer ID
			copy(peer[:], peerBytes)
			d.gradientMap[bucket][peer] = score
	}
		return nil
	})
}

func (d *DHT) Store(ctx context.Context, k, v string) error {
	start := time.Now()
	defer func() {
		d.recordLatency(time.Since(start))
		d.lookupLatency.Observe(float64(time.Since(start).Milliseconds()))
	}()
	
	tp := NewID(k).Pot()
	best := d.findClosest(tp, d.cfg.ReplicationFactor*2)
	
	ver := time.Now().UnixNano()
	hash := sha256.Sum256([]byte(v))
	entry := Entry{v, time.Now().Add(d.cfg.TTL).Unix(), ver, hash}
	
	success := 0
	for i := 0; i < len(best) && success < d.cfg.ReplicationFactor; i++ {
		n := best[i]
		if n.ID == d.me {
			err := d.db.Update(func(tx *bbolt.Tx) error {
				b := tx.Bucket([]byte("store"))
				data, _ := json.Marshal(entry)
				return b.Put([]byte(k), data)
			})
			if err == nil { success++ }
	} else {
			client, err := d.getConn(n.Addr)
			if err!= nil { continue }
			msg := fmt.Sprintf("%s%s%d", k, v, ver)
			args := []string{k, v, d.sign(msg), fmt.Sprint(ver)}
			var ok bool
			if client.Call("DHT.Put", args, &ok) == nil && ok {
				success++
			}
			d.putConn(client)
	}
		d.reinforce(n.ID)
	}
	
	if success > 0 {
		d.successCount.Inc()
		return nil
	}
	return fmt.Errorf("replication failed")
}

func (d *DHT) findClosest(tp float64, n int) []Peer {
	d.pmu.RLock()
	defer d.pmu.RUnlock()
	
	peers := make([]Peer, 0, len(d.peers))
	for _, p := range d.peers {
		if p.LastSeen > time.Now().Unix()-300 {
			peers = append(peers, p)
	}
	}
	
	sort.Slice(peers, func(i, j int) bool {
		return math.Abs(peers[i].Pot-tp) < math.Abs(peers[j].Pot-tp)
	})
	
	if len(peers) > n { peers = peers[:n] }
	return peers
}

func (d *DHT) getConn(addr string) (*rpc.Client, error) {
	if v := d.conns.Get(); v!= nil {
		if c, ok := v.(*rpc.Client); ok {
			if err := c.Call("DHT.Ping", 0, nil); err == nil {
				return c, nil
			}
	}
	}
	return rpc.DialTimeout("tcp", addr, 2*time.Second)
}

func (d *DHT) putConn(c *rpc.Client) {
	d.conns.Put(c)
}

func (d *DHT) Ping(_ int, _ *int) error { return nil }

func (d *DHT) Put(args []string, reply *bool) error {
	if len(args) < 4 { return fmt.Errorf("bad args") }
	if!d.verify(args[0]+args[1]+args[3], args[2]) { return fmt.Errorf("bad auth") }
	
	ver := int64(0)
	fmt.Sscanf(args[3], "%d", &ver)
	hash := sha256.Sum256([]byte(args[1]))
	entry := Entry{args[1], time.Now().Add(d.cfg.TTL).Unix(), ver, hash}
	
	err := d.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte("store"))
		data, _ := json.Marshal(entry)
		return b.Put([]byte(args[0]), data)
	})
	*reply = err == nil
	return err
}

func (d *DHT) Get(args []string, reply *[]string) error {
	if len(args) < 1 { return fmt.Errorf("bad args") }
	var e Entry
	err := d.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte("store"))
		data := b.Get([]byte(args[0]))
		if data == nil { return bbolt.ErrBucketNotFound }
		return json.Unmarshal(data, &e)
	})
	if err == nil && e.Exp > time.Now().Unix() {
	*reply = []string{e.Val}
	}
	return nil
}

func (d *DHT) Peers(args []string, reply *[]Peer) error {
	if len(args) < 2 { return fmt.Errorf("bad args") }
	if!d.verify(args[0], args[1]) { return fmt.Errorf("bad auth") }
	
	d.addPeer(Peer{NewID(args[0]), "", NewID(args[0]).Pot(), 0.1, time.Now().Unix(), false})
	d.pmu.RLock()
	for _, p := range d.peers { *reply = append(*reply, p) }
	d.pmu.RUnlock()
	return nil
}

func (d *DHT) gossip() {
	for {
		select {
		case <-d.ctx.Done(): return
		case <-time.Tick(15*time.Second):
			d.pmu.RLock()
			for _, p := range d.peers {
				go d.exchange(p.Addr)
			}
			d.pmu.RUnlock()
	}
	}
}

func (d *DHT) exchange(addr string) {
	client, err := d.getConn(addr)
	if err!= nil { return }
	defer d.putConn(client)
	
	msg := fmt.Sprint(time.Now().Unix()/60)
	sig := d.sign(msg)
	var ps []Peer
	client.Call("DHT.Peers", []string{msg, sig}, &ps)
	for _, p := range ps {
		if p.ID!= d.me { d.addPeer(p) }
	}
}

func (d *DHT) addPeer(p Peer) {
	d.pmu.Lock()
	defer d.pmu.Unlock()
	if _, ok := d.peers[p.ID];!ok && len(d.peers) < d.cfg.MaxPeers {
		p.Ph = 0.1
		p.LastSeen = time.Now().Unix()
		d.peers[p.ID] = p
	} else if ok {
		p.LastSeen = time.Now().Unix()
		d.peers[p.ID] = p
	}
}

func (d *DHT) decay() {
	for {
		select {
		case <-d.ctx.Done(): return
		case <-time.Tick(60*time.Second):
			d.pmu.Lock()
			for id, p := range d.peers {
				p.Ph *= 0.9
				d.peers[id] = p
			}
			d.pmu.Unlock()
			
			d.gmu.Lock()
			for bucket, grads := range d.gradientMap {
				for id, score := range grads {
					d.gradientMap[bucket][id] = score * 0.95
				}
			}
			d.gmu.Unlock()
	}
	}
}

func (d *DHT) expire() {
	for {
		select {
		case <-d.ctx.Done(): return
		case <-time.Tick(5*time.Minute):
			now := time.Now().Unix()
			d.db.Update(func(tx *bbolt.Tx) error {
				b := tx.Bucket([]byte("store"))
				c := b.Cursor()
				for k, v := c.First(); k!= nil; k, v = c.Next() {
					var e Entry
					json.Unmarshal(v, &e)
					if e.Exp < now { b.Delete(k) }
				}
				return nil
			})
	}
	}
}

func (d *DHT) antiEntropy() {
	t := time.NewTicker(2 * time.Minute)
	for {
		select {
		case <-d.ctx.Done(): return
		case <-t.C: d.repair()
	}
	}
}

func (d *DHT) repair() {
	keys := d.sampleKeys(100)
	for _, k := range keys {
		tp := NewID(k).Pot()
	peers := d.findClosest(tp, 3)
		for _, p := range peers {
			if p.ID == d.me { continue }
			client, err := d.getConn(p.Addr)
			if err!= nil { continue }
			var hash []byte
			if client.Call("DHT.Hash", k, &hash) == nil {
				localHash := d.localHash(k)
				if!equalBytes(localHash[:], hash) {
					var val []string
					if client.Call("DHT.Get", []string{k}, &val) == nil && len(val) > 0 {
						d.Store(context.Background(), k, val[0])
					}
				}
			}
			d.putConn(client)
	}
	}
}

func (d *DHT) sampleKeys(n int) []string {
	keys := []string{}
	d.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte("store"))
		c := b.Cursor()
		for k, _ := c.First(); k!= nil && len(keys) < n; k, _ = c.Next() {
			keys = append(keys, string(k))
	}
		return nil
	})
	return keys
}

func (d *DHT) localHash(k string) [32]byte {
	var e Entry
	d.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte("store"))
		data := b.Get([]byte(k))
		if data!= nil { json.Unmarshal(data, &e) }
		return nil
	})
	return e.Hash
}

func (d *DHT) loadPeers() {
	d.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte("peers"))
		c := b.Cursor()
		for k, v := c.First(); k!= nil; k, v = c.Next() {
			var p Peer
			json.Unmarshal(v, &p)
			d.peers[p.ID] = p
	}
		return nil
	})
}

func (d *DHT) updateMetrics() {
	t := time.NewTicker(10*time.Second)
	for {
		select {
		case <-d.ctx.Done(): return
		case <-t.C:
			d.pmu.RLock()
			peerCount.Set(float64(len(d.peers)))
			d.pmu.RUnlock()
			d.db.View(func(tx *bbolt.Tx) error {
				keyCount.Set(float64(tx.Bucket([]byte("store")).Stats().KeyN))
				return nil
			})
	}
	}
}

func (d *DHT) allow(ip string) bool {
	d.rmu.Lock()
	defer d.rmu.Unlock()
	tokens, ok := d.rate[ip]
	if!ok { tokens = 10 }
	if tokens < 1 { return false }
	d.rate[ip] = tokens - 1
	return true
}

func (d *DHT) refillRate() {
	for {
		select {
		case <-d.ctx.Done(): return
		case <-time.Tick(1*time.Second):
			d.rmu.Lock()
			for ip, tokens := range d.rate {
				if tokens < 10 { d.rate[ip] = math.Min(tokens+1, 10) }
				if tokens <= 0 { delete(d.rate, ip) }
			}
			d.rmu.Unlock()
	}
	}
}

func (d *DHT) sign(msg string) string {
	h := hmac.New(sha256.New, d.secret)
	h.Write([]byte(msg))
	return hex.EncodeToString(h.Sum(nil))
}

func (d *DHT) verify(msg, sig string) bool {
	expected := d.sign(msg)
	return hmac.Equal([]byte(expected), []byte(sig))
}

func (d *DHT) reinforce(id ID) {
	d.pmu.Lock()
	defer d.pmu.Unlock()
	if p, ok := d.peers[id]; ok {
		p.Ph = math.Min(p.Ph+0.3, 1)
		d.peers[id] = p
	}
}

func (d *DHT) recordLatency(dur time.Duration) {
	d.lmu.Lock()
	defer d.lmu.Unlock()
	d.latencies = append(d.latencies, dur)
	if len(d.latencies) > 100 { d.latencies = d.latencies[1:] }
}

func (d *DHT) avgLatency() float64 {
	d.lmu.Lock()
	defer d.lmu.Unlock()
	if len(d.latencies) == 0 { return 0 }
	var sum time.Duration
	for _, d := range d.latencies { sum += d }
	return float64(sum.Milliseconds()) / float64(len(d.latencies))
}

func (d *DHT) Stats() map[string]interface{} {
	d.pmu.RLock()
	pc := len(d.peers)
	d.pmu.RUnlock()
	var kc int
	d.db.View(func(tx *bbolt.Tx) error {
		kc = tx.Bucket([]byte("store")).Stats().KeyN
		return nil
	})
	return map[string]interface{}{
	"peers": pc,
	"keys": kc,
	"avg_latency_ms": d.avgLatency(),
	"id": hex.EncodeToString(d.me[:4]),
	"shortcuts": len(d.shortcuts),
	}
}

func (d *DHT) httpPut(w http.ResponseWriter, r *http.Request) {
	if!d.allow(r.RemoteAddr) { http.Error(w, "rate limit", 429); return }
	var m map[string]string
	json.NewDecoder(r.Body).Decode(&m)
	err := d.Store(r.Context(), m["k"], m["v"])
	json.NewEncoder(w).Encode(map[string]bool{"ok": err == nil})
}

func (d *DHT) httpGet(w http.ResponseWriter, r *http.Request) {
	if!d.allow(r.RemoteAddr) { http.Error(w, "rate limit", 429); return }
	v, ok := d.Get(r.Context(), r.URL.Query().Get("k"))
	json.NewEncoder(w).Encode(map[string]interface{}{"v": v, "ok": ok})
}

func (d *DHT) httpStats(w http.ResponseWriter, r *http.Request) {
	json.NewEncoder(w).Encode(d.Stats())
}

func equalBytes(a, b []byte) bool {
	if len(a)!= len(b) { return false }
	for i := range a {
		if a[i]!= b[i] { return false }
	}
	return true
}

func main() {
	rand.Seed(time.Now().UnixNano())
	cfg := DefaultConfig()
	
	if len(os.Args) > 1 { cfg.Addr = os.Args[1] }
	if len(os.Args) > 2 { cfg.Secret = os.Args[2] }
	if len(os.Args) > 3 { cfg.DBPath = os.Args[3] }
	
	d, err := NewDHT(cfg)
	if err!= nil { panic(err) }
	d.Start()
	
	if len(os.Args) > 4 {
		d.addPeer(Peer{NewID(os.Args[4]), os.Args[4], NewID(os.Args[4]).Pot(), 0.1, time.Now().Unix(), false})
	}
	
	http.Handle("/metrics", promhttp.Handler())
	http.HandleFunc("/put", d.httpPut)
	http.HandleFunc("/get", d.httpGet)
	http.HandleFunc("/stats", d.httpStats)
	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("ok"))
	})
	
	slog.Info("HTTP API listening", "addr", ":"+cfg.HTTPPort)
	http.ListenAndServe(":"+cfg.HTTPPort, nil)
}
