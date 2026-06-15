package metrics

import (
	"sync"
	"sync/atomic"
	"time"
)

type Collector struct {
	messagesIn  int64
	messagesOut int64
	bytesIn     int64
	bytesOut    int64

	latencies []int64 // nanoseconds, ring buffer
	latIdx    int
	latMu     sync.Mutex

	msgPerSec  float64
	rateWin    []int64 // message counts per second window
	rateWinMu  sync.Mutex
	lastMsgIn  int64
	lastTick   time.Time

	mu sync.RWMutex
}

const latBufSize = 10000
const rateWinSize = 60 // 60s sliding window

func New() *Collector {
	c := &Collector{
		latencies: make([]int64, latBufSize),
		rateWin:   make([]int64, rateWinSize),
		lastTick:  time.Now(),
	}
	go c.tickLoop()
	return c
}

func (c *Collector) RecordPublish(bytes int64, latencyNs int64) {
	atomic.AddInt64(&c.messagesIn, 1)
	atomic.AddInt64(&c.bytesIn, bytes)
	c.latMu.Lock()
	c.latencies[c.latIdx%latBufSize] = latencyNs
	c.latIdx++
	c.latMu.Unlock()
}

func (c *Collector) RecordFetch(bytes int64) {
	atomic.AddInt64(&c.messagesOut, 1)
	atomic.AddInt64(&c.bytesOut, bytes)
}

func (c *Collector) Snapshot() Snapshot {
	c.rateWinMu.Lock()
	msgPerSec := c.msgPerSec
	c.rateWinMu.Unlock()

	c.latMu.Lock()
	avgLat, p99Lat := c.computeLatencies()
	c.latMu.Unlock()

	return Snapshot{
		MessagesIn:    atomic.LoadInt64(&c.messagesIn),
		MessagesOut:   atomic.LoadInt64(&c.messagesOut),
		BytesIn:       atomic.LoadInt64(&c.bytesIn),
		BytesOut:      atomic.LoadInt64(&c.bytesOut),
		MessagesPerSec: msgPerSec,
		AvgLatencyMs:  float64(avgLat) / 1e6,
		P99LatencyMs:  float64(p99Lat) / 1e6,
	}
}

type Snapshot struct {
	MessagesIn     int64
	MessagesOut    int64
	BytesIn        int64
	BytesOut       int64
	MessagesPerSec float64
	AvgLatencyMs   float64
	P99LatencyMs   float64
}

func (c *Collector) tickLoop() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	wIdx := 0
	for range ticker.C {
		current := atomic.LoadInt64(&c.messagesIn)
		c.rateWinMu.Lock()
		c.rateWin[wIdx%rateWinSize] = current - c.lastMsgIn
		c.lastMsgIn = current
		// compute rate over window
		var sum int64
		for _, v := range c.rateWin {
			sum += v
		}
		c.msgPerSec = float64(sum) / rateWinSize
		c.rateWinMu.Unlock()
		wIdx++
	}
}

func (c *Collector) computeLatencies() (avg int64, p99 int64) {
	size := c.latIdx
	if size == 0 {
		return 0, 0
	}
	if size > latBufSize {
		size = latBufSize
	}
	vals := make([]int64, size)
	copy(vals, c.latencies[:size])

	var sum int64
	for _, v := range vals {
		sum += v
	}
	avg = sum / int64(size)

	// simple sort-based p99
	sortInt64(vals)
	p99Idx := int(float64(size) * 0.99)
	if p99Idx >= size {
		p99Idx = size - 1
	}
	p99 = vals[p99Idx]
	return avg, p99
}

func sortInt64(a []int64) {
	// insertion sort (sufficient for perf monitoring)
	for i := 1; i < len(a); i++ {
		x := a[i]
		j := i - 1
		for j >= 0 && a[j] > x {
			a[j+1] = a[j]
			j--
		}
		a[j+1] = x
	}
}
