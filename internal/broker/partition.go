package broker

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

type Message struct {
	Offset    int64
	Timestamp int64
	Key       []byte
	Value     []byte
}

type Partition struct {
	ID       int32
	Topic    string
	LeaderID int32

	segments   []*Segment
	nextOffset int64
	hwm        int64 // high watermark
	dataDir    string
	maxSegSize int64

	pool *sync.Pool
	mu   sync.RWMutex

	// batch accumulator
	batch     []*Message
	batchMu   sync.Mutex
	batchTimer *time.Timer
	flushCh   chan struct{}

	// metrics
	msgCount   int64
	bytesCount int64
}

func NewPartition(id int32, topic, dataDir string, maxSegSize int64) (*Partition, error) {
	p := &Partition{
		ID:         id,
		Topic:      topic,
		dataDir:    fmt.Sprintf("%s/%s/%d", dataDir, topic, id),
		maxSegSize: maxSegSize,
		flushCh:    make(chan struct{}, 1),
		pool: &sync.Pool{
			New: func() interface{} { return &Message{} },
		},
	}
	seg, err := newSegment(p.dataDir, 0, maxSegSize)
	if err != nil {
		return nil, err
	}
	p.segments = append(p.segments, seg)
	go p.batchFlusher()
	return p, nil
}

func (p *Partition) Append(key, value []byte) (int64, error) {
	msg := p.pool.Get().(*Message)
	msg.Offset = atomic.AddInt64(&p.nextOffset, 1) - 1
	msg.Timestamp = time.Now().UnixMilli()
	msg.Key = key
	msg.Value = value

	p.batchMu.Lock()
	p.batch = append(p.batch, msg)
	flush := len(p.batch) >= 500
	p.batchMu.Unlock()

	if flush {
		select {
		case p.flushCh <- struct{}{}:
		default:
		}
	}

	atomic.AddInt64(&p.msgCount, 1)
	atomic.AddInt64(&p.bytesCount, int64(len(key)+len(value)))
	return msg.Offset, nil
}

func (p *Partition) batchFlusher() {
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			p.flush()
		case <-p.flushCh:
			p.flush()
		}
	}
}

func (p *Partition) flush() {
	p.batchMu.Lock()
	if len(p.batch) == 0 {
		p.batchMu.Unlock()
		return
	}
	msgs := p.batch
	p.batch = nil
	p.batchMu.Unlock()

	p.mu.Lock()
	defer p.mu.Unlock()

	seg := p.activeSegment()
	for _, msg := range msgs {
		if seg.IsFull() {
			newSeg, err := newSegment(p.dataDir, msg.Offset, p.maxSegSize)
			if err == nil {
				p.segments = append(p.segments, newSeg)
				seg = newSeg
			}
		}
		seg.Append(msg)
		atomic.StoreInt64(&p.hwm, msg.Offset+1)
		p.pool.Put(msg)
	}
}

func (p *Partition) Fetch(fromOffset int64, maxBytes int) ([]*Message, int64) {
	p.flush()
	p.mu.RLock()
	defer p.mu.RUnlock()

	hwm := atomic.LoadInt64(&p.hwm)
	if fromOffset >= hwm {
		return nil, hwm
	}

	var result []*Message
	totalBytes := 0
	for _, seg := range p.segments {
		if seg.baseOffset > fromOffset {
			break
		}
		msgs, err := seg.Read(fromOffset, maxBytes-totalBytes)
		if err != nil {
			continue
		}
		result = append(result, msgs...)
		for _, m := range msgs {
			totalBytes += len(m.Key) + len(m.Value) + headerSize
		}
		if totalBytes >= maxBytes {
			break
		}
	}
	return result, hwm
}

func (p *Partition) HWM() int64        { return atomic.LoadInt64(&p.hwm) }
func (p *Partition) MsgCount() int64   { return atomic.LoadInt64(&p.msgCount) }
func (p *Partition) BytesCount() int64 { return atomic.LoadInt64(&p.bytesCount) }

func (p *Partition) activeSegment() *Segment {
	return p.segments[len(p.segments)-1]
}

func (p *Partition) Close() error {
	p.flush()
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, seg := range p.segments {
		seg.Close()
	}
	return nil
}
