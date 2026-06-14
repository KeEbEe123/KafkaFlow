package broker

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sync"
	"time"
)

const headerSize = 24 // offset(8) + timestamp(8) + keyLen(4) + valueLen(4)

type Segment struct {
	baseOffset int64
	file       *os.File
	indexFile  *os.File
	size       int64
	maxSize    int64
	index      []indexEntry
	createdAt  time.Time
	mu         sync.RWMutex
}

type indexEntry struct {
	offset   int64
	position int64
}

func newSegment(dir string, baseOffset int64, maxSize int64) (*Segment, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}
	logPath := fmt.Sprintf("%s/%020d.log", dir, baseOffset)
	idxPath := fmt.Sprintf("%s/%020d.index", dir, baseOffset)

	f, err := os.OpenFile(logPath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}
	idxF, err := os.OpenFile(idxPath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		f.Close()
		return nil, err
	}
	info, _ := f.Stat()
	return &Segment{
		baseOffset: baseOffset,
		file:       f,
		indexFile:  idxF,
		size:       info.Size(),
		maxSize:    maxSize,
		createdAt:  time.Now(),
	}, nil
}

func (s *Segment) Append(msg *Message) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	pos := s.size
	buf := make([]byte, headerSize+len(msg.Key)+len(msg.Value))
	binary.BigEndian.PutUint64(buf[0:8], uint64(msg.Offset))
	binary.BigEndian.PutUint64(buf[8:16], uint64(msg.Timestamp))
	binary.BigEndian.PutUint32(buf[16:20], uint32(len(msg.Key)))
	binary.BigEndian.PutUint32(buf[20:24], uint32(len(msg.Value)))
	copy(buf[24:], msg.Key)
	copy(buf[24+len(msg.Key):], msg.Value)

	if _, err := s.file.Write(buf); err != nil {
		return err
	}
	s.size += int64(len(buf))

	// sparse index: every 8th entry
	if len(s.index)%8 == 0 {
		idxBuf := make([]byte, 16)
		binary.BigEndian.PutUint64(idxBuf[0:8], uint64(msg.Offset))
		binary.BigEndian.PutUint64(idxBuf[8:16], uint64(pos))
		s.indexFile.Write(idxBuf)
		s.index = append(s.index, indexEntry{offset: msg.Offset, position: pos})
	}
	return nil
}

func (s *Segment) Read(fromOffset int64, maxBytes int) ([]*Message, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	startPos := s.findPosition(fromOffset)
	f, err := os.Open(s.file.Name())
	if err != nil {
		return nil, err
	}
	defer f.Close()

	if _, err := f.Seek(startPos, io.SeekStart); err != nil {
		return nil, err
	}

	var msgs []*Message
	totalBytes := 0
	header := make([]byte, headerSize)

	for totalBytes < maxBytes {
		if _, err := io.ReadFull(f, header); err != nil {
			break
		}
		offset := int64(binary.BigEndian.Uint64(header[0:8]))
		ts := int64(binary.BigEndian.Uint64(header[8:16]))
		keyLen := int(binary.BigEndian.Uint32(header[16:20]))
		valLen := int(binary.BigEndian.Uint32(header[20:24]))

		body := make([]byte, keyLen+valLen)
		if _, err := io.ReadFull(f, body); err != nil {
			break
		}
		if offset < fromOffset {
			continue
		}
		msgs = append(msgs, &Message{
			Offset:    offset,
			Timestamp: ts,
			Key:       body[:keyLen],
			Value:     body[keyLen:],
		})
		totalBytes += headerSize + keyLen + valLen
	}
	return msgs, nil
}

func (s *Segment) findPosition(offset int64) int64 {
	pos := int64(0)
	for _, e := range s.index {
		if e.offset > offset {
			break
		}
		pos = e.position
	}
	return pos
}

func (s *Segment) IsFull() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.size >= s.maxSize
}

func (s *Segment) Close() error {
	s.file.Sync()
	s.indexFile.Sync()
	s.file.Close()
	return s.indexFile.Close()
}
