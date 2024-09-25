package utils

import (
	"bytes"
	"container/heap"
	"encoding/gob"
	"os"
	"sync"
)

// Metadata struct
type Metadata struct {
	Role   string
	Token  string
	Topic  string
	Replay bool
}

// Client Message struct
type ClientMessage struct {
	Payload  *HLCMsg
	Metadata Metadata
}

// Message structure with HLC
type HLCMsg struct {
	ID       string
	Content  string
	Physical int64
	Logical  int64
}

type MessageHeap []*HLCMsg

func (h MessageHeap) Len() int { return len(h) }
func (h MessageHeap) Less(i, j int) bool {
	if h[i].Physical == h[j].Physical {
		return h[i].Logical < h[j].Logical
	}
	return h[i].Physical < h[j].Physical
}
func (h MessageHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *MessageHeap) Push(x interface{}) {
	*h = append(*h, x.(*HLCMsg))
}

func (h *MessageHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[0 : n-1]
	return item
}

type MessageQueue struct {
	mu    sync.Mutex
	heap  *MessageHeap
	clock *HLC
}

func NewMessageQueue() *MessageQueue {
	h := &MessageHeap{}
	heap.Init(h)
	return &MessageQueue{
		heap:  h,
		clock: NewHLC(),
	}
}

func (q *MessageQueue) AddMessage(msg *HLCMsg) {
	q.mu.Lock()
	defer q.mu.Unlock()

	// physical, logical := q.clock.Now()
	// msg := &HLCMsg{
	// 	ID:       id,
	// 	Content:  content,
	// 	Physical: physical,
	// 	Logical:  logical,
	// }

	heap.Push(q.heap, msg)
}

func (q *MessageQueue) PeekNextMessage() *HLCMsg {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.heap.Len() == 0 {
		return nil
	}
	return (*q.heap)[0]
}

func (q *MessageQueue) GetNextMessage() *HLCMsg {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.heap.Len() == 0 {
		return nil
	}
	return heap.Pop(q.heap).(*HLCMsg)
}

func (q *MessageQueue) UpdateClock(remotePhysical, remoteLogical int64) {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.clock.Update(remotePhysical, remoteLogical)
}

// Function to encode a client message struct using gob
func MessageEncode(message *ClientMessage) ([]byte, error) {
	var buffer bytes.Buffer
	encoder := gob.NewEncoder(&buffer)
	err := encoder.Encode(message)
	if err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}

// Function to decode a byte slice into a ClientMessage struct using gob
func MessageDecode(data []byte) (ClientMessage, error) {
	var message ClientMessage
	buffer := bytes.NewBuffer(data)
	decoder := gob.NewDecoder(buffer)
	err := decoder.Decode(&message)
	if err != nil {
		return ClientMessage{}, err
	}
	return message, nil
}

// Save queue to disk
func (q *MessageQueue) SaveQueue() error {
	//TODO
	q.mu.Lock()
	defer q.mu.Unlock()

	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(q); err != nil {
		return err
	}

	return os.WriteFile("data", buf.Bytes(), 0644)
}

// Load queue to disk
func (q *MessageQueue) LoadQueue() error {
	//TODO
	q.mu.Lock()
	defer q.mu.Unlock()

	data, err := os.ReadFile("data")
	if err != nil {
		return err
	}

	buf := bytes.NewBuffer(data)
	dec := gob.NewDecoder(buf)
	return dec.Decode(q)
}

func (m *HLCMsg) DeepCopy() *HLCMsg {
	return &HLCMsg{
		ID:       m.ID,
		Content:  m.Content,
		Physical: m.Physical,
		Logical:  m.Logical,
	}
}

func (h MessageHeap) DeepCopy() MessageHeap {
	copiedHeap := make(MessageHeap, len(h))
	for i, msg := range h {
		copiedHeap[i] = msg.DeepCopy()
	}
	// Create a new heap to ensure it's properly structured
	heap.Init(&copiedHeap)
	return copiedHeap
}
