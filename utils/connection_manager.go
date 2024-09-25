package utils

import (
	"log"
	"net"
	"sync"
	"time"
)

type ConsumerConnection struct {
	ID             string        // Unique identifier for the consumer
	Conn           net.Conn      // The network connection (e.g., TCP, WebSocket)
	Offset         HLCMsg        // The last delivered message for this consumer
	PendingMessage *MessageQueue // Message pending to be delivered
	// Active         bool          // Show status of connection
}

type TopicPool struct {
	Topic       string                         // The topic this pool is for
	Connections map[string]*ConsumerConnection // Map of consumer ID to connection
	Mutex       sync.RWMutex                   // Mutex for thread-safe access
	MessageLog  *MessageQueue                  // Persisten Log of all message
}

type TopicManager struct {
	Pools map[string]*TopicPool // Map of topic name to topic pool
	Mutex sync.RWMutex          // Mutex for thread-safe access
}

func NewTopicManager() *TopicManager {
	return &TopicManager{
		Pools: make(map[string]*TopicPool),
	}
}

func (tm *TopicManager) GetOrCreatePool(topic string) *TopicPool {
	tm.Mutex.Lock()
	defer tm.Mutex.Unlock()

	if pool, exists := tm.Pools[topic]; exists {
		return pool
	}

	pool := &TopicPool{
		Topic:       topic,
		Connections: make(map[string]*ConsumerConnection),
		MessageLog:  NewMessageQueue(),
	}
	tm.Pools[topic] = pool
	return pool
}

func (tm *TopicManager) SubscribeConsumer(topic, consumerId string, conn net.Conn) {
	pool := tm.GetOrCreatePool(topic)

	pool.Mutex.Lock()
	defer pool.Mutex.Unlock()

	pool.Connections[consumerId] = &ConsumerConnection{
		ID:   consumerId,
		Conn: conn,
	}
}

func (tm *TopicManager) PublishMessage(topic string, message HLCMsg) {
	tm.Mutex.RLock()
	pool, exists := tm.Pools[topic]
	tm.Mutex.RUnlock()

	if !exists {
		log.Printf("No subscribers for topic %s", topic)
		return
	}

	pool.Mutex.RLock()
	defer pool.Mutex.RUnlock()

	for _, conn := range pool.Connections {
		go func(c *ConsumerConnection) {
			c.PendingMessage.AddMessage(message)
		}(conn)
	}
}

func (tm *TopicManager) UnsubscribeConsumer(topic, consumerId string) {
	tm.Mutex.RLock()
	pool, exists := tm.Pools[topic]
	tm.Mutex.RUnlock()

	if !exists {
		return
	}

	pool.Mutex.Lock()
	defer pool.Mutex.Unlock()

	if conn, exists := pool.Connections[consumerId]; exists {
		// conn.Conn.Close() // Optionally close the connection
		delete(pool.Connections, consumerId)
	}
}

// func (tm *TopicManager) CleanupInactiveConnections(topic string, timeout time.Duration) {
// 	tm.Mutex.RLock()
// 	pool, exists := tm.Pools[topic]
// 	tm.Mutex.RUnlock()
//
// 	if !exists {
// 		return
// 	}
//
// 	pool.Mutex.Lock()
// 	defer pool.Mutex.Unlock()
//
// 	for id, conn := range pool.Connections {
// 		if time.Since(conn.LastSeen) > timeout {
// 			conn.Conn.Close()
// 			delete(pool.Connections, id)
// 			log.Printf("Closed inactive connection %s for topic %s", id, topic)
// 		}
// 	}
// }
//
// func (tm *TopicManager) UpdateConsumerActivity(topic, consumerId string) {
// 	tm.Mutex.RLock()
// 	pool, exists := tm.Pools[topic]
// 	tm.Mutex.RUnlock()
//
// 	if !exists {
// 		return
// 	}
//
// 	pool.Mutex.Lock()
// 	defer pool.Mutex.Unlock()
//
// 	if conn, exists := pool.Connections[consumerId]; exists {
// 		conn.LastSeen = time.Now()
// 	}
// }

func (tm *TopicManager) ReplayMessageLog(topic, consumerId string) {
	tm.Mutex.RLock()
	pool, exists := tm.Pools[topic]
	tm.Mutex.RUnlock()

	if !exists {
		return
	}

	pool.Mutex.Lock()
	defer pool.Mutex.Unlock()

	copyH := pool.MessageLog.heap.DeepCopy()
	pool.Connections[consumerId].PendingMessage.heap = (*MessageHeap)(&copyH)
}
