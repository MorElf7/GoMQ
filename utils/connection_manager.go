package utils

import (
	"net"
	"os"
	"sync"

	"github.com/dgraph-io/badger/v4"
)

type ConsumerConnection struct {
	ID   string   // Unique identifier for the consumer
	Conn net.Conn // The network connection (e.g., TCP, WebSocket)
	// Offset         HLCMsg        // The last delivered message for this consumer
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

func NewTopicManager(logger *LoggerType) *TopicManager {
	return &TopicManager{
		Pools: make(map[string]*TopicPool),
	}
}

func (tm *TopicManager) GetOrCreatePool(topic string) *TopicPool {
	tm.Mutex.Lock()
	defer tm.Mutex.Unlock()

	if p, exists := tm.Pools[topic]; exists {
		return p
	}

	pool := &TopicPool{
		Topic:       topic,
		Connections: make(map[string]*ConsumerConnection),
		MessageLog:  NewMessageQueue(),
	}
	tm.Pools[topic] = pool
	return pool
}

// Save new message to disk
func (tm *TopicManager) SavePool(db *badger.DB, logger *LoggerType, topic string) {
	pool := tm.GetOrCreatePool(topic)

	err := db.Update(func(txn *badger.Txn) error {
		enc, err := EncodeQueue(pool.MessageLog)
		if err != nil {
			return err
		}
		txn.Set([]byte(topic), enc)
		return nil
	})

	if err != nil {
		logger.Error(err.Error())
		return
	}
}

// Load all saved pools on startup
func (tm *TopicManager) LoadPools(db *badger.DB, logger *LoggerType) {
	err := db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			k := item.Key()
			err := item.Value(func(v []byte) error {
				pool := tm.GetOrCreatePool(string(k))
				q, err := DecodeQueue(v)
				pool.MessageLog = q
				if err != nil {
					return err
				}
				return nil
			})
			if err != nil {
				return err
			}
		}

		return nil
	})

	if err != nil {
		logger.Error(err.Error())
		return
	}
	logger.Info("Load topic done")
}

func (tm *TopicManager) SubscribeConsumer(topic, consumerId string, conn net.Conn) {
	pool := tm.GetOrCreatePool(topic)

	pool.Mutex.Lock()
	defer pool.Mutex.Unlock()

	pool.Connections[consumerId] = &ConsumerConnection{
		ID:             consumerId,
		Conn:           conn,
		PendingMessage: NewMessageQueue(),
	}

}

func (tm *TopicManager) PublishMessage(db *badger.DB, logger *LoggerType, topic string, message *HLCMsg) {
	tm.Mutex.RLock()
	pool, exists := tm.Pools[topic]
	tm.Mutex.RUnlock()
	tm.SavePool(db, logger, topic)

	if !exists {
		logger.Info("No subscribers for topic %s", topic)
		return
	}

	pool.Mutex.RLock()
	defer pool.Mutex.RUnlock()

	for _, conn := range pool.Connections {
		go func(c *ConsumerConnection) {
			// c.Mutex.Lock()
			// defer c.Mutex.Unlock()
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

	if _, exists := pool.Connections[consumerId]; exists {
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

func HandleNetworkErrorByPeer(logger *LoggerType, err error) {
	if opErr, ok := err.(*net.OpError); ok && opErr.Err.Error() == "connection reset by peer" {
		logger.Info("Connection reset by peer detected")
		return
	} else if err.Error() == "EOF" {
		logger.Info("Connection closed by client")
		return
	} else if sysErr, ok := opErr.Err.(*os.SyscallError); ok && sysErr.Err.Error() == "broken pipe" {
		logger.Info("Connection closed by client")
		return
	}

	return
}

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
