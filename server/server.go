package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"time"

	"github.com/MorElf7/go-redis/utils"
	badger "github.com/dgraph-io/badger/v4"
	"github.com/google/uuid"
)

func main() {
	var logger = utils.NewLogger("./log-broker.txt")
	listener, err := net.Listen("tcp", ":8080")
	if err != nil {
		logger.Error("Error starting TCP server: %s", err.Error())
		os.Exit(1)
	}
	defer listener.Close()
	logger.Info("Server is listening on port 8080")

	db, err := badger.Open(badger.DefaultOptions("/tmp/badger"))
	if err != nil {
		logger.Error(err.Error())
		os.Exit(1)
	}
	defer db.Close()

	// Init variables for run
	topicManager := utils.NewTopicManager(logger)
	topicManager.LoadPools(db, logger)

	fmt.Println("Broker listening on port 8080")
	for {
		conn, err := listener.Accept()
		if err != nil {
			logger.Error("Error accepting connection: %s", err.Error())
			continue
		}
		go handleConnection(conn, db, topicManager, logger)
	}
}

func handleConnection(conn net.Conn, db *badger.DB, topicManager *utils.TopicManager, logger *utils.LoggerType) {
	// defer conn.Close()
	reader := bufio.NewReader(conn)
	// writer := bufio.NewWriter(conn)

	// Handshake phase
	// 1 Mb buffer
	buf := make([]byte, 1000000)
	n, err := reader.Read(buf)
	if err != nil {
		utils.HandleNetworkErrorByPeer(logger, err)
		logger.Error("Error reading handshake: %s", err.Error())
	}

	msg, err := utils.MessageDecode(buf[:n])
	// TODO: Add authentication

	if msg.Metadata.Role == "producer" {
		go handleProducer(conn, db, topicManager, &msg, logger)
	} else if msg.Metadata.Role == "consumer" {
		go handleConsumer(conn, topicManager, &msg, logger)
	} else {
		conn.Close()
	}
}

func handleProducer(conn net.Conn, db *badger.DB, topicManager *utils.TopicManager, msg *utils.ClientMessage, logger *utils.LoggerType) {
	defer conn.Close()
	message := msg.Payload
	id := uuid.New()
	message.ID = id.String()
	pool := topicManager.GetOrCreatePool(msg.Metadata.Topic)
	pool.MessageLog.UpdateClock(message.Physical, message.Logical)
	pool.MessageLog.AddMessage(message)

	// Ack to client?
	// writer := bufio.NewWriter(conn)
	// writer.WriteString("Done")
	// writer.Flush()

	topicManager.PublishMessage(db, logger, msg.Metadata.Topic, message)
}

func handleConsumer(conn net.Conn, topicManager *utils.TopicManager, msg *utils.ClientMessage, logger *utils.LoggerType) {
	defer conn.Close()
	topic := msg.Metadata.Topic
	replay := msg.Metadata.Replay
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)
	pool, exist := topicManager.Pools[topic]
	if !exist {
		return
	}
	id := uuid.New().String()
	topicManager.SubscribeConsumer(topic, id, conn)
	defer topicManager.UnsubscribeConsumer(topic, id)
	if replay {
		topicManager.ReplayMessageLog(topic, id)
	}
	pendingMessage := pool.Connections[id].PendingMessage
	for {
		msg := pendingMessage.GetNextMessage()
		if msg == nil {
			continue
		} else {
			clientMsg := &utils.ClientMessage{
				Payload: msg,
				Metadata: utils.Metadata{
					Topic: topic,
				},
			}
			msgEncode, err := utils.MessageEncode(clientMsg)
			if err != nil {
				logger.Error("Error decode message: %s", err)
			}
			maxRetries := 10
			// Attempt sending messages
			for attempt := 1; attempt <= maxRetries; attempt++ {
				logger.Info("Attemp %d sending message to consumer id %s", attempt, id)
				_, err := writer.Write(msgEncode)
				if err != nil {
					utils.HandleNetworkErrorByPeer(logger, err)
				} else {

					writer.Flush()

					// Wait for acknowledgment with a timeout
					ackCh := make(chan string)
					timeoutCh := time.After(2 * time.Second)

					go func() {
						// Try to read acknowledgment from the consumer
						ack, err := reader.ReadString('\n')
						if err != nil {
							utils.HandleNetworkErrorByPeer(logger, err)
							close(ackCh)
							return
						}
						ackCh <- ack
					}()

					// Wait for acknowledgment or timeout
					flag := false
					select {
					case ack := <-ackCh:
						if ack == "ACK\n" {
							flag = true
							logger.Info("Acknowledgment received for message ID: %v\n", msg.ID)
						} else {
							logger.Error("Failed to receive acknowledgment for message ID: %v\n", msg.ID)
						}
					case <-timeoutCh:
						logger.Error("Timeout! No acknowledgment received for message ID: %v\n", msg.ID)
					}

					if flag {
						break
					}

				}
				if attempt == maxRetries {
					// Reach Max Limit of failed attemps then close connection
					logger.Info("Reach max attemp sending message to consumer id %s", id)
					return
				}
			}

		}
	}
}
