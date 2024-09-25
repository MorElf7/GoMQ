package main

import (
	"bufio"
	"log"
	"net"
	"os"
	"time"

	"github.com/MorElf7/go-redis/utils"
	badger "github.com/dgraph-io/badger/v4"
	"github.com/google/uuid"
)

func main() {
	filePath := "/var/log/go-message-queue/go-message-queue-broker.txt"
	dirPath := "/var/log/go-message-queue"

	// Create directory if it doesn't exist
	err := os.MkdirAll(dirPath, 0755)
	if err != nil {
		log.Println(err)
	}

	// Open or create the log file with write permissions
	file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("Failed to open log file: %v", err)
	}
	defer file.Close()

	// Set the output of the log to the file
	log.SetOutput(file)

	listener, err := net.Listen("tcp", ":8080")
	db, err := badger.Open(badger.DefaultOptions("/tmp/badger"))
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	if err != nil {
		log.Println("Error starting TCP server:", err)
		os.Exit(1)
	}
	defer listener.Close()
	log.Println("Server is listening on port 8080")

	// Init variables for run
	topicManager := utils.NewTopicManager()
	// topicManager.GetOrCreatePool("default")

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Println("Error accepting connection:", err)
			continue
		}
		go handleConnection(conn, topicManager)
	}
}

func handleConnection(conn net.Conn, topicManager *utils.TopicManager) {
	// defer conn.Close()
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	// Handshake phase
	var buf []byte
	_, err := reader.Read(buf)
	if err != nil {
		log.Println("Error reading message:", err)
		return
	}

	msg, err := utils.MessageDecode(buf)
	// TODO: Add authentication

	if msg.Metadata.Role == "producer" {
		// Ack to the client?
		// writer.WriteString("Connected")
		// writer.Flush()
		go handleProducer(conn, topicManager, &msg)
	} else if msg.Metadata.Role == "consumer" {
		// Ack to the client?
		// writer.WriteString("Connected")
		// writer.Flush()
		go handleConsumer(conn, topicManager, &msg)
	} else {
		conn.Close()
	}
}

func handleProducer(conn net.Conn, topicManager *utils.TopicManager, msg *utils.ClientMessage) {
	defer conn.Close()
	// Listening for message from producer
	message := msg.Payload
	// log.Printf("Received message: %s", message)
	id := uuid.New()
	message.ID = id.String()
	pool := topicManager.GetOrCreatePool(msg.Metadata.Topic)
	pool.MessageLog.UpdateClock(message.Physical, message.Logical)
	pool.MessageLog.AddMessage(message)

	// Ack to client?
	// writer.WriteString("Done")
	// writer.Flush()

	topicManager.PublishMessage(msg.Metadata.Topic, message)
}

func handleConsumer(conn net.Conn, topicManager *utils.TopicManager, msg *utils.ClientMessage) {
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
			maxRetries := 10
			// Attempt sending messages
			for attempt := 1; attempt <= maxRetries; attempt++ {
				writer.Write(msgEncode)
				writer.Flush()

				// Wait for acknowledgment with a timeout
				ackCh := make(chan string)
				timeoutCh := time.After(2 * time.Second)

				go func() {
					// Try to read acknowledgment from the consumer
					ack, err := reader.ReadString('\n')
					if err != nil {
						log.Println("Error receiving acknowledgment:", err)
						close(ackCh)
						return
					}
					ackCh <- ack
				}()

				// Wait for acknowledgment or timeout
				select {
				case ack := <-ackCh:
					if ack == "ACK" {
						log.Printf("Acknowledgment received for message ID: %v\n", msg.ID)
					} else {
						log.Printf("Failed to receive acknowledgment for message ID: %v\n", msg.ID)
					}
				case <-timeoutCh:
					log.Printf("Timeout! No acknowledgment received for message ID: %v\n", msg.ID)
					// You can add retry logic or mark the message as failed here
				}

				if attempt == maxRetries {
					// Reach Max Limit of failed attemps then close connection
					return
				}
			}

		}
	}
}
