package main

import (
	"bufio"
	"github.com/MorElf7/go-redis/utils"
	"log"
	"net"
	"os"
)

type Client struct {
	conn net.Conn
}

type ConsumerInterface interface {
	HandleMessage(msg *string)
}

type Consumer struct {
	Client
	ConsumerInterface
}

type Producer struct {
	Client
	clock *utils.HLC
}

func (c *Client) ConnectBroker(brokerAdr string) {
	conn, err := net.Dial("tcp", brokerAdr)
	if err != nil {
		log.Println("Error connected to broker:", err)
		return
	}
	c.conn = conn
}

func (c *Client) SendMessageToBroker(msg *utils.ClientMessage) {
	writer := bufio.NewWriter(c.conn)
	msgEncode, err := utils.MessageEncode(msg)
	if err != nil {
		log.Println(err)
	}

	writer.Write(msgEncode)
	writer.Flush()

	return
}

func (c *Consumer) Subscribe(brokerAdr, topic string, replay bool) {
	// Prepare handshake
	clientMessage := &utils.ClientMessage{
		Metadata: utils.Metadata{
			Role:   "consumer",
			Topic:  topic,
			Replay: replay,
		},
	}

	c.ConnectBroker(brokerAdr)
	defer c.conn.Close()
	c.SendMessageToBroker(clientMessage)
	brokerReader := bufio.NewReader(c.conn)
	brokerWriter := bufio.NewWriter(c.conn)
	var buf []byte
	for {
		_, err := brokerReader.Read(buf)
		if err != nil {
			log.Println("Error receiving message from broker:", err)
		}
		msgDecode, err := utils.MessageDecode(buf)
		if err != nil {
			log.Println("Error decoding broker message:", err)
		}

		brokerWriter.WriteString("ACK\n")
		brokerWriter.Flush()

		c.HandleMessage(&msgDecode.Payload.Content)
	}

}

func NewConsumer() *Consumer {
	filePath := "/var/log/go-message-queue/go-message-queue-consumer.txt"
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

	return &Consumer{}
}

func (p *Producer) Publish(brokerAdr, topic, message string) {
	// Prepare handshake
	physical, logical := p.clock.Now()
	hlcMessage := &utils.HLCMsg{
		Content:  message,
		Physical: physical,
		Logical:  logical,
	}

	clientMessage := &utils.ClientMessage{
		Payload: hlcMessage,
		Metadata: utils.Metadata{
			Role:  "producer",
			Topic: topic,
		},
	}

	p.ConnectBroker(brokerAdr)
	defer p.conn.Close()
	p.SendMessageToBroker(clientMessage)
}

func NewProducer() *Producer {
	filePath := "/var/log/go-message-queue/go-message-queue-producer.txt"
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

	return &Producer{
		clock: utils.NewHLC(),
	}
}
