package client

import (
	"bufio"
	"net"

	"github.com/MorElf7/go-redis/utils"
)

type Client struct {
	Conn   net.Conn
	Logger *utils.LoggerType
}

type Consumer struct {
	Client
	run         bool
	EachMessage func(msg string)
}

type Producer struct {
	Client
	Clock *utils.HLC
}

func (c *Client) ConnectBroker(brokerAdr string) error {
	conn, err := net.Dial("tcp", brokerAdr)
	if err != nil {
		c.Logger.Error("Error connected to broker: %s", err)
		return err
	}
	c.Conn = conn
	return nil
}

func (c *Client) SendMessageToBroker(msg *utils.ClientMessage) error {
	writer := bufio.NewWriter(c.Conn)
	msgEncode, err := utils.MessageEncode(msg)
	if err != nil {
		c.Logger.Error(err.Error())
	}

	_, err = writer.Write(msgEncode)
	if err != nil {
		return err
	}
	writer.Flush()

	// Add ack for message published ?

	return nil
}

func (c *Consumer) Subscribe(brokerAdr, topic string, replay bool) error {
	// Prepare handshake
	clientMessage := &utils.ClientMessage{
		Metadata: utils.Metadata{
			Role:   "consumer",
			Topic:  topic,
			Replay: replay,
		},
	}

	c.ConnectBroker(brokerAdr)
	defer c.Conn.Close()
	c.SendMessageToBroker(clientMessage)
	brokerReader := bufio.NewReader(c.Conn)
	brokerWriter := bufio.NewWriter(c.Conn)
	buf := make([]byte, 1000000)
	c.run = true
	for c.run {
		// 1 Mb buffer
		n, err := brokerReader.Read(buf)
		if err != nil {
			return err
		}
		msgDecode, err := utils.MessageDecode(buf[:n])
		if err != nil {
			c.Logger.Error("Error decoding broker message: %s", err)
			continue
		}

		_, err = brokerWriter.WriteString("ACK\n")
		if err != nil {
			return err
		}
		brokerWriter.Flush()

		c.EachMessage(msgDecode.Payload.Content)
	}

	return nil
}

func (c *Consumer) Stop() {
	c.run = false
}

func NewConsumer() *Consumer {
	filePath := "./log-consumer.txt"

	l := utils.NewLogger(filePath)

	return &Consumer{
		Client: Client{
			Logger: l,
		},
	}
}

func (p *Producer) Publish(brokerAdr, topic, message string) error {
	// Prepare handshake
	physical, logical := p.Clock.Now()
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

	err := p.ConnectBroker(brokerAdr)
	if err != nil {
		return err
	}
	defer p.Conn.Close()
	return p.SendMessageToBroker(clientMessage)
}

func NewProducer() *Producer {
	filePath := "./log-producer.txt"

	l := utils.NewLogger(filePath)

	return &Producer{
		Clock: utils.NewHLC(),
		Client: Client{
			Logger: l,
		},
	}
}
