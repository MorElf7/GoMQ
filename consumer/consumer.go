package main

import (
	"fmt"
	"time"

	"github.com/MorElf7/go-redis/client"
	// "github.com/MorElf7/go-redis/utils"
)

func main() {
	for i := 0; i < 1; i++ {
		go SpawnConsumer("test")
	}
	// Loop to keep the main thread alive
	for {
	}
}

func SpawnConsumer(topic string) {
	consumer := client.NewConsumer()
	consumer.EachMessage = func(msg string) {
		fmt.Printf("Received message %s\n", msg)
	}
	go consumer.Subscribe("localhost:8080", topic, true)

	time.Sleep(5 * time.Second)
	consumer.Stop()
	fmt.Println("Done")
}
