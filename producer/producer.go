package main

import (
	"fmt"
	"math/rand"
	"time"

	GoMQ "github.com/MorElf7/GoMQ/client"
)

func main() {
	for i := 0; i < 1; i++ {
		go SpawnProducer("test")
	}
	// Loop to keep the main thread alive
	for {
	}
}

func SpawnProducer(topic string) {
	producer := GoMQ.NewProducer()
	brokerAdr := "localhost:8080"
	for {
		err := producer.Publish(brokerAdr, topic, randomString())
		fmt.Println(err)
		if err != nil {
			return
		}
		fmt.Println("Sent message")
		time.Sleep(1 * time.Second)
	}
}

// Function to generate a random string of length 500
func randomString() string {
	// Define character set (alphanumeric characters)
	charset := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

	length := 500

	// Create a slice to hold the random string
	result := make([]byte, length)

	// Fill the result slice with random characters from charset
	for i := range result {
		result[i] = charset[rand.Intn(len(charset))]
	}

	return string(result)
}
