# Go MQ

This is a message queue developed for Golang from the ground up. This follows a similar architect to Kafka with a broker and a go module for client side methods.
This is still a development.

## Getting Started

### Getting the broker
To get the broker, simply clone the server [directory](https://github.com/MorElf7/GoMQ/tree/master/server). You can do this using git sparse-checkout.
```
git clone --no-checkout https://github.com/MorElf7/GoMQ.git
cd GoMQ
git sparse-checkout init --cone
git sparse-checkout set server
git checkout
```

### Getting the client module
You can get the go module with 
```
go get github.com/MorElf7/GoMQ/client
```

### Examples
Import the module
```go
import (
    GoMQ "github.com/MorElf7/GoMQ/client"
)
```

#### Producer

``` go
// Initialize a new Producer instance
producer := GoMQ.NewProducer()
err := producer.Publish(brokerAdr, topic, message)
```
Here `brokerAdr`, `topic`, and `message` are all string that you need to specify yourself.
Note: There is a small example in [here](https://github.com/MorElf7/GoMQ/blob/master/server/server.go)

#### Consumer

```go
// Initialize a new Consumer instance
consumer := GoMQ.NewConsumer()
consumer.EachMessage = func(msg string) {
    // Implement this function for your app
}
// Last param is to specify whether you want to replay all the message from the start. 
// True for yes and vice versa
go consumer.Subscribe(brokerAdr, topic, true)

// Call when you need to stop connecting to the broker
consumer.Stop()
```

Note: Broker would not remember any consumer or producer, they would treat any client connection as a new connection
You can have access to a small example of an echoing consumer in [here](https://github.com/MorElf7/GoMQ/blob/master/consumer/consumer.go)

## Features
- There are two roles, producer and consumer following a publish/subscribe model
- Messages would be separated by topics. 
- Broker keep a log of every messages being published, using an embedded database
- Consumer, when connect to the broker, can have the option to reload every messages since the creation of the topic or just accept message from that time onwards
- There is a retry and timeout system in place for delivering message to the consumer to ensures delivery
- Every thing is done through TCP connection
- Everything is designed to be consistent and can accept concurrent producers and consumers
- Topic creation is exclusive to producer for a more distinction between producer and consumer roles with producer act more as the admin

### Architecture

#### A high level overview
![](https://github.com/MorElf7/GoMQ/blob/master/images/GoMQ-GoMQ%20High%20Level%20Architecture.drawio.png?raw=true)

#### How the broker works

![](https://github.com/MorElf7/GoMQ/blob/master/images/GoMQ-Broker%20Architecture.drawio.png?raw=true)

### Roles
- Producer: 
    - create topics
    - publish messages
- Consumer: 
    - subscribe to topics
    - receive messages from the topic subscribed

### Performance

Not fully tested. 

## Plan
- [ ] Test capabilities
- [ ] Add authentication, switch from TCP to TLS/TCP for a secured message transmission
- [ ] Create a CLI for the broker to manage the topic tables

