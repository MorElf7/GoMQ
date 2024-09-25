# go-redis

## Idea

Create a server/broker for a message queue using go
Create a go module for the client to interact with the broker

Features:
- Divide into channels, publish/subscribe model
- Require security key to connect to a server
- Reload every messages when startup.
- Log of every messages being received and saved

Persistent & Consistency & Durability:
- Message is saved to disk and can be reload when startup
- Could move this feature to a embedded db for better retrieval and write throughput

Isolation:
- Allow for multiple client to connect and write concurrently

Roles:
- Admin: create/delete topics, access to every messages.
    - Might not be needed
- Producer: create topics, publish messages
- Consumer: subscribe topics, receive messages from the topic subscribed

Use TCP for pub actions and receiving pub messages
Producer open TCP connection

Use HTTP for creating topics, sub/unsub to topics, 


Message type:
From client
{
    data: bytes,
    physical: number,
    logical: number
}

Server handle and saved as
{
    data: bytes,
    physical: number,
    logical: number,
    id: UUID
}
