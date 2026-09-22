# **IPCMessenger**

A lightweight, asynchronous Inter-Process Communication (IPC) library for Python microservices. This library implements a **Request-Response** pattern by combining the strengths of two message brokers:

* **RabbitMQ**: Used for distributing requests (commands). This ensures that if multiple instances of a service are running, requests are load-balanced efficiently.  
* **Redis Pub/Sub**: Used for routing responses back to the specific requester. This provides high-speed, low-latency, point-to-point delivery without the overhead of managing many temporary RabbitMQ queues.

## **📂 Project Structure**

To use this library, ensure your project folder is organized as follows:

your\_project/  
│  
├── ipc\_messenger/  
│   ├── \_\_init\_\_.py      \# High-level Orchestrator & RemoteServiceError  
│   ├── mq\_handler.py    \# RabbitMQ Logic (via aio-pika)  
│   └── cache\_pubsub.py  \# Redis Logic (via redis-py)  
│  
├── service\_a.py         \# Your Requester Service  
└── service\_b.py         \# Your Responder Service

## **📋 Prerequisites & Installation**

### **Dependencies**

The library is built on modern asynchronous Python (asyncio) and requires the following drivers:

* **aio-pika**: Asynchronous RabbitMQ client.  
* **redis**: The official Redis Python client (supporting asyncio).

### **Installation**

Install the required packages via pip:

pip install aio-pika redis

## **💻 Implementation Guide**

### **1\. The Requester (Microservice A)**

This service sends a request to a specific queue and waits asynchronously for a reply.

import asyncio  
from ipc\_messenger import IPCMessenger, RemoteServiceError

async def main():  
    \# Initialize connection settings  
    ipc \= IPCMessenger(  
        rabbitmq\_url="amqp://guest:guest@localhost/",  
        redis\_url="redis://localhost:6379/0"  
    )  
    await ipc.connect()

    try:  
        print("Sending request to 'order\_processing'...")  
        payload \= {"order\_id": 123, "action": "validate"}  
          
        \# This blocks only the current task, not the whole thread  
        response \= await ipc.send\_request("order\_processing", payload, timeout=10)  
        print(f"Server Response: {response}")  
          
    except RemoteServiceError as e:  
        print(f"The remote service failed with error: {e}")  
    except TimeoutError:  
        print("The request timed out.")  
    finally:  
        await ipc.close()

if \_\_name\_\_ \== "\_\_main\_\_":  
    asyncio.run(main())

### **2\. The Responder (Microservice B)**

This service listens to a RabbitMQ queue, processes the data, and automatically routes the response back through the Redis channel provided in the request metadata.

import asyncio  
from ipc\_messenger import IPCMessenger

async def process\_task(data):  
    """Your business logic goes here."""  
    print(f"Processing: {data}")  
    await asyncio.sleep(1) \# Simulate work  
      
    if data.get("order\_id") \== 0:  
        raise ValueError("Invalid order ID\!")  
          
    return {"status": "verified", "timestamp": "2023-10-27T10:00:00Z"}

async def main():  
    ipc \= IPCMessenger("amqp://guest:guest@localhost/", "redis://localhost:6379/0")  
    await ipc.connect()

    print("Worker is listening on 'order\_processing'...")  
    \# This maintains a persistent consumer loop  
    await ipc.start\_listening("order\_processing", process\_task)

if \_\_name\_\_ \== "\_\_main\_\_":  
    asyncio.run(main())

## **⚙️ Configuration & API Reference**

### **IPCMessenger(rabbitmq\_url, redis\_url)**

* **rabbitmq\_url**: Standard AMQP URL (e.g., amqp://user:pass@host:5672/).  
* **redis\_url**: Standard Redis URL (e.g., redis://host:6379/0).

### **await send\_request(queue\_name, payload, timeout=10)**

* **queue\_name**: The RabbitMQ queue to target.  
* **payload**: A dictionary containing the data to be processed.  
* **timeout**: Maximum seconds to wait for a Redis response before raising TimeoutError.  
* **Returns**: The data dictionary sent by the responder.

### **await start\_listening(queue\_name, process\_callback)**

* **queue\_name**: The RabbitMQ queue to consume from.  
* **process\_callback**: An async function that takes one argument (the payload) and returns a dictionary.

## **⚠️ Error Handling**

The library includes a robust error propagation system. If an exception occurs within your process\_callback on the Responder side:

1. The library catches the exception.  
2. It serializes the exception name and message into an error payload.  
3. It sends this error back to the Requester via Redis.  
4. The Requester receives the error and raises a RemoteServiceError.

**Example of catching a remote failure:**

try:  
    result \= await ipc.send\_request("queue", data)  
except RemoteServiceError as e:  
    \# 'e' contains the error message from the other microservice  
    print(f"Remote service crashed: {e}")

## **🔄 Technical Flow**

1. **Requester**: Generates a unique UUID (e.g., response\_channel:xyz).  
2. **Requester**: Subscribes to the Redis channel response\_channel:xyz.  
3. **Requester**: Publishes a message to **RabbitMQ** containing the user data and the channel UUID.  
4. **Responder**: Receives the message from RabbitMQ.  
5. **Responder**: Executes the callback function.  
6. **Responder**: Publishes the result (or error) to **Redis** on channel response\_channel:xyz.  
7. **Requester**: Receives the Redis message, unsubscribes, and returns the data to the caller.