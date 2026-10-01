# **IPCMessenger**

A lightweight, asynchronous Inter-Process Communication (IPC) library for Python microservices. This library implements a **Request-Response** pattern by combining the strengths of two message brokers:

* **RabbitMQ**: Used for distributing requests (commands). This ensures that if multiple instances of a service are running, requests are load-balanced efficiently.  
* **Redis Pub/Sub & KV**: Used for routing responses back to the specific requester. This provides high-speed, point-to-point delivery without the overhead of managing temporary RabbitMQ queues.

---

## **📂 Project Structure**

```text
your_project/  
│  
├── ipc_messenger/  
│   ├── __init__.py      # High-level Orchestrator & RemoteServiceError  
│   ├── mq_handler.py    # RabbitMQ Logic (via aio-pika)  
│   └── cache_pubsub.py  # Redis Logic (via redis-py)  
│  
├── service_a.py         # Your Requester Service  
└── service_b.py         # Your Responder Service
```

---

## **📋 Prerequisites & Installation**

### **Dependencies**

* **aio-pika**: Asynchronous RabbitMQ client.  
* **redis**: Official Redis Python client (supporting asyncio).

```bash
pip install aio-pika redis
```

---

## **🔄 Execution Flow Invariants (Strict 8-Step Lifecycle)**

To prevent race conditions where fast workers process requests before the requester starts listening, `IPCMessenger` executes operations in a strict, deterministic sequence:

1. **[Step 1] Channel Creation**: Generates a unique UUID channel address (e.g., `response_channel:xyz`).
2. **[Step 2] Subscription & Confirmation**: Requester subscribes to Redis and awaits server confirmation before proceeding.
3. **[Step 3] RabbitMQ Dispatch**: Requester pushes the payload and the confirmed response channel ID to RabbitMQ.
4. **[Step 4] Worker Receipt**: Consumer receives the payload from RabbitMQ.
5. **[Step 5] Callback Processing**: Consumer completes processing logic and invokes `ipc_messenger` to send back the response.
6. **[Step 6] Redis Response Dispatch**: Initiates Pub/Sub dispatch to the response channel.
7. **[Step 7] Redis Response Confirmation**: Verifies response was published to Redis.
8. **[Step 8] Requester Receipt**: Requester receives payload on the active PubSub subscriber connection.

---

## **💻 Implementation Guide**

### **1. The Requester (Microservice A)**

```python
import asyncio  
from ipc_messenger import IPCMessenger, RemoteServiceError

async def main():  
    ipc = IPCMessenger(  
        rabbitmq_url="amqp://guest:guest@localhost/",  
        redis_url="redis://localhost:6379/0"  
    )  
    await ipc.connect()

    try:  
        print("Sending request to 'order_processing'...")  
        payload = {"order_id": 123, "action": "validate"}  
          
        response = await ipc.send_request("order_processing", payload, timeout=10)  
        print(f"Server Response: {response}")  
          
    except RemoteServiceError as e:  
        print(f"The remote service failed with error: {e}")  
    except TimeoutError:  
        print("The request timed out.")  
    finally:  
        await ipc.close()

if __name__ == "__main__":  
    asyncio.run(main())
```

### **2. The Responder (Microservice B)**

```python
import asyncio  
from ipc_messenger import IPCMessenger

async def process_task(data):  
    print(f"Processing: {data}")  
    await asyncio.sleep(1)
      
    if data.get("order_id") == 0:  
        raise ValueError("Invalid order ID!")  
          
    return {"status": "verified", "timestamp": "2026-09-30T10:00:00Z"}

async def main():  
    ipc = IPCMessenger("amqp://guest:guest@localhost/", "redis://localhost:6379/0")  
    await ipc.connect()

    print("Worker is listening on 'order_processing'...")  
    await ipc.start_listening("order_processing", process_task)

if __name__ == "__main__":  
    asyncio.run(main())  
```

---

## **⚙️ Configuration & API Reference**

### `IPCMessenger(rabbitmq_url, redis_url)`
* **rabbitmq_url**: Standard AMQP URL (e.g., `amqp://user:pass@host:5672/`).  
* **redis_url**: Standard Redis URL (e.g., `redis://host:6379/0`).

### `await send_request(queue_name, payload, timeout=10)`
* Subscribes to Redis **before** dispatching to RabbitMQ to prevent dropped responses.
* Returns the response dictionary or raises `TimeoutError` / `RemoteServiceError`.

### `await start_listening(queue_name, process_callback)`
* Main consumer listener loop. Automatically routes return values or serialized exceptions back via Redis.