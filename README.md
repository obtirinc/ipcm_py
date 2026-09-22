# **IPCMessenger**

[![Python Version](https://img.shields.io/badge/python-3.8%2B-blue.svg)](https://www.python.org/)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

A lightweight, high-performance asynchronous Inter-Process Communication (IPC) library for Python microservices.

Rather than replacing message brokers, **IPCMessenger structures and standardizes how you use them**. It combines the distinct strengths of **RabbitMQ** and **Redis Pub/Sub** into a single, unified interface that delivers low-latency, point-to-point **Request-Response** messaging without the massive boilerplate overhead.

---

## 💡 **Why IPCMessenger?**

### **The Problem with Raw Message Brokers**
Implementing a robust **Request-Response (RPC)** pattern between decoupled microservices using native brokers requires substantial infrastructure boilerplate:
* **RabbitMQ Complexity:** To receive a response asynchronously, the requesting service must declare a temporary, exclusive callback queue, generate a unique `correlation_id`, attach headers, and maintain complex consumer threads. If a process crashes, orphaned queues can leak resources on the broker.
* **Redis Pub/Sub Limitations:** While Redis Pub/Sub is extremely fast for message delivery, it lacks native queue persistence, worker load balancing, and competing consumer management out of the box.

### **The IPCMessenger Solution: A Best-of-Both-Worlds Architecture**
IPCMessenger abstracts these broker mechanics away by combining them into a standardized hybrid pattern:

1. **RabbitMQ for Workload Distribution & Durability:** Used for distributing incoming requests across competing consumer queues. If your responder microservices scale up or temporarily go offline, RabbitMQ reliably queues and load-balances work without dropping messages.
2. **Redis Pub/Sub for Low-Latency Responses:** Used exclusively for routing responses directly back to the specific requesting process in-memory. This delivers high-speed, point-to-point delivery without the overhead of creating, monitoring, and destroying temporary RabbitMQ queues.

---

## 🌟 **Key Advantages**

* 🛡️ **Reliable, Durable Messaging:** Leverages RabbitMQ's persistent queueing for incoming tasks. Requests remain safely queued during worker restarts, deployment rollouts, or traffic spikes, preventing data loss.
* ⚡ **High Performance & Low Latency:** Bypasses RabbitMQ's disk/queue lifecycle overhead on the response path by utilizing Redis's ultra-fast in-memory Pub/Sub mechanism.
* 🔓 **Complete Microservice Decoupling:** Requesters and Responders operate independently. Requesters don't need to know where responders live, how many worker instances exist, or how they are implemented.
* 🧹 **Zero Queue Leaks & Reduced Broker Load:** Eliminates the classic RabbitMQ RPC anti-pattern of creating and tearing down exclusive temporary `reply_to` queues for every request, preventing broker memory bloating.
* 🚀 **Accelerated Developer Velocity:** Replaces 50+ lines of low-level `aio-pika` connection, exchange, channel, correlation ID, and header handling with a clean 3-line async interface.
* 🤖 **AI & LLM Friendly:** Because multi-step broker boilerplate is abstracted into high-level declarative calls, AI coding agents (ChatGPT, Claude, Copilot) can reliably generate complete, bug-free microservices on the first attempt.
* ⚠️ **Transparent Error Propagation:** Exceptions raised inside remote worker callbacks are automatically captured, serialized, and re-raised locally as a `RemoteServiceError` on the requester side for native `try/except` error handling.
* 📈 **Effortless Horizontal Scaling:** Scale responder instances up or down seamlessly. RabbitMQ automatically handles competing-consumer load balancing across all active workers.
* ⚙️ **Native AsyncIO Integration:** Built from the ground up on modern Python `asyncio` (`aio-pika` and `redis.asyncio`), allowing thousands of concurrent requests to be dispatched without blocking the main event loop.

---

## 📋 **Prerequisites & Dependencies**

### **Prerequisites**
* **Python**: 3.8 or higher (`asyncio` native)
* **RabbitMQ**: An accessible RabbitMQ broker instance (e.g., `amqp://guest:guest@localhost:5672/`)
* **Redis**: An accessible Redis server instance (e.g., `redis://localhost:6379/0`)

### **Dependencies**
* **`aio-pika`**: Asynchronous RabbitMQ client for Python.
* **`redis`**: Official Redis Python client (with `asyncio` support).

---

## 🛠️ **Installation**

### **Option 1: Direct Installation via Git & Pip**
Install the package directly from GitHub into your virtual environment:

```bash
pip install git+https://github.com/obtirinc/ipc_messanger.git
```

### **Option 2: Local Editable Installation (For Development)**
Clone the repository and install it in editable mode:

```bash
# Clone the repository
git clone https://github.com/obtirinc/ipc_messanger.git

# Navigate to the directory
cd ipc_messanger

# Install dependencies and package in editable mode
pip install -e .
```

---

## 📂 **Project Structure**

When integrating `IPCMessenger` into a multi-service project, organize your codebase cleanly as follows:

```text
your_project/
│
├── ipc_messenger/
│   ├── __init__.py      # High-level Orchestrator & RemoteServiceError
│   ├── mq_handler.py    # RabbitMQ Logic (via aio-pika)
│   └── cache_pubsub.py  # Redis Logic (via redis-py)
│
├── service_a.py         # Requester Service (e.g., API Gateway / Client)
└── service_b.py         # Responder Service (e.g., Worker / Processor)
```

---

## 💻 **Implementation Guide**

### **1. The Requester (Microservice A)**

This service dispatches a request to a RabbitMQ task queue and asynchronously awaits a reply over Redis.

```python
import asyncio
from ipc_messenger import IPCMessenger, RemoteServiceError

async def main():
    # Initialize connection settings
    ipc = IPCMessenger(
        rabbitmq_url="amqp://guest:guest@localhost/",
        redis_url="redis://localhost:6379/0"
    )
    await ipc.connect()

    try:
        print("Sending request to 'order_processing'...")
        payload = {"order_id": 123, "action": "validate"}
        
        # Yields control asynchronously without blocking the main event loop
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

This service listens on a RabbitMQ queue as a competing consumer, processes the workload, and routes the result back via Redis.

```python
import asyncio
from ipc_messenger import IPCMessenger

async def process_task(data):
    """Your business logic goes here."""
    print(f"Processing workload: {data}")
    await asyncio.sleep(1)  # Simulate asynchronous work
    
    if data.get("order_id") == 0:
        raise ValueError("Invalid order ID!")
        
    return {"status": "verified", "timestamp": "2026-09-22T10:00:00Z"}

async def main():
    ipc = IPCMessenger(
        rabbitmq_url="amqp://guest:guest@localhost/",
        redis_url="redis://localhost:6379/0"
    )
    await ipc.connect()

    print("Worker listening on queue 'order_processing'...")
    # Maintains a persistent, asynchronous consumer loop
    await ipc.start_listening("order_processing", process_task)

if __name__ == "__main__":
    asyncio.run(main())
```

---

## 🔄 **Technical Flow (Under the Hood)**

```text
[ Requester Service ]                                    [ Responder Worker ]
         │                                                        │
         ├── 1. Generate unique channel UUID ─────────────────────┤
         │     (e.g., response_channel:xyz)                       │
         │                                                        │
         ├── 2. Subscribe to Redis channel: response_channel:xyz  │
         │                                                        │
         ├── 3. Publish payload + channel UUID to RabbitMQ ──────►│
         │                                                        ├── 4. Consume from queue
         │                                                        ├── 5. Execute callback
         │                                                        │
         │◄── 6. Publish result/error to Redis ───────────────────┤
         │       (channel: response_channel:xyz)                  │
         │                                                        │
         ├── 7. Unsubscribe from Redis & return payload ──────────┘
```

---

## ⚙️ **API Reference**

### **`IPCMessenger(rabbitmq_url, redis_url)`**
* **`rabbitmq_url`** *(str)*: AMQP connection string (e.g., `amqp://user:pass@host:5672/`).
* **`redis_url`** *(str)*: Redis connection string (e.g., `redis://host:6379/0`).

### **`await connect()`**
Establishes underlying asynchronous connection pools to both RabbitMQ and Redis.

### **`await send_request(queue_name, payload, timeout=10)`**
* **`queue_name`** *(str)*: Target RabbitMQ queue.
* **`payload`** *(dict)*: JSON-serializable dictionary containing command/request data.
* **`timeout`** *(int/float)*: Maximum time in seconds to wait for a reply before raising `TimeoutError`.
* **Returns**: `dict` containing the response from the remote responder.

### **`await start_listening(queue_name, process_callback)`**
* **`queue_name`** *(str)*: Target RabbitMQ queue to consume from.
* **`process_callback`** *(coroutine)*: An `async` function accepting `payload` (dict) as an argument and returning a result (dict).

### **`await close()`**
Gracefully shuts down active consumers and closes broker connection pools.

---

## ⚠️ **Error Handling & Propagation**

`IPCMessenger` includes a built-in remote exception handling mechanism. If an uncaught exception occurs within the `process_callback` on the Responder side:

1. The Responder catches the exception automatically.
2. It serializes the exception class name and error message into a structured error payload.
3. The Responder sends this error back across the Redis response channel.
4. The Requester receives the payload and raises a **`RemoteServiceError`**, allowing the caller to handle remote failures gracefully.

```python
try:
    result = await ipc.send_request("order_processing", payload)
except RemoteServiceError as e:
    print(f"Remote service exception caught: {e}")
```

---

## 📄 **License**

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
