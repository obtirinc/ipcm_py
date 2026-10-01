import pytest
import asyncio
from ipc_messenger import IPCMessenger

@pytest.mark.asyncio
async def test_live_ipc_roundtrip(rabbitmq_url, redis_url):
    ipc_client = IPCMessenger(rabbitmq_url=rabbitmq_url, redis_url=redis_url)
    ipc_worker = IPCMessenger(rabbitmq_url=rabbitmq_url, redis_url=redis_url)

    await ipc_client.connect()
    await ipc_worker.connect()

    async def echo_callback(payload):
        return {"status": "echo", "received": payload}

    # Start worker listener task asynchronously
    listener_task = asyncio.create_task(
        ipc_worker.start_listening("test_integration_queue", echo_callback)
    )

    # Give consumer worker time to establish queue listener
    await asyncio.sleep(0.5)

    try:
        # Perform live request-response roundtrip
        response = await ipc_client.send_request(
            queue_name="test_integration_queue",
            payload={"ping": "pong"},
            timeout=5
        )
        assert response == {"status": "echo", "received": {"ping": "pong"}}
    finally:
        listener_task.cancel()
        await ipc_client.close()
        await ipc_worker.close()