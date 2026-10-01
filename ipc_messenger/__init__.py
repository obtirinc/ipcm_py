import asyncio
import json
from .mq_handler import RabbitHandler
from .cache_pubsub import RedisHandler

class RemoteServiceError(Exception):
    """Raised when the remote microservice encounters an exception."""
    pass

class IPCMessenger:
    def __init__(self, rabbitmq_url: str, redis_url: str):
        self.mq = RabbitHandler(rabbitmq_url)
        self.redis = RedisHandler(redis_url)

    async def connect(self):
        await asyncio.gather(self.mq.connect(), self.redis.connect())

    async def close(self):
        await asyncio.gather(self.mq.close(), self.redis.close())

    async def send_request(self, queue_name: str, payload: dict, timeout: int = 10) -> dict:
        # Step 1: Generate unique ephemeral response channel ID
        channel_id = self.redis.generate_channel_id()
        
        # Step 2: Subscribe requester to channel and confirm subscription BEFORE publishing request
        pubsub = await self.redis.create_subscription(channel_id)
        
        # Step 3: Push payload and ephemeral channel address to RabbitMQ
        await self.mq.publish_request(queue_name, payload, channel_id)

        # Step 8: Wait for response on established PubSub connection
        try:
            response = await self.redis.wait_for_response(pubsub, channel_id, timeout)
            
            if response.get("status") == "error":
                raise RemoteServiceError(response.get("message"))
            return response.get("data")
            
        except asyncio.TimeoutError:
            raise TimeoutError(f"Request to queue '{queue_name}' timed out.")

    async def start_listening(self, queue_name: str, process_callback):
        async def on_message(message):
            async with message.process():
                body = json.loads(message.body)
                resp_channel = body.get("response_channel")
                
                # Step 4: Consumer receives payload from RabbitMQ
                print(f"[IPC][Step 4] RabbitMQ consumer received payload on queue '{queue_name}' for response_channel '{resp_channel}' | Status: SUCCESS")

                try:
                    result = await process_callback(body.get("data"))
                    payload = {"status": "success", "data": result}
                    # Step 5: Consumer processing complete
                    print(f"[IPC][Step 5] Consumer callback processing succeeded for channel '{resp_channel}' | Status: SUCCESS")
                except Exception as e:
                    payload = {"status": "error", "message": f"{type(e).__name__}: {str(e)}"}
                    print(f"[IPC][Step 5] Consumer callback processing failed for channel '{resp_channel}': {e} | Status: FAILED")

                if resp_channel:
                    # Steps 6 & 7: Dispatch response via Redis PubSub
                    await self.redis.publish_response(resp_channel, payload)

        await self.mq.start_consumer(queue_name, on_message)
        # Keep the listener alive
        await asyncio.Future()

__all__ = ["IPCMessenger", "RemoteServiceError"]