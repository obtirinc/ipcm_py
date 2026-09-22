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
        # 1. Prepare the response channel via Redis
        channel_id = self.redis.generate_channel_id()
        
        # 2. Send the request via RabbitMQ
        await self.mq.publish_request(queue_name, payload, channel_id)

        # 3. Wait for the response via Redis
        try:
            response = await self.redis.wait_for_response(channel_id, timeout)
            
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
                
                try:
                    result = await process_callback(body.get("data"))
                    payload = {"status": "success", "data": result}
                except Exception as e:
                    payload = {"status": "error", "message": f"{type(e).__name__}: {str(e)}"}

                if resp_channel:
                    await self.redis.publish_response(resp_channel, payload)

        await self.mq.start_consumer(queue_name, on_message)
        # Keep the listener alive
        await asyncio.Future()