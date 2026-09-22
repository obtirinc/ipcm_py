import json
import uuid
import asyncio
import redis.asyncio as redis

class RedisHandler:
    def __init__(self, redis_url: str):
        self.redis_url = redis_url
        self.client = None

    async def connect(self):
        """Initializes the Redis client connection."""
        self.client = redis.from_url(self.redis_url)

    async def close(self):
        """
        Performs a full cleanup of the Redis client connection.
        This should be called when the microservice is shutting down.
        """
        if self.client:
            await self.client.aclose()
            self.client = None

    def generate_channel_id(self) -> str:
        """Generates a unique ID for the ephemeral response channel."""
        return f"response_channel:{uuid.uuid4()}"

    async def wait_for_response(self, channel_id: str, timeout: int):
        """
        Subscribes to a channel, waits for a response, and ensures
        all ephemeral resources (subscriptions) are cleaned up immediately after.
        """
        if not self.client:
            raise RuntimeError("Redis client is not connected. Call connect() first.")

        pubsub = self.client.pubsub()
        await pubsub.subscribe(channel_id)
        
        try:
            async def listen_loop():
                async for message in pubsub.listen():
                    # 'message' type is the actual data; 'subscribe' is just confirmation
                    if message['type'] == 'message':
                        return json.loads(message['data'])
            
            # Wait for the response within the given timeframe
            return await asyncio.wait_for(listen_loop(), timeout=timeout)
        
        except asyncio.TimeoutError:
            # Re-raise to be handled by the orchestrator
            raise asyncio.TimeoutError(f"No response received on {channel_id} within {timeout}s")
        
        finally:
            # RESOURCE CLEANUP: 
            # 1. Unsubscribe from the specific channel to free up Redis server memory
            await pubsub.unsubscribe(channel_id)
            # 2. Close the pubsub instance to release the connection back to the pool
            await pubsub.close()

    async def publish_response(self, channel_id: str, data: dict):
        """Publishes the response data to the specified Redis channel."""
        if not self.client:
            raise RuntimeError("Redis client is not connected.")
        await self.client.publish(channel_id, json.dumps(data))