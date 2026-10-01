import json
import uuid
import asyncio
import logging
import redis.asyncio as redis

class RedisHandler:
    def __init__(self, redis_url: str):
        self.redis_url = redis_url
        self.client = None

    async def connect(self):
        """Initializes the Redis client connection."""
        if not self.client:
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
        """Step 1: Generates a unique ID for the ephemeral response channel."""
        channel_id = f"response_channel:{uuid.uuid4()}"
        print(f"[IPC][Step 1] Ephemeral response channel created: '{channel_id}' | Status: SUCCESS")
        return channel_id

    async def create_subscription(self, channel_id: str):
        """
        Step 2: Subscribes the requester to the channel and awaits explicit confirmation
        from Redis before allowing the request flow to proceed.
        """
        if not self.client:
            raise RuntimeError("Redis client is not connected. Call connect() first.")

        print(f"[IPC][Step 2] Subscribing requester to Redis channel '{channel_id}'...")
        try:
            pubsub = self.client.pubsub()
            await pubsub.subscribe(channel_id)

            # Read confirmation message from Redis server to guarantee subscription active
            async for message in pubsub.listen():
                if message and message.get('type') == 'subscribe':
                    print(f"[IPC][Step 2] Subscription confirmed for Redis channel '{channel_id}' | Status: SUCCESS")
                    return pubsub
        except Exception as e:
            print(f"[IPC][Step 2] Subscription failed for Redis channel '{channel_id}': {e} | Status: FAILED")
            raise e

    async def wait_for_response(self, pubsub, channel_id: str, timeout: int):
        """
        Step 8: Awaits the response on the confirmed PubSub subscription channel.
        Includes a Redis KV cache fallback check for high-speed edge cases.
        """
        if not self.client:
            raise RuntimeError("Redis client is not connected.")

        kv_key = f"kv_{channel_id}"

        try:
            # Check KV store in case worker finished near-instantly
            cached = await self.client.get(kv_key)
            if cached:
                print(f"[IPC][Step 8] Requester received response via Redis KV Cache '{kv_key}' | Status: SUCCESS")
                try:
                    await self.client.delete(kv_key)
                except Exception:
                    pass
                return json.loads(cached)

            async def listen_loop():
                async for message in pubsub.listen():
                    if message and message.get('type') == 'message':
                        return json.loads(message['data'])

            result = await asyncio.wait_for(listen_loop(), timeout=timeout)
            print(f"[IPC][Step 8] Requester received response via Redis PubSub channel '{channel_id}' | Status: SUCCESS")
            return result

        except asyncio.TimeoutError:
            # Final fallback check on KV store before raising TimeoutError
            cached = await self.client.get(kv_key)
            if cached:
                print(f"[IPC][Step 8] Requester retrieved fallback response on Redis KV '{kv_key}' after timeout window | Status: SUCCESS")
                try:
                    await self.client.delete(kv_key)
                except Exception:
                    pass
                return json.loads(cached)
            print(f"[IPC][Step 8] Timeout waiting for response on channel '{channel_id}' after {timeout}s | Status: FAILED")
            raise asyncio.TimeoutError(f"No response received on {channel_id} within {timeout}s")

        finally:
            # Resource cleanup
            try:
                await pubsub.unsubscribe(channel_id)
                await pubsub.close()
            except Exception:
                pass

    async def publish_response(self, channel_id: str, data: dict):
        """
        Steps 6 & 7: Sends the response back to the requester via Redis PubSub and KV cache.
        """
        if not self.client:
            raise RuntimeError("Redis client is not connected.")

        print(f"[IPC][Step 6] Initiating response dispatch via Redis PubSub to channel '{channel_id}'...")
        try:
            json_payload = json.dumps(data)
            kv_key = f"kv_{channel_id}"

            # Write to KV cache first to prevent packet loss
            await self.client.set(kv_key, json_payload, ex=60)

            # Publish to Pub/Sub
            sub_count = await self.client.publish(channel_id, json_payload)
            print(f"[IPC][Step 7] Response successfully published to Redis channel '{channel_id}' (Active Subscribers: {sub_count}) | Status: SUCCESS")
        except Exception as e:
            print(f"[IPC][Step 7] Failed to publish response to Redis channel '{channel_id}': {e} | Status: FAILED")
            raise e 