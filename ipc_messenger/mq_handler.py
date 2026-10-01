import json
import aio_pika

class RabbitHandler:
    def __init__(self, rabbitmq_url: str):
        self.rabbitmq_url = rabbitmq_url
        self.connection = None

    async def connect(self):
        self.connection = await aio_pika.connect_robust(self.rabbitmq_url)

    async def close(self):
        if self.connection:
            await self.connection.close()

    async def publish_request(self, queue_name: str, payload: dict, response_channel: str):
        """
        Step 3: Pushes the payload together with the confirmed ephemeral channel address to RabbitMQ.
        """
        print(f"[IPC][Step 3] Pushing payload with response_channel '{response_channel}' to RabbitMQ queue '{queue_name}'...")
        try:
            channel = await self.connection.channel()
            message_body = json.dumps({
                "response_channel": response_channel,
                "data": payload
            }).encode('utf-8')

            await channel.default_exchange.publish(
                aio_pika.Message(body=message_body),
                routing_key=queue_name
            )
            print(f"[IPC][Step 3] Payload successfully pushed to RabbitMQ queue '{queue_name}' | Status: SUCCESS")
        except Exception as e:
            print(f"[IPC][Step 3] Failed to push payload to RabbitMQ queue '{queue_name}': {e} | Status: FAILED")
            raise e

    async def start_consumer(self, queue_name: str, on_message_callback):
        channel = await self.connection.channel()
        # Ensure we only handle one message at a time per worker instance
        await channel.set_qos(prefetch_count=1)
        queue = await channel.declare_queue(queue_name, durable=True)
        await queue.consume(on_message_callback) 