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
        channel = await self.connection.channel()
        message_body = json.dumps({
            "response_channel": response_channel,
            "data": payload
        }).encode('utf-8')

        await channel.default_exchange.publish(
            aio_pika.Message(body=message_body),
            routing_key=queue_name
        )

    async def start_consumer(self, queue_name: str, on_message_callback):
        channel = await self.connection.channel()
        # Ensure we only handle one message at a time per worker instance
        await channel.set_qos(prefetch_count=1)
        queue = await channel.declare_queue(queue_name, durable=True)
        await queue.consume(on_message_callback)