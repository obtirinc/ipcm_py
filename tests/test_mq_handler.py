import pytest
import json
from unittest.mock import AsyncMock, patch
from ipc_messenger.mq_handler import RabbitHandler

@pytest.mark.asyncio
@patch("aio_pika.connect_robust")
async def test_rabbit_publish_request(mock_connect, rabbitmq_url):
    mock_connection = AsyncMock()
    mock_channel = AsyncMock()
    mock_exchange = AsyncMock()
    
    mock_connect.return_value = mock_connection
    mock_connection.channel.return_value = mock_channel
    mock_channel.default_exchange = mock_exchange

    handler = RabbitHandler(rabbitmq_url)
    await handler.connect()

    payload = {"data": "test_payload"}
    channel_id = "response_channel:1234"

    await handler.publish_request("test_queue", payload, channel_id)

    mock_exchange.publish.assert_awaited_once()
    published_msg = mock_exchange.publish.call_args[0][0]
    decoded_body = json.loads(published_msg.body.decode('utf-8'))

    assert decoded_body["response_channel"] == channel_id
    assert decoded_body["data"] == payload