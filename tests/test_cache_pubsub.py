import pytest
from unittest.mock import AsyncMock, patch
from ipc_messenger.cache_pubsub import RedisHandler

@pytest.mark.asyncio
async def test_generate_channel_id(redis_url):
    handler = RedisHandler(redis_url)
    channel_id = handler.generate_channel_id()
    
    assert channel_id.startswith("response_channel:")
    assert len(channel_id.split(":")[1]) > 0

@pytest.mark.asyncio
@patch("redis.asyncio.from_url")
async def test_redis_connect_and_close(mock_from_url, redis_url):
    mock_client = AsyncMock()
    mock_from_url.return_value = mock_client
    
    handler = RedisHandler(redis_url)
    await handler.connect()
    assert handler.client is not None
    
    await handler.close()
    mock_client.aclose.assert_awaited_once()
    assert handler.client is None

@pytest.mark.asyncio
async def test_publish_response_unconnected_raises_error(redis_url):
    handler = RedisHandler(redis_url)
    with pytest.raises(RuntimeError, match="Redis client is not connected"):
        await handler.publish_response("test_channel", {"status": "ok"})