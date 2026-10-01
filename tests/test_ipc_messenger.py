import pytest
from unittest.mock import AsyncMock, patch
from ipc_messenger import IPCMessenger, RemoteServiceError

@pytest.fixture
def mock_ipc(rabbitmq_url, redis_url):
    with patch("ipc_messenger.RabbitHandler") as MockRabbit, \
         patch("ipc_messenger.RedisHandler") as MockRedis:
        
        mock_mq = AsyncMock()
        mock_redis = AsyncMock()
        
        MockRabbit.return_value = mock_mq
        MockRedis.return_value = mock_redis
        
        ipc = IPCMessenger(rabbitmq_url=rabbitmq_url, redis_url=redis_url)
        ipc.mq = mock_mq
        ipc.redis = mock_redis
        
        yield ipc

@pytest.mark.asyncio
async def test_send_request_success(mock_ipc):
    mock_ipc.redis.generate_channel_id.return_value = "response_channel:test_id"
    mock_ipc.redis.create_subscription.return_value = AsyncMock()
    mock_ipc.redis.wait_for_response.return_value = {
        "status": "success",
        "data": {"result": "ok"}
    }

    result = await mock_ipc.send_request("test_queue", {"action": "ping"})

    assert result == {"result": "ok"}
    mock_ipc.mq.publish_request.assert_awaited_once_with(
        "test_queue", {"action": "ping"}, "response_channel:test_id"
    )

@pytest.mark.asyncio
async def test_send_request_remote_error(mock_ipc):
    mock_ipc.redis.generate_channel_id.return_value = "response_channel:test_id"
    mock_ipc.redis.create_subscription.return_value = AsyncMock()
    mock_ipc.redis.wait_for_response.return_value = {
        "status": "error",
        "message": "ValueError: Invalid parameter"
    }

    with pytest.raises(RemoteServiceError, match="ValueError: Invalid parameter"):
        await mock_ipc.send_request("test_queue", {"invalid": True})