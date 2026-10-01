import pytest

def pytest_addoption(parser):
    parser.addoption(
        "--rabbitmq-url",
        action="store",
        default="amqp://guest:guest@localhost:5672/",
        help="RabbitMQ connection URL"
    )
    parser.addoption(
        "--redis-url",
        action="store",
        default="redis://localhost:6379/0",
        help="Redis connection URL"
    )

@pytest.fixture
def rabbitmq_url(request):
    return request.config.getoption("--rabbitmq-url")

@pytest.fixture
def redis_url(request):
    return request.config.getoption("--redis-url")