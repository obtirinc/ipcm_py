from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="ipc_messenger",
    version="0.1.0",
    author="Obtir Inc",
    author_email="dev@obtir.com",
    description="A lightweight, asynchronous IPC library using RabbitMQ and Redis",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/obtirinc/ipcm_py",
    packages=find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.8",
    install_requires=[
        "aio-pika>=9.0.0",
        "redis>=4.2.0",
    ],
    extras_require={
        "dev": [
            "pytest>=7.0.0",
            "pytest-asyncio>=0.21.0",
            "build",
            "twine",
        ],
        "test": [
            "pytest>=7.0.0",
            "pytest-asyncio>=0.21.0",
        ],
    },
)