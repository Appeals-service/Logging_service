import asyncio
import logging

from aio_pika import connect_robust
from aio_pika.abc import AbstractIncomingMessage
from aio_pika.exceptions import ChannelPreconditionFailed
from aiofiles import open

from settings import settings

logger = logging.getLogger(__name__)
logging.basicConfig(format=settings.LOGGING_FORMAT)
logger.setLevel(settings.LOGGING_LEVEL)


async def process_message(message: AbstractIncomingMessage) -> None:
    logger.info("Message received")
    async with message.process():
        async with open("./logs.txt", "a", encoding="utf-8") as file:
            await file.write(f"\n{message.body.decode()}")


async def main() -> None:
    connection = await connect_robust(settings.get_rmq_url())
    channel = await connection.channel()
    await channel.set_qos(prefetch_count=100)
    queue = await channel.declare_queue(settings.RABBITMQ_QUEUE_NAME)

    await queue.consume(process_message)
    logger.info("Waiting for messages...")

    try:
        await asyncio.Future()
    finally:
        await connection.close()


if __name__ == "__main__":
    asyncio.run(main())
