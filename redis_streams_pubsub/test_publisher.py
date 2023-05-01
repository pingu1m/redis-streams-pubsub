import asyncio
import uuid

from loguru import logger
from publisher_subscriber import PublisherSubscriber


async def publish_messages(stream_name, interval=0.1):
    run_id = str(uuid.uuid4())
    logger.warning(f"publisher started run_id: {run_id}")
    async with PublisherSubscriber() as pubsub:
        message_count = 0
        try:
            while True:
                message = {"run_id": run_id, "event_id": f"value: {message_count}-{run_id}"}
                await pubsub.publish(stream_name, message)
                logger.info(f"Published message to {stream_name}: {message}")
                await asyncio.sleep(interval)
                message_count += 1
        except asyncio.CancelledError:
            logger.info("Publisher stopped.")


async def main():
    stream_name = "mystream15"

    # Start the publisher and let it run for 10 seconds
    publisher_task = asyncio.create_task(publish_messages(stream_name))
    await asyncio.sleep(60)
    publisher_task.cancel()
    await publisher_task


if __name__ == "__main__":
    asyncio.run(main())
