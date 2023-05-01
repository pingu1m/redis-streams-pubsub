import asyncio

from loguru import logger
from publisher_subscriber import PublisherSubscriber


async def handle_message(message_info):
    message_id, message = message_info
    run_id = message.get("run_id", "")
    event_id = message.get("event_id", "")
    logger.warning(f"i-sub finished: {message_id} | {run_id} | {event_id}")
    logger.warning("msg iteration")
    await asyncio.sleep(3)  # Simulate processing time
    return message_id


async def handle_messages_parallel_batch(messages):
    logger.warning(f"MSG SIZE: {len(messages)} ids: {[i[0] for i in messages]}")
    tasks = [asyncio.create_task(handle_message(message)) for message in messages]
    # Process completed tasks as soon as they finish
    for task in asyncio.as_completed(tasks):
        result = await task
        logger.info(f"DONE:   {result}")


async def handle_messages_sequential(messages):
    logger.warning(f"MSG SIZE: {len(messages)} ids: {[i[0] for i in messages]}")
    for message_id, message in messages:
        run_id = message.get("run_id", "")
        event_id = message.get("event_id", "")
        logger.warning(f"i-sub finished: {message_id} | {run_id} | {event_id}")
        logger.warning("msg iteration")
        await asyncio.sleep(3)  # Simulate processing time


async def independent_subscriber(stream_name):
    logger.warning(f"Independent subscriber started for {stream_name}")
    from_url_kwargs = {
        "url": "redis://localhost",
    }
    try:
        async with PublisherSubscriber(from_url_kwargs) as pubsub:
            async for channel, messages in pubsub.independent_subscribe([stream_name]):
                logger.warning(f"i-sub received: {channel} - {len(messages)} msgs")
                match channel:
                    case "mystream15":
                        await handle_messages_sequential(messages)
                    case _:
                        pass
                logger.warning("outer iteration")
    except asyncio.CancelledError:
        logger.warning("Independent subscriber stopped.")

    await pubsub.disconnect()


async def main():
    stream_name = "mystream15"
    # await independent_subscriber(stream_name)
    subscriber_task = asyncio.create_task(independent_subscriber(stream_name))
    # await asyncio.sleep(10)
    # subscriber_task.cancel()
    await subscriber_task


if __name__ == "__main__":
    asyncio.run(main())
