import asyncio

from loguru import logger
from publisher_subscriber import PublisherSubscriber as PubSub


async def handle_messages(messages):
    for message_id, message in messages:
        run_id = message.get("run_id", "")
        event_id = message.get("event_id", "")
        logger.warning(f"group-sub finished: {message_id} | {run_id} | {event_id}")
        await asyncio.sleep(1)  # Simulate processing time


async def group_subscriber(pubsub, group_name, stream_name, consumer_name):
    logger.info(f"Group consumer {consumer_name} started for group {group_name} on stream {stream_name}")
    try:
        async for channels in pubsub.group_subscriber(group_name, stream_name, consumer_name, mode="leftoff"):
            for channel, messages in channels:
                logger.info(f"group-sub received: c {consumer_name} {channel} - {len(messages)}")
                match channel:
                    case "mystream15":
                        await handle_messages(messages)
                    case _:
                        pass
    except asyncio.CancelledError:
        logger.info("Group subscriber stopped.")


async def main():
    stream_name = "mystream15"
    group_name = "mygroup"
    consumer_name = "consumer1"
    from_url_kwargs = {
        "url": "redis://localhost",
    }

    async with PubSub(from_url_kwargs) as pubsub:
        subscriber_task = asyncio.create_task(group_subscriber(pubsub, group_name, stream_name, consumer_name))
        await subscriber_task


if __name__ == "__main__":
    asyncio.run(main())
