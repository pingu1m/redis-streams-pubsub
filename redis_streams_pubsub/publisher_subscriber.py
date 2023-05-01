import redis
from loguru import logger
from redis import asyncio as aioredis


class PublisherSubscriber:
    # def __init__(self, redis_host="redis://localhost"):
    def __init__(self, from_url_kwargs):
        self.from_url_kwargs = from_url_kwargs
        self.redis = None

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self.disconnect()

    async def connect(self):
        self.redis = await aioredis.from_url(decode_responses=True, **self.from_url_kwargs)

    async def disconnect(self):
        await self.redis.close()

    async def get_streams(self):
        keys = await self.redis.keys("*")
        streams = []
        for key in keys:
            key_type = await self.redis.type(key)
            if key_type == b'stream':
                streams.append(key)
        return streams

    async def get_subscribers(self):
        return await self.redis.client_list()

    async def publish(self, stream_name, message):
        await self.redis.xadd(stream_name, message)

    async def independent_subscribe(self, stream_names: list | dict, earliest=False):
        if isinstance(stream_names, dict):
            latest_ids = stream_names
        else:
            start_at = "$" if not earliest else "0"
            latest_ids = {stream_name: start_at for stream_name in stream_names}

        while True:
            message = await self.redis.xread(latest_ids, block=0)
            if message:
                for stream_message in message:
                    stream_name, messages = stream_message
                    stream_name = stream_name
                    latest_ids[stream_name] = messages[-1][0]
                    yield stream_name, messages

    async def group_subscriber(self, group_name, stream_name, consumer_name, mode="latest"):

        try:
            await self.redis.xgroup_create(stream_name, group_name, mkstream=True)
        except redis.exceptions.ResponseError as e:
            logger.info(f"GROUP {group_name} already exists")

        # Make mode enum
        if mode == "latest":
            await self.redis.xgroup_setid(stream_name, group_name, "$")
        if mode == "earliest":
            await self.redis.xgroup_setid(stream_name, group_name, "0")

        # Consumers are auto created when mentioned
        # await self.redis.xgroup_createconsumer(stream_name, group_name, consumer_name)

        while True:
            # TODO: replace > with 0 for catchup
            message = await self.redis.xreadgroup(
                group_name, consumer_name, {stream_name: ">"}, block=0
            )
            if message:
                yield message
