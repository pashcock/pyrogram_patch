import asyncio
import typing
from abc import abstractmethod, ABC

import aioredis

from pyrogram_patch.fsm.base_storage import BaseStorage
from pyrogram_patch.fsm.states import State

try:
    import ujson as json
except ImportError:
    import json


class AioRedisAdapterBase(ABC):
    """Base aioredis adapter class."""

    def __init__(
            self,
            host: str = "localhost",
            port: int = 6379,
            db: typing.Optional[int] = None,
            password: typing.Optional[str] = None,
            ssl: typing.Optional[bool] = None,
            pool_size: int = 10,
            loop: typing.Optional[asyncio.AbstractEventLoop] = None,
            prefix: str = "fsm",
            state_ttl: typing.Optional[int] = None,
            data_ttl: typing.Optional[int] = None,
            bucket_ttl: typing.Optional[int] = None,
            **kwargs,
    ):
        self._host = host
        self._port = port
        self._db = db
        self._password = password
        self._ssl = ssl
        self._pool_size = pool_size
        self._kwargs = kwargs
        self._prefix = (prefix,)

        self._state_ttl = state_ttl
        self._data_ttl = data_ttl
        self._bucket_ttl = bucket_ttl

        self._redis: typing.Optional["aioredis.Redis"] = None
        self._connection_lock = asyncio.Lock()

    @abstractmethod
    async def get_redis(self) -> aioredis.Redis:
        """Get Redis connection."""
        pass

    async def close(self):
        """Grace shutdown."""
        pass

    async def wait_closed(self):
        """Wait for grace shutdown finishes."""
        pass

    async def set(self, name, value, ex=None, **kwargs):
        """Set the value at key ``name`` to ``value``."""
        if ex == 0:
            ex = None
        return await self._redis.set(name, value, ex=ex, **kwargs)

    async def get(self, name, **kwargs):
        """Return the value at key ``name`` or None."""
        return await self._redis.get(name, **kwargs)

    async def delete(self, *names):
        """Delete one or more keys specified by ``names``"""
        return await self._redis.delete(*names)

    async def keys(self, pattern, **kwargs):
        """Returns a list of keys matching ``pattern``."""
        return await self._redis.keys(pattern, **kwargs)

    async def flushdb(self):
        """Delete all keys in the current database."""
        return await self._redis.flushdb()


class AioRedisAdapterV2(AioRedisAdapterBase):
    """Redis adapter for aioredis v2."""

    async def get_redis(self) -> aioredis.Redis:
        """Get Redis connection."""
        async with self._connection_lock:  # to prevent race
            if self._redis is None:
                self._redis = aioredis.Redis(
                    host=self._host,
                    port=self._port,
                    db=self._db,
                    password=self._password,
                    ssl=self._ssl,
                    max_connections=self._pool_size,
                    decode_responses=True,
                    **self._kwargs,
                )
        return self._redis


class RedisStorage(BaseStorage):
    def __init__(self,
                 host: str = 'localhost',
                 port: int = 6379,
                 db: int = 0,
                 password: typing.Optional[str] = None,
                 ssl: typing.Optional[bool] = None,
                 pool_size: int = 10,
                 prefix: str = 'fsm',  # key_gen for multibots
                 loop: typing.Optional[asyncio.AbstractEventLoop] = None,
                 state_ttl: typing.Optional[int] = 3600,
                 data_ttl: typing.Optional[int] = 3600,
                 **kwargs
                 ):

        self._host = host
        self._port = port
        self._db = db
        self._password = password
        self._ssl = ssl
        self._pool_size = pool_size
        self._kwargs = kwargs
        self._prefix = prefix

        self._state_ttl = state_ttl
        self._data_ttl = data_ttl

        self._redis: typing.Optional[AioRedisAdapterBase] = None
        self._connection_lock = asyncio.Lock()

    async def _get_adapter(self) -> AioRedisAdapterBase:
        """Get adapter based on aioredis version."""
        if self._redis is None:
            connection_data = dict(
                host=self._host,
                port=self._port,
                db=self._db,
                password=self._password,
                pool_size=self._pool_size,
                **self._kwargs,
            )
            self._redis = AioRedisAdapterV2(**connection_data)
            await self._redis.get_redis()
        return self._redis

    def key_gen(self, key, state: bool = False) -> str:
        if state:
            return self._prefix + key + ':state'
        else:
            return self._prefix + key + ':data'

    async def checkup(self, key) -> State:
        redis = await self._get_adapter()

        state = await redis.get(self.key_gen(key, True))
        if not state:
            return State('*', self, key)
        return State(state, self, key)

    async def set_state(self, state: str, key: str):
        key = self.key_gen(key, True)
        redis = await self._get_adapter()
        if state is None:
            await redis.delete(key)
        else:
            await redis.set(key, state, ex=self._state_ttl)

    async def set_data(self, data: dict, key: str):
        key = self.key_gen(key)
        if data is None:
            data = {}
        temp_data = await self.get_data(key)
        temp_data.update(data)
        redis = await self._get_adapter()
        if temp_data:
            await redis.set(key, json.dumps(data), ex=self._data_ttl)
        else:
            await redis.delete(key)

    async def get_data(self, key: str) -> dict:
        key = self.key_gen(key)
        redis = await self._get_adapter()
        raw_result = await redis.get(key)
        if raw_result:
            return json.loads(raw_result)
        return {}

    async def finish_state(self, key: str) -> None:
        redis = await self._get_adapter()
        await redis.delete(self.key_gen(key, True))
        await redis.delete(self.key_gen(key))
