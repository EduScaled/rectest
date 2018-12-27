import ast
import asyncio
import json
from json import JSONDecodeError

import pytest
from aiokafka import AIOKafkaConsumer
from aiokafka.structs import TopicPartition

import settings


def value_deserializer(value):
    value = value.decode("utf-8")
    try:
        return json.loads(value)
    except JSONDecodeError:
        return ast.literal_eval(value)


@pytest.yield_fixture(scope='session')
def event_loop():
    loop = asyncio.get_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope='session')
async def kafka_messages():
    loop = asyncio.get_event_loop()

    topics = ["rs", "fs", "teos", "uploads", "lrs"]

    consumer = AIOKafkaConsumer(
        *topics,
        loop=loop, bootstrap_servers=settings.KAFKA_URL,
        value_deserializer=value_deserializer
    )
    await consumer.start()
    offsets = [await consumer.end_offsets([TopicPartition(topic, 0)]) for topic in topics]

    await asyncio.sleep(60)
    for offset in offsets:
        for tp, offset_value in offset.items():
            consumer.seek(tp, offset_value)
    try:
        d = await consumer.getmany(timeout_ms=10 * 1000)
        return dict((tp.topic, messages) for tp, messages in d.items())
    finally:
        await consumer.stop()
