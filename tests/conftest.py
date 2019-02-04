import ast
import asyncio
import json
import time
from json import JSONDecodeError
from typing import Callable
from urllib.parse import urljoin

import aiohttp
import pytest
from aiokafka import AIOKafkaConsumer
from aiokafka.structs import TopicPartition
from selenium import webdriver
from selenium.webdriver.common.by import By

import settings
from tests.utils import send_keys, click


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


@pytest.yield_fixture(scope='module')
def create_consumer(event_loop, topics) -> Callable[[], AIOKafkaConsumer]:
    def _create_consumer() -> AIOKafkaConsumer:
        return AIOKafkaConsumer(
            *topics,
            loop=event_loop, bootstrap_servers=settings.KAFKA_URL,
            value_deserializer=value_deserializer
        )

    return _create_consumer


@pytest.yield_fixture(scope='module', autouse=True)
async def kafka_offsets(create_consumer, topics):
    consumer = create_consumer()
    try:
        await consumer.start()
        return [await consumer.end_offsets([TopicPartition(topic, 0)]) for topic in topics]
    finally:
        await consumer.stop()


@pytest.fixture(scope='module')
async def kafka_messages(create_consumer, kafka_offsets):
    consumer = create_consumer()
    time.sleep(60)
    try:
        await consumer.start()
        messages = {}
        for offset in kafka_offsets:
            for tp, offset_value in offset.items():
                consumer.seek(tp, offset_value)
                d = await consumer.getmany(tp, timeout_ms=10 * 1000)
                messages.update(
                    {tp.topic: [message.value for message in messages] for tp, messages in d.items()}
                )
        return messages
    finally:
        await consumer.stop()


@pytest.yield_fixture(scope='session')
def selenium():
    driver = webdriver.Chrome()
    driver.get(settings.LEADER_LOGIN_URL)
    send_keys(driver, (By.XPATH, '//*[@id="loginEmail"]'), settings.LEADER_LOGIN)
    send_keys(driver, (By.XPATH, '//*[@id="loginPassword"]'), settings.LEADER_PASSWORD)
    click(driver, (By.XPATH, '//*[@id="sbmt"]'))
    time.sleep(5)
    yield driver
    driver.quit()


@pytest.fixture(scope='module')
def fs_messages(kafka_messages: dict):
    return kafka_messages.get('fs', [])


@pytest.fixture(scope='module')
async def facts(fs_messages):
    result = []
    headers = {'Authorization': f'Token {settings.FACT_STRORAGE_SERVER_TOKEN}'}
    async with aiohttp.ClientSession(headers=headers) as session:
        for message in fs_messages:
            fact_id = message["id"]["fact"]["uuid"]
            url = urljoin(settings.FACT_STORAGE_SERVER_URL, f'/api/v1/facts/{fact_id}')
            async with session.get(url) as resp:
                assert resp.status == 200
                fact = await resp.json()
                fact.pop('hash')
                fact.pop('created_at')
                assert fact.pop('uuid') == fact_id
                result.append(fact)
    return result
