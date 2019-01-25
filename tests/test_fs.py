import copy
from typing import List
from urllib.parse import urljoin

import aiohttp
import pytest

import settings


@pytest.fixture()
def teos_messages(kafka_messages: dict):
    return kafka_messages.get('teos', [])


@pytest.mark.asyncio
async def test_check_teos_messages(teos_messages: List[dict]):
    """Test count RS messages"""
    assert len(teos_messages) == 1
    value = teos_messages[0].value
    value.pop('timestamp')
    assert value == {
        'action': 'create', 'source': 'teos', 'type': 'result',
        'id': {'test': {'uuid': 'a69ed2f7-ac5e-45ea-9f1c-4635657e2970'}, 'user': {'unti_id': settings.ACTOR}},
        'title': 'Заголовок теста'
    }


@pytest.fixture()
def fs_messages(kafka_messages: dict):
    return kafka_messages.get('fs', [])


@pytest.mark.asyncio
async def test_check_fs_messages(fs_messages: List[dict]):
    assert len(fs_messages) == 1
    value = copy.deepcopy(fs_messages[0].value)
    value.pop('timestamp')
    value.pop('id')
    assert value == {
        'action': 'create', 'title': '',
        'type': 'fact', 'source': 'factstorage', 'version': 1
    }


@pytest.mark.asyncio
async def test_check_get_fact_by_api(fs_messages: List[dict]):
    headers = {'Authorization': f'Token {settings.FACT_STRORAGE_SERVER_TOKEN}'}
    async with aiohttp.ClientSession(headers=headers) as session:
        for message in fs_messages:
            fact_id = message.value["id"]["fact"]["uuid"]
            url = urljoin(settings.FACT_STORAGE_SERVER_URL, f'/facts/{fact_id}')
            async with session.get(url) as resp:
                assert resp.status == 200
                fact = await resp.json()
                fact.pop('hash')
                fact.pop('created_at')
                assert fact["uuid"] == fact_id
                assert fact == {
                    'uuid': fact_id,
                    'actor': [settings.ACTOR],
                    'type': 'teos.test.result', 'result': {
                        'scale_group': {'uuid': '8e052e42-9f84-4a50-801b-794011c38558',
                                        'scales': {'61d6133e-e472-4582-909a-30dbec510ff7': 3,
                                                   'ce5af48f-54a1-41e0-b431-61388c7e6683': 5}}}, 'source': 'teos',
                    'handler': 'teos_test_result', 'meta': {'test': {'uuid': 'a69ed2f7-ac5e-45ea-9f1c-4635657e2970'}},
                    'description': None, 'is_active': True, 'fact_class': None
                }
