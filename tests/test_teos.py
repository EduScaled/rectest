from typing import List
from urllib.parse import urljoin

import aiohttp
import pytest
from selenium.webdriver.common.by import By

import settings
from tests.utils import click, send_keys, make_checked


@pytest.yield_fixture(scope='module')
def topics():
    return ["rs", "fs", "teos"]


@pytest.fixture(scope='module')
def complete_teos_test(selenium):
    selenium.get(settings.TEOS_TEST_URL)
    click(selenium, (By.XPATH, '/html/body/div/main/div/a'))
    selenium.get(settings.TEOS_TEST_URL)
    make_checked(selenium, (By.XPATH, '//*[@id="w0"]/div[1]/div[3]/div[2]/div[1]/label/input'))
    make_checked(selenium, (By.XPATH, '//*[@id="w0"]/div[1]/div[5]/div[2]/div[3]/div/label/input'))
    send_keys(selenium, (By.XPATH, '//*[@id="w0"]/div[1]/div[8]/div[2]/div/input'), '150')
    send_keys(selenium, (By.XPATH, '//*[@id="w0"]/div[1]/div[11]/div[2]/div[1]/div[2]/div/div/input'), '1')
    send_keys(selenium, (By.XPATH, '//*[@id="w0"]/div[1]/div[11]/div[2]/div[2]/div[2]/div/div/input'), '2')
    click(selenium, (By.XPATH, '//*[@id="w0"]/div[2]/button'))


@pytest.fixture()
def teos_messages(kafka_messages: dict):
    return kafka_messages.get('teos', [])


@pytest.fixture()
def rs_messages(kafka_messages: dict):
    return kafka_messages.get('rs', [])


@pytest.fixture()
async def recommendations(rs_messages: List[dict]):
    headers = {'Authorization': f'Token {settings.REC_STORAGE_SERVER_TOKEN}'}
    result = []
    async with aiohttp.ClientSession(headers=headers) as session:
        for message in rs_messages:
            recommendation_id = message["id"]["recommendation"]["uuid"]
            url = urljoin(settings.REC_STORAGE_SERVER_URL, f'/api/recommendations/{recommendation_id}')
            async with session.get(url) as resp:
                assert resp.status == 200
                recommendation = await resp.json()
                assert recommendation["uuid"] == recommendation_id
                result.append(recommendation)
    return result


@pytest.mark.usefixtures('complete_teos_test')
def test_check_teos_messages(teos_messages: List[dict]):
    """Test count RS messages"""
    assert teos_messages == [
        {
            'action': 'create', 'source': 'teos', 'type': 'result',
            'id': {'test': {'uuid': 'a69ed2f7-ac5e-45ea-9f1c-4635657e2970'}, 'user': {'unti_id': settings.TEOS_ACTOR}},
            'title': 'Заголовок теста', 'timestamp': teos_messages[0]['timestamp']
        }
    ]


@pytest.mark.usefixtures('complete_teos_test')
def test_fs_messages(fs_messages):
    assert fs_messages == [
        {
            'id': {'fact': {'uuid': fs_messages[0]['id']['fact']['uuid']}},
            'action': 'create', 'title': '', 'type': 'fact', 'source': 'factstorage', 'version': 1,
            'timestamp': fs_messages[0]['timestamp']
        }
    ]


@pytest.mark.usefixtures('complete_teos_test')
def test_facts(facts):
    assert facts == [
        {
            'actor': [settings.TEOS_ACTOR],
            'type': 'teos.test.result', 'result': {
            'scale_group': {'uuid': '8e052e42-9f84-4a50-801b-794011c38558',
                            'scales': {'61d6133e-e472-4582-909a-30dbec510ff7': 3,
                                       'ce5af48f-54a1-41e0-b431-61388c7e6683': 6}}}, 'source': 'teos',
            'handler': 'teos_test_result', 'meta': {'test': {'uuid': 'a69ed2f7-ac5e-45ea-9f1c-4635657e2970'}},
            'description': None, 'is_active': True, 'fact_class': None
        }
    ]


@pytest.mark.usefixtures('complete_teos_test')
def test_rs_messages(rs_messages):
    def build_rs_message(i):
        return {
            'id': {'recommendation': {'uuid': rs_messages[i]['id']['recommendation']['uuid']}},
            'action': 'create', 'title': '', 'type': 'recommendation', 'source': 'recstorage', 'version': 1,
            'timestamp': rs_messages[i]['timestamp']
        }

    assert rs_messages == [build_rs_message(i) for i in range(7)]


@pytest.mark.usefixtures('complete_teos_test')
def test_recommendations(complete_teos_test, recommendations: List[dict]):
    recommendations = sorted(recommendations, key=lambda x: x['object_id'])
    assert recommendations == [
        {'uuid': recommendations[0]['uuid'], 'actor': [str(settings.TEOS_ACTOR)], 'activity': 'read',
         'object_id': '05dc9dc7-a3e7-46e0-9a59-dd8867016e3d', 'source': 'RALL'},
        {'uuid': recommendations[1]['uuid'], 'actor': [str(settings.TEOS_ACTOR)], 'activity': 'read',
         'object_id': '51aa9cc1-04bc-447e-8a20-b42f369b8c36', 'source': 'RALL'},
        {'uuid': recommendations[2]['uuid'], 'actor': [str(settings.TEOS_ACTOR)], 'activity': 'read',
         'object_id': '5de894ef-3148-43bb-9cca-907d52303ebc', 'source': 'RALL'},
        {'uuid': recommendations[3]['uuid'], 'actor': [str(settings.TEOS_ACTOR)], 'activity': 'read',
         'object_id': 'b4faa314-1596-43a1-bf55-dd1d44e41a1f', 'source': 'RALL'},
        {'uuid': recommendations[4]['uuid'], 'actor': [str(settings.TEOS_ACTOR)], 'activity': 'read',
         'object_id': 'c2d92a32-d883-49ed-8263-1392fc4b8303', 'source': 'RALL'},
        {'uuid': recommendations[5]['uuid'], 'actor': [str(settings.TEOS_ACTOR)], 'activity': 'read',
         'object_id': 'e12e4b95-c93d-40a0-bd0e-5a85e697846a', 'source': 'RALL'},
        {'uuid': recommendations[6]['uuid'], 'actor': [str(settings.TEOS_ACTOR)], 'activity': 'read',
         'object_id': 'f1c27597-9077-4ee3-a341-55d049aaec3a', 'source': 'RALL'}
    ]
