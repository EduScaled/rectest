import os
from typing import List

import pytest
from selenium.webdriver.common.by import By

import settings
from tests.utils import click, send_keys, find_element


@pytest.yield_fixture(scope='module')
def topics():
    return ["fs", "uploads"]


@pytest.fixture(scope='module')
def complete_uploads(selenium):
    selenium.get(settings.UPLOADS_URL)
    click(selenium, (By.XPATH, '/html/body/div/main/div[2]/a/button'))
    selenium.get(settings.UPLOADS_URL)
    click(selenium, (By.XPATH, '/html/body/div[1]/main/div[3]/div[2]/div/div[1]/div[2]/button'))

    file_input = find_element(selenium, (By.XPATH, '//*[@id="v-pills-home-3062"]/label/input'))
    selenium.execute_script('arguments[0].classList.remove("hidden")', file_input)

    send_keys(selenium, (By.XPATH, '//*[@id="v-pills-home-3062"]/label/input'), os.path.abspath(__file__))
    click(selenium, (By.XPATH, '/html/body/div[1]/main/div[3]/div[2]/div/form/div[2]/div[1]/button'))


@pytest.fixture()
def uploads_messages(kafka_messages: dict):
    return kafka_messages.get('uploads', [])


@pytest.mark.usefixtures('complete_uploads')
def test_check_uploads_messages(uploads_messages: List[dict]):
    """Test count RS messages"""
    assert uploads_messages == [
        {
            'id': {'user_result': {'id': uploads_messages[0]['id']['user_result']['id']}},
            'title': '1310 Егор Рудометкин', 'version': None, 'source': 'uploads', 'action': 'create',
            'type': 'user_result', 'timestamp': uploads_messages[0]['timestamp']
        },
        {
            'id': {'user_result': {'id': uploads_messages[1]['id']['user_result']['id']}},
            'title': '1310 Егор Рудометкин', 'version': None, 'source': 'uploads', 'action': 'update',
            'type': 'user_result', 'timestamp': uploads_messages[1]['timestamp']
        }
    ]


@pytest.mark.usefixtures('complete_uploads')
def test_fs_messages(fs_messages):
    assert fs_messages == [
        {
            'id': {'fact': {'uuid': fs_messages[0]['id']['fact']['uuid']}},
            'action': 'create', 'title': '', 'type': 'fact', 'source': 'factstorage', 'version': 1,
            'timestamp': fs_messages[0]['timestamp']
        }
    ]


@pytest.mark.usefixtures('complete_uploads')
def test_facts(facts):
    assert facts == [
        {
            'actor': [settings.UPLOADS_ACTOR], 'type': 'uploads.user.result',
            'result': {'event': '75ed4570-f5ea-409a-8bf2-4f136901a3c1',
                       'cell_address': {'level': '2', 'sublevel': '3',
                                        'competence': '4b222989-5bdf-439d-8f8b-62ea07b8cd5c'}},
            'source': 'uploads', 'handler': 'uploads_result',
            'meta': {
                'url': facts[0]['meta']['url'],
                'user_result': {'uuid': facts[0]['meta']['user_result']['uuid']}
            }, 'description': None, 'is_active': True, 'fact_class': None
        }
    ]
