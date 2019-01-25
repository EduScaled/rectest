from urllib.parse import urljoin

import aiohttp
import pytest

import settings


@pytest.yield_fixture(scope='module')
def topics():
    return ["fs", "lrs"]


@pytest.fixture(scope='module')
async def complete_lrs():
    data = [
        {
            "actor": {"objectType": "Agent", "account": {"name": "20543", "homePage": "https://my.2035.university/"}},
            "verb": {"id": "https://my.2035.university/xapi/v1/verbs/uploaded",
                     "display": {"ru": "загружено", "en": "uploaded"}},
            "object": {
                "id": "http://university2035.ru/task/2034", "objectType": "Activity",
                "definition": {
                    "name": {"ru": "Командная встреча 19.11.2018"}, "description": {
                        "ru": "Встреча была не только командная (наставники, тьютор, команда), но и с экспертом "
                              "Алексеем Коротковым. На встрече было обговорено ТЗ для создания протеза "
                              "(фото и документ ниже), также был показан главный герой ПО (фото ниже). "
                    },
                    "type": "https://my.2035.university/xapi/v1/activities/project_task/",
                    "extensions": {
                        "http://university2035.ru/task/2034": {
                            "expected_result": "1) Определиться с ТЗ 2) Определить работаем ли мы сами с протезом "
                                               "(т.е. сами его конструируем) или же делаем только ПО",
                            "status": "{closed}", "priority": "4", "attachments": [
                                "{ТЗ на командной.jpg:f5be3e9b029a6dbe619a53ead05c06fc.jpg}"
                                "{Главный герой.jpg:e85cf7a4ab81bbc9d4eb51c876fb045b.jpg}"
                                "{ТЗ (записи с командной).docx:2851ed751d3307cb62a1ab31f14e9d39.docx}"
                            ]
                        }
                    }
                }
            },
            "result": {
                "extensions": {
                    "https://my.2035.university/xapi/v1/results/file": {
                        "level": 3, "sublevel": 2,
                        "competence": "4dc07ab1-a3fd-4fae-88c0-700a77c88a1c",
                        "link": "http://university2035.ru/digital_footprint/31/",
                        "title": "Задача «Командная встреча 19.11.2018»",
                        "linkTitle": "Задача в рамках проекта «Cerebroom»",
                        "place": None,
                        "start": "2018-11-19T17:00:00+03:00",
                        "end": "2018-11-19T19:00:00+03:00"
                    }
                },
                "completion": True, "success": True
            },
            "context": {
                "team": {
                    "objectType": "Group", "name": "92",
                    "member": [{"objectType": "Agent",
                                "account": {
                                    "name": "20169",
                                    "homePage": "https://my.2035.university/"}},
                               {"objectType": "Agent",
                                "account": {
                                    "name": "20533",
                                    "homePage": "https://my.2035.university/"}},
                               {"objectType": "Agent",
                                "account": {
                                    "name": "20563",
                                    "homePage": "https://my.2035.university/"}},
                               {"objectType": "Agent",
                                "account": {
                                    "name": "20237",
                                    "homePage": "https://my.2035.university/"}},
                               {"objectType": "Agent",
                                "account": {
                                    "name": "20543",
                                    "homePage": "https://my.2035.university/"}}],
                    "account": {"name": "92",
                                "homePage": "http://university2035.ru/community/"
                                }
                }
            },
            "authority": {
                "mbox": "mailto:info@university2035.ru", "name": "university2035compractice",
                "objectType": "Agent"
            }
        }
    ]
    headers = {
        'Authorization': f'Basic {settings.LRS_AUTH}',
        'X-Experience-API-Version': '1.0.3',
    }
    async with aiohttp.ClientSession(headers=headers) as session:
        url = urljoin(settings.LRS_SERVER_URL, '/data/xAPI/statements')
        async with session.post(url, json=data) as resp:
            assert resp.status == 200


@pytest.fixture()
def lrs_messages(kafka_messages: dict):
    return kafka_messages.get('lrs', [])


@pytest.mark.usefixtures('complete_lrs')
def test_lrs_messages(lrs_messages):
    assert lrs_messages == [
        {
            'action': 'create', 'timestamp': lrs_messages[0]['timestamp'], 'source': 'lrs', 'type': 'create',
            'id': {
                'id': lrs_messages[0]['id']['id'],
                'authority': {'account': {'name': 'Compractice', 'homePage': 'http://compractice.com'},
                              'name': 'Compractice', 'objectType': 'Agent'}
            }, 'title': ''
        }
    ]


@pytest.mark.usefixtures('complete_lrs')
def test_fs_messages(fs_messages):
    assert fs_messages == [
        {
            'id': {'fact': {'uuid': fs_messages[0]['id']['fact']['uuid']}},
            'action': 'create', 'title': '', 'type': 'fact', 'source': 'factstorage', 'version': 1,
            'timestamp': fs_messages[0]['timestamp']
        }
    ]
