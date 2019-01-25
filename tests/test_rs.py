from typing import List
from urllib.parse import urljoin

import aiohttp
import pytest

import settings


@pytest.fixture()
def rs_messages(kafka_messages: dict):
    return kafka_messages.get('rs', [])


@pytest.mark.asyncio
async def test_check_rs_messages(rs_messages: dict):
    """Test count RS messages"""
    assert len(rs_messages) == 7


@pytest.mark.asyncio
async def test_check_get_recommendations_by_api(rs_messages: List[dict]):
    """Test retrieve recommendations"""
    headers = {'Authorization': f'Token {settings.REC_STORAGE_SERVER_TOKEN}'}
    recommendations = []
    async with aiohttp.ClientSession(headers=headers) as session:
        for message in rs_messages:
            recommendation_id = message.value["id"]["recommendation"]["uuid"]
            url = urljoin(settings.REC_STORAGE_SERVER_URL, f'/api/recommendations/{recommendation_id}')
            async with session.get(url) as resp:
                assert resp.status == 200
                recommendation = await resp.json()
                assert recommendation["uuid"] == recommendation_id
                recommendation.pop('uuid')
                recommendations.append(recommendation)
    recommendations.sort(key=lambda x: x['object_id'])
    assert recommendations == [
        {'actor': [str(settings.ACTOR)], 'activity': 'read', 'object_id': '05dc9dc7-a3e7-46e0-9a59-dd8867016e3d',
         'source': 'RALL'},
        {'actor': [str(settings.ACTOR)], 'activity': 'read', 'object_id': '51aa9cc1-04bc-447e-8a20-b42f369b8c36',
         'source': 'RALL'},
        {'actor': [str(settings.ACTOR)], 'activity': 'read', 'object_id': '5de894ef-3148-43bb-9cca-907d52303ebc',
         'source': 'RALL'},
        {'actor': [str(settings.ACTOR)], 'activity': 'read', 'object_id': 'b4faa314-1596-43a1-bf55-dd1d44e41a1f',
         'source': 'RALL'},
        {'actor': [str(settings.ACTOR)], 'activity': 'read', 'object_id': 'c2d92a32-d883-49ed-8263-1392fc4b8303',
         'source': 'RALL'},
        {'actor': [str(settings.ACTOR)], 'activity': 'read', 'object_id': 'e12e4b95-c93d-40a0-bd0e-5a85e697846a',
         'source': 'RALL'},
        {'actor': [str(settings.ACTOR)], 'activity': 'read', 'object_id': 'f1c27597-9077-4ee3-a341-55d049aaec3a',
         'source': 'RALL'}
    ]
