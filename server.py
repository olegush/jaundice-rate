from enum import Enum
from functools import partial
from collections import OrderedDict

from aiohttp import web
import aiohttp
from aiohttp import ClientConnectionError, ClientResponseError, ClientTimeout, InvalidURL
import asyncio
from async_timeout import timeout
import pymorphy2

from adapters import *
from text_tools import split_by_words, calculate_jaundice_rate
from async_tools import create_handy_nursery


CHARGED_NEG_WORDS_PATH = 'charged_dict/negative_words.txt'
CHARGED_POS_WORDS_PATH = 'charged_dict/positive_words.txt'
ARTICLES_URLS = [
    'https://inosmi.ru/politic/20190725/245519455.html',
    'https://inosmi.ru/politic/20190813/245626410.html',
    'https://inosmi.ru/politic/20190813/245623806.html',
    'https://inosmi.ru/politic/20190813/245623936.html',
    'https://inosmi.ru/social/20190725/245517253.html',
    'https://inosmi.ru/politic/20190725/2455919834.html',
    'https://lenta.ru/news/2019/07/25/moshennichestvo/',
    'https://inosmi.ru/politic/20190813/245625612.html',
    'https://inosmi.ru/politic/20190810/245615782.html',
    'https://inosmi.ru/politic/20190807/245592596.html',
    ]
ARTICLES_URLS_MAX = 20
TIMEOUTS = {'fetch': 30, 'split': 3}


class ProcessingStatus(Enum):
    OK = 'OK'
    FETCH_ERROR = 'FETCH_ERROR'
    CONN_ERROR = 'CONN_ERROR'
    PARSING_ERROR = 'PARSING_ERROR'
    TIMEOUT = 'TIMEOUT'


morph = pymorphy2.MorphAnalyzer()


def get_charged_words(path):
    with open(path) as file:
        charged_words = [word.strip() for word in file]
    return charged_words


async def fetch(session, url, timeout_fetch):
    timeout = ClientTimeout(total=timeout_fetch)
    async with session.get(url, timeout=timeout, raise_for_status=True) as response:
        return await response.text()


async def process_article(session, morph, charged_words, url, timeouts):
    status = score = words_count = None
    try:
        html = await fetch(session, url, timeouts['fetch'])
        text = SANITIZERS['inosmi_ru'](html, url)
        async with timeout(timeouts['split']):
            words = await split_by_words(morph, text)
        words_count = len(words)
        score = calculate_jaundice_rate(words, charged_words)
        status = ProcessingStatus.OK.name
    except (ClientConnectionError, ClientResponseError, InvalidURL):
        status = ProcessingStatus.CONN_ERROR.name
    except ArticleNotFound as e:
        status = ProcessingStatus.PARSING_ERROR.name
    except asyncio.TimeoutError:
        status = ProcessingStatus.TIMEOUT.name
    return OrderedDict(zip(['status', 'url', 'score', 'words_count'], [status, url, score, words_count]))


async def coro_test_process_article(session, url, timeouts, asserting):
    charged_words = get_charged_words(CHARGED_NEG_WORDS_PATH) +  get_charged_words(CHARGED_POS_WORDS_PATH)
    assert await process_article(session, morph, charged_words, url, timeouts) == asserting


async def main_test_process_article():
    async with aiohttp.ClientSession() as session:
        async with create_handy_nursery() as nursery:
            nursery.start_soon(coro_test_process_article(
                session,
                'https://lenta.ru/news/2019/07/25/moshennichestvo/',
                {'fetch': 30, 'split': 3},
                {"status": "PARSING_ERROR", "url": "https://lenta.ru/news/2019/07/25/moshennichestvo/", "score": None, "words_count": None}
            ))
            nursery.start_soon(coro_test_process_article(
                session,
                'https://inosmi.ru/politic/20190725/2455919834.html',
                {'fetch': 30, 'split': 3},
                {"status": "CONN_ERROR", "url": "https://inosmi.ru/politic/20190725/2455919834.html", "score": None, "words_count": None},
            ))
            nursery.start_soon(coro_test_process_article(
                session,
                'https://inosmi.ru/politic/20190813/245623806.html',
                {'fetch': 30, 'split': 0.001},
                {"status": "TIMEOUT", "url": "https://inosmi.ru/politic/20190813/245623806.html", "score": None, "words_count": None},
            ))


def test_process_article():
    asyncio.run(main_test_process_article())


async def handle(urls, charged_words, request):
    if len(urls) > ARTICLES_URLS_MAX:
        resp = {"error": "too many urls in request, should be 10 or less"}
        return web.json_response(resp, status=400)
    async with aiohttp.ClientSession(raise_for_status=True) as session:
        async with create_handy_nursery() as nursery:
            tasks = []
            for url in urls:
                task = nursery.start_soon(process_article(session, morph, charged_words, url, TIMEOUTS))
                tasks.append(task)
            tasks_resulted, _ = await asyncio.wait(tasks)
    resp = [task.result() for task in tasks_resulted]
    return web.json_response(resp)


if __name__ == '__main__':
    charged_words = get_charged_words(CHARGED_NEG_WORDS_PATH) +  get_charged_words(CHARGED_POS_WORDS_PATH)
    app = web.Application()
    handle_r = partial(handle, ARTICLES_URLS, charged_words)
    app.add_routes([web.get('/', handle_r)])
    web.run_app(app)
