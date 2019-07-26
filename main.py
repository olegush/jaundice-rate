from contextlib import contextmanager, asynccontextmanager
from enum import Enum
from ssl import SSLError
from contextlib import redirect_stderr
import time
import logging

import aionursery
import aiohttp
from aiohttp import ClientConnectionError, ClientResponseError, InvalidURL
import asyncio
from async_timeout import timeout
import pymorphy2

from adapters import *
from text_tools import split_by_words, calculate_jaundice_rate


TEST_ARTICLES = [
    'https://inosmi.ru/politic/20190725/245519455.html',
    'https://inosmi.ru/social/20190725/245517253.html',
    'https://inosmi.ru/politic/20190725/2455919834.html',
    'https://lenta.ru/news/2019/07/25/moshennichestvo/',
    ]


class ProcessingStatus(Enum):
    OK = 'OK'
    FETCH_ERROR = 'FETCH_ERROR'
    CONN_ERROR = 'CONN_ERROR'
    PARSING_ERROR = 'PARSING_ERROR'
    TIMEOUT = 'TIMEOUT'


def get_charged_words(path):
    with open(path) as file:
        charged_words = [word.strip() for word in file]
    return charged_words


CHARGED_WORDS = get_charged_words('charged_dict/negative_words.txt') + \
                get_charged_words('charged_dict/positive_words.txt')

TIMEOUT = 30

logging.basicConfig(level=logging.INFO)

async def fetch(session, url):
    async with timeout(TIMEOUT):
        async with session.get(url) as response:
            response.raise_for_status()
            return await response.text()


@asynccontextmanager
async def create_handy_nursery():
    try:
        async with aionursery.Nursery() as nursery:
            yield nursery
    except aionursery.MultiError as e:
        if len(e.exceptions) == 1:
            raise e.exceptions[0]
        raise


@contextmanager
def timer():
    t = time.monotonic()
    try:
        yield
        # time.monotonic() - t
    finally:
        logging.info(time.monotonic() - t)
        return '1'


def tst():
    with timer() as t:
        time.sleep(1.8)
        print(f't = {t}')

#tst()

async def process_article(session, morph, charged_words, url):
    try:
        #with timer() as t:
        html = await fetch(session, url)
        #start_time = time.monotonic()
        title, text = SANITIZERS['inosmi_ru'](html, url)
        words = split_by_words(morph, text)
        words_count = len(words)
        score = calculate_jaundice_rate(words, charged_words)
        #end_time = time.monotonic() - start_time
        #logging.info(end_time)

        status = ProcessingStatus.OK.name
    except (ClientConnectionError, ClientResponseError, InvalidURL):
        title = 'Connection Error'
        status = ProcessingStatus.CONN_ERROR.name
        score, words_count = None, None
    except ArticleNotFound as e:
        title = f'This article from {e.hostname}'
        status = ProcessingStatus.PARSING_ERROR.name
        score, words_count = None, None
    except asyncio.TimeoutError:
        title = 'TimeoutError'
        status = ProcessingStatus.TIMEOUT.name
        score, words_count = None, None
    return status, title, score, words_count


async def main():
    morph = pymorphy2.MorphAnalyzer()
    async with aiohttp.ClientSession() as session:
        async with create_handy_nursery() as nursery:
            tasks = []
            for url in TEST_ARTICLES:
                task = nursery.start_soon(process_article(session, morph, CHARGED_WORDS, url))
                tasks.append(task)
            tasks_resulted, _ = await asyncio.wait(tasks)
    print([task.result() for task in tasks_resulted])

if __name__ == '__main__':
    asyncio.run(main())
    #tst()
