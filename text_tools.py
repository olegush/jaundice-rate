import time
import logging
from contextlib import contextmanager
import asyncio
import string

import pymorphy2
import aiohttp

from async_tools import create_handy_nursery


logging.basicConfig(level=logging.INFO)

morph = pymorphy2.MorphAnalyzer()


def _clean_word(word):
    word = word.replace('«', '').replace('»', '').replace('…', '')
    # FIXME какие еще знаки пунктуации часто встречаются ?
    word = word.strip(string.punctuation)
    return word


@contextmanager
def timer():
    t = time.monotonic()
    try:
        yield
    finally:
        logging.info(time.monotonic() - t)


async def split_by_words(morph, text):
    """Учитывает знаки пунктуации, регистр и словоформы, выкидывает предлоги."""
    words = []
    with timer():
        for word in text.split():
            await asyncio.sleep(0)
            cleaned_word = _clean_word(word)
            normalized_word = morph.parse(cleaned_word)[0].normal_form
            if len(normalized_word) > 2 or normalized_word == 'не':
                words.append(normalized_word)
    return words


def calculate_jaundice_rate(article_words, charged_words):
    """Расчитывает желтушность текста, принимает список "заряженных" слов и ищет их внутри article_words."""
    if not article_words:
        return 0.0
    found_charged_words = [word for word in article_words if word in set(charged_words)]
    score = len(found_charged_words) / len(article_words) * 100
    return round(score, 2)


def test_calculate_jaundice_rate():
    assert -0.01 < calculate_jaundice_rate([], []) < 0.01
    assert 33.0 < calculate_jaundice_rate(['все', 'аутсайдер', 'побег'], ['аутсайдер', 'банкротство']) < 34.0


async def coro_test_split_by_words(text, asserting):
    assert await split_by_words(morph, text) == asserting


async def main_test_split_by_words():
    async with aiohttp.ClientSession() as session:
        async with create_handy_nursery() as nursery:
            tasks = []
            tasks.append(nursery.start_soon(coro_test_split_by_words(
                'Во-первых, он хочет, чтобы',
                ['во-первых', 'хотеть', 'чтобы']
            )))
            tasks.append(nursery.start_soon(coro_test_split_by_words(
                '«Удивительно, но это стало началом!»',
                ['удивительно', 'это', 'стать', 'начало']
            )))
        tasks_resulted, _ = await asyncio.wait(tasks)


def test_split_by_words():
    asyncio.run(main_test_split_by_words())
