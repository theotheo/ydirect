# -*- coding: utf-8 -*-
import logging
import re
from collections import defaultdict
from datetime import datetime

import unicodecsv as csv
from tqdm import tqdm, trange

from grab import Grab

logging.basicConfig(filename='{}.log'.format(datetime.now().strftime('%Y-%m-%d-%H:%M')), level=logging.ERROR)

g = Grab()
g.setup(encoding='utf-8', connect_timeout=3, timeout=5)
URL = 'http://direct.yandex.ru/search?&rid=213&text={0}&page={1}'


def get_number_pages(query):
    """
    Количество страниц с объявлениями
    Args:
        query: фраза для скрепинга.
    Returns:
        Количество страниц объявлений
    """

    print(query)
    full_url = URL.format(query, 0)
    g.go(full_url)

    # проверка последней цифры
    sel = g.doc.select('//a[@class="b-pager__page"]')
    if sel.exists():
        pages = sel[-1].text()
    else:
        print('pass', full_url)

    # если в конце стоит многоточие
    if pages == '\u2026':
        pages = g.doc.select('//a[@class="b-pager__page"]')[-2].text()
        full_url = URL.format(query, int(pages))
        g.go(full_url)
        pages = g.doc.select('//a[@class="b-pager__page"]')[-1].text()

    return int(pages)


def fetch_ads(query):
    """
    Качаем объявления по одной фразу
    Args:
        query: фраза для скрепинга.
    Returns:
        Генератор соскрепленных объявлений.
    """
    nb_pages = get_number_pages(query)

    for page in trange(nb_pages, desc='pages'):
        full_url = URL.format(query, page)
        g.go(full_url)

        for item in tqdm(g.doc.select('//div[@class="banner-selection"]'), desc='ads'):
            ad = defaultdict(str)

            # получаем заголовок, текст объявления, ...
            xpath_elem_map = {
                './div[@class="ad"]/div[@class="ad-link"]': 'title',
                './div[@class="ad"]/div': 'text',
                './div[@class="ad"]/span/span[@class="domain"]': 'domain',
                './div[@class="ad"]/span/a[@class="vcard"]/@href': 'url'
            }

            for xpath, elem in xpath_elem_map.items():
                sel = item.select(xpath)
                if sel.exists():
                    ad[elem] = sel.text()

            try:
                # получаем ссылку на карточку объявления
                g1 = Grab()
                g1.go(ad['url'])

                xpath_elem_map = {
                    '//h1': 'firm',
                    '//div[@class="contact-item call-button-container"]/div[@class="large-text"]': 'phone',
                    '//a[@class="email"]': 'email'
                }

                for xpath, elem in xpath_elem_map.items():
                    sel = g1.doc.select(xpath)
                    if sel.exists():
                        ad[elem] = sel.text()

            except Exception as e:
                logging.error(e)

            # не хотим сохранять url
            del ad['url']

            yield ad


def fetch_queries_to_file(queries, filename='firms.csv'):
    """
    Качаем все объявления в файл
    Args:
        queries: Список фраз для скрепинга.
        filename: Имя файла, в который сохраняем.
    """

    with open(filename, 'wb') as f:
        fieldnames = ['firm', 'phone', 'email', 'title', 'text', 'domain']
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        # номер телефона будем использовать за уникальное поле
        uniq_phones = []

        for query in queries:
            for ad in fetch_ads(query):
                # если объявление с таким номером уже есть, не добавляем его
                if ad['phone'] not in uniq_phones:
                    writer.writerow(ad)
                    uniq_phones.append(ad['phone'])

if __name__ == "__main__":
    # список запросов
    words = [
        'застройщики москвы и московской области',
        'новостройки с отделкой в подмосковье от застройщика'
    ]

    fetch_queries_to_file(words)
