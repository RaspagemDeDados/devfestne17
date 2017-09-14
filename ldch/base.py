import collections
import datetime
import json
from random import randint, choice, shuffle
from urllib.parse import quote

import pymongo
import requests
import scrapy
import stem
import stem.control
from ldch import settings
from scrapy.crawler import CrawlerProcess
from scrapy.settings import Settings
from twisted.internet import reactor


def parse_int(v):
    return int(v.replace('.', ''))


def parse_float(v):
    return float(v.replace('.', '')
                  .replace('%', '')
                  .replace('R$', '')
                  .replace(',', '.'))


def change_tor_circuit():
    with stem.control.Controller.from_port() as tor:
        tor.authenticate()
        tor.signal(stem.Signal.NEWNYM)


def web_archive(url, user_agent, proxy):
    "Aciona arquivamento da web.archive.org e retorna a URL."

    if not hasattr(web_archive, '_cache'):
        web_archive._cache = {}

    if url in web_archive._cache:
        return web_archive._cache[url]

    url1 = 'http://web.archive.org/save/' + url
    url2 = 'http://web.archive.org/__wb/sparkline?output=json&collection=web&url='
    url2 += quote(url)

    with requests.Session() as session:
        session.proxies.update({'http': proxy})
        session.headers.update({'User-Agent': user_agent})
        session.get(url1)
        req2 = session.get(url2)
    payload = json.loads(req2.content.decode())

    wa_url = 'http://web.archive.org/web/%s/%s' % (payload['last_ts'], url)
    web_archive._cache[url] = wa_url
    return wa_url


class LdchMiddleware:
    "Prepara requisições e posprocessa resultados de raspagens."

    def process_start_requests(self, start_requests, spider):
        "Prepara uma requisição."

        requests = list(start_requests)
        shuffle(requests)

        for request in requests:
            request.meta['proxy'] = spider.settings.get('HTTP_PROXY')
            request.headers['User-Agent'] = choice(spider.settings.get('USER_AGENTS'))
            yield request

    def process_spider_output(self, response, result, spider):
        """Adiciona URL e data final da extração ao resultado. Depois aciona o
        arquivamento da página no web.archive.org e salva o resultado no MongoDB.
        """
        url = response.url
        user_agent = choice(spider.settings.get('USER_AGENTS'))
        mongo_uri = spider.settings.get('MONGO_URI')
        mongo_collection = spider.name
        proxy = spider.settings.get('HTTP_PROXY')
        now = datetime.datetime.now()

        for item in result:
            # ignora itens que não são resultados (ex. requisições)
            if not isinstance(item, collections.MutableMapping):
                yield item
                continue

            # adiciona metadados
            item['__url'] = response.url
            item['__date'] = now
            item['__request_body'] = response.request.body.decode()

            # arquiva na web.archive.org caso seja possível
            if response.request.method == 'GET':
                item['__web_archive_url'] = web_archive(url, user_agent, proxy)

            self._save_result(mongo_uri, mongo_collection, item)
            yield item

    def _save_result(self, mongo_uri, collection, item):
        with pymongo.MongoClient(mongo_uri) as client:
            db = client['ldch']
            db[collection].insert_one(item)


class LdchSpider(scrapy.Spider):
    """Base para os spiders do LDCH.

    Subclasses de LdchSpider podem implementar o método `start_requests()` que
    retorna uma iterador de `scrapy.Requests()`. Cada `scrapy.Request` tem uma
    URL e um callback. O callback poderá retornar um iterador de itens
    (`dict`s) que representa os dados raspados. `finish_item()` deverá ser
    aplicado em cada item antes de ser "`yield`ado".

    Variações do esquema acima são possíveis dentro das possibilidades do
    Scrapy.

    Todos os nomes de `Spider`s devem terminar com a string "Spider", por
    exemplo "TceRemunSpider".
    """

    fields = None

    @property
    def name(self):
        n = self.__class__.__name__
        assert n.endswith("Spider"), "Spider name does not ends with 'Spider'"
        return n[:n.index('Spider')]

    def tuple_to_dict(self, args):
        """Utilitário para transformar um iterável num dicionário.

        A propriedade `value_names` é uma lista ordenada de títulos que será
        para cada elemento do iterador.
        """
        if self.fields is not None and len(args) != len(self.fields):
            raise ValueError("Length of arguments is different from value_names")

        result = {}
        for (name, parser), value in zip(self.fields, args):
            value = value.strip()
            if value in ['', '-']:
                value = None
            result[name] = parser(value)

        return result



class TorTestSpider(LdchSpider):

    def start_requests(self):
        for i in range(40):
            url = 'https://check.torproject.org?%d' % i
            yield scrapy.Request(url, self.parse)

    def parse(self, response):
        yield {
            'h1': response.xpath('//h1/text()').extract_first(),
            'strong': response.xpath('//strong/text()').extract_first()
        }


def run_spiders():
    from ldch.tcm import TcmRemuneracaoSpider

    def random_wait_time():
        start, end = settings.TOR_CHANGE_CIRCUIT_INTERVAL_RANGE
        return randint(start, end)

    def change_tor_circuit_randomly():
        try:
            change_tor_circuit()
        finally:
            reactor.callLater(random_wait_time(), change_tor_circuit_randomly)

    scrapy_settings = Settings()
    scrapy_settings.setmodule(settings)
    proc = CrawlerProcess(scrapy_settings)

    for klass in [TcmRemuneracaoSpider]:
        proc.crawl(klass)

    reactor.callLater(random_wait_time(), change_tor_circuit_randomly)
    proc.start()


if __name__ == '__main__':
    run_spiders()
