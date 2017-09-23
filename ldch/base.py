import datetime
import json
import urllib.parse
from random import randint, choice

import pymongo
import requests
import scrapy
import scrapy.dupefilters
import scrapy.exceptions
import scrapy.item
import scrapy.signals
import stem
import stem.control
from scrapy.crawler import CrawlerProcess
from scrapy.settings import Settings
from twisted.internet import reactor

from ldch import settings


def date_range(start, end=None):
    try:
        start_year, start_month = start
    except:
        start_year = start
        start_month = 1
    if end is not None:
        end_year, end_month = end
    else:
        now = datetime.datetime.now()
        end_year = now.year
        end_month = now.month - 1
    for year in range(start_year, end_year+1):
        for month in range(1, 13):
            if year == start_year and month < start_month:
                continue
            if year == end_year and month > end_month:
                continue
            yield (year, month)


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


def web_archive(url, user_agent=None, proxy=None):
    "Aciona arquivamento da web.archive.org e retorna a URL."

    if not hasattr(web_archive, '_cache'):
        web_archive._cache = {}

    if url in web_archive._cache:
        return web_archive._cache[url]

    url1 = 'http://web.archive.org/save/' + url
    url2 = 'http://web.archive.org/__wb/sparkline?output=json&collection=web&url='
    url2 += urllib.parse.quote(url)

    with requests.Session() as session:
        if proxy is not None:
            session.proxies.update({'http': proxy})
        if user_agent is not None:
            session.headers.update({'User-Agent': user_agent})
        session.get(url1)
        req2 = session.get(url2)
    payload = json.loads(req2.content.decode())

    wa_url = 'http://web.archive.org/web/%s/%s' % (payload['last_ts'], url)
    web_archive._cache[url] = wa_url
    return wa_url


class Database:
    "Classe de acesso ao banco."

    def __init__(self):
        self.db = pymongo.MongoClient(settings.MONGO_URI)

    def __enter__(self):
        return self.db.__enter__()['ldch']

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.db.__exit__(exc_type, exc_val, exc_tb)


class LdchMiddleware:
    "Middleare para preparação de requisições."

    def process_request(self, request, spider):
        # atribúi um user agent aleatório

        user_agent = choice(settings.USER_AGENTS)
        request.headers.setdefault(b'User-Agent', user_agent)

        # atribúi o proxy
        if settings.ENABLE_TOR_PROXY:
            request.meta['proxy'] = settings.HTTP_PROXY


class LdchSignalHandler:
    "Lida com sinais do Scrapy."

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        ext = cls()
        crawler.signals.connect(ext.spider_error, signal=scrapy.signals.spider_error)
        crawler.signals.connect(ext.item_scraped, signal=scrapy.signals.item_scraped)
        crawler.signals.connect(ext.response_downloaded, signal=scrapy.signals.response_downloaded)
        return ext

    def spider_error(self, failure, response, spider):
        "Registra exceções no banco de dados."

        with Database() as db:
            error = self._create_error('exception', response, response.request, spider)
            error['traceback'] = failure.getTraceback()
            db['Errors'].insert_one(error)

    def item_scraped(self, item, response, spider):
        "Salva itens no banco de dados."

        with Database() as db:
            new_page = {
                'url': response.url,
                'request_body': response.request.body.decode()
            }
            page = db['Meta'].find_one(new_page)
            if page:
                page_id = page['_id']
            else:
                if response.request.method == 'GET':
                     new_page['web_archive'] = self._web_archive(response.url)
                result = db['Meta'].insert_one(new_page)
                page_id = result.inserted_id

            item['__meta'] = page_id
            db[spider.name].insert_one(item)

    def response_downloaded(self, response, request, spider):
        "Registra erros HTTP no banco de dados."

        if response.status >= 400:
            with Database() as db:
                error = self._create_error('http_error', response, request, spider)
                error['status'] = response.status
                db['Errors'].insert_one(error)

    @classmethod
    def _web_archive(self, url):
        user_agent = choice(settings.USER_AGENTS)
        proxy = None
        if settings.ENABLE_TOR_PROXY:
            proxy = settings.HTTP_PROXY
        return web_archive(url, user_agent, proxy)

    @classmethod
    def _create_error(cls, type, response, request, spider):
        return {
            'type':  type,
            'spider': spider.name,
            'url': response.url,
            'method': request.method,
            'request_body': request.body.decode()
        }


class LdchDupeFilter(scrapy.dupefilters.RFPDupeFilter):
    "Ignora requisições duplicadas, inclusive as já registradas previamente no banco."

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        with Database() as db:
            for meta in db['Meta'].find():
                request = scrapy.Request(meta['url'], body=meta['request_body'])
                self.request_seen(request)


class LdchSpider(scrapy.Spider):
    """Base para os spiders do LDCH.

    Todos os nomes de spiders devem terminar com a string 'Spider'.
    """

    fields = None

    @property
    def name(self):
        n = self.__class__.__name__
        assert n.endswith("Spider"), "Spider name does not ends with 'Spider'"
        return n[:n.index('Spider')]

    def list_to_item(self, args):
        "Transforma uma lista num item de acordo com os campos e validação da variável `fields`."

        if self.fields is not None and len(args) != len(self.fields):
            raise ValueError("Length of arguments is different from fields")

        result = {}
        for (name, parser), value in zip(self.fields, args):
            value = value.strip()
            if value in ['', '-']:
                value = None
            result[name] = parser(value)

        return result

    def dict_to_item(self, dict):
        "Transforma dicionário num item de acordo com os campos e validação da variável `fields`."

        if len(dict) != len(self.fields):
            raise ValueError("Length of dictionary is different from fields")
        for name, converter in self.fields:
            dict[name] = converter(dict[name])
        return dict


class TorTestSpider(LdchSpider):

    def start_requests(self):
        for i in range(2):
            url = 'https://check.torproject.org?%d' % i
            yield scrapy.Request(url, self.parse)

    def parse(self, response):
        yield {
            'h1': response.xpath('//h1/text()').extract_first().strip(),
            'strong': response.xpath('//strong/text()').extract_first().strip()
        }


def run_spiders():
    from ldch.tcm import TcmRemuneracaoSpider
    from ldch.tce import TceRemuneracaoSpider
    from ldch.tcm import TcmDespesasSpider

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

    for klass in [TcmDespesasSpider]:
        proc.crawl(klass)
    if settings.ENABLE_TOR_PROXY:
        reactor.callLater(random_wait_time(), change_tor_circuit_randomly)
    proc.start()


if __name__ == '__main__':
    run_spiders()
