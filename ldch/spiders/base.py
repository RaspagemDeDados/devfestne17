import datetime
import importlib
import json
import logging.handlers
import sys
import traceback
from random import randint, choice
from urllib.parse import quote

import pymongo
import pymongo.errors
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

logger = logging.getLogger(__name__)


def date_range(start, end=None):
    try:
        start_year, start_month = start
    except:
        start_year = start
        start_month = 1
    if end:
        try:
            end_year, end_month = end
        except:
            end_year = end
            end_month = datetime.datetime.now().month - 1
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
    url2 += quote(url)

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


def register_error(type, request=None, spider=None, **data):
    """Registra erros no banco."""

    error = {
        'type': type,
        'when': datetime.datetime.now()
    }
    if request:
        error.update({
            'url': request.url,
            'method': request.method,
            'request_body': request.body.decode()
        })
    if spider:
        error['spider'] = spider.name
    if data:
        error.update(data)
    with Database() as db:
        db['Errors'].insert_one(error)


class Database:
    "Classe de acesso ao banco."

    def __init__(self, clear_exceptions=True):
        self.clear = clear_exceptions
        self.db = pymongo.MongoClient(settings.MONGO_URI)

    def __enter__(self):
        return self.db.__enter__()['ldch']

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.db.__exit__(exc_type, exc_val, exc_tb)
        if isinstance(exc_type, pymongo.errors.PyMongoError):
            logger.exception("Falha ao operar o banco.")
            if not self.clear:
                return True
        return False


class LdchMiddleware:
    "Middleare para preparação de requisições."

    def process_request(self, request, spider):
        # atribúi um user agent aleatório

        user_agent = choice(settings.USER_AGENTS)
        request.headers.setdefault(b'User-Agent', user_agent)

        # atribúi o proxy
        if settings.ENABLE_TOR_PROXY:
            request.meta['proxy'] = settings.HTTP_PROXY

    def process_exception(self, request, exception, spider):
        if isinstance(exception, scrapy.exceptions.IgnoreRequest):
            return

        trace = traceback.format_exception(type(exception), exception, exception.__traceback__)
        trace = ''.join(trace)

        register_error(
            'downloader_exception',
            request=request,
            spider=spider,
            traceback=trace
        )


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

        register_error(
            'spider_exception',
            request=response.request,
            spider=spider,
            traceback=failure.getTraceback()
        )

    def item_scraped(self, item, response, spider):
        "Salva itens no banco de dados."

        logger.debug("Salvando item %r %s" % (response.request, item))
        try:
            result = None
            with Database(clear_exceptions=False) as db:
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

                    new_page['when'] = datetime.datetime.now()
                    result = db['Meta'].insert_one(new_page)
                    page_id = result.inserted_id

                item['__meta'] = page_id
                db[spider.name].insert_one(item)
        except pymongo.errors.PyMongoError:
            logger.exception("Falha ao salvar item %r" % response.request)
            if result:
                db['Meta'].remove({'_id': result.inserted_id})

    def response_downloaded(self, response, request, spider):
        "Registra erros HTTP no banco de dados."

        if response.status >= 400:
            register_error(
                'http_error',
                request=request,
                spider=spider,
                status=response.status
            )

    @classmethod
    def _web_archive(cls, url):
        try:
            user_agent = choice(settings.USER_AGENTS)
            proxy = None
            if settings.ENABLE_TOR_PROXY:
                proxy = settings.HTTP_PROXY
            return web_archive(url, user_agent, proxy)
        except:
            msg = "Falha ao registrar URL em web.archive.org: %s" % url
            logger.exception(msg)
            register_error(
                'web_archive_error',
                url=url
            )
            return None


class LdchDupeFilter(scrapy.dupefilters.RFPDupeFilter):
    "Ignora requisições duplicadas, inclusive as já registradas previamente no banco."

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        for request in self.find_requests_to_ignore():
            self.request_seen(request)

    @classmethod
    def find_requests_to_ignore(cls):
        def _find():
            with Database() as db:
                query = {'url': {'$exists': True}}
                fields = {'url': 1, 'request_body': 1}
                for meta in db['Meta'].find(query, fields):
                    yield meta

                if settings.SKIP_FAILED_URLS_HTTP_ERRORS:
                    query['type'] = 'http_error'
                    for url in db['Errors'].find(query, fields):
                        yield url

                if settings.SKIP_FAILED_URLS_EXCEPTIONS:
                    query['type'] = 'downloader_exception'
                    for url in db['Errors'].find(query, fields):
                        yield url

        for url in _find():
            yield scrapy.Request(url['url'], body=url['request_body'])


class LdchSpider(scrapy.Spider):
    """Base para os spiders do LDCH.

    Todos os nomes de spiders devem terminar com a string 'Spider'.
    """

    fields = None

    @property
    def name(self):
        name = self.__class__.__name__
        assert name.endswith("Spider"), "Nome do spider deve terminar com 'Spider'"
        return name[:name.index('Spider')]

    def list_to_item(self, args):
        "Transforma uma lista num item de acordo com os campos e validação da variável `fields`."

        if self.fields is not None and len(args) != len(self.fields):
            raise ValueError("Quantidade de elementos diferente da de campos.")

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

    # alguma duração dentro do intervalo de troca de circuit
    def random_wait_time():
        start, end = settings.TOR_CHANGE_CIRCUIT_INTERVAL_RANGE
        return randint(start, end)

    # solicita ao TOR para trocar de circuito
    def change_tor_circuit_randomly():
        try:
            change_tor_circuit()
        finally:
            reactor.callLater(random_wait_time(), change_tor_circuit_randomly)

    # carrega classes de spiders passadas em sys.argv
    spiders = set()
    for arg in sys.argv[1:]:
        last_dot = arg.rfind('.')
        module_name = arg[:last_dot]
        klass_name = arg[last_dot+1:]

        try:
            module = importlib.import_module(module_name)
            klass = getattr(module, klass_name)
            spiders.add(klass)
        except (ImportError, AttributeError):
            logger.critical('Impossível encontrar %s' % arg)
            return 1

    # cria o crawler
    scrapy_settings = Settings()
    scrapy_settings.setmodule(settings)
    proc = CrawlerProcess(scrapy_settings)

    # registra spiders
    for klass in spiders:
        proc.crawl(klass)

    # solicita a troca de circuito periodicamente
    if settings.ENABLE_TOR_PROXY:
        reactor.callLater(random_wait_time(), change_tor_circuit_randomly)

    # inicia o processo
    proc.start()


if __name__ == '__main__':
    run_spiders()
