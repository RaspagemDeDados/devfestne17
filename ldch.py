import collections
import datetime
import json
from random import randint, choice, shuffle
from urllib.parse import quote, urlencode

import pymongo
import requests
import scrapy
import stem
import stem.control
from scrapy.crawler import CrawlerProcess
from twisted.internet import reactor

SETTINGS = {
    # Scrapy
    'DOWNLOAD_DELAY': 4,
    'CONCURRENT_REQUESTS_PER_DOMAIN': 2,

    'AUTOTHROTTLE_ENABLED': True,
    'AUTOTHROTTLE_START_DELAY': 5,
    'AUTOTHROTTLE_TARGET_CONCURRENCY': 1,

    'DOWNLOADER_STATS': True,
    'RANDOMIZE_DOWNLOAD_DELAY': True,

    'SPIDER_MIDDLEWARES': {
        'ldch.LdchMiddleware': 500
    },

    # Projeto (acessíveis pelo Spider)
    'MONGO_URI': 'mongodb://localhost/ldch',
    'HTTP_PROXY': 'http://localhost:8118',
    'TOR_CHANGE_CIRCUIT_INTERVAL_RANGE': (100, 400),
    'USER_AGENTS': [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.90 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.90 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.101 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/603.3.8 (KHTML, like Gecko) Version/10.1.2 Safari/603.3.8',
        'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.90 Safari/537.36',
        'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; WOW64; rv:54.0) Gecko/20100101 Firefox/54.0',
        'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.101 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.90 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.101 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36',
        'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:54.0) Gecko/20100101 Firefox/54.0',
        'Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; rv:11.0) like Gecko',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.113 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36',
        'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:54.0) Gecko/20100101 Firefox/54.0',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.12; rv:54.0) Gecko/20100101 Firefox/54.0',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/52.0.2743.116 Safari/537.36 Edge/15.15063',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_5) AppleWebKit/603.2.4 (KHTML, like Gecko) Version/10.1.1 Safari/603.2.4',
        'Mozilla/5.0 (Windows NT 10.0; WOW64; rv:55.0) Gecko/20100101 Firefox/55.0',
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.90 Safari/537.36',
        'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:55.0) Gecko/20100101 Firefox/55.0',
        'Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.90 Safari/537.36',
        'Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.101 Safari/537.36',
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.90 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:55.0) Gecko/20100101 Firefox/55.0',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.90 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/603.3.8 (KHTML, like Gecko) Version/10.1.2 Safari/603.3.8',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.101 Safari/537.36',
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.78 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.78 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:54.0) Gecko/20100101 Firefox/54.0',
        'Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36',
        'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.90 Safari/537.36',
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.101 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; WOW64; Trident/7.0; rv:11.0) like Gecko',
        'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.113 Safari/537.36',
        'Mozilla/5.0 (X11; Linux x86_64; rv:52.0) Gecko/20100101 Firefox/52.0',
        'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:55.0) Gecko/20100101 Firefox/55.0',
        'Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.79 Safari/537.36 Edge/14.14393',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.113 Safari/537.36',
        'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.12; rv:55.0) Gecko/20100101 Firefox/55.0',
        'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.101 Safari/537.36',
        'Mozilla/5.0 (iPad; CPU OS 10_3_3 like Mac OS X) AppleWebKit/603.3.8 (KHTML, like Gecko) Version/10.0 Mobile/14G60 Safari/602.1',
        'Mozilla/5.0 (Windows NT 6.1; rv:54.0) Gecko/20100101 Firefox/54.0',
        'Mozilla/5.0 (Windows NT 6.3; WOW64; rv:54.0) Gecko/20100101 Firefox/54.0',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.78 Safari/537.36',
        'Mozilla/5.0 (X11; Linux x86_64; rv:55.0) Gecko/20100101 Firefox/55.0',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.11; rv:54.0) Gecko/20100101 Firefox/54.0',
        'Mozilla/5.0 (X11; Linux x86_64; rv:54.0) Gecko/20100101 Firefox/54.0',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.101 Safari/537.36',
        'Mozilla/5.0 (Windows NT 6.1; Trident/7.0; rv:11.0) like Gecko',
        'Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:54.0) Gecko/20100101 Firefox/54.0',
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu Chromium/60.0.3112.78 Chrome/60.0.3112.78 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36',
        'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.78 Safari/537.36',
        'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:52.0) Gecko/20100101 Firefox/52.0',
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu Chromium/59.0.3071.109 Chrome/59.0.3071.109 Safari/537.36',
        'Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:55.0) Gecko/20100101 Firefox/55.0',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36 OPR/46.0.2597.57',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.90 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.101 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.101 Safari/537.36',
        'Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.90 Safari/537.36',
        'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36',
        'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0;  Trident/5.0)',
        'Mozilla/5.0 (Windows NT 5.1; rv:52.0) Gecko/20100101 Firefox/52.0',
        'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.90 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_3) AppleWebKit/600.5.17 (KHTML, like Gecko) Version/8.0.5 Safari/600.5.17',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_5) AppleWebKit/603.3.8 (KHTML, like Gecko) Version/10.1.2 Safari/603.3.8',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.90 Safari/537.36',
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.78 Safari/537.36 OPR/47.0.2631.55',
        'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.90 Safari/537.36',
        'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2272.118 Safari/537.36',
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36',
        'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.90 Safari/537.36',
        'Mozilla/5.0 (Windows NT 6.3; WOW64; Trident/7.0; rv:11.0) like Gecko',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_4) AppleWebKit/603.1.30 (KHTML, like Gecko) Version/10.1 Safari/603.1.30',
        'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:37.0) Gecko/20100101 Firefox/37.0',
        'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:40.0) Gecko/20100101 Firefox/40.1',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.90 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36',
        'Mozilla/5.0 (Windows NT 6.1; rv:55.0) Gecko/20100101 Firefox/55.0',
        'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.135 Safari/537.36',
        'Mozilla/5.0 (X11; Fedora; Linux x86_64; rv:54.0) Gecko/20100101 Firefox/54.0',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.10; rv:54.0) Gecko/20100101 Firefox/54.0',
        'Mozilla/5.0 (Windows NT 6.1; rv:52.0) Gecko/20100101 Firefox/52.0',
        'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.0; Trident/5.0;  Trident/5.0)',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; WOW64; rv:52.0) Gecko/20100101 Firefox/52.0'
    ]
}


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

    url1 = 'http://web.archive.org/save/' + url
    url2 = 'http://web.archive.org/__wb/sparkline?output=json&collection=web&url='
    url2 += quote(url)

    with requests.Session() as session:
        session.proxies.update({'http': proxy})
        session.headers.update({'User-Agent': user_agent})
        session.get(url1)
        req2 = session.get(url2)
    payload = json.loads(req2.content.decode())
    return 'http://web.archive.org/web/%s/%s' % (payload['last_ts'], url)


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

        result = list(result)
        if not all(isinstance(r, collections.Mapping) for r in result):
            return

        web_archive_url = web_archive(url, user_agent, proxy)

        for item in result:
            if not isinstance(item, collections.Mapping):
                continue
            item['__web_archive_url'] = web_archive_url
            self._save_results(mongo_uri, mongo_collection, item)
            yield item

    def _save_results(self, mongo_uri, collection, item):
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

    value_names = None

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
        if self.value_names is not None and len(args) != len(self.value_names):
            raise ValueError("Length of arguments is different from value_names")
        return dict(zip(self.value_names, args))

    def finish_item(self, item, response):
        item.update({
            '__url': response.url,
            '__date': datetime.datetime.now()
        })


class TceRemuneracaoSpider(LdchSpider):
    "Raspa a Tabela de Remuneração consolidada do TCE"

    value_names = (
        'Matrícula', 'Nome', 'Cargo comissionado', 'Vencimento básico',
        'Percentual Adicional', 'Valor adicional', 'Remuneração variável',
        'Outras vantagens', 'CET/RTI', 'Abono permanência',
        'Total da remuneração', 'Dedução IR', 'Dedução previdência',
        'Teto constitucional', 'Total de descontos', 'Remuneração líquida'
    )

    def start_requests(self):
        for url in self.generate_urls_by_year(2013, 2017):
            yield scrapy.Request(url)

    def generate_urls_by_year(self, start, end):
        urls = []
        now = datetime.datetime.now()
        for year in range(start, end + 1):
            for month in range(1, 13):
                if now.year == year and now.month == month:
                    break
                urls.append(
                    'https://www.tce.ba.gov.br/component/cdsremuneracao/?ano=%d&mes=%d&pesquisar=Pesquisar&tmpl=component&view=consolidado'
                    % (year, month)
                )
        #urls = ['https://www.tce.ba.gov.br/component/cdsremuneracao/?ano=%d&mes=%d&pesquisar=Pesquisar&tmpl=component&view=consolidado&ano=2015&mes=1']
        return urls


    def parse(self, resp):
        positions = resp.xpath("//b[contains(text(), 'CARGO:')]/../text()").extract()[3:]
        positions = (p.strip() for p in positions)
        positions = [p for p in positions if p != ""]

        extraction_date = resp.xpath("//b[contains(text(), 'Mês/Ano')]/../text()").extract_first().strip()

        for i, table in enumerate(resp.css('.cTable tbody')):
            for employee in table.css('tr'):
                employee = self.extract_employee(employee.xpath('td/text()').extract())
                if not employee:
                    continue
                employee['Cargo'] = positions[i].strip()
                employee['Data de apuração'] = extraction_date
                self.finish_item(employee, resp)
                yield employee

        if (i or 0) + 1 != len(positions):
            raise Exception("Quantidade de cargos diferente da quantidade de tabelas")

    def extract_employee(self, values):
        empl = []
        for value in values:
            value = value.strip()
            if value in ["", "-"]:
                empl.append(None)
                continue
            try:
                empl.append(int(value))
            except:
                try:
                    empl.append(parse_float(value))
                except:
                    empl.append(value)
        if type(empl[0]) != int:
            return None
        return self.tuple_to_dict(empl)


class TcmPessoalSpider(LdchSpider):

    start_urls = ["https://www.tcm.ba.gov.br/portal-da-cidadania/pessoal/"]

    value_names = [
        "Nome", "Matrícula", "Tipo Servidor", "Cargo", "Salário Base",
        "Salário Vantagens", "Salário Gratificação"
    ]

    def parse(self, response):
        cdMunicipios = "//select[@id='municipios']/option[@value != '']/@value"
        cdMunicipios = response.xpath(cdMunicipios).extract()

        dsMunicipios = "//select[@id='municipios']/option[@value != '']/text()"
        dsMunicipios = response.xpath(dsMunicipios).extract()

        cont = 0
        for cdMunicipio, dsMunicipio in zip(cdMunicipios, dsMunicipios):
            dsMunicipio = dsMunicipio.strip()
            cont += 1
            if cont == 3:
                break
            url = "http://www.tcm.ba.gov.br/Webservice/public/index.php/entidades?" + urlencode({
                'cdMunicipio': cdMunicipio
            })
            yield scrapy.Request(url, callback=self.encontrar_entidades,
                                 dont_filter=True, meta={
                'cdMunicipio': cdMunicipio,
                'dsMunicipio': dsMunicipio
            })

    def encontrar_entidades(self, response):
        entidades = json.loads(response.body_as_unicode())

        for entidade in entidades:
            url = "http://www.tcm.ba.gov.br/Webservice/public/index.php/tipoRegime?" + urlencode({
                'cdMunicipio': response.meta['cdMunicipio'],
                'cdEntidade': entidade['cdEntidade']
            })
            meta = response.meta.copy()
            meta.update({
                'cdEntidade': entidade['cdEntidade'],
                'dsEntidade': entidade['dsEntidade']
            })
            yield scrapy.Request(url, callback=self.encontrar_regimes,
                                 dont_filter=True, meta=meta)

    def encontrar_regimes(self, response):
        now = datetime.datetime.now()
        regimes = json.loads(response.body_as_unicode())
        for regime in regimes:
            for ano in range(200, now.year+1):
                for mes in range(1, 13):
                    if ano == now.year and mes > now.month+1:
                        continue
                    url = "http://www.tcm.ba.gov.br/portal-da-cidadania/pessoal"
                    meta = response.copy()
                    meta.update({
                        'tp_Regime': regime['tp_Regime'],
                        'de_Regime': regime['de_Regime']
                    })

                    yield scrapy.FormRequest(url, callback=self.extrair_tabela,
                                             dont_filter=True,  meta=meta,
                                             formdata={
                        'municipios': response.meta['cdMunicipio'],
                        'txtEntidade': response.meta['dsEntidade'],
                        'entidades': response.meta['cdEntidade'],
                        'ano': str(ano),
                        'mes': str(mes),
                        'tipoRegime': regime['tp_Regime'],
                        'pesquisar': 'Pesquisar'
                    })

    def extrair_tabela(self, response):
        for linha in response.css("#tabelaResultado tbody tr"):
            funcionario = linha.xpath("th/text()").extract()
            funcionario = self.tuple_to_dict(funcionario)
            funcionario.update({
                'Município': response.meta['dsMunicipio'],
                'Entidade': response.meta['dsEntidade'],

            })
            self.finish_item(funcionario, response)
            yield funcionario



# class TceVencimentoBasico(scrapy.Spider):
#     name = 'tcevencimentobasico'
#     start_urls = ['http://www.tce.ba.gov.br/institucional/transparencia/gestao-de-pessoas/43-institucional/transparencia/2416-tabela-de-vencimentos']
#
#     def parse(self, resp):
#         for table in resp.css('.tvTbody'):
#             position = table.xpath('(preceding::strong/span)[last()]/text()').extract_first()
#             group = table.xpath('(preceding::h2)[last()]/text()').extract_first()
#             for i, row in enumerate(table.css('tr')):
#                 vencimento = row.xpath('(td[not(*)]|td/p)/text()').extract()
#                 try:
#                     if len(vencimento) == 0:
#                         continue
#                     if len(vencimento) == 2:
#                         vencimento = self.extract_vencimento_2(vencimento)
#                     else:
#                         vencimento = self.extract_vencimento_3(vencimento)
#                 except ValueError:
#                     logging.error("Vencimento não extraído com sucesso:\n"
#                         "Cargo: %s\n"
#                         "Registro: %s" % (position, vencimento))
#                     continue
#
#                 vencimento['Cargo'] = position
#                 vencimento['Grupo'] = group
#                 yield vencimento
#
#     def extract_vencimento_3(self, values):
#         return {
#             'Classe': values[0].strip(),
#             'Referência': int(values[1].strip()),
#             'Parcela fixa': parse_float(values[2][3:])
#         }
#
#     def extract_vencimento_2(self, values):
#         return {
#             'Subsídio': values[0],
#             'Parcela fixa': parse_float(values[1])
#         }


class TorTestSpider(LdchSpider):

    def start_requests(self):
        for i in range(40):
            url = 'https://check.torproject.org?%d' % i
            yield scrapy.Request(url, self.parse)

    def parse(self, response):
        item = {
            'h1': response.xpath('//h1/text()').extract_first(),
            'strong': response.xpath('//strong/text()').extract_first()
        }
        self.finish_item(item, response)
        yield item


def main():
    def random_wait_time():
        start, end = SETTINGS['TOR_CHANGE_CIRCUIT_INTERVAL_RANGE']
        return randint(start, end)

    def change_tor_circuit_randomly():
        try:
            change_tor_circuit()
        finally:
            reactor.callLater(random_wait_time(), change_tor_circuit_randomly)

    proc = CrawlerProcess(SETTINGS)
    for klass in [TcmPessoalSpider, TceRemuneracaoSpider]:
        proc.crawl(klass)

    reactor.callLater(random_wait_time(), change_tor_circuit_randomly)
    proc.start()


if __name__ == '__main__':
    main()
