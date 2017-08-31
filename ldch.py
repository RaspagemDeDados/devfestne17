import datetime
import json
import pymongo
import requests
import scrapy
import stem
import stem.control

from twisted.internet import reactor
from scrapy.crawler import CrawlerProcess
from random import randint, choice, shuffle
from urllib.parse import quote


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
    'TOR_HTTP_PROXY': 'http://localhost:8118',
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


def web_archive(url, user_agent):
    "Aciona arquivamento da web.archive.org e retorna a URL."

    url1 = 'http://web.archive.org/save/' + url
    url2 = 'http://web.archive.org/__wb/sparkline?output=json&collection=web&url='
    url2 += quote(url)

    headers = {
        'User-Agent': user_agent
    }
    req1 = requests.get(url1, headers=headers)
    req2 = requests.get(url2, cookies=req1.cookies, headers=headers)

    payload = json.loads(req2.content.decode())
    ts = payload['last_ts']
    return 'http://web.archive.org/web/%s/%s' % (ts, url)


class LdchMiddleware:
    "Posprocessa o resultado de uma raspagem."

    def process_start_requests(self, start_requests, spider):
        """Faz a requisição utilizar um proxy"""
        requests = list(start_requests)
        shuffle(requests)

        for request in requests:
            request.meta['proxy'] = spider.settings.get('TOR_HTTP_PROXY')
            request.headers['User-Agent'] = choice(spider.settings.get('USER_AGENTS'))
            yield request

    def process_spider_output(self, response, result, spider):
        """Adiciona URL e data final da extração ao resultado. Depois aciona o
        arquivamento da página no web.archive.org e salva o resultado no MongoDB.
        """
        url = response.url
        user_agent = choice(spider.settings.get('USER_AGENTS'))
        mongo_uri = spider.settings.get('MONGO_URI')
        mongo_collection = spider.name[:spider.name.index('Spider')]

        web_archive_url = web_archive(url, user_agent)
        for item in result:
            item['web_archive_url'] = web_archive_url
            self._save_results(mongo_uri, mongo_collection, item)
            yield item

    def _save_results(self, mongo_uri, collection, item):
        with pymongo.MongoClient(mongo_uri) as client:
            db = client['ldch']
            db[collection].insert_one(item)


class LdchSpider(scrapy.Spider):
    "Base para os spiders do LDCH"

    value_names = None

    @property
    def name(self):
        return self.__class__.__name__

    def tuple_to_dict(self, args):
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
            yield scrapy.Request(url, self.process_url)

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

    def process_url(self, resp):
        for item in self.parse(resp):
            self.finish_item(item, resp)
            yield item


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
                yield employee

        if i+1 != len(positions):
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


class TorSpider(LdchSpider):

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
    for klass in [TceRemuneracaoSpider]:
        proc.crawl(klass)

    reactor.callLater(random_wait_time(), change_tor_circuit_randomly)
    proc.start()


if __name__ == '__main__':
    main()
