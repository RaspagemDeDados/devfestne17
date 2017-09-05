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
from settings import SETTINGS


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

        for item in result:
            if not isinstance(item, collections.Mapping):
                yield item
                continue
            item['_web_archive_url'] = web_archive(url, user_agent, proxy)
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

    def finish_item(self, item, response):
        item.update({
            '_url': response.url,
            '_date': datetime.datetime.now()
        })


class TceRemuneracaoSpider(LdchSpider):
    "Raspa a Tabela de Remuneração consolidada do TCE"

    fields = (
        ('Matrícula', int), ('Nome', str), ('Cargo comissionado', str),
        ('Vencimento básico', parse_float), ('Percentual Adicional', parse_float),
        ('Valor adicional', parse_float), ('Remuneração variável', parse_float),
        ('Outras vantagens', parse_float), ('CET/RTI', parse_float), ('Abono permanência', parse_float),
        ('Total da remuneração', parse_float), ('Dedução IR', parse_float),
        ('Dedução previdência', parse_float), ('Teto constitucional', parse_float),
        ('Total de descontos', parse_float), ('Remuneração líquida', parse_float)
    )

    def start_requests(self):
        now = datetime.datetime.now()
        start_year = self.settings['START_YEAR']
        for year in range(start_year, now.year+1):
            for month in range(1, 13):
                if now.year == year and now.month == month:
                    break
                yield scrapy.Request(
                    'https://www.tce.ba.gov.br/component/cdsremuneracao/?ano=%d&mes=%d&pesquisar=Pesquisar&tmpl=component&view=consolidado'
                    % (year, month))
                # 'https://www.tce.ba.gov.br/component/cdsremuneracao/?ano=%d&mes=%d&pesquisar=Pesquisar&tmpl=component&view=consolidado&ano=2015&mes=1'


    def parse(self, resp):
        positions = resp.xpath("//b[contains(text(), 'CARGO:')]/../text()").extract()[3:]
        positions = (p.strip() for p in positions)
        positions = [p for p in positions if p != ""]

        extraction_date = resp.xpath("//b[contains(text(), 'Mês/Ano')]/../text()").extract_first().strip()

        for i, table in enumerate(resp.css('.cTable tbody')):
            for funcionario in table.css('tr'):
                funcionario = funcionario.xpath('td/text()').extract()
                if len(funcionario) == 1:
                    assert funcionario[0].strip().startswith("* A remuneração")
                    continue

                funcionario = self.tuple_to_dict(funcionario)
                funcionario['Cargo'] = positions[i].strip()
                funcionario['Data de apuração'] = extraction_date
                self.finish_item(funcionario, resp)
                yield funcionario

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

    fields = [
        ('Nome', str), ('Matrícula', int), ('Tipo Servidor', str),
        ('Cargo', str), ('Salário Base', parse_float),
        ('Salário Vantagens', parse_float),
        ('Salário Gratificação', parse_float)
    ]

    def parse(self, response):
        cdMunicipios = "//select[@id='municipios']/option[@value != '']/@value"
        cdMunicipios = response.xpath(cdMunicipios).extract()

        dsMunicipios = "//select[@id='municipios']/option[@value != '']/text()"
        dsMunicipios = response.xpath(dsMunicipios).extract()

        for cdMunicipio, dsMunicipio in zip(cdMunicipios, dsMunicipios):
            dsMunicipio = dsMunicipio.strip()
            # TODO mudar para sem cdMunicipio?
            url = "http://www.tcm.ba.gov.br/Webservice/public/index.php/entidades?" + urlencode({
                'cdMunicipio': cdMunicipio
            })
            yield scrapy.Request(url, callback=self.encontrar_entidades,
                                 dont_filter=True, meta={
                'cdMunicipio': cdMunicipio.strip(),
                'dsMunicipio': dsMunicipio.strip()
            })

    def encontrar_entidades(self, response):
        entidades = json.loads(response.body_as_unicode())

        for entidade in entidades:
            url = "http://www.tcm.ba.gov.br/Webservice/public/index.php/tipoRegime?" + urlencode({
                'cdMunicipio': response.meta['cdMunicipio'],
                'cdEntidade': entidade['cdEntidade'].strip()
            })
            meta = response.meta.copy()
            meta.update({
                'cdEntidade': entidade['cdEntidade'].strip(),
                'dsEntidade': entidade['dsEntidade'].strip()
            })
            yield scrapy.Request(url, callback=self.encontrar_regimes,
                                 dont_filter=True, meta=meta)

    def encontrar_regimes(self, response):
        now = datetime.datetime.now()
        regimes = json.loads(response.body_as_unicode())
        for regime in regimes:
            start_year = self.settings['START_YEAR']
            for ano in range(start_year, now.year+1):
                for mes in range(1, 13):
                    if ano == now.year and mes > now.month+1:
                        continue
                    url = "http://www.tcm.ba.gov.br/portal-da-cidadania/pessoal"
                    meta = response.meta.copy()
                    meta.update({
                        'tp_Regime': regime['tp_Regime'],
                        'de_Regime': regime['de_Regime']
                    })

                    # TODO mudar para CSV
                    yield scrapy.FormRequest(url, callback=self.extrair_tabela,
                                             dont_filter=True,  meta=meta,
                                             formdata={
                        'municipios': response.meta['cdMunicipio'],
                        'txtEntidade': response.meta['dsEntidade'],
                        'entidades': response.meta['cdEntidade'],
                        'ano': str(ano),
                        'mes': str(mes),
                        'tipoRegime': regime['tp_Regime'],
                        #'tipo': 'csv',
                        'pesquisar': 'Pesquisar'
                    })

    def extrair_tabela(self, response):
        for linha in response.css("#tabelaResultado tbody tr"):
            funcionario = linha.xpath("td/text()").extract()
            funcionario = self.tuple_to_dict(funcionario)
            funcionario.update({
                'Município': response.meta['dsMunicipio'].strip(),
                'Entidade': response.meta['dsEntidade'].strip()
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
    for klass in [TceRemuneracaoSpider]:
        proc.crawl(klass)

    reactor.callLater(random_wait_time(), change_tor_circuit_randomly)
    proc.start()


if __name__ == '__main__':
    main()
