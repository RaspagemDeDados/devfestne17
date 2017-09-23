import csv
import datetime
import io
import json
from urllib.parse import urlencode

import scrapy

from ldch.spiders.base import LdchSpider, parse_float


class TcmRemuneracaoSpider(LdchSpider):

    start_urls = ["http://www.tcm.ba.gov.br/portal-da-cidadania/pessoal/"]

    fields = [
        ('Nome', str), ('Matrícula', str), ('Tipo Servidor', str),
        ('Cargo', str), ('Salário Base', parse_float),
        ('Salário Vantagens', parse_float),
        ('Salário Gratificação', parse_float)
    ]

    def parse(self, response):
        municipios_id = response.xpath("//select[@id='municipios']/option[@value != '']/@value").extract()
        municipios_nome = response.xpath("//select[@id='municipios']/option[@value != '']/text()").extract()

        for municipio_id, municipio_nome in zip(municipios_id, municipios_nome):
            url = "http://www.tcm.ba.gov.br/Webservice/public/index.php/entidades?" + urlencode({
                'cdMunicipio': municipio_id.strip()
            })
            meta = {
                'municipio_nome': municipio_nome.strip()
            }
            yield scrapy.Request(url, meta=meta, callback=self.extrair_entidades)

    def extrair_entidades(self, response):
        entidades = json.loads(response.body_as_unicode())

        for entidade in entidades:
            entidade_id = entidade['cdEntidade'].strip()
            entidade_nome = entidade['dsEntidade'].strip()

            agora = datetime.datetime.now()
            ano = self.settings['START_YEAR']
            for ano in range(ano, agora.year + 1):
                for mes in range(1, 13):
                    if ano == agora.year and mes > agora.month + 1:
                        continue

                    url = "http://www.tcm.ba.gov.br/Webservice/public/index.php/exportar/pessoal?" + urlencode({
                        'entidades': entidade_id,
                        'ano': str(ano),
                        'mes': str(mes),
                        'tipo': 'csv'
                    })
                    meta = {
                        'competencia': '%d-%02d' % (ano, mes),
                        'entidade_nome': entidade_nome,
                        'municipio_nome': response.meta['municipio_nome']
                    }
                    yield scrapy.Request(url, callback=self.extrair_tabela, meta=meta)

    def extrair_tabela(self, response):
        texto = response.body.decode()
        texto = texto.split('\r\n')[2:-2]
        texto = '\n'.join(texto)

        for linha in csv.DictReader(io.StringIO(texto)):
            del linha['']
            remuneracao = self.dict_to_item(linha)
            remuneracao['Município'] = response.meta['municipio_nome']
            remuneracao['Entidade'] = response.meta['entidade_nome']
            remuneracao['Competência'] = response.meta['competencia']
            yield remuneracao

