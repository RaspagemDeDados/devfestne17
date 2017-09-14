import datetime
import json

import scrapy
from ldch.base import LdchSpider, parse_float


class TcmRemuneracaoSpider(LdchSpider):

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

        url = "http://www.tcm.ba.gov.br/Webservice/public/index.php/entidades"
        yield scrapy.Request(url, callback=self.encontrar_entidades, dont_filter=True, meta={
                'cdMunicipios': cdMunicipios,
                'dsMunicipios': dsMunicipios
        })

    def encontrar_entidades(self, response):
        entidades = json.loads(response.body_as_unicode())
        now = datetime.datetime.now()

        for entidade in entidades:
            cdEntidade = entidade['idUnidadeJurisdicionada'].strip()
            dsEntidade = entidade['nm_Unidade'].strip()

            for cdMunicipio, dsMunicipio in zip(response.meta['cdMunicipios'], response.meta['dsMunicipios']):
                cdMunicipio = cdMunicipio.strip()
                dsMunicipio = dsMunicipio.strip()

                ano = self.settings['START_YEAR']
                for ano in range(ano, now.year + 1):
                    for mes in range(1, 13):
                        if ano == now.year and mes > now.month + 1:
                            continue
                        url = "https://www.tcm.ba.gov.br/portal-da-cidadania/pessoal"
                        formdata = {
                             'municipios': cdMunicipio,
                             'txtEntidade': dsEntidade,
                             'entidades': cdEntidade,
                             'ano': str(ano),
                             'mes': str(mes),
                             'tipoRegime': '',
                             'tipo': 'csv',
                             'pesquisar': 'Pesquisar'
                         }
                        meta = {
                            'competencia': (ano, mes),
                            'dsMunicipio': dsMunicipio,
                            'dsEntidade': dsEntidade
                        }
                        yield scrapy.FormRequest(url, callback=self.extrair_tabela, dont_filter=True, meta=meta,
                                                 formdata=formdata)

    def extrair_tabela(self, response):
        # for linha in csv.DictReader(io.StringIO(response.body.decode())):
        #     pass

        for linha in response.css("#tabelaResultado tbody tr"):
            funcionario = linha.xpath("td/text()").extract()
            funcionario = self.tuple_to_dict(funcionario)
            funcionario.update({
                'Município': response.meta['dsMunicipio'].strip(),
                'Entidade': response.meta['dsEntidade'].strip(),
                'Competência': '%d%02d' % response.meta['competencia']
            })
            yield funcionario
