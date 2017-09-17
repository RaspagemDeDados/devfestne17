from urllib.parse import urlencode

import scrapy

from ldch import settings
from ldch.base import LdchSpider, parse_float, date_range


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
        for ano, mes in date_range(settings.START_YEAR):
            url = 'https://www.tce.ba.gov.br/component/cdsremuneracao/?' + urlencode({
                'ano': ano,
                'mes': mes,
                'pesquisar': 'Pesquisar',
                'tmpl': 'component',
                'view': 'consolidado'

            })
            yield scrapy.Request(url, callback=self.parse_tabela)

    def parse_tabela(self, response):
        cargos = response.xpath("//b[contains(text(), 'CARGO:')]/../text()").extract()[3:]
        cargos = (p.strip() for p in cargos)
        cargos = [p for p in cargos if p != ""]

        competencia = response.xpath("//b[contains(text(), 'Mês/Ano')]/../text()").extract_first().strip()
        mes, ano = competencia.split('/')

        i = None
        for i, table in enumerate(response.css('.cTable tbody')):
            for remuneracao in table.css('tr'):
                remuneracao = remuneracao.xpath('td/text()').extract()
                if len(remuneracao) == 1:
                    assert remuneracao[0].strip().startswith("* A remuneração")
                    continue

                remuneracao = self.list_to_item(remuneracao)
                remuneracao['Cargo'] = cargos[i].strip()
                remuneracao['Competência'] = '%s-%s' % (mes, ano)
                yield remuneracao

        if (i or 0) + 1 != len(cargos):
            raise Exception("Quantidade de cargos diferente da quantidade de tabelas")


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
