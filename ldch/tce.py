import datetime

import scrapy
from ldch.base import LdchSpider, parse_float


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

        i = None
        for i, table in enumerate(resp.css('.cTable tbody')):
            for funcionario in table.css('tr'):
                funcionario = funcionario.xpath('td/text()').extract()
                if len(funcionario) == 1:
                    assert funcionario[0].strip().startswith("* A remuneração")
                    continue

                funcionario = self.tuple_to_dict(funcionario)
                funcionario['Cargo'] = positions[i].strip()
                funcionario['Data de apuração'] = extraction_date
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
