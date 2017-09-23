# Robôs a serviço da cidadania

Repositório para raspagem de dados de orgãos públicos baianos.

## Iniciando o projeto
Ajuste as opções desejadas no módulo `ldch.settings` e execute os
seguintes comandos:

```bash
$ python3 setup.py develop
$ start_ldch \
    ldch.spiders.tce.TceRemuneracaoSpider \
    ldch.spiders.tcm.TcmRemuneracaoSpider
```


## Iniciando o projeto com Docker

```bash
$ docker-compose run
$ start_ldch \
    ldch.spiders.tce.TceRemuneracaoSpider \
    ldch.spiders.tcm.TcmRemuneracaoSpider
```


## Acesso aos dados

Todos os dados estão armazenados de acordo com `ldch.settings`, por
padrão, no banco `ldch`.


## TODO

* Tratar erros do TCE
    * ano sem remuneração (ex 2013)
    * falhas de banco de dados que aparentam ser um mês sem remuneração
