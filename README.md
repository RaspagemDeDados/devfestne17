# Robôs a serviço da cidadania

Repositório para raspagem de dados de orgãos públicos baianos.

## Iniciando o projeto
Ajuste as opções desejadas no arquivo `settings.py` e execute os
seguintes comandos.

```bash
$ python3 setup.py develop
$ start_ldch
```


## Requisitos
* Docker
* docker-compose

## Instalação e Execução

```bash
$ docker-compose run
$ ldch_devfestne17
```


## TODO

* Tratar erros do TCE
    * ano sem remuneração (ex 2013)
    * falhas de banco de dados que aparentam ser um mês sem remuneração
