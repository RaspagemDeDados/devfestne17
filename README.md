# Robôs a serviço da cidadania

Repositório para produção de código da palestra (ainda não aceita) no DevFest Nordeste 2017.

## Requisitos

* **Docker**

## Instalação

```bash
$ make serve
```

## Execução

```bash
$ make shell
$ python lhcd.py
```  

## TODO

* Ajustar os logs (verbosidade padrão);
* Tratar erros do TCE:
    * ano sem remuneração (ex 2013);
    * falhas de banco de dados que causam o mesmo erro acima.
