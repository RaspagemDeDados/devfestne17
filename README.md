# Robôs a serviço da cidadania

Repositório para produção de código da palestra (ainda não aceita) no DevFest Nordeste 2017.

## Instalação

    $ python3 setup.py install

Alguns programas são necessários para o lhcd.py funcionar:
  
* **Tor**: com a porta de controle padrão;
* **Proxy SOCKS5/HTTP**: para tunelar o SOCKS5 do Tor até o Scrapy, que só aceita HTTP:
    * Por exemplo o Privoxy com a opção `forward-socks5t / 127.0.0.1:9050` e o Tor com a porta de proxy padrão.
* **MongoDB**:

## TODO

* Ajustar os logs (verbosidade padrão);
* Tratar erros do TCE:
    * ano sem remuneração (ex 2013);
    * falhas de banco de dados que causam o mesmo erro acima.
