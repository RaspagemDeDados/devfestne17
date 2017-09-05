# Set the base image to Ubuntu
# FROM ubuntu:14.04
FROM python:3.4

# File Author / Maintainer
MAINTAINER Rodolfo Silva

RUN apt-get update

WORKDIR /app

## Copy and set entrypoint
COPY entrypoint.sh /usr/local/bin/

ENTRYPOINT ["/bin/bash"]
CMD ["/usr/local/bin/entrypoint.sh"]
