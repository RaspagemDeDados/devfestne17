# Makefile
# Author: Rodolfo Silva <contato@rodolfosilva.com>

serve:
	cd ./docker && sudo docker-compose up

shell:
	sudo docker exec -it api_devfestne17_backend bash $(filter-out $@,$(MAKECMDGOALS))

pip:
	sudo docker exec -it api_devfestne17_backend bash -c "pip $(filter-out $@,$(MAKECMDGOALS))"

shutdown:
	sudo docker ps | grep -v "CONTAINER ID" | awk '{print $1}' | xargs sudo docker stop
