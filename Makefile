#!make
include .env
export

# Open docker container in interactive mode
## Usage Example:
## 	- make it consumer - opens consumer container in interactive mode
it:
	docker exec -ti $(filter-out $@,$(MAKECMDGOALS)) bash

# Runs container logs
## Usage Example:
## - make logs consumer
logs:
	docker logs -f $(filter-out $@,$(MAKECMDGOALS))


# Wipes out all docker containers in machine
clean:
	# stop all containers
	docker stop $$(docker ps -a -q)

	# destroy all containers
	docker rm --force $$(docker ps -a -q)

	# destroy all images
	# docker rmi --force $$(docker images -q)

	# destroy all volumes
	docker volume rm $$(docker volume ls -q --filter dangling=true)

	# remove if something is still dangling
	docker system prune