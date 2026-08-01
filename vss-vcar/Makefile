PROJ_DIR := $(abspath $(dir $(lastword $(MAKEFILE_LIST))))

VENV_DIR ?= /opt/deploy/.venv
REMOTIVE ?= $(if $(wildcard $(VENV_DIR)/bin/remotive),$(VENV_DIR)/bin/remotive,remotive)

COMPOSE := docker compose -f $(PROJ_DIR)/build/vss_hmi_vcar/docker-compose.yml

export REMOTIVEBROKER_TAG ?= 1.23.0

.PHONY: help gen up down logs unit clean

help:
	@echo "gen    regenerate build/ from instances/main.instance.yaml"
	@echo "up     build + start the 2-ECU car (BCM, VC)"
	@echo "down   stop and remove the car"
	@echo "logs   follow container logs"
	@echo "unit   run the VC decision-table self-check (no broker needed)"
	@echo "clean  down + rm -rf build/"

gen:
	cd $(PROJ_DIR) && $(REMOTIVE) topology build -f instances/main.instance.yaml build

up:
	@[ -f $(PROJ_DIR)/build/vss_hmi_vcar/docker-compose.yml ] || $(MAKE) gen
	$(COMPOSE) up --build -d

down:
	$(COMPOSE) down --remove-orphans

logs:
	$(COMPOSE) logs --tail=200 -f

unit:
	cd $(PROJ_DIR)/models/vc/python && python3 -m vc.control

clean:
	-@[ -f $(PROJ_DIR)/build/vss_hmi_vcar/docker-compose.yml ] && $(MAKE) down
	rm -rf $(PROJ_DIR)/build
