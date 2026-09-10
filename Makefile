# Developer entrypoints. Run `make help` for the list.
SHELL := /bin/bash
COMPOSE := docker compose -f infra/docker-compose.yml
COMPOSE_FULL := docker compose -f infra/docker-compose.yml -f infra/docker-compose.part2.yml

# Some external drives mount noexec, so the venv's console scripts won't run
# directly. Always drive tools through the interpreter.
VENV ?= .venv
PY := $(VENV)/bin/python3

PART1_SERVICES := services/meter-simulator services/batch-file-generator services/ingestion
PART2_SERVICES := services/stream-processor services/batch-processor services/serving-api services/alert-engine

.DEFAULT_GOAL := help
.PHONY: help venv install install-part2 lint fmt typecheck test test-integration \
        generate-seed up up-full down clean logs seed topics demo

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
	  awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

venv: ## Create the local virtualenv
	python3 -m venv $(VENV)
	$(PY) -m pip install -U pip

install: venv ## Install energy_common + Part 1 services (editable) + dev tools
	$(PY) -m pip install -e "libs/common[dev]"
	@for svc in $(PART1_SERVICES); do $(PY) -m pip install -e "$$svc[dev]"; done

install-part2: ## Additionally install the Part 2 service skeletons (heavier: pyspark, fastapi)
	@for svc in $(PART2_SERVICES); do $(PY) -m pip install -e "$$svc[dev]"; done

lint: ## Ruff lint the whole workspace
	$(PY) -m ruff check .

fmt: ## Auto-format + fix
	$(PY) -m ruff format .
	$(PY) -m ruff check --fix .

typecheck: ## mypy the library and Part 1 services (source only)
	$(PY) -m mypy libs/common/src $(addsuffix /src,$(PART1_SERVICES))

test: ## Unit tests (no infrastructure needed)
	$(PY) -m pytest -m "not integration"

test-integration: ## Integration tests (requires `make up`)
	$(PY) -m pytest -m integration

generate-seed: ## Regenerate data/seed/*.csv from energy_common.seeds
	$(PY) scripts/generate_seed.py --households $${SIM_HOUSEHOLD_COUNT:-200} --seed $${SIM_SEED:-42}

up: ## Start the Part 1 stack (Kafka, Postgres, Prometheus, sources, ingestion)
	$(COMPOSE) up -d --build

up-full: ## Start the full stack including the Part 2 services
	$(COMPOSE_FULL) up -d --build

down: ## Stop and remove containers, keep volumes
	$(COMPOSE_FULL) down

clean: ## Stop everything, delete volumes + generated data
	$(COMPOSE_FULL) down -v
	rm -rf data/landing/* data/lake/* data/checkpoints reports/out

logs: ## Tail logs from all running services
	$(COMPOSE_FULL) logs -f --tail=100

topics: ## Create Kafka topics with the agreed partitioning
	./scripts/create_topics.sh

seed: ## Load dimension data into Postgres
	./scripts/seed_database.sh

demo: ## End-to-end demo (Part 1). DEMO_FULL=1 for the Part 2 path.
	./scripts/demo.sh
