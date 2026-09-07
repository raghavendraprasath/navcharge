# NavCharge Phase 1. Local Docker Compose only.
#
# Contract:
#   make up     brings the full stack from cold, creates topics, applies migrations
#   make seed   loads FIR geometry, aircraft types, and rate cards
#   make test   runs the property, unit, and integration suites
#   make lint   runs the linters
#
# make infra brings up only the datastores and the broker, which is the useful
# target while the admission and rating services do not exist yet.

SHELL := /bin/bash
.SHELLFLAGS := -eu -o pipefail -c

COMPOSE := docker compose
NETWORK := navcharge

# Datastores and the broker. Everything that has no local build.
INFRA := postgres mongo redis kafka kafka-ui

# Topic partition counts and retention are from docs/phase1-build-spec.md
# section 4. Retention is in milliseconds.
RETENTION_7D  := 604800000
RETENTION_30D := 2592000000

# Optional argument for make logs, for example: make logs S=admission
S ?=

.DEFAULT_GOAL := help

.PHONY: help check-env up infra build down clean ps health logs \
        topics topics-describe consume migrate seed seed-postgres seed-mongo \
        psql mongosh rediscli test test-admission test-rating \
        lint lint-admission lint-python

help: ## List the available targets
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

check-env:
	@test -f .env || { \
		echo "ERROR: .env is missing. Copy .env.example to .env and fill in POSTGRES_PASSWORD."; \
		exit 1; \
	}
	@grep -Eq '^POSTGRES_PASSWORD=.+' .env || { \
		echo "ERROR: POSTGRES_PASSWORD is empty in .env."; \
		exit 1; \
	}

# ---------------------------------------------------------------------------
# Stack lifecycle
# ---------------------------------------------------------------------------

up: check-env ## Bring up the full stack, create topics, apply migrations
	$(COMPOSE) up -d --wait
	@$(MAKE) --no-print-directory topics
	@$(MAKE) --no-print-directory migrate

infra: check-env ## Bring up datastores and broker only, create topics, apply migrations
	$(COMPOSE) up -d --wait $(INFRA)
	@$(MAKE) --no-print-directory topics
	@$(MAKE) --no-print-directory migrate

build: check-env ## Rebuild the locally built service images
	$(COMPOSE) build

down: ## Stop the stack and keep the volumes
	$(COMPOSE) down --remove-orphans

clean: ## Stop the stack and delete the volumes, so the next up is genuinely cold
	$(COMPOSE) down --remove-orphans --volumes

ps: ## Show container state
	$(COMPOSE) ps

health: ## Show the health status of every container
	@$(COMPOSE) ps --format 'table {{.Service}}\t{{.Status}}'

logs: ## Follow logs, optionally for one service: make logs S=admission
	$(COMPOSE) logs -f --tail=200 $(S)

# ---------------------------------------------------------------------------
# Kafka
# ---------------------------------------------------------------------------

# Auto-creation is disabled on the broker, so the partition counts here are the
# only thing that sets them. Ordering per airframe depends on the key, and the
# partition count is what makes that ordering worth having.
topics: ## Create the Phase 1 topics with their partition counts and retention
	@$(COMPOSE) exec -T kafka kafka-topics.sh --bootstrap-server localhost:9092 \
		--create --if-not-exists --topic usage.positions.v1 \
		--partitions 12 --replication-factor 1 --config retention.ms=$(RETENTION_7D)
	@$(COMPOSE) exec -T kafka kafka-topics.sh --bootstrap-server localhost:9092 \
		--create --if-not-exists --topic billing.charges.v1 \
		--partitions 6 --replication-factor 1 --config retention.ms=$(RETENTION_30D)
	@$(COMPOSE) exec -T kafka kafka-topics.sh --bootstrap-server localhost:9092 \
		--create --if-not-exists --topic usage.positions.dlq \
		--partitions 3 --replication-factor 1 --config retention.ms=$(RETENTION_30D)
	@echo "topics ready"

topics-describe: ## Describe the topics, including per partition state
	@$(COMPOSE) exec -T kafka kafka-topics.sh --bootstrap-server localhost:9092 --describe

consume: ## Tail a topic from the beginning: make consume TOPIC=usage.positions.v1
	@test -n "$(TOPIC)" || { echo "ERROR: set TOPIC, for example make consume TOPIC=usage.positions.v1"; exit 1; }
	$(COMPOSE) exec -T kafka kafka-console-consumer.sh --bootstrap-server localhost:9092 \
		--topic $(TOPIC) --from-beginning --property print.key=true

# ---------------------------------------------------------------------------
# Schema and seed data
# ---------------------------------------------------------------------------

# Numbered SQL applied in lexical order. Not applied through the image
# entrypoint, because that runs only against an empty data directory and would
# make schema iteration require a full volume wipe.
migrate: check-env ## Apply db/postgres/migrations/*.sql in order
	@files=$$(ls -1 db/postgres/migrations/*.sql 2>/dev/null || true); \
	if [ -z "$$files" ]; then echo "no migrations present yet, nothing to apply"; exit 0; fi; \
	for f in $$files; do \
		echo "applying $$f"; \
		$(COMPOSE) exec -T postgres psql -v ON_ERROR_STOP=1 -q -f "/$$f"; \
	done; \
	echo "migrations applied"

seed: seed-postgres seed-mongo ## Load FIR geometry, aircraft types, and rate cards

seed-postgres: check-env ## Load db/postgres/seed/*.sql in order
	@files=$$(ls -1 db/postgres/seed/*.sql 2>/dev/null || true); \
	if [ -z "$$files" ]; then echo "no Postgres seed present yet, nothing to load"; exit 0; fi; \
	for f in $$files; do \
		echo "loading $$f"; \
		$(COMPOSE) exec -T postgres psql -v ON_ERROR_STOP=1 -q -f "/$$f"; \
	done; \
	echo "Postgres seed loaded"

# Run in a throwaway container on the compose network rather than on the host,
# so that seeding needs no host Python environment.
seed-mongo: check-env ## Load the rate card catalog into Mongo
	@if [ ! -f db/mongo/seed_ratecards.py ]; then \
		echo "no Mongo seed present yet, nothing to load"; exit 0; \
	fi
	docker run --rm --network $(NETWORK) \
		--env-file .env \
		-e MONGO_URI=mongodb://mongo:27017 \
		-v "$(CURDIR)/db/mongo:/seed:ro" \
		python:3.11-slim \
		sh -c 'pip install --quiet --no-cache-dir pymongo && python /seed/seed_ratecards.py'

# ---------------------------------------------------------------------------
# Shells
# ---------------------------------------------------------------------------

psql: ## Open a psql shell
	$(COMPOSE) exec postgres psql

mongosh: ## Open a mongosh shell
	$(COMPOSE) exec mongo mongosh

rediscli: ## Open a redis-cli shell
	$(COMPOSE) exec redis redis-cli

# ---------------------------------------------------------------------------
# Tests and linting
# ---------------------------------------------------------------------------
# The guards below exist only while the service trees are absent. Remove them
# once services/admission and services/rating are in place, so that a missing
# suite fails loudly instead of passing quietly.

test: test-admission test-rating ## Run every test suite

test-admission: ## Run the admission service tests, including Testcontainers
	@if [ -x services/admission/gradlew ]; then \
		cd services/admission && ./gradlew test; \
	else \
		echo "SKIP: services/admission does not exist yet"; \
	fi

test-rating: ## Run the rating engine unit and property tests
	@if [ -d services/rating/tests ]; then \
		cd services/rating && python -m pytest -q; \
	else \
		echo "SKIP: services/rating does not exist yet"; \
	fi

lint: lint-admission lint-python ## Run every linter

lint-admission:
	@if [ -x services/admission/gradlew ]; then \
		cd services/admission && ./gradlew check -x test; \
	else \
		echo "SKIP: services/admission does not exist yet"; \
	fi

lint-python:
	@dirs=""; \
	for d in services/rating services/poller db/mongo; do \
		if [ -d "$$d" ]; then dirs="$$dirs $$d"; fi; \
	done; \
	if [ -z "$$dirs" ]; then echo "SKIP: no Python trees exist yet"; exit 0; fi; \
	python -m ruff check $$dirs
