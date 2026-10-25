# Usage: make up PROFILE=core | make demo | make down | make ci
PROFILE ?= core
COMPOSE := docker compose --env-file .run/compose.env
PROFILE_FLAGS := $(foreach p,$(PROFILE),--profile $(p))
INFRA := postgres redpanda connect seaweedfs lakekeeper-migrate lakekeeper qdrant
VENV := .venv
PY := $(VENV)/bin/python

.PHONY: up down demo build bootstrap ci lint typecheck test versions dbt-fixture compose-config clean-volumes logs

.run/compose.env: versions.env $(wildcard .env) .env.example infra/seaweedfs/s3.json.tmpl
	python3 scripts/render.py

build: .run/compose.env
	$(COMPOSE) --profile core --profile apps build

# Core = infra, then one-time bootstrap, then the apps (Spark streams, embedder, Dagster).
up: .run/compose.env
ifneq (,$(filter core,$(PROFILE)))
	$(COMPOSE) --profile core up -d --wait $(INFRA)
	python3 scripts/bootstrap.py
	$(COMPOSE) --profile core --profile apps $(PROFILE_FLAGS) up -d
else
	$(COMPOSE) $(PROFILE_FLAGS) up -d
endif

bootstrap: .run/compose.env
	python3 scripts/bootstrap.py

demo: .run/compose.env
	python3 scripts/smoke.py

down: .run/compose.env
	$(COMPOSE) --profile core --profile apps --profile tools --profile transform down --remove-orphans

clean-volumes: .run/compose.env
	$(COMPOSE) --profile core --profile apps --profile tools --profile transform down -v --remove-orphans

logs: .run/compose.env
	$(COMPOSE) --profile core --profile apps logs --tail=100

# ---- CI (same commands as .github/workflows/ci.yml) ----
$(VENV)/.installed: requirements-dev.txt requirements-dev.constraints.txt
	python3 -m venv $(VENV)
	$(PY) -m pip install -q -r requirements-dev.txt -c requirements-dev.constraints.txt
	touch $@

lint: $(VENV)/.installed
	$(VENV)/bin/ruff check . && $(VENV)/bin/ruff format --check .

typecheck: $(VENV)/.installed
	$(VENV)/bin/mypy

test: $(VENV)/.installed
	$(VENV)/bin/pytest -q

versions:
	python3 scripts/check_versions.py

compose-config: .run/compose.env
	$(COMPOSE) --profile core --profile apps --profile tools --profile transform config -q

# Needs a Trino at localhost:8080 (CI uses a service container; locally: make up PROFILE=transform).
dbt-fixture: $(VENV)/.installed
	cd dbt_fixture && ../$(VENV)/bin/dbt build --profiles-dir . --target ci

ci: lint typecheck test versions compose-config dbt-fixture
