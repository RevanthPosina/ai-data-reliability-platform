# Usage: make up PROFILE=core | make demo | make down | make ci
PROFILE ?= core
COMPOSE := docker compose --env-file .run/compose.env
PROFILE_FLAGS := $(foreach p,$(PROFILE),--profile $(p))
INFRA := postgres redpanda connect seaweedfs lakekeeper-migrate lakekeeper qdrant
REDPANDA_IMAGE_REF := $(shell sed -n 's/^REDPANDA_IMAGE=//p' versions.env)
TRINO_IMAGE_REF := $(shell sed -n 's/^TRINO_IMAGE=//p' versions.env)
VENV := .venv
PY := $(VENV)/bin/python

.PHONY: up down demo build bootstrap ci lint typecheck test versions dbt-fixture dbt-parse gold-build gold-backfill compose-config clean-volumes logs gen gen-determinism gen-max-rate test-spark g3-check g6-check g8-check g10a-check g11a-results g2-check g4-check contracts-export contracts-check contracts-ci

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
	$(VENV)/bin/ruff check . && $(VENV)/bin/ruff format --check . && $(MAKE) dbt-parse

typecheck: $(VENV)/.installed
	$(VENV)/bin/mypy

test: $(VENV)/.installed
	$(VENV)/bin/pytest -q

versions:
	python3 scripts/check_versions.py

compose-config: .run/compose.env
	$(COMPOSE) --profile core --profile apps --profile tools --profile transform config -q

# Parses dbt_gold without a connection.
dbt-parse: $(VENV)/.installed
	$(VENV)/bin/dbt parse --project-dir dbt_gold --profiles-dir dbt_gold

# Throwaway Trino on :28080 with the CI catalogs (memory + file-metastore Iceberg "fx"), then the dbt_fixture
# adapter smoke test and the G4 FX scenarios. Same steps as the CI job; the container is always removed.
dbt-fixture: $(VENV)/.installed
	-docker rm -f adrp-ci-trino >/dev/null 2>&1
	docker run -d --name adrp-ci-trino --memory 2g -p 28080:8080 \
	  -v $(CURDIR)/infra/trino/jvm.config:/etc/trino/jvm.config:ro \
	  -v $(CURDIR)/infra/trino/config.properties:/etc/trino/config.properties:ro \
	  -v $(CURDIR)/infra/trino/ci/catalog/memory.properties:/etc/trino/catalog/memory.properties:ro \
	  -v $(CURDIR)/infra/trino/ci/catalog/fx.properties:/etc/trino/catalog/fx.properties:ro \
	  $(TRINO_IMAGE_REF) >/dev/null
	@for i in $$(seq 1 120); do curl -fs localhost:28080/v1/info | grep -q '"starting":false' && break; sleep 1; done
	(cd dbt_fixture && TRINO_PORT=28080 ../$(VENV)/bin/dbt build --profiles-dir . --target ci); rc=$$?; \
	  if [ $$rc -eq 0 ]; then TRINO_PORT=28080 $(PY) scripts/g4_fixture.py; rc=$$?; fi; \
	  docker rm -f adrp-ci-trino >/dev/null; exit $$rc

ci: lint typecheck test versions compose-config dbt-fixture contracts-ci

# ---- G4 gold (dbt_gold on Trino; needs the transform profile running on :8080) ----
gold-build: $(VENV)/.installed
	$(VENV)/bin/dbt build --project-dir dbt_gold --profiles-dir dbt_gold --target dev --exclude test_type:unit

# One-day backfill: make gold-backfill DAY=2026-01-01 (ADR-0027). Literal date only.
gold-backfill: $(VENV)/.installed
	@echo "$(DAY)" | grep -Eq '^[0-9]{4}-[0-9]{2}-[0-9]{2}$$' || { echo "usage: make gold-backfill DAY=YYYY-MM-DD" >&2; exit 2; }
	$(VENV)/bin/dbt build --project-dir dbt_gold --profiles-dir dbt_gold --target dev --exclude test_type:unit --vars '{backfill_day: "$(DAY)"}'


# ---- G1 generator ----
SEED ?= 42
gen: .run/compose.env
	python3 scripts/generate.py run --seed $(SEED) --duration $(or $(DURATION),30) --rate $(or $(RATE),0)

gen-determinism: .run/compose.env
	python3 scripts/generate.py determinism --seed $(SEED) --txns $(or $(TXNS),5000)

gen-max-rate: .run/compose.env
	python3 scripts/generate.py max-rate --seed $(SEED) --reps $(or $(REPS),5) --duration $(or $(DURATION),30) --warmup $(or $(WARMUP),5)

# ---- G2 CDC ingestion ----
# DESTRUCTIVE for this project's Docker volumes: starts from empty volumes so the snapshot is real.
g2-check: .run/compose.env $(VENV)/.installed
	$(MAKE) clean-volumes
	$(COMPOSE) --profile core up -d --wait $(INFRA)
	python3 scripts/bootstrap.py --skip-connector
	$(PY) scripts/g2_check.py
	$(COMPOSE) --profile core --profile apps up -d

contracts-export: $(VENV)/.installed
	$(PY) scripts/contracts.py export $(if $(OVERWRITE),--overwrite,)

contracts-check: $(VENV)/.installed
	$(PY) scripts/contracts.py check

# Same steps as the CI job: a throwaway registry that has only ever seen the committed baselines.
contracts-ci: $(VENV)/.installed
	-docker rm -f adrp-contracts-registry >/dev/null 2>&1
	docker run -d --name adrp-contracts-registry -p 28081:8081 $(REDPANDA_IMAGE_REF) redpanda start --mode dev-container \
	  --smp 1 --memory 1G --overprovisioned --schema-registry-addr=0.0.0.0:8081 >/dev/null
	@for i in $$(seq 1 60); do curl -fs localhost:28081/subjects >/dev/null && break; sleep 1; done
	$(PY) scripts/contracts.py --registry http://localhost:28081 check --register-baseline; rc=$$?; \
	  docker rm -f adrp-contracts-registry >/dev/null; exit $$rc

# ---- G3 ----
# Property test of the production MERGE against the pure model, on a local Iceberg table (needs the Spark image).
test-spark: .run/compose.env
	$(COMPOSE) --profile tools run --rm spark-test tests/spark

# DESTRUCTIVE for this project's volumes and event logs. Fault hooks (SIGKILL windows) are enabled only for this run.
g3-check: .run/compose.env $(VENV)/.installed
	$(MAKE) clean-volumes
	rm -rf runs/data/eventlog && mkdir -p runs/data/eventlog
	$(COMPOSE) --profile core up -d --wait $(INFRA)
	python3 scripts/bootstrap.py
	ADRP_FAULT_HOOKS=1 $(COMPOSE) --profile core --profile apps up -d
	$(PY) scripts/g3_check.py
	$(COMPOSE) --profile core --profile apps up -d

# DESTRUCTIVE for this project's volumes and event logs.
g6-check: .run/compose.env $(VENV)/.installed
	$(MAKE) clean-volumes
	rm -rf runs/data/eventlog && mkdir -p runs/data/eventlog
	$(COMPOSE) --profile core up -d --wait $(INFRA)
	python3 scripts/bootstrap.py
	$(COMPOSE) --profile core --profile apps up -d
	$(PY) scripts/g6_check.py

# DESTRUCTIVE for this project's volumes and event logs. Compacting cdc topics + fault hooks are enabled only for this run.
g8-check: .run/compose.env $(VENV)/.installed
	$(MAKE) clean-volumes
	rm -rf runs/data/eventlog && mkdir -p runs/data/eventlog
	$(COMPOSE) --profile core up -d --wait $(INFRA)
	python3 scripts/bootstrap.py --forget-ready-topics
	ADRP_FAULT_HOOKS=1 $(COMPOSE) --profile core --profile apps up -d
	$(PY) scripts/g8_check.py
	$(COMPOSE) --profile core --profile apps up -d

# DESTRUCTIVE for this project's volumes and event logs. TOPICS=compact uses the g8 topic config.
g10a-check: .run/compose.env $(VENV)/.installed
	$(COMPOSE) --profile core --profile apps build
	$(MAKE) clean-volumes
	rm -rf runs/data/eventlog && mkdir -p runs/data/eventlog
	$(COMPOSE) --profile core up -d --wait $(INFRA)
	python3 scripts/bootstrap.py --skip-connector $(if $(filter compact,$(TOPICS)),--forget-ready-topics,)
	ADRP_FAULT_HOOKS=1 $(COMPOSE) --profile core --profile apps up -d
	$(PY) scripts/g10a_check.py --topics $(or $(TOPICS),default)
	$(COMPOSE) --profile core --profile apps up -d

# ---- G11a (reduced): R2 and R4 from committed manifests; no Docker, nothing is run or measured ----
g11a-results: $(VENV)/.installed
	$(PY) notebooks/g11a_results.py --inputs docs/results/g11a-inputs.json --out docs/results --check-readme

# ---- G4 gold ----
# DESTRUCTIVE for this project's volumes and event logs. Core infra, spark-streams and trino; no embedder, dagster or exporter.
g4-check: .run/compose.env $(VENV)/.installed
	$(MAKE) clean-volumes
	rm -rf runs/data/eventlog && mkdir -p runs/data/eventlog
	$(COMPOSE) --profile core up -d --wait $(INFRA)
	python3 scripts/bootstrap.py
	$(COMPOSE) --profile core --profile apps --profile transform up -d spark-streams trino
	$(PY) scripts/g4_check.py
	$(COMPOSE) --profile transform stop trino
	$(COMPOSE) --profile core --profile apps up -d
