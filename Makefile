# The lag laboratory. `make help` lists the targets.
#
# Every target is a thin wrapper around `python -m lab ...` or
# `docker compose ...`; read the recipe to see exactly what runs

PY        ?= python3
COMPOSE   ?= docker compose
PLANS     := $(sort $(wildcard experiments/*.json))

.PHONY: help venv up up-delayed setup smoke watch status experiments plots report \
        demo-binlog demo-delayed down clean

help:            ## list targets
	@grep -E '^[a-z-]+:.*##' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  make %-14s %s\n", $$1, $$2}'

venv:            ## install the two Python dependencies into .venv
	$(PY) -m venv .venv && .venv/bin/pip install -q -r requirements.txt
	@echo "now: source .venv/bin/activate"

up:              ## start primary + replica (waits until both are healthy)
	$(COMPOSE) up -d --wait

up-delayed:      ## also start the delayed replica
	$(COMPOSE) --profile delayed up -d --wait

setup:           ## wire replication and install the control schema
	$(PY) -m lab setup

smoke:           ## a 1-minute check: one run single-threaded, one with 8 workers
	$(PY) -m lab run --experiment smoke --tx-per-client 300 --threads 0
	$(PY) -m lab run --experiment smoke --tx-per-client 300 --threads 8
	$(PY) -m lab plot results/smoke

watch:           ## print live lag twice a second (run in a 2nd terminal during a run)
	$(PY) -m lab watch

status:          ## replication status of every replica
	$(PY) -m lab status

experiments:     ## run every plan in experiments/ (about 1.6 h on the reference machine)
	@for p in $(PLANS); do $(PY) -m lab plan $$p || exit 1; done

plots:           ## (re)draw every chart from the stored samples
	$(PY) -m lab plot

report:          ## (re)write results/SUMMARY.md
	$(PY) -m lab report

demo-binlog:     ## annotated row events, commit ids and checksums, live
	$(PY) -m lab demo-binlog

demo-delayed:    ## undo a DROP TABLE with the delayed replica (needs `make up-delayed setup`)
	$(PY) -m lab demo-delayed

down:            ## stop the containers (data volumes kept)
	$(COMPOSE) --profile delayed down

clean:           ## stop and delete the data volumes
	$(COMPOSE) --profile delayed down -v
