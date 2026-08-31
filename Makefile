# Convenience commands for the local stack and data pipeline.
# Usage: make <target>
#
# SCOPE. This is a SUBSET, not the full command set. run.ps1 additionally
# carries every measurement: measure-plain / measure-tls / measure-crypto,
# latency-setup, pipeline, kill-worker, make-certs, status, and the TLS and
# encrypted producer arms. Each of those is a sequenced protocol - warm-up
# discarded, drain, settle, cache flush, measured arm, report - rather than a
# single command, and every seam between those steps has lost a run at least
# once. Reproducing any figure in docs/ therefore goes through run.ps1.
#
# An earlier version of both files claimed they were kept in step as
# equivalents. They were not, and this note replaces the claim rather than
# repairing it.

COMPOSE = docker compose
GEN_DIR = data-generator

.PHONY: help up down clean ps logs topics generate produce produce-stream produce-stream-docker load-graph serve-prep submit-job resume-job sink-logs latency query-scored

help: ## show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

up: ## build images and start the whole stack
	$(COMPOSE) up -d --build

down: ## stop the stack (keep data volumes)
	$(COMPOSE) down

clean: ## stop the stack and delete all data volumes
	$(COMPOSE) down -v

ps: ## list running services
	$(COMPOSE) ps

logs: ## tail logs from all services
	$(COMPOSE) logs -f

topics: ## list Kafka topics
	$(COMPOSE) exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list

generate: ## generate the synthetic dataset into data-generator/out
	cd $(GEN_DIR) && python generator.py --out ./out

produce: ## replay the dataset into Kafka (batch)
	cd $(GEN_DIR) && python kafka_producer.py --file out/transactions.csv --bootstrap localhost:29092 --topic transactions.raw

produce-stream: ## paced replay (200x) FROM THE HOST - convenience only, never for latency
	cd $(GEN_DIR) && python kafka_producer.py --file out/transactions.csv --realtime --speed 200 --bootstrap localhost:29092 --topic transactions.raw

# Use this, not produce-stream, for anything that ends in a latency figure.
# ingested_at is stamped by the producer and scored_at_job by Flink. Run from the
# host those are two clocks: containers live in a VM whose clock drifts from the
# host's and is resynced periodically. Measured offsets of +205 ms and -279 ms
# minutes apart land straight in the decision-path figure, and once produced a
# stable 640 ms tail that responded to no amount of tuning because it was never
# latency at all. Inside the network the producer, Flink and ClickHouse share one
# clock. Optional length:  make produce-stream-docker COUNT=7000
produce-stream-docker: ## paced replay from INSIDE the network - required for latency work
	docker run --rm -i --network fraud-detection_fraudnet \
	  -v "$(CURDIR)/$(GEN_DIR):/gen" -w /gen fraud-sink-writer:latest \
	  python kafka_producer.py --file out/transactions.csv --realtime --speed 200 \
	    --bootstrap kafka:9092 --topic transactions.raw $(if $(COUNT),--limit $(COUNT),)

load-graph: ## load the account population into Neo4j
	$(COMPOSE) exec -T neo4j cypher-shell -u neo4j -p $${NEO4J_PASSWORD:-fraud_neo4j} < infra/neo4j/import.cypher

serve-prep: ## copy the trained ONNX model + feature spec next to the Flink job
	cp ml/models/model.onnx ml/models/feature_names.json stream-processor/

PYFILES = /opt/flink/usrjobs/config.py,/opt/flink/usrjobs/capabilities.py,/opt/flink/usrjobs/features.py,/opt/flink/usrjobs/geo.py,/opt/flink/usrjobs/rules.py,/opt/flink/usrjobs/enrichment.py,/opt/flink/usrjobs/receiver_store.py,/opt/flink/usrjobs/fusion.py,/opt/flink/usrjobs/payload_crypto.py

submit-job: serve-prep ## submit the PyFlink CEP+ML job (EMPTY keyed state)
	$(COMPOSE) exec jobmanager flink run -d -py /opt/flink/usrjobs/fraud_job.py \
	  --pyFiles $(PYFILES)

resume-job: serve-prep ## submit, restoring keyed state from the newest retained checkpoint
	@# Without -s the job starts with empty keyed state: offsets are committed
	@# so nothing is re-read and no error appears, but every sender's velocity
	@# and structuring window starts blank and the rules depending on history
	@# cannot fire until it rebuilds. Resuming is a separate target because
	@# restoring the WRONG state silently would be worse than starting clean.
	@CHK=$$($(COMPOSE) exec -T jobmanager sh -c "ls -dt /opt/flink/checkpoints/*/chk-* 2>/dev/null | head -1" | tr -d '\r'); \
	if [ -z "$$CHK" ]; then \
	  echo "no retained checkpoint found - start fresh with: make submit-job"; exit 1; \
	fi; \
	echo "restoring keyed state from $$CHK"; \
	$(COMPOSE) exec jobmanager flink run -d -s "$$CHK" -py /opt/flink/usrjobs/fraud_job.py \
	  --pyFiles $(PYFILES)

sink-logs: ## tail the sink-writer (ClickHouse/Neo4j persistence) logs
	$(COMPOSE) logs -f sink-writer

latency: ## end-to-end latency percentiles vs the <300ms design target
	cd stream-processor && python latency_report.py

verify-audit: ## recompute the audit hash chain and report any tampering
	cd sink-writer && python verify_audit.py

query-scored: ## quick ClickHouse check: decision counts in transactions_scored
	$(COMPOSE) exec clickhouse clickhouse-client -u $${CLICKHOUSE_USER:-fraud} \
	  --password $${CLICKHOUSE_PASSWORD:-fraud_ch} -q \
	  "SELECT decision, count() FROM fraud.transactions_scored GROUP BY decision ORDER BY decision"
