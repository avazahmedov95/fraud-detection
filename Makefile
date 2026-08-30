# Convenience commands for the local stack and data pipeline.
# Usage: make <target>

COMPOSE = docker compose
GEN_DIR = data-generator

.PHONY: help up down clean ps logs topics generate produce produce-stream load-graph serve-prep submit-job resume-job sink-logs latency query-scored

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

produce-stream: ## replay paced to original timing (200x), simulating a live stream
	cd $(GEN_DIR) && python kafka_producer.py --file out/transactions.csv --realtime --speed 200 --bootstrap localhost:29092 --topic transactions.raw

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
