# The everyday targets of run.ps1, for make users: make <target>.

COMPOSE = docker compose
GEN_DIR = data-generator

# Credentials and ports: .env, copied from .env.example on each machine.
-include .env
export

.PHONY: help up down clean ps logs topics generate produce produce-stream no-active-job fresh-taskmanager export-model seed-confirmed serve-prep submit-job resume-job sink-logs verify-audit query-scored

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
	cd $(GEN_DIR) && python kafka_producer.py --file out/transactions.csv --bootstrap 127.0.0.1:29092 --topic transactions.raw

produce-stream: ## paced replay (200x)
	cd $(GEN_DIR) && python kafka_producer.py --file out/transactions.csv --realtime --speed 200 --bootstrap 127.0.0.1:29092 --topic transactions.raw

export-model: ## the trained model to ONNX for the job, in a container (infra/ml/Dockerfile)
	docker build -q -t fraud-ml-export -f infra/ml/Dockerfile .
	docker run --rm -v "$(CURDIR):/repo" fraud-ml-export

seed-confirmed: ## load the history's confirmed fraud accounts into Redis
	cd ml && python seed_confirmed.py --port $${REDIS_HOST_PORT}

serve-prep: ## copy the trained ONNX model, its cutoff and the second look's band next to the Flink job
	cp ml/models/model.onnx ml/models/thresholds.json stream-processor/
	if [ -f ml/models/second_look.json ]; then cp ml/models/second_look.json stream-processor/; fi

# Every module fraud_job.py imports, transitively - boundary_audit.py checks both
# submitters against it (--pyFiles, not the mount, is what reaches sys.path).
PYFILES = /opt/flink/usrjobs/config.py,/opt/flink/usrjobs/capabilities.py,/opt/flink/usrjobs/features.py,/opt/flink/usrjobs/geo.py,/opt/flink/usrjobs/rules.py,/opt/flink/usrjobs/receiver_store.py,/opt/flink/usrjobs/fusion.py,/opt/flink/usrjobs/payload_crypto.py

# Refuse a second job beside a running one: two KafkaSources score every event
# twice. run.ps1's Assert-NoActiveJob, in sh.
no-active-job:
	@ACTIVE=$$(curl -s --max-time 10 http://localhost:8081/jobs/overview | \
	  grep -o '"state":"[A-Z_]*"' | grep -v -E 'FAILED|CANCELED|FINISHED'); \
	if [ -n "$$ACTIVE" ]; then \
	  echo "A JOB IS ALREADY ACTIVE - not submitting a second one. Cancel it first:"; \
	  echo "  curl -X PATCH 'http://localhost:8081/jobs/<jid>?mode=cancel'"; exit 1; \
	fi

# A fresh TaskManager JVM before every submission: cancelled jobs do not return
# Metaspace. run.ps1's Restart-TaskManager, in sh.
fresh-taskmanager: no-active-job
	@OLD=$$(curl -s --max-time 10 http://localhost:8081/taskmanagers | \
	  grep -o '"id":"[^"]*"' | tr '\n' ' '); \
	$(COMPOSE) restart taskmanager >/dev/null; \
	i=0; while [ $$i -lt 40 ]; do \
	  for id in $$(curl -s --max-time 10 http://localhost:8081/taskmanagers | \
	      grep -o '"id":"[^"]*"'); do \
	    case " $$OLD " in *" $$id "*) ;; \
	      *) echo "fresh TaskManager registered"; exit 0;; esac; \
	  done; \
	  i=$$((i+1)); sleep 3; \
	done; \
	echo "no fresh TaskManager registered within 120 s"; exit 1

submit-job: serve-prep fresh-taskmanager ## fresh TaskManager, then the PyFlink job (EMPTY keyed state)
	$(COMPOSE) exec jobmanager flink run -d -py /opt/flink/usrjobs/fraud_job.py \
	  --pyFiles $(PYFILES)

resume-job: serve-prep fresh-taskmanager ## submit, restoring keyed state from the newest retained checkpoint
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

sink-logs: ## tail the sink-writer (ClickHouse persistence) logs
	$(COMPOSE) logs -f sink-writer

verify-audit: ## recompute the audit hash chain and report any tampering
	cd sink-writer && python verify_audit.py

query-scored: ## quick ClickHouse check: decision counts in transactions_scored
	$(COMPOSE) exec clickhouse clickhouse-client -u $${CLICKHOUSE_USER} \
	  --password $${CLICKHOUSE_PASSWORD} -q \
	  "SELECT decision, count() FROM fraud.transactions_scored GROUP BY decision ORDER BY decision"
