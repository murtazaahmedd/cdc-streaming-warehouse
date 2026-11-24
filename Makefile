.PHONY: install demo test up connector workload sink down
install:
	pip install -r requirements.txt
demo:              ## offline: simulated Debezium stream with crashes, then reconcile against the source
	python -m cdc.simulate
test:
	python -m pytest -q
up:                ## full stack: Postgres, Redpanda, Debezium Connect, Console (http://localhost:8080)
	docker compose -f docker/docker-compose.yml up -d
connector:         ## register the Debezium Postgres source connector
	curl -s -X POST -H "Content-Type: application/json" --data @connect/postgres-source.json http://localhost:8083/connectors
workload:          ## generate inserts, updates, deletes and a schema change in Postgres
	docker compose -f docker/docker-compose.yml exec -T postgres psql -U shop -d shop < docker/workload.sql
sink:              ## consume the CDC topics into warehouse.duckdb
	pip install -q -r requirements-kafka.txt && python -m cdc.consumer
down:
	docker compose -f docker/docker-compose.yml down -v
