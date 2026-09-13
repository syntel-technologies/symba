.PHONY: proto ensure-proto test lint typecheck run imports l1 l2 up down migrate openapi openapi-check frontend

PY := .venv/bin/python

proto:
	$(PY) -m grpc_tools.protoc -Iproto \
		--python_out=src --grpc_python_out=src --pyi_out=src \
		proto/symba/v1/*.proto
	# move generated symba/v1 up into the package
	@rm -rf src/symba/v1_gen && true

# Host-side codegen for bare `uv run` / pytest (Docker builds run this in Dockerfile).
ensure-proto:
	@test -f src/symba/v1/data_plane_pb2.py || $(MAKE) proto

imports:
	$(PY) tools/check_imports.py

lint:
	$(PY) -m ruff check src tests tools
	$(PY) -m ruff format --check src tests tools

typecheck:
	$(PY) -m pyright

l1:
	$(PY) -m pytest -m l1 -q

l2:
	$(PY) -m pytest -m l2 -q

test:
	$(PY) -m pytest -q

up:
	docker compose up --build

down:
	docker compose down -v

# Run Flyway migrations only (against the compose postgres). `up` already runs
# this via the flyway service; use this to re-apply after adding a V###__*.sql.
migrate:
	docker compose run --rm flyway migrate

run: ensure-proto
	$(PY) -m symba.main

# Regenerate the committed OpenAPI schema (frontend/openapi.json). Run after any
# route/DTO change, then commit; CI's openapi-check gate fails otherwise.
openapi:
	$(PY) tools/dump_openapi.py

openapi-check:
	$(PY) tools/check_openapi.py

# Generate the TypeScript client from the committed schema and build the SPA.
frontend:
	cd frontend && npm install && npm run gen:api && npm run build
