.PHONY: test test-integration migrate up down

test:
	go test ./...

test-integration: up migrate
	DATABASE_URL="postgres://inteldigest:inteldigest@localhost:5432/inteldigest?sslmode=disable" \
		go test -tags integration ./internal/db/...

migrate:
	@for f in $$(ls migrations/*.up.sql 2>/dev/null | sort); do \
		echo "Applying $$f ..."; \
		psql "$(DATABASE_URL)" -f "$$f"; \
	done

up:
	docker compose up -d postgres

down:
	docker compose down
