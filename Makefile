.PHONY: test test-integration migrate up down

test:
	go test ./...

test-integration:
	DATABASE_URL="postgres://inteldigest:inteldigest@localhost:5432/inteldigest?sslmode=disable" \
	REDIS_URL="redis://localhost:6379" \
		go test -p 1 -tags=integration -count=1 ./internal/db ./internal/queue ./internal/worker

migrate:
	@for f in $$(ls migrations/*.up.sql 2>/dev/null | sort); do \
		echo "Applying $$f ..."; \
		psql "$(DATABASE_URL)" -f "$$f"; \
	done

up:
	docker compose up -d postgres

down:
	docker compose down
