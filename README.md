# IntelDigest

Motor de ingesta y enriquecimiento de artículos técnicos. Recibe la URL de un artículo, extrae su contenido y usa IA para devolver un JSON estructurado con categorías, nivel de dificultad, puntos clave y tecnologías mencionadas.

## Arquitectura

```
Cliente ──POST /api/v1/digests──▸ API ──encola──▸ Redis ──▸ Worker ──▸ Scraper ──▸ OpenRouter (IA)
                                  │                                       │
                                  │               PostgreSQL ◂────────────┘
                                  │                   │
          GET /api/v1/digests/{id}│◂──────────────────┘
```

| Componente | Tecnología | Rol |
|---|---|---|
| **API** | Go, `net/http` | Recibe URLs, crea jobs, expone resultados |
| **Worker** | Go | Consume la cola, scrapea, llama a IA, guarda resultados |
| **PostgreSQL** | 17-alpine | Persiste jobs y resultados (JSONB) |
| **Redis** | 7-alpine | Cola de trabajos entre API y Worker |
| **OpenRouter** | API HTTP | Genera el digest estructurado vía GPT |

## Estado actual

| Componente | Estado |
|---|---|
| PostgreSQL + migraciones | ✅ Funcional |
| API (`POST`, `GET`, `/healthz`) | ✅ Funcional |
| Validación de URL (anti-SSRF) | ✅ Funcional |
| Cola Redis | ⬚ Pendiente |
| Worker + pool de goroutines | ⬚ Pendiente |
| Scraper | ⬚ Pendiente |
| Cliente OpenRouter | ⬚ Pendiente |
| Frontend React | ⬚ Pendiente |

Los jobs se crean en estado `pending` pero no se procesan todavía: no hay worker ni cola.

## Requisitos

- Go 1.22+
- Docker y Docker Compose
- `psql` (para migraciones manuales y tests de integración)
- `curl` y `jq` (para verificar los endpoints)

## Levantar el proyecto

```bash
# 1. Levantar PostgreSQL (y Redis, declarado para después)
docker compose up -d

# 2. Aplicar migraciones
psql "postgres://inteldigest:inteldigest@localhost:5432/inteldigest?sslmode=disable" \
  -f migrations/001_create_jobs.up.sql

# 3. Arrancar la API
DATABASE_URL="postgres://inteldigest:inteldigest@localhost:5432/inteldigest?sslmode=disable" \
  go run ./cmd/api
```

El servidor escucha en `:8080` por defecto. Configurable con `API_PORT`.

## Probar con curl

```bash
# Health check
curl -s http://localhost:8080/healthz | jq .
# {"status":"ok"}

# Crear un digest
curl -s -X POST http://localhost:8080/api/v1/digests \
  -H 'Content-Type: application/json' \
  -d '{"url":"https://go.dev/blog/go1.22"}' | jq .
# {"job_id":"<uuid>","message":"processing"}

# Consultar el estado
curl -s http://localhost:8080/api/v1/digests/<uuid> | jq .
# {"id":"<uuid>","url":"...","status":"pending","created_at":"...","updated_at":"..."}

# URL inválida → 400
curl -s -X POST http://localhost:8080/api/v1/digests \
  -H 'Content-Type: application/json' \
  -d '{"url":"http://127.0.0.1/x"}' | jq .
# {"error":"url: private/reserved address \"127.0.0.1\""}

# Job inexistente → 404
curl -s http://localhost:8080/api/v1/digests/00000000-0000-0000-0000-000000000000 | jq .
# {"error":"job not found"}
```

## Tests

```bash
# Unitarios (no necesitan infraestructura)
go test ./...

# Integración (necesitan PostgreSQL corriendo)
DATABASE_URL="postgres://inteldigest:inteldigest@localhost:5432/inteldigest?sslmode=disable" \
  go test -tags integration ./internal/db/...
```

Los tests de integración crean y destruyen una base `inteldigest_test` automáticamente. Cada test corre en una transacción que se revierte, sin residuos.

| Suite | Tests | Infra |
|---|---|---|
| `internal/models` | 3 | Ninguna |
| `internal/config` | 3 | Ninguna |
| `internal/api` | 7 | Ninguna (fake repo) |
| `internal/db` | 3 | PostgreSQL |
| **Total** | **16** | |

## Estructura

```
cmd/
  api/            # Servidor HTTP (wiring)
  worker/         # Consumidor de cola (pendiente)
internal/
  api/            # Handlers, router, interfaz del repositorio
  config/         # Carga de env vars
  db/             # Repositorio PostgreSQL, interfaz DBTX
  models/         # Job, estados, validación de URL
  queue/          # Abstracción Redis (pendiente)
  scraper/        # Extracción de contenido (pendiente)
migrations/       # SQL versionado
```
