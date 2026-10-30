# Migración Python — M01: baseline de compatibilidad
Objetivo: cerrar solo M01; rama `migration/python-foundation`; base `48734314bc570d5ecc1d837cbc94aacc5d5b47d1`.
Entrega: `ask-on-risk`, una unidad documental (~250 líneas nuevas); checks estructurales y suite Go offline, sin integración.

## Specs

- S1: «lee plan migration y de aqui iniciamos la migración desde la primera tarea de la migración, te doy autorización para iniciar solo la primera tarea como indica el plan de migración.» Referencia autorizada: [PLAN-MIGRATION.md](../../PLAN-MIGRATION.md), sección 7, M01: «Inventario de contratos C1–C10 y escenarios Go; base fijada; diferencias aceptadas inicialmente vacías».
- S2: «Cuando termines no implementes la siguiente y me dices que hiciste, no info detallada de cada línea de código sino lo que implementaste por encima, decisiones, arquitectura, etc. de manera acotada no muy extensa.»

## Tasks

- M01 [S1–S2] — in_progress (inventario y checks completos; registrando commit); ruta: exploración delegada C1–C10 + redacción delegada por backstop de contexto; commit: pendiente; alcance: documentación de baseline y este ledger, sin cambios de comportamiento.

## Log

### L1 — Solicitud original

lee plan migration y de aqui iniciamos la migración desde la primera tarea de la migración, te doy autorización para iniciar solo la primera tarea como indica el plan de migración. Cuando termines no implementes la siguiente y me dices que hiciste, no info detallada de cada línea de código sino lo que implementaste por encima, decisiones, arquitectura, etc. de manera acotada no muy extensa.

### L2 — Alcance y evidencia inicial

- Base completa: `48734314bc570d5ecc1d837cbc94aacc5d5b47d1`, `docs: plan Next.js deployment and require Mermaid flowcharts`.
- Estado inicial: rama `main`; único archivo no versionado: `PLAN-MIGRATION.md`, aportado por la persona. Se preserva sin modificar ni incluir en el commit de M01.
- Rama creada: `migration/python-foundation`; no push, PR ni merge autorizados.
- Referencia técnica: Go intacto; SQL existente en `migrations/`; PostgreSQL fuente de verdad y Redis Streams transporte; receiver actual de metadatos no durable.
- Solo M01 está activa. M02–M20 no forman parte de esta autorización. No se crea `backend/` ni se eligen versiones Python/dependencias.
- Rutas de escritura permitidas: `docs/migration/compatibility-baseline.md`, `odd/tasks/python-migration-m01.md`. README, PLAN, AGENTS y el plan aportado permanecen intactos.
- Riesgo: ninguno; documentación pasiva, sin cambios de contratos, seguridad, concurrencia, dependencias o comportamiento.
- RED/GREEN: no aplica a documentación pasiva; verificar referencias, símbolos de tests, C1–C10, base, ausencia de diferencias aceptadas y diff.
- Herramienta local: `go version go1.27.3 linux/amd64`; `go.mod` requiere `1.27.1`, sin modificación. Checks con `GOTOOLCHAIN=local GOPROXY=off GOSUMDB=off` para no descargar herramientas ni dependencias.
- RDD: `gentle-ai review mode status` → enabled por configuración global, exit 0; evaluar solo la unidad documental, sin iniciar review por checkbox.
- Checks actuales, ejecutados sin descargas con `GOTOOLCHAIN=local GOPROXY=off GOSUMDB=off`: `go test -count=1 ./...` → exit 0 (ocho paquetes con tests; `cmd/api` sin tests), `go build ./...` → exit 0 y `go vet ./...` → exit 0. Job `job-1`, duración 6 s; ninguna salida stderr de error en la notificación de cierre.
- Integración PostgreSQL/Redis, servicios, migraciones y consumo de crédito: no ejecutados ni autorizados.
- Reversión de esta unidad: retirar únicamente los dos documentos nuevos; ningún código, dato ni proceso cambia.

### L3 — Inventario y verificación observada (2026-10-30)

- Entrega: `docs/migration/compatibility-baseline.md`, C1–C10 con escenarios observables y referencias a fuentes/tests, semántica JSON y límites operativos; diferencias aceptadas 0.
- Subagentes: explore `mvvmfx63-1-4slh` para el mapa y worker `mvvmllwo-2-0pu3` para la redacción por backstop. Padre contrastó handler/modelo y corrigió dos detalles: URL sin esquema y uso de script Python documental, no ejecución de backend Python.
- Worker: validador inline, exit 0; reportó 32 enlaces y 92 símbolos únicos. Padre: script estructural inline, stdout `PASS: C1-C10 10/10; enlaces 32/32; referencias de tests 107/107; base completa; diferencias 0; Go/documentación existente intactos; sin backend Python.`, stderr vacío, exit 0. Se cuentan referencias repetidas, no 107 tests distintos.
- `git diff --exit-code <base> -- cmd internal migrations Makefile go.mod go.sum README.md PLAN.md AGENTS.md` → exit 0: implementación y documentos previos intactos.
- Native ASSESS sobre archivos aún no versionados: `nativeRisk=unassessable` por requerir declaración explícita de untracked; sin tier nativo verificable ni aprobación. Plan devuelto: self-verification sí, independentVerifier no; no hay ítem de alto riesgo porque solo se describe comportamiento existente.
- No aplica RED/GREEN ni comparación diferencial: no cambió comportamiento. Integración y race no ejecutados; M01 documental no los requiere. No se inicia review nativa por checkbox ni se interpreta una evaluación fallida como tier bajo.
- Alcance del commit local: exclusivamente baseline y ledger (~230 líneas); `PLAN-MIGRATION.md` sigue sin versionar, intacto. Sin push/PR/merge.
- Próxima tarea del plan: M02, **no iniciada ni autorizada**. Detención requerida por S2.
