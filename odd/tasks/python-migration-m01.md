# Migración Python — B0: baseline, tooling y configuración
Estado: M03 verificada y cerrada; detenerse antes de M04; rama hija `migration/python-foundation-config`; base M03 `df0b783199ed866b83120911686a78701da4877f`.
Entrega: un commit por M; estrategia `auto-chain`, `feature-branch-chain`; checks Python/Go sin servicios; M03 concentra módulo, regresiones y docs en una única unidad de compatibilidad.

## Specs

- S1: «lee plan migration y de aqui iniciamos la migración desde la primera tarea de la migración, te doy autorización para iniciar solo la primera tarea como indica el plan de migración.» Referencia autorizada: [PLAN-MIGRATION.md](../../PLAN-MIGRATION.md), sección 7, M01: «Inventario de contratos C1–C10 y escenarios Go; base fijada; diferencias aceptadas inicialmente vacías».
- S2: «Cuando termines no implementes la siguiente y me dices que hiciste, no info detallada de cada línea de código sino lo que implementaste por encima, decisiones, arquitectura, etc. de manera acotada no muy extensa.»

- S3: «Ahora procede con M02 y M03, por cada M hecho haces un commit, cuando termines uno, me haces un resumen lo que hiciste y esperas a mi confirmación para que termines con M03». Referencia M02 en [PLAN-MIGRATION.md](../../PLAN-MIGRATION.md): «Paquete Python instalable, dependencias justificadas/bloqueadas, runner de tests y guía sin dotenv».
- S4: Referencia M03: «Cargadores separados API/worker, defaults, límites y duraciones compatibles; tests sin conexiones externas». La confirmación requerida por S3 fue otorgada en S5.
- S5: «ya lo revisé, continua con el M03».
- S6: Selección humana de revisión: «Cadena desde rama de migración — recomendada». No modifica la regla S3 de un commit por M ni autoriza publicación.

## Tasks

- M01 [S1–S2] — done; ruta: exploración delegada C1–C10 + redacción delegada por backstop de contexto; commit de entrega: `9b059625344756ff185beee656c6d66a806befe1`; evidencia: validación estructural y tests/build/vet exit 0; solo baseline y ledger, sin cambios de comportamiento.
- M02 [S3] — done; ruta: worker por backstop de contexto + verificación independiente; Risk: item 5 (dependencias/entorno); único commit `df0b783199ed866b83120911686a78701da4877f`, `build(python): establish reproducible backend tooling`.
- M03 [S3–S6] — done; ruta: worker por backstop de contexto + verify independiente y recheck acotado PASS; Risk: item 3 (contrato público de entorno); único commit identificado por `feat(config): preserve backend environment contracts` desde base `df0b783199ed866b83120911686a78701da4877f`; SHA real en espejo Engram tras observar commit, sin commit extra de bookkeeping.

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
- Commit de entrega observado: `9b059625344756ff185beee656c6d66a806befe1`, `docs(migration): capture backend compatibility baseline`, 2 archivos, 225 inserciones. `git diff --cached --check` → exit 0 antes del commit.
- ASSESS del rango committed-only desde la base también devolvió `unassessable` por el plan humano sin versionar; evaluación nativa no disponible, sin aprobación ni review iniciada. No se reintenta ni se altera el archivo humano para resolverlo; no falta un verificador independiente aplicable a este cambio documental.
- Este cierre agrega únicamente evidencia del SHA y estado final en el ledger, en un commit local de registro. El SHA de entrega anterior identifica el inventario; no se intenta una autorreferencia de hash.
- Al cerrar M01, próxima tarea M02 no iniciada ni autorizada. Ese límite histórico queda actualizado por L4/S3.

### L4 — Autorización M02 y pausa antes de M03

Ahora procede con M02 y M03, por cada M hecho haces un commit, cuando termines uno, me haces un resumen lo que hiciste y esperas a mi confirmación para que termines con M03

- Estado reconciliado: misma rama `migration/python-foundation`, HEAD `298a5d60b6ee82d1426e1415aab847e036b4b4c9`; solo `PLAN-MIGRATION.md` humano sin versionar, se preserva intacto y no se incluye en el commit.
- Alcance técnico M02: usar `backend/` en este repositorio siguiendo la recomendación del plan; paquete src-layout instalable y mínimo, smoke tests y guía; sin handlers, loaders de configuración, servicios ni lógica de negocio.
- Entorno disponible: `/usr/bin/python3` 3.14.8 y `/usr/bin/uv` 0.13.1. Validar compatibilidad de paquetes con documentación oficial y metadata actual antes de fijar Python y dependencias; no instalar herramientas globales ni cambiar Go.
- Una sola herramienta de entorno/lock: preferir uv ya disponible; entorno/cache locales del proyecto, sin cargar dotenv. Instalar dependencias locales necesarias para M02, no arrancar servicios ni acceder secretos.
- Rutas de escritura worker: `backend/pyproject.toml`, `backend/uv.lock`, `backend/.python-version`, `backend/README.md`, `backend/src/inteldigest/__init__.py`, `backend/tests/test_package.py`, `.gitignore`, `README.md` (solo enlace/nota de tooling experimental). Padre mantiene este ledger.
- Risk: item 5 (nuevo empaquetado, entorno Python y dependencias). Verificador independiente requerido antes del commit; no equivale a activar review nativa ni contestar consentimiento.
- Checks previstos: RED de import del paquete ausente; GREEN de smoke/import desde paquete instalado; lock verificado y sync frozen sin dotenv; wheel/sdist y smoke desde wheel si tooling lo permite; suite Go offline para conservar referencia. Sin pruebas de integración.
- No agregar lint/typecheck/dependencias extra sin necesidad. Las bibliotecas del stack propuesto requieren justificación y versión compatible; ni URLs de ejemplo históricas ni la versión más reciente por sí solas prueban compatibilidad.
- Reversión: únicamente nuevo tooling en backend y notas/ignore asociados. SQL, Go, Makefile y contratos públicos existentes permanecen intactos.
- El hash real del único commit M02 se guardará en el espejo Engram después de observarlo; el ledger incluido en ese mismo commit lo identifica por mensaje único y base, sin crear otro commit de bookkeeping.

### L5 — M02 implementada, verificación en curso

- Worker `mvvnfu5a-3-043m`: paquete src-layout mínimo e inerte; Python 3.14.8, uv 0.13.1, lock generado, dependencias del stack justificadas, guía, smoke test y artefactos wheel/sdist. Sin loaders M03. RED: `python3 -c 'import inteldigest'` antes de crear paquete → ModuleNotFoundError, stdout vacío, exit 1.
- Worker: lock/sync/test exit 0, pytest 1 passed; primer build aislado falló exit 2 por incluir virtualenv en sdist; excluyó artefactos locales y verificó build sin aislamiento + import de wheel instalada exit 0. Ese fallo inicial no se oculta.
- Padre: `uv run --no-env-file --frozen --offline python -m pytest` con cache local/descargas de Python deshabilitadas → 1 passed, exit 0. `GOTOOLCHAIN=local GOPROXY=off GOSUMDB=off go test -count=1 ./...` → ocho paquetes OK, `cmd/api` sin tests, exit 0.
- ASSESS: nativeRisk unassessable por archivos untracked; escalado item 5 aplicado; plan requiere self-verification y verificador independiente. No hay aprobación nativa ni reviewDue provisto.
- Verificador independiente `mvvnq3qn-4-ipk1` en curso, pruebas offline en estado aislado. Se le señaló comprobar también la resolución de build-system.requires durante el primer sync, no solo el build posterior.
- Documentación oficial consultada por padre vía Context7: uv configuration-files.md (UV_NO_ENV_FILE=1), projects/sync.md (lock --check), projects/build.md (dependencias de build no se bloquean automáticamente por aislamiento); FastAPI tutorial y how-to/migrate-from-pydantic-v1-to-pydantic-v2 (instalación plain sin extras, Python3.14 con Pydantic2). URLs fuente: https://github.com/astral-sh/uv/blob/main/docs/concepts/configuration-files.md ; https://github.com/astral-sh/uv/blob/main/docs/concepts/projects/sync.md ; https://github.com/astral-sh/uv/blob/main/docs/concepts/projects/build.md ; https://fastapi.tiangolo.com/tutorial/ ; https://fastapi.tiangolo.com/how-to/migrate-from-pydantic-v1-to-pydantic-v2/ .
- En esta etapa faltaba veredicto independiente; su cierre se registra en L6. M03 permanece bloqueada por confirmación humana.

### L6 — Cierre M02 y pausa autorizada antes de M03

- Verificador `mvvnq3qn-4-ipk1`: **PASS S3**, sin blockers. En scratch aislado, Python 3.14.8 y uv 0.13.1; `uv lock --check --offline`, `uv sync --frozen --offline`, `uv run --no-env-file --frozen --offline python -m pytest` (1 passed), `uv build --offline --no-build-isolation`, inspección tar/zip e instalación/import de wheel desde site-packages: todos exit 0. Cache reutilizada dentro del proyecto; sin dotenv ni conexiones a servicios.
- Archivo fuente y wheel excluyen .venv, .uv-cache, dist, .pytest_cache y __pycache__. Prueba wheel en ambiente independiente verifica import real y versión 0.1.0, sin resolver desde src.
- Advisory sin bloqueo: smoke test automatizado verifica metadata/dependencias; import del paquete propio lo cubre la prueba manual de wheel instalada. No se abre corrección por advisory.
- Advisory sin bloqueo: primer sync aislado puede resolver build-system.requires dinámicamente fuera de uv.lock; build documentado sin aislamiento usa Hatchling de dev bloqueado. Se explicitó en backend/README.md. Reconstrucción offline con cache disponible, no cache-empty ni garantía byte a byte.
- Selección runtime observada en lock: aiohttp 3.14.5, asyncpg 0.32.0, FastAPI 0.162.0, lxml 6.1.3, Pydantic 2.14.2, readability-lxml 0.9, redis 6.4.0 y Uvicorn 0.54.0. Tools: Hatchling 1.33.0, HTTPX 0.28.1, pytest 8.4.2 y pytest-asyncio 1.4.1. Versiones transitivas y hashes en uv.lock generado.
- RDD: evaluación nativa no disponible por declaración untracked; sin aprobación ni review iniciada. Se cumplió la verificación independiente del plan de ASSESS por item 5. No se modifica el plan humano ni se repite la evaluación fallida para forzar clasificación.
- Go, SQL, Makefile, PLAN y AGENTS permanecen intactos frente a base M02; README solo agrega enlace. No se implementó configuración, API, worker o scraping Python. No se ejecutó integración, race, servicios ni migraciones; no son checks de M02.
- Único commit local M02: código/tooling, lock, tests, docs y ledger juntos, mensaje `build(python): establish reproducible backend tooling`. Se resuelve su identidad con `git log --format=%H --grep='^build(python): establish reproducible backend tooling$' -1`; memoria recibe SHA observado después, sin autorreferencia imposible ni segundo commit.
- Detención expresa S3 al cerrar M02: «cuando termines uno, me haces un resumen lo que hiciste y esperas a mi confirmación para que termines con M03». Esa confirmación posterior se registra en L7. Sin push/PR/merge.

### L7 — Confirmación M03 y estrategia de revisión

ya lo revisé, continua con el M03

- Estado reconciliado: M02 terminado en `df0b783199ed866b83120911686a78701da4877f`; solo PLAN-MIGRATION.md humano untracked; no editar/versionar ese archivo ni acceder dotenv.
- Selección humana tras consulta de presupuesto: «Cadena desde rama de migración — recomendada». Se conserva como decisión estable `delivery_strategy=auto-chain`, `chain_strategy=feature-branch-chain` para no preguntar otra vez. La cadena sigue siendo local; no autoriza push, PR ni merge.
- Slice 1: `migration/python-foundation`, M01/M02 hasta df0b783 (389 líneas manuales netas frente a 4873431, excluyendo 790 líneas generadas de uv.lock). Slice 2: rama hija `migration/python-foundation-config` desde df0b783, exclusivamente M03; mantiene la dependencia B0 del plan, sin producto adicional. Próximas ramas no se crean ahora.
- Rutas permitidas al worker: `backend/src/inteldigest/config.py`, `backend/tests/test_config.py`, `backend/README.md`, `README.md` (solo estado/enlace de configuración). Sin cambios a Go, SQL, lock, dependencias, Makefile ni nuevos entrypoints.
- M03 porta nombres y semántica del entorno desde internal/config/config.go y escenarios config_test.go; loaders API/worker separados, valores por defecto/límites, diferencias unset/empty, nombre worker generado, duraciones Go y control de overflow; errores seguros, sin secretos ni I/O a servicios.
- Scope C10 y límites de C8; no cambia política SSRF, no implementa red/DB/worker/API Python y no avanza M04. No Pydantic coerciones automáticas ni conversiones que pierdan precisión de nanosegundos inadvertidamente.
- Checks: RED de reglas por interfaz pública antes del source; GREEN focalizado y suite Python; regresión de smoke M02; compatibilidad contra referencia Go y/o ejemplos observados con estado aislado. Preservar código/salidas/exit de comandos, sin servicios ni integración. El parser nuevo no modifica helpers Go compartidos, por lo que no aplica diferencial de regresión Go por cambio de código compartido.
- Risk: item 3 (contrato público de configuración cuyo port debe evitar cambios no solicitados). Verificación independiente requerida después de self-check y ASSESS; native review solo según switch y ruta provider, sin inferir aprobación si assessment no disponible.
- El volumen previsto se concentra en una sola conducta de compatibilidad; se conserva el único commit M03 pedido por S3, sin quitar tests ni fragmentar artificialmente por presupuesto. Reversión: solo módulo/config tests y documentación asociada de M03.
- SHA real de único commit se registra en espejo Engram después de observarlo; ledger en el commit lo identifica por mensaje y base, evitando commit adicional de bookkeeping.

### L8 — M03 implementada; paridad aún no satisfecha

- Worker `mvvoglle-5-vwf4`: módulo config.py pasivo con dataclasses inmutables, load_api/load_worker y duraciones enteras ns; tests/config y docs. RED colección sin módulo → ModuleNotFoundError, exit 2; GREEN focalizado 61 y suite completa 62 tests; Go internal/config y git diff --check exit 0. Sin stage/commit ni cambios Go/dependencias.
- Lectura del padre detectó dos posibles discrepancias de duración/URL. Probe público reproducido: `REDIS_DIAL_TIMEOUT=1s0` aceptado incorrectamente; `REDIS_URL=redis://cache/a` devuelve error genérico de host en lugar del específico `database path must be a non-negative integer`. Script con aserciones sobre salida pública → exit 1 (RED observado, no check pasado).
- ASSESS con escalate item 3 no devolvió envelope por fallo del tool; no se infiere clasificación ni aprobación y no se reintenta. La verificación independiente se requiere igualmente por Risk item 3 ya declarado.
- Verificador `mvvoqe8c-6-3cag` terminó FAIL: matriz pública Go/Python 167 casos, 144 iguales y 23 discrepancias en 7 clases de blockers; suite Python existente 62 verdes no los detectaba. Go config tests scratch exit 0. Sin servicios/dotenv ni cambios al checkout.
- Blockers corroborados: cola cero sin unidad en duraciones (1s0/2h0/500ms0/1m0); motivo exacto de database path ocultado; DB int64 overflow aceptado; schemes Redis mayúsculas rechazados; puertos no numéricos aceptados; bracket host inválido aceptado; espacios de query rechazados indebidamente. No se aceptan diferencias contractuales.
- La prueba de os.environ omitido también falla con REDIS_DIAL_TIMEOUT=invalid heredado (reproducción exit 1); aislar el entorno de ese test durante la corrección para mantener el runner determinista. Advisory privado MinInt64 negativo no altera loaders públicos (rechazan negativos), no agrega un requisito nuevo.
- Se inició única corrección conjunta del worker original, con tests públicos RED específicos y mensajes exactos. Su evidencia está en L9; solo reverify de los blockers, sin nueva exploración ni sweep. M03 continúa in_progress, ningún commit aún.

### L9 — Corrección conjunta y GREEN del padre

- Worker de corrección `mvvp2p72-7-k8un`: solo config.py y test_config.py. RED antes de source: 22 failed, 61 passed, exit 1. GREEN: 85 config y 86 suite, exit 0; test con REDIS_DIAL_TIMEOUT=invalid heredado → 1 passed; Go internal/config y diff check exit 0.
- Cambios: rechazar cero final sin unidad, tipo de error DB específico sin enmascararlo, límite Atoi int64 DB, scheme case-insensitive conservando URL raw, validación sintáctica de puerto/brackets sin imponer rango de conexión, query con espacios permitidos; aislar opcionales del test os.environ.
- Padre, estado corregido: `uv run --no-env-file --frozen --offline python -m pytest tests/test_config.py -q` → 85 passed; mismo runner suite completa → 86 passed; replay de los dos probes RED → stdout `PASS: both parent RED probes now GREEN with exact public errors.`, exit 0. Flags UV_CACHE_DIR local, UV_PYTHON_DOWNLOADS=never, UV_NO_ENV_FILE=1. Sin servicios ni dotenv.
- Padre: `GOTOOLCHAIN=local GOPROXY=off GOSUMDB=off go test -count=1 ./...` → ocho paquetes OK y cmd/api sin tests, exit 0. Go source/SQL/Makefile/dependencias intactos mediante git diff exit 0 frente a df0b783.
- ASSESS del nuevo candidato corregido devolvió envelope: nativeRisk unassessable por declaración untracked; escalate item 3 aplicado, self-verification e independentVerifier requeridos. Sin candidato/reviewDue provisto ni aprobación; no ejecutar START inferido ni repetir evaluación fallida. Verificación independiente inicial + recheck cubren ese requisito, no native review.
- Recheck independiente `mvvp98he-8-48i2`, limitado a 23 casos discrepantes/7 clases y sentinels contiguos, terminó PASS; evidencia final en L10.

### L10 — Cierre verificado M03

- Recheck independiente: 23/23 blockers anteriores corregidos y 6/6 sentinels compatibles, exit 0; sin blockers previos restantes. Verificó solo las clases reportadas, no una promesa de equivalencia exhaustiva de cualquier entrada imaginable.
- Pruebas de corrección: 28 passed, 57 deselected; entorno heredado inválido: 1 passed, 84 deselected; suite aislada: 86 passed. Todas exit 0. Scratch explícito sin dotenv, offline; `PYTHONPATH=src` apuntó a copia aislada para compatibilidad de código M03, no se presenta como prueba de wheel instalada (M02 tiene esa evidencia).
- API pública Python: `load_api(env=None)` y `load_worker(env=None)`, valores inmutables; mappings explícitos para pruebas o os.environ al omitir; duraciones enteras `_ns`, defaults/límites, consumidor único, y validación sin DNS ni conexiones/logs. Sin entrypoints ni implementación operativa Python.
- Se preservaron los errores exactos de las discrepancias detectadas sin credenciales; rechazos comprobados sin mutar mapping ni stdout/stderr donde corresponde. Suite M02 sigue verde.
- No se ocultan checks fallidos iniciales: RED de módulo ausente exit 2, comparación inicial FAIL 23/167, probes del padre exit 1 y regressions previas a corrección 22 failed. Todos los blockers corroborados cerraron después con GREEN; diferencia aceptada de contrato: ninguna.
- Native ASSESS final: no disponible por archivos untracked, sin aprobación/receipt. Risk item 3 verificado independientemente; no se infiere START ni se repite un fallo para forzar clasificación. Integración/race/servicios/migraciones omitidos por alcance: loaders pasivos y sin cambio Go. Go suite actual exit 0.
- Commit único de M03, con comportamiento/tests/docs/ledger juntos: `feat(config): preserve backend environment contracts`; referencia recuperable con `git log --format=%H --grep='^feat(config): preserve backend environment contracts$' -1`, SHA real guardado en espejo después del commit. El volumen responde a una sola área de compatibilidad con regresiones explícitas; se mantiene S3 (un commit por M) y rama hija separada para revisión según S6.
- Límite final: M04 no autorizada ni iniciada. PLAN-MIGRATION.md humano permanece intacto sin versionar; Go, SQL, dependencias y Makefile intactos. Sin push/PR/merge; entrega de resumen acotado y detención al terminar M03.
