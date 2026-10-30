# Baseline de compatibilidad Go — M01

Este inventario describe el comportamiento observado en Go que sirve como referencia para M01; no cambia helpers ni define requisitos adicionales. La migración solo puede comparar y preservar estos contratos: los huecos marcados «solo código» no son garantías ya cubiertas por tests.

## Alcance y evidencia

- **Tarea:** M01, S1–S2; única tarea autorizada. Es un documento de referencia, no implementación Python ni una hoja de ruta nueva.
- **Base:** `48734314bc570d5ecc1d837cbc94aacc5d5b47d1` (rama `migration/python-foundation`).
- **Plan:** [PLAN-MIGRATION.md](../../PLAN-MIGRATION.md), sección 4 para C1–C10 y sección 7 para M01. El alcance funcional sigue en [PLAN.md](../../PLAN.md); el procedimiento de migración queda en ese plan temporal.
- **Ledger/evidencia ODD:** [odd/tasks/python-migration-m01.md](../../odd/tasks/python-migration-m01.md). El ledger pertenece al orquestador y no se edita en esta unidad.
- Cada test Go sin etiqueta `integration` aquí citado estaba presente y quedó **ejecutado** por la suite indicada al final. Los tests etiquetados `integration` están presentes pero **NO ejecutados**. «Solo código» significa observable en la implementación, pero no afirmado como escenario comprobado por un test citado.
- No se ejecutó integración, servicios, migraciones, un backend Python ni pruebas diferenciales. Se usó un script Python local solo para verificar la estructura documental. El receiver es un handoff síncrono en memoria; no es persistencia de resultado.
- Inventario acotado al comportamiento observado en las rutas citadas; los casos de test se usan como evidencia reproducible, no como promesa de cobertura total.
- **Diferencias aceptadas: 0.** No hay entradas aceptadas. Una discrepancia futura requiere decisión expresa; este baseline no la aprueba implícitamente.

## Cómo leer los resultados

- Los ejemplos JSON muestran semántica de campos y valores, no una exigencia de orden literal de claves, salvo el test del mensaje Redis que compara su serialización literal.
- Los errores HTTP usan `Content-Type: application/json`; el helper codifica JSON con salto de línea final. Aquí se expresan como objetos JSON, no como bytes canónicos.
- UUID y timestamps generados son variables. `time.Time` determina su serialización JSON; el repositorio crea timestamps UTC, pero eso no garantiza UTC para cualquier valor inyectado a un `Job`.
- En `Job`, `result` y `error` tienen `omitempty`: cuando sus punteros son `nil`, las claves se omiten, no se serializan como `null`. El campo `result` no queda rellenado por el receiver actual.
- Las etiquetas **unitario ejecutado**, **integración presente/no ejecutada** y **solo código** separan evidencia y límites; ningún bloque implica cobertura completa o exhaustiva.

## C1 — HTTP, modelos y validación inicial de URL

**Fuente:** [internal/api/handler.go](../../internal/api/handler.go), [internal/models/models.go](../../internal/models/models.go). Tests en [internal/api/handler_test.go](../../internal/api/handler_test.go) y [internal/models/models_test.go](../../internal/models/models_test.go).

| Entrada / escenario | Resultado observable | Evidencia |
|---|---|---|
| `GET /healthz` | `200`, `{"status":"ok"}`. | Unitario ejecutado: `TestHealthz`. |
| `POST /api/v1/digests` con `{"url":"https://example.com/article"}` | `202`, JSON con `job_id` y `message:"processing"`; un job pending es creado y publicado (orden de efectos C2). | Unitario ejecutado: `TestPostDigest_ValidURL`, `TestPostDigest_PublishesPersistedJob`. |
| POST con cuerpo no JSON | `400`, `{"error":"invalid JSON body"}`; no continúa a validar/crear. | Unitario ejecutado: `TestPostDigest_MalformedBody`. |
| POST URL vacía o whitespace | `400`, `{"error":"url: empty"}`. | Unitario ejecutado parcialmente: `TestPostDigest_InvalidURL` verifica status; `TestValidateURL` verifica motivo vacío. |
| POST con `ftp://example.com` | `400`, error `url: scheme must be http or https, got "ftp"`. | Unitario ejecutado: `TestPostDigest_InvalidURL`, `TestValidateURL`. |
| URL `not-a-url` | `400`; el parser no encuentra esquema; el validador rechaza con `url: scheme must be http or https, got ""`. | Unitario ejecutado para validador: `TestValidateURL`; respuesta HTTP exacta solo código. |
| POST URL `http://127.0.0.1/x`, `localhost`, RFC1918 o metadata `169.254.169.254` | `400`, error `url: private/reserved address "<host>"`. | Unitario ejecutado: `TestPostDigest_InvalidURL`, `TestValidateURL`. |
| GET de UUID existente | `200`, objeto `Job` con `id`, `url`, `status`, `created_at`, `updated_at`; nil `result`/`error` se omiten. | Unitario ejecutado: `TestGetDigest_Found`; omisión definida en modelo, no aserción JSON específica. |
| GET de UUID ausente | `404`, `{"error":"job not found"}`. | Unitario ejecutado: `TestGetDigest_NotFound` (status); cuerpo se deduce del helper/código. |
| GET con ID no UUID | `400`, `{"error":"invalid job ID"}`. | Unitario ejecutado: `TestGetDigest_BadID` (status); cuerpo, código. |
| Error no-not-found de repo al leer o crear | `500`, `{"error":"internal error"}`; se registra error interno en log. | Solo código; fakes citados no inyectan errores de repositorio. |

**Solo código / no ampliar el contrato:** `ValidateURL` no hace DNS; acepta únicamente esquema HTTP(S), hostname no vacío y deniega `localhost` e IP literal loopback/privada/link-local. No garantiza política de red para nombres DNS. `json.Decoder.Decode` no configura `DisallowUnknownFields`, no verifica explícitamente EOF tras el primer valor y no restringe `Content-Type`; coerciones/valores ausentes del JSON, body vacío/extra y métodos/rutas no registrados (incluido el fallthrough de ServeMux) no son escenarios cubiertos aquí. Varios POST idénticos no tienen clave de idempotencia y pueden crear jobs distintos; comportamiento observado en el flujo, no test citado.

## C2 — Persistencia antes de publicación

**Fuente:** [internal/api/handler.go](../../internal/api/handler.go); contexto operativo en [README.md](../../README.md#api-behavior).

| Entrada / escenario | Resultado observable | Evidencia |
|---|---|---|
| POST válido; repositorio fake conserva la fila antes de invocar publisher | `202`; exactamente un mensaje publicado con schema v1, mismo `job_id` y URL; el job ya está en repo al publicar. | Unitario ejecutado: `TestPostDigest_PublishesPersistedJob`. |
| El publisher falla tras crear | `503`, objeto con exactamente `error:"queue unavailable"` y el `job_id` creado; la fila permanece. | Unitario ejecutado: `TestPostDigest_PublishFailureReturnsCreatedJobID`. |
| El publisher falla | El handler no revierte persistencia ni promete atomicidad entre DB y Redis; respuesta conserva ID del job. | Unidad ejecutada anterior; pérdida/ambigüedad de ACK Redis y posible entrada aceptada descritas en README, no simuladas por ese test. |

## C3 — Esquema y transiciones PostgreSQL

**Fuente:** [migrations/001_create_jobs.up.sql](../../migrations/001_create_jobs.up.sql), [internal/db/repository.go](../../internal/db/repository.go), [internal/models/models.go](../../internal/models/models.go).

| Entrada / escenario | Resultado observable | Evidencia |
|---|---|---|
| Crear job con URL | Inserta UUID, URL, `pending`, timestamps `now` UTC desde Go y devuelve esos valores. | Integración presente/no ejecutada: `TestCreateJob`. |
| Buscar UUID inexistente | `ErrNotFound` (envuelve semánticamente `pgx.ErrNoRows`). | Integración presente/no ejecutada: `TestGetJobByID`, `TestGetJobByIDNotFound`. |
| Claim en una fila `pending` | Un único `UPDATE ... WHERE id=$1 AND status='pending'`; `processing`, limpia `error`, actualiza `updated_at`; `true` si una fila afectada. Claim repetido/no elegible retorna `false`. | Unitario ejecutado: `TestTryMarkProcessing`, `TestTryMarkProcessingWrapsExecError`. |
| Dos conexiones intentan reclamar simultáneamente la misma fila pendiente | Exactamente un ganador. | Integración presente/no ejecutada: `TestTryMarkProcessingConcurrentClaims`. |
| Diagnóstico de scrape | Actualiza `error` solo con estado `processing`, no cambia estado, máximo 1024 runas; filas no elegibles devuelven `false`. | Unitario ejecutado: `TestRecordScrapeError`, `TestRecordScrapeErrorDoesNotUpdateNonProcessingJob`. Persistencia y terminales: integración presente/no ejecutada `TestRepositoryProcessingAndDiagnosticPersistence`. |
| Crear/leer columnas | Tabla `jobs`: `id UUID PK`, `url TEXT`, `status TEXT` default pending, `result JSONB`, `error TEXT`, `created_at`/`updated_at TIMESTAMPTZ`; resultado/error SQL NULL se leen como punteros nil. | Migración y código; lectura real solo integración presente/no ejecutada `TestGetJobByID`. |

## C4 — Mensaje y publicación Redis Streams

**Fuente:** [internal/queue/message.go](../../internal/queue/message.go), [internal/queue/publisher.go](../../internal/queue/publisher.go).

| Entrada / escenario | Resultado observable | Evidencia |
|---|---|---|
| Mensaje completo v1 | JSON `schema_version`, `job_id`, `url`; `job_id` UUID en string. Este test compara literalmente el JSON, incluido orden actual de serialización. | Unitario ejecutado: `TestMessageJSONContract`. |
| `schema_version` distinto de 1, UUID cero, URL ausente/no absoluta/no HTTP(S) | Rechazo de `Validate` antes de publicar. | Unitario ejecutado: `TestMessageValidateRejectsUnsupportedOrIncompleteMessage`. |
| Mensaje válido a stream configurado | Un `XADD` al stream indicado con campo Redis `data` cuyo valor es string JSON del contrato v1. | Unitario ejecutado: `TestPublishWritesContractToConfiguredStream`. |
| XADD falla | Error conserva causa (`errors.Is`) y nombra el stream. | Unitario ejecutado: `TestPublishWrapsRedisErrorWithStream`. |
| Publicación con Redis real | Payload puede leerse y validarse desde stream configurado. | Integración presente/no ejecutada: `TestPublisherPublishesMessageToRedisStream`. |

**Límite:** el orden de claves JSON está afirmado por `TestMessageJSONContract` sobre serialización Go; no debe confundirse con semántica general de objetos JSON ni extrapolarse a HTTP.

## C5 — Lectura, claim, receiver y ACK

**Fuente:** [internal/queue/consumer.go](../../internal/queue/consumer.go), [internal/worker/worker.go](../../internal/worker/worker.go), [internal/worker/receiver.go](../../internal/worker/receiver.go).

| Entrada / escenario | Resultado observable | Evidencia |
|---|---|---|
| Crear grupo ausente | `XGROUP CREATE stream group 0 MKSTREAM`; solo BUSYGROUP exacto se considera existente idempotente. Otros errores preservan causa. | Unitario ejecutado: `TestConsumerEnsureGroupUsesStartAndOnlyAcceptsBusyGroup`. |
| Leer grupo | `XREADGROUP` solo entradas nuevas `>`, `COUNT 1`, `NOACK=false`; entrega conserva ID original y campos sin decodificarlos. | Unitario ejecutado: `TestConsumerReadCommandAndDelivery`. |
| Read timeout vacío | `(delivery vacía, false, nil)`; NOGROUP se distingue y error/cancelación se conserva. ACK usa `XACK` con el ID exacto y no borra entrada. | Unitario ejecutado: `TestConsumerReadNormalEmptyAndPreservesFailures`, `TestConsumerAckUsesEntryIDAndCloseOwnsClient`. |
| Payload ausente/malformado, versión/UUID/URL inválidos | `OutcomeSkipped` con error seguro; no lookup, claim, scrape, receiver ni ACK. | Unitario ejecutado: `TestProcessRejectsInvalidPayloadWithoutEffects`. |
| Ganador del claim | Scrapea URL persistida y entrega artículo síncronamente al receiver; ningún ACK normal. | Unitario ejecutado: `TestProcessClaimWinnerScrapesAndDeliversWithoutAck`. |
| Job `processing` ya existente o claim perdido en estado processing | `OutcomeProcessingDuplicate`; no ACK, scrape ni entrega. | Unitario ejecutado: `TestProcessProcessingDuplicateDoesNotAckOrScrape`, `TestProcessClaimLossPendingDoesNotAckOrScrape`. |
| Job `completed` o `failed` | Duplicado terminal: ACK solo entrada recibida, sin scrape/receiver ni mutar job. | Unitario ejecutado: `TestProcessTerminalDuplicateAcksOriginalStreamID`; PostgreSQL/PEL real, integración presente/no ejecutada en `TestWorkerIntegrationRestartDoesNotRecoverPendingAndTerminalDuplicatesAck`. |
| Scrape falla | Guarda diagnóstico acotado seguro si puede, entrega error tipado al receiver; estado se mantiene processing y no ACK. Fallos operativos/cancelación no equivalen a scrape fallido. | Unitarios ejecutados: `TestProcessScrapeFailureDiagnosesAndDeliversTypedError`, `TestProcessDiagnosisPersistenceFailureStillDeliversRealScrapeError`, `TestProcessJobCancellationDuringScrapeSkipsDiagnosisAndOutput`. Persistencia/PEL real: integración presente/no ejecutada `TestWorkerIntegrationDuplicateClaimAndScrapeErrorRemainUnacked`. |

**Limitación observada:** artículo/error de `Result` es handoff volátil. No hay persistencia de resultado ni estado terminal del scrape; duplicados terminales solo se ACKean. Normal exitoso/fallido y estado processing no reciben ACK. La integración de reinicio deja trabajo pendiente en PEL y no lo recupera automáticamente.

## C6 — Pool acotado y apagado

**Fuente:** [internal/worker/pool.go](../../internal/worker/pool.go), [cmd/worker/main.go](../../cmd/worker/main.go).

| Entrada / escenario | Resultado observable | Evidencia |
|---|---|---|
| Pool por defecto/validación | 4 workers, drain 10s, backoff 1s; tamaño permitido 1–64 y timeouts positivos; workers persistentes secuenciales, sin cola acumulativa. | Unitario ejecutado: `TestPoolOptionsAndConstructorValidation`, `TestPoolFixedConcurrencyAndSequentialSlotReuse`. |
| Lectura vacía/error transitorio/error de job | Lectura vacía continúa sin retry del job; error de lectura usa backoff cancelable; error de procesamiento no expone texto crudo ni detiene slot. | Unitarios ejecutados: `TestPoolEmptyReadAndPerJobErrorContinueWithoutRetry`, `TestPoolTransientReadErrorBackoffCanCancel`. |
| Grupo ausente envuelto | Pool detiene admisión y conserva causa detectable, sin propagar detalle inseguro. | Unitario ejecutado: `TestPoolWrappedGroupMissingStopsAndPreservesCause`. |
| Cancelación durante lectura | Entrega tardía posterior al cierre de admisión no se procesa; trabajos ya confirmados tienen ventana de drain. | Unitarios ejecutados: `TestPoolLateDeliveryAfterCancellationIsNotProcessed`, `TestPoolDrainTimerRunsWhileLongReaderAndConfirmedJobRemain`. |
| Run | Se ejecuta una vez y espera join de workers; el timer de drain cancela contexto de jobs, pero bloqueos de lectura/operaciones pueden prolongar la salida. | Unitario ejecutado: `TestPoolCanceledContextAndSingleRunLifecycle`, `TestPoolDrainTimerRunsWhileLongReaderAndConfirmedJobRemain`. Arranque/cierre CLI: tests en `cmd/worker` revisados como suite unitaria; ver limitación final. |

**Límite:** `WORKER_SHUTDOWN_TIMEOUT` es ventana de drain, no deadline rígido de salida del proceso.

## C7 — Política SSRF, DNS, IP, TLS y redirecciones

**Fuente:** [internal/scraper/network.go](../../internal/scraper/network.go), [internal/scraper/scraper.go](../../internal/scraper/scraper.go).

| Entrada / escenario | Resultado observable | Evidencia |
|---|---|---|
| URL HTTP(S) absoluta, hostname válido, puertos implícito/80/443 | Se admite sintácticamente. Userinfo, esquema no HTTP(S), puertos no estándar, host ambiguo, metadata, IP no pública, encoded/legacy IPv4 se rechazan como `security_rejected`. | Unitario ejecutado: `TestValidateURLPolicy`, `TestPublicAddressPolicy`, `TestLegacyEncodedIPv4FormsRejectBeforeDNSOrDial`. |
| IP literal pública | Sin DNS, conecta al literal aprobado; hostname original sigue para Host/TLS. IP denegada no llega al dialer. | Unitario ejecutado: `TestSafeDialerChecksLiteralIPWithoutResolver`, `TestSafeDialerRejectsBlockedLiteralWithoutDial`. |
| Host DNS con respuesta pública / privada / mixta / vacía | Cada conexión resuelve de nuevo; solo marca pública se conecta a IP literal; respuesta privada/mixta/vacía se rechaza antes de conectar. Deadline compartida entre resolución y dial. | Unitarios ejecutados: `TestSafeDialerPinsApprovedAddressAndSharesDeadline`, `TestSafeDialerRejectsBlockedOrMixedDNSBeforeDial`, `TestSafeDialerResolvesFreshAndDoesNotRedialBlockedRebinding`. |
| Redirect | Revalida destino antes de request; downgrade HTTPS→HTTP y destinos privados/metadata son rechazados; DNS se verifica nuevamente por hop. | Unitarios ejecutados: `TestRedirectDestinationPolicyRejectsBeforeDestinationRequest`, `TestHTTPSDowngradeRejectedBeforeHTTPRequest`, `TestRedirectHopMixedDNSRejectsBeforeSecondDial`, `TestDNSRebindingBetweenRedirectHopsRejectsBeforeSecondDial`. |
| Transporte | Proxy nil, sin cookie jar, KeepAlive desactivado, HTTP/2 desactivado; TLS mínimo 1.2 y validación normal activada. | Unitario ejecutado: `TestTransportHasNoProxyCookieOrHTTP2EscapeHatches`; SNI/certificado: `TestInvalidTLSCertificateCauseAndOriginalSNI`. |

## C8 — Límites de red/cuerpo y diagnósticos seguros

**Fuente:** [internal/scraper/scraper.go](../../internal/scraper/scraper.go), [internal/scraper/types.go](../../internal/scraper/types.go), [internal/db/repository.go](../../internal/db/repository.go), [cmd/worker/main.go](../../cmd/worker/main.go).

| Entrada / escenario | Resultado observable | Evidencia |
|---|---|---|
| Defaults scraper | Connect 5s, TLS 5s, headers 10s, lectura cuerpo 10s, deadline total 30s, cuerpo descomprimido 5,242,880 bytes, 5 redirects; hasta 10 redirects configurable. Duraciones deben ser positivas. | Unitario ejecutado: `TestScraperOptionsAreExplicitAndBounded`; config defaults y límites C10. |
| Headers o cuerpo lentos | `ScrapeError(timeout, fetch)`; deadline global se comparte entre hops, no se reinicia. | Unitarios ejecutados: `TestHeaderAndTotalBodyBudgetsTimeout`, `TestEveryHopUsesOneGlobalRequestDeadline`, `TestTLSHandshakeBudgetExpires`. |
| Cuerpo al máximo / máximo +1 | Exactamente límite alcanza extractor; +1 da `body_too_large`, no llega extractor. Conteo acumulativo incluye sniffing. | Unitarios ejecutados: `TestExactMaximumHTMLReachesInjectedExtractorAndPlusOneFails`, `TestResponseBodyReadLimitIsCumulativeAcrossSniffAndRead`, `TestBodyLimitExactMaxMaxPlusOneAndChunked`. |
| Gzip | Transporte descomprime automáticamente; límite y `BytesRead` cuentan bytes del cuerpo descomprimido completo, no texto extraído ni bytes comprimidos. | Unitarios ejecutados: `TestScrapeWithStatsCountsDecompressedGzipBodyNotText`, `TestAutomaticGzipDecodingProducesArticleText`, `TestGzipExpansionAndCorruptionAreBounded`. |
| Causa de red/parser contiene URL/token | Mensaje de `ScrapeError` solo expone clasificación controlada; causa sigue accesible por `errors.Is/As`. Receiver/logs usan metadatos allowlisted, no contenido. | Unitarios ejecutados: `TestScrapeErrorIsSafeAndUnwrapsCause`, worker: `TestProcessSuccessLogUsesScrapeBytesAndExcludesContent`. |

## C9 — Extracción, MIME, charset y metadatos

**Fuente:** [internal/scraper/scraper.go](../../internal/scraper/scraper.go), [internal/scraper/types.go](../../internal/scraper/types.go), fixtures [article.html](../../internal/scraper/testdata/article.html) y [no_article.html](../../internal/scraper/testdata/no_article.html).

| Entrada / escenario | Resultado observable | Evidencia |
|---|---|---|
| HTML de artículo con contenido editorial y clutter | Texto útil conserva contenido, excluye navegación/publicidad probada; título, idioma, URL pedida/final y `FetchedAt` UTC. | Unitario ejecutado: `TestScrapeExtractsCompleteArticleUsingValidatedFinalURL`; fixture `article.html`. |
| HTML sin resultado Readability útil | Error `extraction` en etapa `extract`; no artículo vacío exitoso. | Unitario ejecutado: `TestScrapeRejectsEmptyReadabilityResult`; fixture `no_article.html`. |
| Content-Type ausente | Sniff de máximo 512 bytes acepta HTML reconocible en el inicio; no reconocible/HTML después del límite rechaza como contenido no soportado. MIME binario o malformado también rechaza. | Unitario ejecutado: `TestMIMEParsingBoundedSniffAndUnsupportedEncoding`. |
| Charset válido ISO-8859-1 | Decodifica `café`; conversión ocurre después del límite sobre cuerpo wire/descomprimido. | Unitario ejecutado: `TestMIMECharsetIsDecoded`, `TestCharsetExpansionBeyondWireLimitStillReachesExtractor`. |
| Encoding desconocido/apilado o charset inválido | Rechazo categorizado `unsupported_content`; encoding comprimido no soportado como codificación declarada. | Unitario ejecutado: `TestMIMEParsingBoundedSniffAndUnsupportedEncoding`. Gzip estándar automático tratado en C8. |

**Límite:** la extracción se verifica con fixtures concretos y no promete igualdad literal entre versiones/lenguajes ni un contrato de texto completo universal.

## C10 — Configuración y procesos API/worker

**Fuente:** [internal/config/config.go](../../internal/config/config.go), [cmd/api/main.go](../../cmd/api/main.go), [cmd/worker/main.go](../../cmd/worker/main.go).

| Variable / escenario | Default, límite o efecto observable | Evidencia |
|---|---|---|
| `DATABASE_URL`, `REDIS_URL` | Requeridas para loader común; Redis URL debe ser `redis://`/`rediss://` con host y path DB no negativo. Errores no deben filtrar credenciales. | Unitarios ejecutados: `TestLoadRequiresDatabaseURL`, `TestLoadRequiresRedisURL`, `TestLoadRejectsInvalidRedisSettings`, `TestLoadWorkerRejectsCredentialLeakInErrors`. |
| API `API_PORT`, `REDIS_STREAM`, `REDIS_DIAL_TIMEOUT`, `REDIS_PUBLISH_TIMEOUT` | `8080`, `inteldigest:jobs`, 5s, 2s. Duraciones con sintaxis Go positiva, p. ej. `1500ms`; API usa sólo sus opciones. | Unitarios ejecutados: `TestLoadDefaults`, `TestLoadRedisSettings`, `TestLoadIgnoresMalformedWorkerAndScraperSettings`. |
| Worker `WORKER_POOL_SIZE`, group/name | `4`, rango 1–64; grupo `inteldigest:workers` no blank; name explicitado no blank, si no hostname+UUID único. | Unitarios ejecutados: `TestLoadWorkerDefaults`, `TestLoadWorkerAutogeneratesUniqueConsumerNames`, `TestLoadWorkerRejectsInvalidSettings`. |
| Worker block/operation/DB/shutdown | 2s (mín. 1ms), 5s, 5s, 10s; bloque + operation produce deadline de lectura; todos los otros límites temporales positivos, sintaxis `time.ParseDuration`. | Unitarios ejecutados: `TestLoadWorkerDefaults`, `TestLoadWorkerExplicitValues`, `TestLoadWorkerRejectsTimeoutAdditionOverflow`. |
| Scraper settings | connect/TLS 5s, header/read 10s, request 30s; body 5,242,880 bytes (requiere margen max+1); redirects 5, rango 0–10. | Unitarios ejecutados: `TestLoadWorkerDefaults`, `TestLoadWorkerExplicitValues`, `TestLoadWorkerRejectsInvalidSettings`. |
| API con opciones worker inválidas | `config.Load` ignora settings exclusivos worker/scraper; no se invalida arranque de API por ellas. Worker loader valida sus propios campos. | Unitario ejecutado: `TestLoadIgnoresMalformedWorkerAndScraperSettings`. |
| Entrypoints | API y worker son procesos separados. Worker registra señales antes de config/dependencias, asegura grupo antes de lectura y cierra recursos tras join; fallos exponen códigos estables sin causa sensible. | Unitarios ejecutados: `TestRunMapsValidatedWorkerSettingsAndSignalLifecycle`, `TestRunWaitsForPoolAndSafeErrorsPreserveCause`, `TestMetadataReceiverIsSynchronousNonPersistentAndSafe`; el código de `cmd/api` no tiene test propio observado. No implica verificación de servicios vivos. |

## Verificación registrada y límites

El padre ejecutó desde la base de referencia, con toolchain/dependencias locales y sin descargas:

| Comando exacto | Resultado |
|---|---|
| `GOTOOLCHAIN=local GOPROXY=off GOSUMDB=off go test -count=1 ./...` | Exit 0; ocho paquetes con tests, `cmd/api` sin tests. Pruebas normales/unitarias; no incluyen `integration`. |
| `GOTOOLCHAIN=local GOPROXY=off GOSUMDB=off go build ./...` | Exit 0. |
| `GOTOOLCHAIN=local GOPROXY=off GOSUMDB=off go vet ./...` | Exit 0. |

Go local observado: `go1.27.3 linux/amd64`; `go.mod` requiere Go `1.27.1`. La integración PostgreSQL/Redis **no se ejecutó**. Los tests `//go:build integration` citados son evidencia de escenarios definidos en código, no resultado de ejecución. No hubo ejecución diferencial porque no se cambió código compartido; no se escribieron tests nuevos.

Los tres comandos son checks contra la base Go, informados por el padre, no ejecución adicional de esta unidad documental. La suite ordinaria excluye tests con build tag `integration`; su mera presencia en el inventario no eleva la evidencia de compatibilidad.

## Diferencias aceptadas

**Cantidad: 0.** (Sin entradas.) Ninguna discrepancia contractual de C1–C10 recibe aprobación en M01; cualquier decisión futura se registra fuera de este baseline y con autorización explícita.
