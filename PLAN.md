# PLAN — Worker, pool y scraper

**Estado:** aprobado con aclaraciones sobre fallo de scraping y presupuesto HTTP global. Implementación autorizada con TDD estricto en `feat/worker-scraper` y commits convencionales por unidad de trabajo.

**Base:** `ROADMAP.md`, sección 2 y Decisiones transversales. Las decisiones de la tarea 1 se conservan por referencia a su plan versionado (`git show d15fde3:PLAN.md`, secciones 1, 1b y 2): no se reabre el transporte, el contrato del mensaje, la publicación desde API ni la frontera del ACK.

La exploración confirmó el publisher y `Message` en `internal/queue`, las interfaces `JobRepository`/`JobPublisher` en `internal/api`, `CreateJob`/`GetJobByID` en `internal/db`, los estados y campos en `internal/models` y la composición/señales en `cmd/api`. `cmd/worker` es un stub. El esquema ya tiene `error TEXT`, pero no propiedad, lease ni heartbeat. No se requieren cambios de API ni de esquema en esta tarea.

**Resultado esperado:** un worker que consume, reclama el estado de forma atómica, limita concurrencia y entrega texto o error tipado. Es una etapa incremental, no un pipeline completo: los jobs procesados permanecen `processing` y sus entradas sin ACK hasta incorporar consolidación. No se anuncia recuperación automática ni finalización de jobs.

## 1. Consumo de la cola

### Arranque idempotente

`cmd/worker` carga configuración, abre y verifica PostgreSQL/Redis, construye repositorio, consumer y scraper, asegura el grupo y recién entonces inicia el pool. Ante fallo de arranque cierra recursos y sale con código no cero. Reutiliza `DATABASE_URL`, `REDIS_URL`, `REDIS_STREAM` y `REDIS_DIAL_TIMEOUT` existentes.

En `internal/queue`, añadir un consumer separado del publisher con operaciones acotadas `EnsureGroup`, `Read`, `Ack` y `Close`, inyectables mediante interfaces pequeñas. `EnsureGroup` ejecuta:

```text
XGROUP CREATE <stream> <group> 0 MKSTREAM
```

- `0` permite consumir también entradas publicadas antes del primer arranque; no usar `$`.
- Solo el error Redis `BUSYGROUP` cuenta como éxito idempotente. Autenticación, timeout, conexión, tipo de key incorrecto y otros errores impiden arrancar.
- Un grupo existente conserva su posición y PEL: no usar `SETID`, destruir/recrear grupo ni borrar consumers.
- Todas las instancias comparten grupo; cada proceso tiene nombre de consumer único (configuración explícita o generación automática con hostname y UUID). No compartir una identidad entre procesos vivos.

### Lectura y cadencia

Cada goroutine del pool ejecuta un ciclo secuencial, con como máximo una entrega en vuelo:

```text
XREADGROUP GROUP <group> <consumer> COUNT 1 BLOCK <milisegundos> STREAMS <stream> >
```

Las goroutines del mismo proceso comparten su identidad de consumer; la distribución de nuevas entradas la hace Redis. Se omite `NOACK`. El bloqueo por defecto es `2s`: un timeout de bloqueo sin entradas es normal y reinicia el ciclo, sin polling extra ni busy loop. El contexto y timeout de socket de lectura deben permitir ese bloqueo y acotar la operación (bloqueo + timeout operacional); `BLOCK 0` queda prohibido.

Errores de conexión usan una espera operacional fija de `1s`, cancelable, antes de una nueva lectura; no son reintentos de scraping ni de negocio. Desactivar reintentos automáticos opacos de comandos del cliente y limitar cada operación. Una lectura ejecutada en Redis cuya respuesta se pierde puede dejar una entrada en PEL: no se presume que no hubo entrega ni se promete recuperarla en esta tarea. Si se pierde el grupo durante ejecución, detener con error observable, no recrearlo silenciosamente.

### Decisión sobre pendientes del PEL

**El reclamo automático queda fuera de esta tarea**, incluso para jobs que todavía estén `pending`. No se implementan `XAUTOCLAIM`, `XCLAIM`, barrido del PEL ni relectura automática del historial propio (`0` en `XREADGROUP`); se consumen nuevas entregas con `>`.

Consultar PostgreSQL antes de reclamar permitiría recuperar algunos `pending` o limpiar terminales con seguridad, pero no resolvería el caso decisivo: un job `processing` puede pertenecer a un worker lento o a uno muerto. Añadir recuperación parcial ahora dejaría la misma limitación central y ampliaría operaciones y tests sin completar el contrato de recuperación.

Es aceptable para este portfolio como hito incremental explícito: demuestra consumo, exclusión atómica, backpressure y scraping seguro, no tolerancia completa a caídas. Una caída antes del claim puede dejar `pending` en PEL; después del claim puede dejar `processing`; ambos pueden quedar detenidos indefinidamente. Reiniciar el worker no los rescata. No usar la antigüedad del PEL o `updated_at` como permiso para robarlos ni resetearlos mientras pudiera existir un propietario vivo.

La recuperación operativa automatizada se diseñará como trabajo posterior, coordinado con consolidación/reintentos y propiedad en PostgreSQL. Esta entrega debe documentar la limitación; no se considerará apta para producción con recuperación garantizada.

## 2. Transición de estado y detección de duplicados

El worker decodifica el campo `data`, comprueba campos requeridos y tipos del JSON y reutiliza `queue.Message.Validate()` para versión, UUID y URL. Luego obtiene el job por `job_id`. La URL persistida es la autoridad: exigir coincidencia con el mensaje y usar la del job para scraping. El mensaje no concede permiso para procesar.

`internal/db` añade `TryMarkProcessing(ctx, id) (claimed bool, err error)` con una única sentencia, en autocommit:

```sql
UPDATE jobs
SET status = 'processing', error = NULL, updated_at = NOW()
WHERE id = $1 AND status = 'pending';
```

`RowsAffected() == 1` concede el claim; `0` no. PostgreSQL serializa el conflicto sobre la fila y reevalúa la condición: dos consumidores del mismo `job_id` no pueden ganar ambos. No alcanza con consultar y después actualizar incondicionalmente. La garantía es exclusión de esta transición, no exactamente una vez ni atomicidad entre PostgreSQL, Redis y la red.

| Estado/resultado observado | Trabajo externo | ACK de esa entrada |
|---|---|---|
| `pending`, claim ganado | Ejecutar scraping y entregar su salida | No: todavía no hay estado terminal durable |
| `pending`, claim perdido | No ejecutar; volver a consultar estado | Solo si esa nueva consulta demuestra `completed` o `failed` |
| `processing` | No retomar, no esperar en bucle por ese job, no ejecutar scraping | No; conservar pendiente y registrar omisión |
| `completed` o `failed` | Omitir scraping y cualquier etapa posterior | Sí, `XACK` inmediato del ID de Stream leído |
| Job inexistente, estado desconocido o error de DB | No ejecutar | No; diagnóstico y entrada pendiente |
| Payload inválido, versión no soportada o URL distinta de DB | No ejecutar | No; sin descarte silencioso ni DLQ en esta tarea |

Si tras perder el claim la consulta sigue mostrando `pending`, registrar la inconsistencia y no procesar ni ACKear; no introducir bucles de claim o resets de estado. Si el resultado de un UPDATE es incierto por error de conexión, tampoco ejecutar scraping: un commit pudo haber ocurrido y requiere recuperación posterior.

`XACK` usa siempre el ID de Stream, no `job_id`; no elimina la entrada con `XDEL`. Un error de ACK no revierte estado ni vuelve a ejecutar trabajo externo. La limpieza posterior de esa entrega queda sujeta a la limitación del PEL de la sección 1.

El éxito del scraping **no equivale a `completed`**; su fallo **no equivale todavía a `failed`**. Esta tarea preserva la frontera de ACK ya aprobada. No se simulan estados terminales en ejecución para facilitar la demostración.

## 3. Pool de goroutines

Añadir `internal/worker` para orquestación independiente de Redis, SQL y HTTP concretos. Depende de interfaces de consumer, repositorio, scraper y receptor de salida, definidas del lado consumidor.

- Iniciar exactamente `WORKER_POOL_SIZE` goroutines persistentes, default `4`.
- Cada goroutine lee una entrada y completa validación, claim, scraping y entrega/diagnóstico antes de leer otra.
- No hay dispatcher con cola de jobs en memoria ni goroutine nueva por mensaje. Cuando todas están ocupadas, **se deja de leer**; nuevas entradas esperan en Redis.
- El límite es como máximo N entregas/scrapings activos; no depende de la cantidad acumulada en el Stream. El PEL puede crecer por la ausencia temporal de consolidación, pero no una cola de trabajo en memoria.
- Clientes Redis/pgx y transporte HTTP pueden compartirse si son seguros para concurrencia; dimensionar el pool de conexiones Redis para al menos N lectores bloqueantes más margen para operaciones de control/ACK (N+2). El parser/extractor tiene instancia por invocación. Los fakes y el receptor también deben ser seguros bajo concurrencia.

### SIGINT/SIGTERM

1. Cancelar el contexto de admisión para impedir nuevas lecturas y nuevos claims. Una lectura ya bloqueada no se interrumpe inmediatamente en el socket de `go-redis`: termina dentro de su presupuesto finito (`WORKER_STREAM_BLOCK` + `WORKER_QUEUE_OPERATION_TIMEOUT`, default `7s`; normalmente al vencer el BLOCK de `2s`). Revisar la cancelación al retornar antes de reclamar; una entrega recibida durante esa carrera se deja sin ACK si aún no comenzó.
2. Dar a trabajos en vuelo una ventana de drenaje de `WORKER_SHUTDOWN_TIMEOUT` (default `10s`), manteniendo abiertos DB y Redis. Esta ventana comienza al cancelar admisión y corre en paralelo con la salida de los lectores, no después de ella.
3. Al vencerla, cancelar el contexto de procesamiento: solicitudes HTTP, resolución, dial, lectura y operaciones DB deben respetarlo. La cancelación de apagado no es un fallo de negocio ni se ACKea.
4. Esperar el `WaitGroup` de todos los lectores y trabajos antes de cerrar clientes/pool de PostgreSQL. Si el presupuesto de lectura configurado supera la ventana de drenaje, el cierre total puede superar esa ventana; no imponer `lectura <= drenaje` como requisito de seguridad ni anunciar un deadline duro de salida. No cerrar recursos que todavía usan goroutines ni descartar resultados mediante goroutines huérfanas.

El extractor HTML elegido es síncrono y no ofrece cancelación cooperativa en su API. Se revisa el contexto antes/después de parsear y se limita estrictamente su entrada; si SIGTERM llega durante el parseo, se espera a que esa llamada termine. Los `10s` son ventana de drenaje, **no garantía de interrupción dura del CPU del parser**. No envolverlo en una goroutine que se abandona al cancelar. Un SIGKILL externo puede dejar `processing`/PEL; no se presume apagado limpio en ese caso.

## 4. Scraper

`internal/scraper` expone `Scrape(ctx, rawURL) (Article, error)`, tipos de resultado/error y construcción con límites explícitos. Solo descarga HTML y extrae texto: no consulta DB, cambia estados, llama IA ni decide ACK. Resolver/dialer/extractor se inyectan internamente para pruebas; no existe bandera productiva para permitir destinos privados.

### HTTP seguro

- Solo URL absoluta `http`/`https`, con hostname y sin credenciales. Rechazar representaciones ambiguas de IP, IPv6 con zona y esquemas alternativos. Permitir únicamente puerto `80` para HTTP y `443` para HTTPS, implícitos o explícitos.
- Validar IP literal y **resolver/verificar DNS en cada conexión real**. Usar `netip`, normalizar IPv4 mapeada en IPv6 y bloquear loopback, privadas/ULA, link-local, unspecified, multicast, CGNAT y rangos reservados/especiales/no públicos de IPv4/IPv6. `IsGlobalUnicast` por sí solo no alcanza. Mantener las reglas explícitas y testeadas, sin consultas externas a listas durante ejecución.
- Bloquear endpoints de metadata/control de nube, incluidos `169.254.169.254`, `169.254.170.2`, `100.100.100.200` y `168.63.129.16`, además de nombres de metadata conocidos. La comprobación de IP sigue siendo obligatoria aunque el hostname no sea conocido.
- Si DNS devuelve alguna dirección bloqueada, rechazar el destino entero. El dialer conecta a una **IP literal ya validada**, sin segunda resolución del hostname: evita la carrera de DNS rebinding. Conservar Host original y SNI/verificación TLS del hostname; nunca usar `InsecureSkipVerify`.
- Transporte dedicado, sin proxies de entorno (`Proxy: nil`) ni rutas de conexión alternativas que salteen el dialer seguro. Para simplificar la auditoría inicial, deshabilitar keep-alive y HTTP/2; cada salto abre una conexión validada. Es una elección conservadora de esta tarea, no una afirmación de que reutilizar una conexión ya validada sea por sí mismo inseguro.
- `CheckRedirect` valida cada URL destino resuelta y aplica la misma política; el dialer valida sus IPs antes de conectar. Máximo `5` saltos y prohibición de downgrade HTTPS → HTTP. No basta con validar la URL original ni con descubrir el destino privado después de haber enviado la petición.
- Cliente sin cookies ni credenciales de usuario y con headers mínimos. No descargar imágenes, iframes, scripts ni URLs descubiertas en el HTML.

### Límites

- Conexión/DNS: `5s`; TLS handshake: `5s`; headers: `10s`.
- Lectura de cuerpo: presupuesto total de `10s` desde recepción de headers, cancelando la petición al vencer; no solo timeout de inactividad que se reinicia con cada byte.
- Petición completa, incluyendo redirects: `30s`, acotada además por el contexto padre. Los presupuestos son acumulativos dentro de ese límite, no se renuevan por redirect.
- Compatibilidad en el peor caso: hasta `6` conexiones (petición inicial + `5` redirects), sin keep-alive, podrían consumir `60s` solo en conexión/TLS (`5s + 5s` por salto); por eso un único deadline global de `30s`, compartido por todos los saltos, recorta cada presupuesto al tiempo restante y aborta al agotarse, sin garantizar completar los `5` redirects.
- Cuerpo: máximo `5 MiB` **descomprimidos**. Leer como máximo `max+1` para detectar exceso incluso sin `Content-Length`; un header mayor al máximo permite rechazo temprano, pero nunca es la única comprobación. Cerrar cuerpo/conexión en todas las salidas.
- Mantener la descompresión gzip controlada del cliente sin fijar manualmente `Accept-Encoding`; rechazar encodings desconocidos o apilados, sin descompresores posteriores sin límite. El límite se aplica sobre el lector descomprimido, antes de parsear.
- Aceptar respuestas exitosas HTML (`text/html`, `application/xhtml+xml`), con detección acotada si falta tipo; rechazar binarios/tipos no soportados. Texto principal vacío produce error de extracción, no un éxito usable por IA.

### Extracción elegida

Usar **`codeberg.org/readeck/go-readability/v2`**, mediante `FromReader` sobre el HTML ya descargado y limitado, con la URL final validada como base; obtener texto con `RenderText` y metadata mediante su API. No usar `FromURL`, que introduciría otra ruta HTTP fuera de las protecciones.

Justificación: port de Mozilla Readability orientado a eliminar navegación/publicidad y recuperar el artículo, sin browser, JavaScript ni selectores por sitio. Se prefiere al simple stripping de etiquetas, que conserva ruido. El paquete anterior `github.com/go-shiori/go-readability` está deprecado en favor de este sucesor. Referencias: [documentación v2](https://pkg.go.dev/codeberg.org/readeck/go-readability/v2) y [aviso del paquete anterior](https://pkg.go.dev/github.com/go-shiori/go-readability).

Fijar una versión compatible con el Go del proyecto al implementar, sin dependencia flotante. Páginas que solo renderizan el artículo mediante JavaScript, PDFs y paywalls no se resuelven aquí.

## 5. Contrato de salida hacia la tarea de IA

`Article`, en `internal/scraper`, contiene:

| Campo | Uso |
|---|---|
| `Text string` | Texto principal plano, no HTML; único contenido del artículo que se entrega a IA |
| `RequestedURL string` | URL persistida del job |
| `FinalURL string` | URL efectiva tras redirects validados |
| `Title string` | Título extraído, opcional |
| `Language string` | Idioma declarado/extraído, opcional; no inferido por IA aquí |
| `FetchedAt time.Time` | Instante UTC de descarga |

`internal/worker` entrega al receptor una salida con `JobID uuid.UUID`, `Article` si hubo éxito o `ScrapeError` si hubo fallo. ID de Stream/grupo/consumer quedan en el contexto de orquestación para la futura consolidación, no en el input de IA. El scraper no conoce `job_id` y el cliente de IA no dependerá de Redis.

El receptor es una interfaz síncrona/contextual para conectar la siguiente etapa sin acoplar el scraper a OpenRouter. En esta tarea `cmd/worker` conecta un receptor explícito que registra solo metadata técnica del resultado y **no almacena ni loggea `Text`**. Tests conectan un fake que comprueba el contrato completo. No se publica otro Stream, no se persiste HTML/texto ni se guardan resultados en un mapa creciente.

Esta entrega en memoria **no es durable**: al reiniciar se pierde el texto y no se puede reanudar un `processing` automáticamente. La tarea 3 reutilizará el contrato; la tarea 4 compondrá persistencia terminal y ACK. El worker no oculta esa frontera con un placeholder que marque éxito.

### Errores tipados

`ScrapeError` incluye `Kind`, `Stage` (`validation`, `fetch`, `extract`), `HTTPStatus` cuando corresponde, mensaje seguro y causa envuelta (`Unwrap`, compatible con `errors.Is`/`errors.As`). El worker agrega `job_id`/ID de entrega al contexto operativo sin perder esa causa. No persistir el error crudo de `net/http`, que puede incluir URL/query o detalles internos.

| Kind | Clasificación para la futura política de tarea 4 |
|---|---|
| `security_rejected` | Permanente: esquema, puerto, IP/DNS, redirect o downgrade rechazado |
| `timeout` | Transitorio: agotamiento de presupuesto de red/lectura |
| `body_too_large` | Permanente bajo los límites actuales |
| `network` | Transitorio por defecto; preservar causa para refinamiento posterior |
| `http_status` | `408`, `429` y `5xx`: transitorio; otros `4xx`: permanente; otros status no aceptados conservan código explícito |
| `unsupported_content` | Permanente: MIME/encoding no soportado |
| `extraction` | Permanente para ese contenido: parseo sin artículo/texto útil |
| `canceled` | Cancelación operacional; no confundir con timeout ni fallo definitivo |

Los errores TLS de certificado inválido se distinguen por causa y se clasifican permanentes, aunque provengan de la etapa de red. Esta clasificación es una señal tipada, **no ejecuta reintentos ahora**. `context.Canceled` de SIGTERM no se registra como scraping fallido definitivo; `DeadlineExceeded` del presupuesto del scraper sí es `timeout`.

## 6. Cambios en internal/db

Conservar `CreateJob` y `GetJobByID`; añadir métodos concretos y una interfaz mínima propia del worker, sin ampliar innecesariamente la interfaz de la API:

1. `TryMarkProcessing(ctx, id) (bool, error)`: UPDATE condicional de la sección 2, comprobación de filas afectadas y errores envueltos.
2. `RecordScrapeError(ctx, id, safeReason) (bool, error)`: guardar diagnóstico saneado, limitado a `1024` caracteres, solo si el job sigue `processing`:

```sql
UPDATE jobs
SET error = $2, updated_at = NOW()
WHERE id = $1 AND status = 'processing';
```

El worker genera `safeReason` desde el tipo/código, mensaje seguro y status HTTP, por ejemplo `scraping/body_too_large: response exceeds configured limit`. El repositorio recibe un string seguro, no depende de `internal/scraper` ni serializa causas de red.

`0` filas significa que ya no se cumple la condición; no sobrescribir estados terminales. Un fallo de persistencia se loggea como etapa DB y deja la entrega sin ACK. Cada operación tiene timeout/contexto; no mantener una transacción abierta durante scraping.

**La frontera es deliberada:** en fallo se conserva `status = processing` y se registra solo diagnóstico, no `failed`, `result` ni reintento. `Job.Error` puede estar presente mientras `processing`; es diagnóstico intermedio, no evidencia de finalización, y GET debe seguir interpretándose por `status`. No introducir un estado adicional ni migración. En éxito no se escribe texto en `result` JSONB ni se fabrica un digest.

La tarea 4 deberá reemplazar este manejo provisional por transiciones terminales, formato de error final y ACK tras persistencia. Tampoco se añadirá un `UpdateStatus` genérico que permita saltarse la exclusión.

## 7. Configuración

Añadir campos/validación en `internal/config` y ejemplos en `.env.example`, manteniendo nombres uppercase snake case y duraciones Go (`5s`). Configuración inválida falla al arrancar; valores exclusivos del worker no deben impedir iniciar la API por una validación ajena a su proceso.

| Variable nueva | Default | Validación/uso |
|---|---|---|
| `WORKER_POOL_SIZE` | `4` | Entero positivo, máximo `64` como protección operacional |
| `WORKER_CONSUMER_GROUP` | `inteldigest:workers` | Nombre no vacío; compartido por instancias |
| `WORKER_CONSUMER_NAME` | Autogenerado | Si se proporciona, no vacío y único entre procesos; documentar unicidad |
| `WORKER_STREAM_BLOCK` | `2s` | Positivo, mínimo `1ms`, convertido a milisegundos Redis |
| `WORKER_QUEUE_OPERATION_TIMEOUT` | `5s` | Positivo; group/ACK y margen de lectura sobre BLOCK |
| `WORKER_DB_TIMEOUT` | `5s` | Positivo; límite por consulta/UPDATE |
| `WORKER_SHUTDOWN_TIMEOUT` | `10s` | Positivo; ventana de drenaje antes de cancelar |
| `SCRAPER_CONNECT_TIMEOUT` | `5s` | Positivo; DNS/dial |
| `SCRAPER_TLS_TIMEOUT` | `5s` | Positivo; handshake |
| `SCRAPER_HEADER_TIMEOUT` | `10s` | Positivo; espera de headers |
| `SCRAPER_READ_TIMEOUT` | `10s` | Positivo; presupuesto completo del cuerpo |
| `SCRAPER_REQUEST_TIMEOUT` | `30s` | Positivo; techo total de red incluyendo redirects |
| `SCRAPER_MAX_BODY_BYTES` | `5242880` | Entero positivo; comprobar overflow en max+1 |
| `SCRAPER_MAX_REDIRECTS` | `5` | Entero entre `0` y `10`; `0` prohíbe redirects |

Reutilizar la carga común y añadir validación/lectura exclusiva del worker sin romper `config.Load()` ni defaults existentes de `cmd/api`. Validar coherencia de presupuestos y documentar que manda el plazo menor del contexto/timeout global. No añadir secretos de OpenRouter, parámetros de reintentos de negocio ni umbral de idle del PEL.

## 8. Logging y observabilidad mínima

Usar logging estructurado de la biblioteca estándar, con mensajes de código en inglés. Por entrega, incluir `job_id` si pudo validarse, `stream_entry_id`, consumer/grupo, etapa y duración, sin cuerpo del mensaje:

- `job_received`: contrato validado y entrega recibida.
- `processing_started`: solo después del UPDATE con una fila afectada.
- `job_skipped`: estado observado y motivo (`already_processing`, `terminal_duplicate`, `claim_lost`).
- `scraping_completed`: duración, bytes leídos y longitud de texto; sin título ni texto.
- `scraping_failed`: kind, etapa, status HTTP y clasificación; sin error crudo que contenga URLs.
- `scrape_error_recorded`/`scrape_error_record_failed`: evidencia del diagnóstico intermedio.
- `job_acked`/`job_ack_failed`: ID exacto de entrega y motivo terminal.

También registrar arranque y grupo existente/creado, fallos de dependencias, espera operacional y fases de apagado. Mensajes inválidos usan solo ID de Stream y motivo seguro: no inventar un UUID de correlación.

No loggear URL completa, query/fragment, cookies, headers de autenticación, HTML, texto ni credenciales de DB/Redis. Los `processing` omitidos deben dejar visible que no se recuperaron ni ACKearon. No confundir `scraping_completed` con job completado.

Salud mínima: comprobaciones de dependencias al arrancar y errores observables durante ejecución; proceso vivo no significa job finalizado. Esta tarea no modifica `/healthz` de API ni abre un segundo servidor HTTP de métricas/health. Readiness continua y alertas por antigüedad/tamaño del PEL quedan para trabajo operacional posterior.

## 9. Tests

Mantener `testing` estándar y fakes, sin servicios externos en la suite por defecto. Al implementar comportamiento determinista, observar primero el test fallando y después pasando; este documento no afirma haber ejecutado esos tests.

### Unitarios sin Redis/PostgreSQL reales

- **Consumer fake:** startup creado/existente (`BUSYGROUP`) y otros errores; parámetros exactos de `XREADGROUP`, `COUNT 1`, `>`, sin `NOACK`; timeout de bloqueo normal, errores/cancelación y ausencia de comandos de claim o borrado.
- **Payload:** `data` ausente/tipo incorrecto, JSON roto/campos faltantes, versión inválida, UUID inválido, URL inválida, discrepancia con DB; no scraping ni ACK en casos inválidos.
- **Worker con repo fake:** claim ganado/perdido, reconsulta terminal, `processing`, inexistente, error de consulta/UPDATE y resultado incierto. Solo el ganador scrapea; únicamente terminales reciben ACK. Éxito de scraping no cambia a `completed` ni ACKea; fallo guarda razón pero no `failed` ni ACK.
- **Pool:** máximo N trabajos en vuelo, no nuevas lecturas mientras están ocupados, repositorio/receptor seguros, cancelación de lectura, drenaje y cancelación de trabajos, cierre después del WaitGroup. Coordinar con canales/barreras, no sleeps ni llamadas a internet.
- **DBTX fake:** SQL condicional y filas afectadas, parámetros del diagnóstico, no sobrescritura terminal y errores propagados. No sustituye la prueba de concurrencia real de PostgreSQL.
- **Config:** defaults, límites/overflow, duraciones inválidas, nombre automático/expreso y compatibilidad de arranque de API.

### Scraper con httptest y red inyectada

La seguridad no se desactiva para usar un servidor local: las pruebas de política comprueban que loopback es rechazado; las de HTTP usan un resolver fake con IP pública sintética y un dialer de test que conduce únicamente ese destino validado hacia `httptest.Server`. Ese cableado se mantiene en `_test.go`, nunca en una allowlist o variable productiva.

Probar esquemas/puertos/credenciales, IPv4/IPv6 y mapped IPv4, metadata, rangos especiales, DNS mixto, representaciones ambiguas y cambio de resolución. Verificar que el dial real recibe la IP literal validada, no re-resuelve el hostname, y que no se llama al destino si fue bloqueado.

El servidor de prueba produce redirects relativos/absolutos hacia loopback, privadas y metadata, exceso de saltos/downgrade; verificar que el siguiente endpoint no recibió petición. Añadir timeout de headers y cuerpo lento, cancelación, fallo de red, statuses `404`/`429`/`5xx`, MIME no HTML, encoding no soportado, gzip grande, cuerpo chunked sin `Content-Length`, tamaño exacto máximo y máximo+1. Un fixture de artículo comprueba extracción de texto útil sin navegación/scripts y contenido vacío. No ejecutar peticiones contra internet real.

### Integración real con build tag integration

- **PostgreSQL:** conservar convenciones de base `inteldigest_test`, migraciones y limpieza existentes. Para la carrera usar conexiones/transacciones independientes contra la misma fila comprometida: dos claims concurrentes, exactamente uno gana. Verificar diagnóstico persistido y protección de terminales. La transacción de rollback de un único test no demuestra esa carrera.
- **Redis:** streams/grupos aislados y limpieza con `t.Cleanup`; grupo idempotente, entradas anteriores al arranque leídas con ID inicial `0`, nuevas entregas distribuidas sin duplicar ID, `XACK` terminal y `processing` retenido en PEL.
- **Worker + Redis/PostgreSQL reales, scraper fake:** dos entradas con el mismo `job_id`, un solo scraping; caída/reinicio deja pendiente sin reclamar y el worker sigue leyendo entradas nuevas. Documentar esa limitación con evidencia, no como éxito de recuperación.

Comandos previstos tras implementar: `go test ./...`, `go test -race ./...`, `go build ./cmd/api ./cmd/worker` y `go test -tags=integration ./internal/db ./internal/queue ./internal/worker` con Redis/PostgreSQL de prueba disponibles. Los tests reales quedan separados de la suite unitaria y nunca apuntan a producción.

## 10. Fuera de alcance

- Llamadas a OpenRouter, claves/modelo, esquema de respuesta de IA y validación del digest: tarea 3.
- Consolidación/persistencia del resultado final y estados `completed`/`failed` definitivos: tarea 4. El diagnóstico `error` durante `processing` es el único avance de persistencia de fallo aquí.
- Un job cuyo scraping falla permanece en `processing`, con el diagnóstico guardado en `error`, sin ACK ni recuperación hasta una tarea futura de recuperación; esta limitación también aplica sin caída del worker.
- Reintentos de negocio, reset automático a `pending`, backoff de scraping/IA y dead-letter queue.
- Reclamo automático del PEL, recuperación de jobs atascados, lease/heartbeat, expiración/fencing y identidad durable de propietario. Un timeout de red o idle de Redis no habilita reclamación.
- Persistencia/reanudación de texto extraído, segunda cola de etapa, almacenamiento de HTML y pipeline durable entre tareas.
- Outbox, idempotency key de POST, recuperación de jobs creados pero no publicados y modificaciones del contrato de publicación ya resuelto.
- Browser/headless, JavaScript, PDFs, bypass de paywalls, autenticación de sitios, selectores por dominio y seguimiento de recursos embebidos.
- Nuevos endpoints, frontend, cambios de esquema, métricas/alertas completas, readiness HTTP del worker y cambios de health checks de API.

**Pase concedido:** plan aprobado con los dos ajustes anteriores. La implementación deberá demostrar exclusión atómica, límites de concurrencia/HTTP y ausencia de llamadas a destinos bloqueados; no deberá presentar este hito como procesamiento completo o recuperación ante caídas.
