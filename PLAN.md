# PLAN — Cola Redis y publicación desde la API

**Estado:** propuesta para revisión. No implementar hasta aprobación.

## 1. Decisión de estructura en Redis

Elegir **Redis Streams con consumer group** (`XADD` para publicar). Frente a una lista con `BRPOP`, que elimina el mensaje al retirarlo y puede perderlo si el proceso cae antes de completar el trabajo, Streams retiene las entradas y registra entregas pendientes por consumidor. El trade-off es más operaciones y configuración (grupo, ACK y recuperación de pendientes), a cambio de poder detectar y reclamar mensajes no confirmados.

En la siguiente tarea, el worker confirmará (`XACK`) solo después de terminar y persistir una salida terminal; mientras procesa, la entrada permanece pendiente. Si el worker cae a mitad del procesamiento, **el mensaje no se pierde ni se reencola automáticamente**: queda pendiente en el PEL (pending entries list) hasta que un consumidor lo reclame mediante `XAUTOCLAIM` tras un umbral de inactividad. Sin un consumidor que reclame pendientes, puede quedar pendiente indefinidamente; se debe implementar esa recuperación en la tarea del worker. Esto es entrega *at least once*, no exactamente una vez.

Con más tiempo, para robustez de producción añadiría outbox transaccional en PostgreSQL para cerrar la ventana entre insertar el job y publicar en Redis, además de métricas/alertas para antigüedad del PEL, recuperación y fallos. Streams por sí solo no cierra la brecha de doble escritura.

## 1b. Idempotencia ante reentrega

El worker consultará el job en PostgreSQL por `job_id` antes de scraping, IA o cualquier operación costosa y aplicará estas reglas:

- `pending`: puede iniciar procesamiento. La transición debe ser condicional (`pending` → `processing`) en PostgreSQL; si no se pudo reclamar atómicamente, no se procesa. Esta tarea no añade ese método; queda como requisito explícito de la tarea del worker.
- `processing`: **no lo retoma ni lo descarta como completado**. Sin locking, identidad/lease del propietario ni heartbeat, no se puede saber si otro worker sigue activo o quedó muerto. Deja el mensaje pendiente sin ACK para recuperación/decisión posterior; esto evita trabajo concurrente y corrupción, pero requiere que la siguiente tarea defina recuperación de jobs atascados (no basta con reclamar el mensaje de Redis). La tarea de cola no debe habilitar reclamación ciega de una entrada cuyo job siga `processing`.
- `completed` o `failed`: no vuelve a procesar ni llama servicios externos; reconoce el estado en PostgreSQL antes del trabajo y hace ACK de la entrega duplicada.
- Dos entregas seguidas: la transición condicional solo deja que una ejecución pase desde `pending`; las otras no duplican trabajo. El ACK se realiza solo para una entrega cuyo job ya esté en estado terminal o cuya ejecución haya quedado durablemente terminal. No se incorpora `attempt` al mensaje: el intento/entrega pertenece al mecanismo de transporte, no al contrato de negocio.

Para que Streams recupere un job muerto que quedó `processing` sin permitir que otro worker lo robe mientras sigue vivo, hace falta una lease/heartbeat o una política de expiración con reclamación atómica en PostgreSQL. No se incluye ahora; queda como limitación/riesgo a resolver en el plan del worker antes de activar recuperación automática de entradas pendientes.

## 2. Contrato del mensaje

Publicar una entrada de Stream cuyo campo `data` sea exactamente este JSON UTF-8:

```json
{"schema_version":1,"job_id":"<uuid>","url":"https://example.com/article"}
```

- `schema_version`: entero `1`, requerido; permite evolucionar el payload sin reinterpretar silenciosamente mensajes antiguos.
- `job_id`: UUID string igual al ID persistido en PostgreSQL; clave de correlación e idempotencia.
- `url`: string validada y persistida por la API.
- No incluye estado, resultado, error ni número de intento. PostgreSQL es la fuente de verdad del job; Redis aporta entrega. La siguiente tarea consumirá y validará versión, UUID y URL antes de ejecutar trabajo.

El nombre lógico del Stream y del consumer group se configuran/establecen como constantes de aplicación con valores predeterminados documentados; todas las instancias de worker compartirán el mismo grupo. Los nombres concretos se documentarán en el paquete/configuración junto al contrato, sin depender del nombre del contenedor.

## 3. Cambios en `internal/api`

El handler `POST /api/v1/digests` mantiene validación y creación en PostgreSQL, y publica sincrónicamente el mensaje **después de `CreateJob` y antes de responder `202`**. No se responde éxito antes de confirmar que Redis aceptó la publicación; así el cliente no recibe un job aparentemente encolado cuando la publicación falló.

Si Redis falla después de crear el job, responder `503 Service Unavailable` con un cuerpo como `{ "error": "queue unavailable", "job_id": "<uuid>" }` (mensaje genérico, sin detalles internos), registrar el error con `job_id` y conservar el job `pending`. No se debe fingir que se revirtió la inserción: esta tarea no cuenta con transacción distribuida ni outbox. El cliente puede consultar/reintentar, pero la API actual no tiene clave de idempotencia; por ello este fallo puede dejar jobs huérfanos o crear otro job si se repite el POST. La limitación queda explícita y se recomienda outbox para despliegue de alta fiabilidad, no como alcance de esta tarea.

Añadir una interfaz `JobPublisher` en `internal/api` (lado consumidor), con una operación de publicación que reciba contexto y el job creado o los campos necesarios para construir el mensaje. Inyectarla en el handler/router junto al repositorio, manteniendo un fake para tests unitarios. Fallo de publicación se devuelve al handler como error envuelto con contexto; el publisher no decide códigos HTTP ni escribe respuestas.

## 4. Paquete `internal/queue`

Expondrá un publisher Redis concreto y el tipo compartido del mensaje (ubicado donde permita que API y futuro worker usen un solo contrato sin dependencia circular; preferentemente `internal/queue`). La superficie pública se limita a construir/configurar el publisher, publicar un job/mensaje y cerrarlo; errores envueltos con operación y stream, sin incluir credenciales.

Depende del cliente Redis elegido y de `internal/models` solo si usa directamente `models.Job`; evitar que la API dependa de detalles del protocolo. `cmd/api` compone configuración, cliente y publisher, y los inyecta en API. Este paquete **no consume**, no crea lógica de pool, no reclama pendientes, no hace scraping y no implementa reintentos de procesamiento. No debe implementar reintentos de publicación ocultos o ilimitados; los límites de conexión/operación se fijan por configuración/contexto.

## 5. Configuración

Añadir a `internal/config` y `.env.example`:

- `REDIS_URL`: obligatoria para `cmd/api` al habilitar publicación; URL completa, por ejemplo `redis://localhost:6379/0` en desarrollo. Sin default de credenciales productivas. `cmd/worker` la compartirá en la siguiente tarea.
- `REDIS_STREAM`: nombre lógico del Stream, default `inteldigest:jobs`.
- `REDIS_DIAL_TIMEOUT`: timeout de conexión inicial, default razonable de 5 s.
- `REDIS_PUBLISH_TIMEOUT`: límite por operación de publicación, default razonable de 2 s (contexto del request puede acotarlo antes).

No añadir aún configuración exclusiva del worker (grupo/consumer, concurrencia, recuperación, espera de reintento). Si el nombre del consumer group se decide constante del contrato y no configurable por entorno, documentarlo; si la operación de creación del grupo queda a cargo del worker, se configura en esa siguiente tarea. Las variables actuales `DATABASE_URL` y `API_PORT` conservan su comportamiento.

## 6. Manejo de errores y reconexión

La API debe fallar rápido al iniciar si `REDIS_URL` falta, es inválida o no puede establecer una conexión inicial dentro del timeout: cerrar recursos ya abiertos, registrar el componente afectado y terminar con código no cero. No aceptar tráfico que crea jobs mientras el publisher no está disponible. Esto permite que el supervisor/orquestador reinicie la instancia y evita una degradación silenciosa.

El cliente Redis puede reconectar en operaciones posteriores con límites y backoff acotados propios del cliente; cada publicación tiene timeout y devuelve error si no se confirma dentro del límite. El handler responde `503` ante fallo operacional de Redis. El ACK/persistencia/recuperación de mensajes no pertenece a la API ni a esta tarea. En despliegue, la readiness de API debe reflejar dependencia de Redis/PostgreSQL; `/healthz` actual es liveness simple y no debería volverse una comprobación externa bloqueante sin definirlo aparte.

## 7. Tests

**Unitarios sin Redis real (default):**

- Handler con repositorio fake y publisher fake: publica solo tras crear con éxito; payload contiene versión, ID y URL correctos; publicación exitosa conserva `202` y respuesta actual; fallo del repo no publica; fallo del publisher no responde `202`, produce `503` y deja observable que el job ya fue creado.
- Serialización/deserialización del contrato, campos requeridos, UUID/versión inválidos y compatibilidad de la versión soportada.
- Publisher: construcción de payload y propagación contextual de errores; las pruebas no deben depender de sleep ni de una instancia externa.
- Configuración: `REDIS_URL` requerida y defaults/timeouts válidos.

**Integración con Redis real:** bajo build tag `integration`, probar conexión, publicación, lectura de la entrada y formato del campo `data` en un Redis efímero/configurado para tests. Aislar nombres de Stream y limpiar recursos; no ejecutar esta prueba en la suite unitaria por defecto. Como en Postgres, documentar el comando/requisito del servicio de integración. No probar consumo, recuperación/claim ni ACK en esta tarea.

## 8. Fuera de alcance

- Consumo de Redis desde `cmd/worker`, creación/uso del consumer group en ejecución, pool de goroutines y procesamiento.
- Scraping, cliente OpenRouter, parseo/validación del digest y actualización de `processing`, `completed` o `failed`.
- Reintentos de procesamiento, backoff de negocio, dead-letter queue y política de descarte.
- Lease/heartbeat o recuperación segura de jobs que quedaron `processing`; debe resolverse antes de habilitar reclamación automática en el worker.
- Outbox transaccional, endpoint de reintento de jobs, idempotency key del POST y compensación automática de jobs huérfanos. Riesgo conocido sin resolver aquí: si vence el timeout local del publish pero `XADD` sí se ejecutó en Redis y solo se perdió la confirmación de red, la API lo trata como fallo y responde `503` con el job `pending`; si el cliente reintenta el POST completo, puede crear un segundo job para el mismo artículo al no existir idempotency key.
- Cambios de esquema de PostgreSQL, frontend o rediseño de endpoints.

## Preguntas abiertas

- **Recuperación de `processing` atascado:** el worker no puede distinguir con seguridad un worker lento/vivo de uno muerto sin lease/heartbeat. Antes de implementar recuperación automática de Stream entries, la tarea del worker debe elegir un mecanismo de propiedad/expiración; esta tarea fija que no se debe retomar ni ACKear a ciegas ese caso.
