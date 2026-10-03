# Roadmap

Este documento delimita las cinco tareas siguientes; cada una tendrá su propio `PLAN.md` antes de implementarse. No fija detalles de implementación que corresponden a esos planes.

## 1. Cola Redis y publicación desde la API
- **Entra:** publicar en Redis los jobs creados; **fuera:** consumo y procesamiento.
- **Contrato para la siguiente:** mensaje versionable con `job_id` y `url`; el worker consume el mismo formato. La política de entrega/recuperación de la cola también debe quedar definida aquí.
- **Base existente:** `internal/api` conserva validación y creación; `internal/db` persiste el job; `internal/models` aporta UUID, URL y estado `pending`.

## 2. Worker, pool y scraper
- **Entra:** consumir mensajes, limitar concurrencia, marcar `processing` y extraer contenido con protecciones SSRF, timeout y tamaño; **fuera:** llamada a IA y consolidación final.
- **Contrato para la siguiente:** entregar texto extraído y errores tipados/contextualizados al cliente de IA; acordar qué datos del job conserva cada etapa.
- **Base existente:** usa los estados y tipos de `internal/models`; actualiza mediante `internal/db`; `internal/api` sigue siendo la entrada/consulta, sin procesar trabajos en la petición.

## 3. Cliente OpenRouter y salida estructurada
- **Entra:** petición al modelo configurable, salida JSON con esquema estricto y mapeo/validación a tipos de dominio; **fuera:** persistencia del resultado y política de reintentos del job.
- **Contrato para la siguiente:** resultado tipado o error clasificado (transitorio/permanente) con contexto de etapa; definir la estructura que se almacena en `result` JSONB.
- **Base existente:** `internal/models` define/definirá el tipo del digest; `internal/db` ya persiste el resultado JSONB; configuración común se carga desde `internal/config`.

## 4. Consolidación, `failed` y reintentos
- **Entra:** persistir éxito, registrar fallo legible y decidir reintentos acotados; **fuera:** cambios de producto al esquema del digest y presentación visual.
- **Contrato para la siguiente:** `GET /api/v1/digests/{id}` devuelve una forma estable con `status`, `result` cuando está completo y `error` cuando falla; documentar qué estados intermedios observa el cliente.
- **Base existente:** `internal/db` necesita transiciones/actualizaciones además de crear y obtener; `internal/models.Job` ya contempla `pending`, `processing`, `completed`, `failed`, `result` y `error`; `internal/api` ya expone GET.

## 5. Frontend básico
- **Entra:** enviar una URL y consultar/mostrar el job hasta su estado final; **fuera:** definir todavía stack, componentes o diseño.
- **Contrato/dependencias de API:** consume `POST /api/v1/digests` (202 con `job_id`) y `GET /api/v1/digests/{id}`; acordar polling y respuestas de estados intermedios. Si frontend y API tienen orígenes distintos, habilitar CORS con orígenes explícitos; confirmar que la respuesta GET incluya todo lo necesario y que el volumen esperado no requiera paginación/listado.
- **Base existente:** depende de las rutas y forma de respuesta de `internal/api`, y de los estados/campos serializados de `internal/models`; no requiere que el frontend acceda a `internal/db`.

## Decisiones transversales

- **Mensaje Redis:** establecer como mínimo `job_id` (UUID) y `url`; no duplicar estado ni resultado, que pertenecen a PostgreSQL. Decidir antes de la tarea 1 si hace falta `schema_version` o metadatos de intento, y mantener el contrato compatible entre productor y consumidor.
- **Configuración:** `DATABASE_URL` es compartida por `cmd/api` y `cmd/worker`. `REDIS_URL` también será compartida. API: `API_PORT` y configuración CORS si aplica. Worker: concurrencia, configuración de reintentos/esperas y límites del scraper. OpenRouter: `OPENROUTER_API_KEY` y modelo son exclusivos del worker; referer/título son opcionales. Mantener secretos fuera del repositorio.
- **Errores:** cada etapa (cola, scraping, IA y parseo/validación) debe añadir contexto sin perder la causa; clasificar transitorios frente a permanentes para reintentos; al agotarlos, guardar `failed` y un mensaje seguro, legible y consistente en `Job.Error`. Los logs deben permitir correlacionar etapas por `job_id` sin exponer secretos ni contenido sensible.
- **Despliegue/operación:** API y worker son procesos separados que deben compartir configuración y dependencias de PostgreSQL/Redis, y poder escalarse/reiniciarse independientemente. Definir health checks apropiados para ambos (liveness y disponibilidad de dependencias), señal de apagado y comportamiento ante caída del worker; no confundir salud del HTTP API con éxito del procesamiento.

## Preguntas abiertas

- La elección de lista Redis vs. Streams y su comportamiento ante caída/reentrega debe resolverse en la tarea 1: `CONTEXT.md` la deja explícitamente abierta y afecta el contrato operativo del worker.
- El número y espera de reintentos y el enfoque/librería de extracción del scraper quedan para sus tareas respectivas, tal como indica `CONTEXT.md`.
- Confirmar si el frontend se servirá desde otro origen (y por tanto necesita CORS) es necesario antes de desplegarlo; no bloquea acordar ahora la política de orígenes explícitos.
