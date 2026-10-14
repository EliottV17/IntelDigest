# IntelDigest — Plan único del proyecto

## Objetivo y alcance

Proyecto personal de portfolio que demuestra una API asíncrona en Go, Redis Streams como broker y procesamiento con IA mediante una API key de OpenRouter. El resultado final será una demo local comprensible y verificable, no un servicio de producción ni una plataforma SaaS.

Una persona envía la URL pública de un artículo técnico, recibe un `job_id` y consulta luego un digest estructurado o un error seguro. La API responde sin hacer scraping ni llamadas a IA dentro de la petición.

Este archivo es la única fuente del objetivo, las etapas y el trabajo pendiente. [README.md](README.md) explica cómo usar la implementación actual; [AGENTS.md](AGENTS.md) contiene convenciones, comandos y reglas internas. Los planes y registros anteriores se conservan en el historial de Git, no como documentos paralelos.

## Resultado final esperado

```text
POST URL -> API -> PostgreSQL: pending -> Redis Streams
                                               |
                                             Worker
                                               |
                              claim atómico: processing
                                               |
                                      Scraper -> OpenRouter
                                               |
                          validar digest -> persistir completed
                          o guardar error seguro -> persistir failed
                                               |
                                    ACK después de persistir
                                               |
                          GET job -> estado + resultado/error
```

- Dos procesos independientes: `cmd/api` y `cmd/worker`.
- PostgreSQL como fuente de verdad del job y del digest final en `result JSONB`.
- Redis Streams con grupos de consumidores para desacoplar recepción y procesamiento.
- Un único cliente HTTP OpenRouter, una API key y un modelo configurable compatible con salida estructurada.
- Pool acotado, protección SSRF y límites explícitos de red, tamaño y tiempo.
- Demostración por terminal con `curl`, sin frontend.

## Etapas terminadas

| Etapa | Entrega implementada |
|---|---|
| 1. Base de datos y API | Migración SQL versionada, creación y consulta de jobs, validación de URL, `POST /api/v1/digests`, `GET /api/v1/digests/{id}` y `/healthz` |
| 2. Publicación y consumo Redis | Mensaje `schema_version=1`, `job_id`, `url`; publicación después de persistir; consumer group y lectura de entradas nuevas |
| 3. Worker y scraper | Claim atómico `pending -> processing`, pool fijo, apagado con drenaje, descarga segura y extracción de texto con Readability |

Estas etapas tienen tests unitarios y harnesses de integración. El historial conserva la verificación de PostgreSQL/Redis reales con scraper falso; eso no equivale a una demo HTTP/IA completa ni prueba que los servicios estén disponibles hoy.

### Límite exacto de la implementación actual

El pipeline termina en un receptor síncrono en memoria que registra metadatos y descarta el texto. Todavía no llama a OpenRouter, no guarda un digest y no marca `completed` ni `failed` después del scraping. Un error de scraping puede registrar un diagnóstico intermedio, pero el job sigue `processing`.

Las entradas procesadas normalmente permanecen pendientes de ACK en Redis. Solo los duplicados cuyo job ya está en estado terminal se ACKean. No hay reintentos ni recuperación automática de trabajos después de una caída.

Ya están decididos Redis Streams, Readability y las migraciones SQL versionadas; no volver a tratarlos como elecciones abiertas.

## Etapas pendientes, en orden

### 4. Contrato del digest y cliente OpenRouter

**Entrega:** cliente aislado, testeable y con resultado tipado. Todavía no confundirlo con un job terminado.

- Acordar el JSON del digest y su idioma antes de implementar. Base recomendada: título, resumen, puntos clave, categorías, tecnologías mencionadas y dificultad. Definir campos obligatorios y límites; no agregar análisis ajenos al objetivo.
- Crear tipos de dominio y validar el contenido recibido, no solo que sea JSON sintácticamente válido.
- Implementar un cliente HTTP propio en `internal/ai`, con Bearer auth, modelo configurable y salida con esquema estricto cuando el modelo/proveedor lo soporte.
- Usar `OPENROUTER_API_KEY` y proponer `OPENROUTER_MODEL`; el cliente y la configuración de modelo todavía no existen. La configuración de IA será exclusiva del worker.
- Verificar documentación vigente y compatibilidad antes de fijar el modelo y los parámetros. No elegir un modelo por un ejemplo histórico.
- Limitar entrada, respuesta y duración. Ante un artículo que exceda el presupuesto acordado, devolver un error explícito en lugar de truncarlo silenciosamente.
- Tratar el artículo como datos no confiables, no como instrucciones para el modelo.
- Probar éxito, respuestas malformadas, rechazo del esquema, errores HTTP y timeout mediante un servidor HTTP de pruebas. No consumir crédito para tests automatizados.

**Cierre:** se obtiene un digest válido o un error clasificado y seguro; no se filtran claves ni contenido completo en logs.

### 5. Conectar IA y consolidar el job

**Entrega:** circuito de procesamiento completo, con resultado durable antes del ACK.

- Revisar el contrato de `internal/worker/receiver.go` y conectar el cliente de IA al procesamiento existente sin aumentar la concurrencia fuera del pool.
- En éxito, validar el digest y guardar `completed`, `result` y limpieza del error previo.
- En fallo de scraping, IA o validación, guardar `failed` con un mensaje seguro. Clasificar la causa no implica activar reintentos automáticos.
- ACKear la entrada Redis solo después de confirmar la persistencia del estado terminal. Si falla esa escritura, no simular éxito ni ACKear.
- Mantener transiciones condicionadas por estado y el tratamiento idempotente de duplicados terminales.
- Confirmar que GET expone una respuesta estable: estado intermedio, digest final o error según corresponda.
- Probar el circuito con PostgreSQL/Redis reales y dobles deterministas para servicios externos; ejecutar la integración de forma aislada y serializada.

**Cierre:** una URL válida termina en `completed` con digest consultable; un fallo de procesamiento termina en `failed` con diagnóstico seguro. Las entradas alcanzan ACK únicamente después del estado terminal persistido.

### 6. Cerrar y presentar la demo de portfolio

**Entrega:** una persona puede reproducir y explicar el proyecto siguiendo el README.

- Actualizar README y ejemplos de configuración para reflejar el pipeline terminado. Los archivos dotenv continúan bajo manejo humano.
- Documentar compilación, arranque de ambos procesos y consulta por `curl` hasta estado terminal.
- Realizar una demo real de scraping + OpenRouter con autorización para usar la API key y consumir crédito; no es un test rutinario.
- Mostrar un caso exitoso y un fallo controlado, y conservar evidencia sin secretos.
- Explicar el papel de Redis, la separación API/worker, el pool, la protección SSRF y la validación del resultado de IA.
- Documentar honestamente las limitaciones operativas y comprobar que el repositorio no contiene credenciales.

**Cierre del proyecto:** demo local reproducible, digest persistido y consultable, errores observables, tests aplicables y documentación coherente. No hace falta desplegarlo para cumplir este alcance.

## Fuera de alcance de esta entrega

- Frontend React u otra interfaz visual.
- Usuarios, autenticación, multi-tenancy, facturación o paneles administrativos.
- Despliegue cloud, Kubernetes o una plataforma de observabilidad.
- Agentes de IA, RAG, múltiples proveedores, modelos de fallback o un sistema de prompts administrable.
- Reintentos automáticos de negocio, DLQ, leases distribuidos, heartbeats o recuperación automática de la PEL.
- Outbox transaccional o idempotencia del POST.

Se conservan las protecciones existentes; simplificar el producto no autoriza debilitar la seguridad del scraper ni la validación de datos.

## Limitaciones que deben seguir explícitas al finalizar

- Una caída después del claim puede dejar un job en `processing` y una entrada en la PEL. Esta demo no promete recuperación automática ni procesamiento exactamente una vez.
- Un fallo al confirmar el ACK puede dejar una entrada pendiente aun con el resultado ya persistido. Persistir antes de ACK evita perder el resultado, pero no constituye un sistema de recuperación.
- Persistir en PostgreSQL y publicar en Redis no es una transacción única: un error de publicación devuelve `503` con el `job_id`; reenviar el POST puede crear otro job.
- Sin autenticación, cuotas ni operación endurecida, no exponer esta demo libremente a Internet.

## Próximo paso de desarrollo

Este plan ordena las recomendaciones, pero no autoriza por sí solo cambios de código ni operaciones. El siguiente paso de desarrollo, cuando se solicite, es acordar el contrato del digest de la etapa 4. La reorganización documental actual no implementa ninguna etapa pendiente.
