# IntelDigest API — Contexto del proyecto

Este documento es contexto estable del proyecto, no una orden de trabajo. Las tareas concretas se piden por separado (ver "Primera tarea"). Trabajamos con ODD: explora el repo antes de cambiar nada, pregunta solo lo que no puedas decidir con seguridad, y entrega en unidades de trabajo pequeñas y verificables.

## Qué es

Motor de ingesta y enriquecimiento de datos no estructurados. Recibe la URL de un artículo técnico o blog, extrae su contenido en segundo plano y usa IA para devolver un JSON estructurado y estrictamente tipado (categorías, nivel de dificultad, puntos clave, tecnologías mencionadas, etc.).

Es un proyecto de portfolio: prioriza claridad, código idiomático y decisiones que se puedan explicar en una entrevista.

## Stack

- **Backend (API y Worker):** 100% Go.
- **Base de datos:** PostgreSQL (jobs y resultados).
- **Message broker:** Redis (cola de trabajos; desacopla la API del Worker).
- **IA:** OpenRouter (modelos GPT) mediante un cliente HTTP propio contra `https://openrouter.ai/api/v1/chat/completions`.
- **Frontend (opcional, básico, más adelante):** React + TypeScript. Fuera de alcance hasta que API y Worker funcionen.

### Preferencias de código

- Prefiere la biblioteca estándar de Go donde sea razonable (`net/http` con el `ServeMux` de patrones, `log/slog`, `context`, `encoding/json`) y evita frameworks web.
- Dependencias externas mínimas y justificadas: driver de PostgreSQL (`pgx`), cliente de Redis (`go-redis`), UUID y una herramienta de migraciones. Antes de añadir cualquier otra, di por qué.
- Errores explícitos y envueltos con contexto (`fmt.Errorf("...: %w", err)`). Sin `panic` en flujo normal.
- Configuración solo por variables de entorno, sin secretos en el repo.

## Estructura del repositorio

Monorepo Go con dos binarios que comparten paquetes internos:

```
cmd/
  api/        # servidor HTTP
  worker/     # consumidor de la cola
internal/
  config/     # carga de variables de entorno
  db/         # acceso a PostgreSQL (repositorio de jobs)
  models/     # structs compartidos (Job, Digest, estados)
  queue/      # abstracción sobre Redis
  scraper/    # extracción de contenido de una URL
  ai/         # cliente de OpenRouter
migrations/   # SQL versionado
```

Es una propuesta: si al explorar ves una razón para ajustarla, dila antes de mover cosas.

## Flujo asíncrono

El servidor web nunca debe bloquearse por scraping o llamadas a IA.

1. **Recepción (API)**
   - `POST /api/v1/digests` con `{ "url": "https://ejemplo.com/articulo" }`.
   - Valida el payload, genera un UUID, guarda el job en PostgreSQL con estado `pending` y encola `{job_id, url}` en Redis.
   - Responde `202 Accepted` con el `job_id` y un mensaje de "en proceso". Objetivo: respuesta en menos de 50 ms (no hace nada pesado en la petición).
2. **Procesamiento (Worker)**
   - Escucha la cola de Redis de forma continua.
   - Procesa los trabajos de forma concurrente con un pool de goroutines de tamaño configurable (no una goroutine ilimitada por trabajo).
   - Marca el job como `processing`.
   - **Paso A, scraping:** descarga la URL y extrae el texto principal.
   - **Paso B, IA:** envía el texto a OpenRouter pidiendo salida JSON estricta.
3. **Consolidación (Worker)**
   - Valida el JSON recibido contra los structs de Go antes de guardarlo.
   - Actualiza el job a `completed` con el resultado, o a `failed` con un motivo legible si algo falla.
4. **Consulta (API)**
   - `GET /api/v1/digests/{job_id}` devuelve el estado y, si está `completed`, el JSON estructurado.

### Estados del job

`pending` → `processing` → `completed` | `failed`

## Consideraciones técnicas

### Tipado fuerte

El resultado de la IA se mapea a structs de Go bien definidos y se valida antes de persistirlo. Si el JSON no encaja con el esquema, el job termina en `failed`; no se guarda basura. El resultado se persiste en una columna `JSONB`.

### OpenRouter

- Endpoint: `https://openrouter.ai/api/v1/chat/completions` (formato compatible con OpenAI).
- **Autenticación:** header `Authorization: Bearer <OPENROUTER_API_KEY>`.
- `HTTP-Referer` y `X-Title` son **opcionales** y solo sirven para atribución de la app en OpenRouter. No son de autenticación.
- Salida estructurada: usa `response_format` con `json_schema` (modo estricto). No todos los modelos ni proveedores soportan structured outputs; el modelo debe ser configurable por variable de entorno y hay que verificar que el elegido lo soporte. Si se necesita, restringe el enrutamiento a proveedores que acepten los parámetros pedidos.
- Timeouts explícitos en el cliente HTTP y manejo de respuestas de error y de límites de tasa.

### Seguridad y robustez del scraper

La URL viene del cliente y el servidor la descarga, así que:

- Acepta solo `http` y `https`.
- Bloquea destinos internos o privados (loopback, rangos privados, metadata de nube) para evitar SSRF, también tras redirecciones.
- Timeout de conexión y lectura, límite de tamaño del cuerpo y límite de redirecciones.

### Cola

Redis como broker. Al elegir la estructura (lista con `BRPOP` vs. Redis Streams con grupos de consumidores), considera qué pasa si el worker cae a mitad de un trabajo. Propón la opción más simple que no pierda jobs de forma silenciosa y explica el trade-off.

## Decisiones abiertas

Resuélvelas al llegar a cada punto, con la opción más simple y explicando el porqué; pregunta solo si no puedes decidir con seguridad:

- Herramienta de migraciones.
- Lista vs. Streams en Redis.
- Reintentos ante fallos transitorios (cuántos y con qué espera).
- Librería o enfoque para extraer el texto principal del HTML.

## Modo de trabajo

- Workflow: ODD.
- TDD: **[completar: estricto]**.
- Cada unidad de trabajo cierra con un commit convencional en la rama de la feature, con tests y documentación junto al comportamiento.

## Primera tarea

Alcance acotado: base de datos y API HTTP mínima. **Sin worker, sin scraping y sin IA todavía.**

1. Inicializa el módulo Go y la estructura base mínima necesaria (`cmd/api`, `internal/config`, `internal/db`, `internal/models`).
2. PostgreSQL: migración inicial con la tabla `jobs` (`id` UUID, `url`, `status`, `result` JSONB nullable, `error` nullable, `created_at`, `updated_at`), más la conexión y un repositorio con crear y obtener por id.
3. API:
   - `POST /api/v1/digests` → valida la URL, crea el job en `pending` y responde `202` con el `job_id`. Por ahora no encola nada (la cola llega en la siguiente tarea).
   - `GET /api/v1/digests/{job_id}` → devuelve el estado del job (`404` si no existe).
   - Un endpoint de salud (`/healthz`).
4. `docker-compose.yml` con PostgreSQL (y Redis ya declarado para la siguiente tarea) y un `.env.example`.

Criterio de hecho: se levanta con Docker Compose, los dos endpoints funcionan con `curl` y hay tests para la validación de la URL y el repositorio.

## Siguientes tareas (para después, no ahora)

1. Cola en Redis y publicación desde la API.
2. Worker con pool de goroutines, y scraper con las protecciones descritas.
3. Cliente de OpenRouter con salida estructurada y validación contra los structs.
4. Consolidación de resultados, estados `failed` y reintentos.
5. Frontend básico en React + TypeScript.
