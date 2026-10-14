# IntelDigest — Convenciones y reglas para agentes

## Forma de trabajar

- Revisar la rama y `git status` antes de editar. Preservar cambios ajenos.
- Leer [README.md](README.md) para uso y configuración; [PLAN.md](PLAN.md) es la única fuente de alcance y planificación. Este archivo no contiene el roadmap.
- Trabajar con ODD: explorar primero, delimitar el alcance, resolver dudas relevantes, implementar en unidades pequeñas y verificar antes de dar por terminado.
- No convertir una consulta o propuesta en autorización para escribir código. No agregar funcionalidades, dependencias ni infraestructura fuera de la tarea acordada.
- Para cambios de comportamiento con tests aplicables, observar RED antes del cambio, implementar lo mínimo y verificar GREEN. Incluir cobertura del comportamiento previo que comparte el código modificado.
- Mantener comportamiento, tests y documentación juntos en una unidad revisable. Commits convencionales solo cuando estén autorizados; push, PR y merge requieren su propia autorización.
- Informar checks fallidos, omitidos o pendientes. No presentar evidencia histórica como verificación del estado actual.

## Convenciones de código

- Go idiomático y biblioteca estándar primero: `net/http`, `context`, `encoding/json` y `log/slog`. Sin frameworks web ni abstracciones innecesarias.
- Justificar nuevas dependencias antes de agregarlas. Mantener `go.mod` y `go.sum` coherentes cuando su modificación esté autorizada.
- Interfaces pequeñas definidas en el consumidor; fakes deterministas para aislar servicios externos en tests.
- Errores explícitos con contexto y `%w`; no usar `panic` como flujo normal.
- Propagar `context.Context`, cancelación y límites de tiempo. Evitar goroutines ilimitadas y colas de memoria sin cota.
- Configuración por variables de entorno, sin cargadores dotenv en la aplicación.
- Identificadores, comentarios de código y mensajes de commit en inglés; documentación existente del proyecto en su idioma correspondiente.
- Al terminar cada sección o unidad de implementación, y cuando un cambio lo requiera, revisar y actualizar el README y/o el diagrama afectado en la misma unidad. Reflejar el comportamiento realmente implementado y sus límites; no marcar funcionalidades previstas como terminadas.
- Los diagramas de flujo y arquitectura deben usar bloques Mermaid con `flowchart`; no usar esquemas ASCII. El README representa el estado actual y PLAN.md el objetivo, claramente diferenciado.
- Cuando cambien comandos, configuración, mensajes o contratos, actualizar la documentación relacionada en la misma unidad.

## Límites arquitectónicos y contratos

- `cmd/api` y `cmd/worker` son procesos separados que comparten paquetes `internal/`. No ejecutar scraping ni IA dentro del handler HTTP.
- PostgreSQL es la fuente de verdad del job; Redis transporta mensajes. Preservar el formato versionado `schema_version`, `job_id`, `url` salvo un cambio de contrato explícitamente acordado.
- La creación en PostgreSQL precede a la publicación Redis. Un fallo de publicación no revierte el registro: la API devuelve `503` con el `job_id`. No prometer atomicidad entre ambos sistemas.
- Preservar el claim atómico condicionado por estado. No sustituirlo por una lectura seguida de una escritura incondicional.
- La validación inicial de URL no resuelve DNS. La política de destinos se aplica al descargar: validar DNS/IP, conectar a IP validada, mantener TLS normal y revalidar redirecciones.
- No debilitar SSRF, habilitar proxies implícitos ni desactivar verificación TLS. Mantener límites de red, cuerpo y redirecciones.
- Las opciones exclusivas del worker no deben cambiar la validación de arranque de la API.
- La ventana de drenaje de apagado no es una garantía de salida del proceso en ese plazo exacto.
- No registrar claves, credenciales, texto completo del artículo ni contenido sensible. Usar metadatos y errores seguros y acotados.

## Compilación y checks

La versión requerida es la declarada en `go.mod` (actualmente Go `1.27.1`). No cambiarla como parte incidental de otra tarea.

| Comando | Uso |
|---|---|
| `go build ./...` | Compilar todos los paquetes y comandos sin arrancar servicios |
| `go test ./...` | Tests unitarios sin PostgreSQL/Redis externos |
| `go test -race ./...` | Tests unitarios con detector de carreras |
| `go vet ./...` | Análisis estático |
| `make test` | Alias de los tests unitarios |
| `make test-integration` | Integración local serializada; requiere autorización y servicios listos |

Formatear los archivos Go modificados con `gofmt`. Ejecutar el test focalizado y los checks pertinentes al cambio; para documentación pasiva, verificar estructura, referencias y diff sin inventar evidencia de runtime.

Con dependencias listas, migración aplicada y variables exportadas por la persona:

```sh
go run ./cmd/api
go run ./cmd/worker
```

Son dos procesos: ejecutarlos en terminales separadas. Estos comandos realizan operaciones reales sobre servicios; no son checks de compilación y no se ejecutan como parte implícita de una exploración.

## Entorno, secretos y operaciones

- No leer, editar, buscar contenido, mostrar diffs, cargar con `source` ni acceder indirectamente a `.env` o `.env.*`, incluido `.env.example`. No intentar eludir restricciones de acceso.
- Usar configuración proporcionada por la persona y `internal/config/` como referencia. El texto pegado en una conversación no prueba el contenido del archivo en disco.
- Docker Compose puede cargar `.env` implícitamente. No ejecutarlo bajo esta restricción sin autorización específica que resuelva ese acceso.
- No arrancar/parar servicios, aplicar migraciones, borrar bases/volúmenes ni modificar datos como parte implícita de una revisión.
- `make up` inicia solo PostgreSQL, no Redis. `make down` detiene servicios. `make migrate` requiere `psql` y `DATABASE_URL` exportada, y modifica la base indicada. Acordar su ejecución antes de usarlos.
- No usar credenciales de producción ni llamar a servicios de IA pagos sin autorización. Los tests automatizados deben usar respuestas deterministas, no consumir crédito.

## Seguridad de los tests de integración

- `make test-integration` fija URLs locales y ejecuta `internal/db`, `internal/queue` e `internal/worker` con `-tags=integration -p 1 -count=1`.
- Los harnesses PostgreSQL eliminan y recrean `inteldigest_test`, y la eliminan en cleanup. Obtener autorización explícita para ese efecto; no ejecutar contra instancias compartidas.
- No ejecutar estos paquetes ni invocaciones de integración simultáneamente: comparten el nombre de la base de tests.
- Los tests Redis usan streams/grupos únicos y eliminan solo sus propias claves; nunca `FLUSHDB`/`FLUSHALL`.
- No modificar la base de aplicación `inteldigest` ni borrar su volumen para verificar tests.
- La integración del worker usa PostgreSQL/Redis reales con scraper falso. No equivale a ejecutar el CLI productivo ni a demostrar el circuito HTTP/IA real completo.
