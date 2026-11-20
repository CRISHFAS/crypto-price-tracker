# Decisiones de diseño

Cada decisión indica qué se eligió, qué alternativas había, por qué y qué consecuencias tiene. Lo que no se pudo verificar está marcado como «sin verificar».

## 1. Desarrollar en GitHub Codespaces y no en Docker Desktop local
- **Decisión:** GitHub Codespaces (máquina Linux de 2 núcleos y 7,8 GiB de RAM) con Docker Engine dentro, mediante la característica docker-in-docker.
- **Alternativas:** Docker Desktop en el PC, que se descartó, y Docker Engine en WSL2, que queda como segunda fase.
- **Motivo:** el PC tiene 7,8 GB de RAM y un Celeron de 2 hilos, y Windows con el navegador ya consumen unos 3 GB: la máquina virtual de Docker, la JVM de Kafka, la de Spring Boot y el editor no caben con margen.
- **Consecuencias:** el entorno se reconstruye desde GitHub y se borra sin dejar rastro, a cambio de una cuota gratuita de 120 horas-núcleo al mes (unas 60 horas en 2 núcleos) y de que `/tmp` se vacía al detener el codespace.

## 2. Kafka 4.x en modo KRaft con la imagen oficial
- **Decisión:** la imagen `apache/kafka:4.3.1` en un solo nodo KRaft, con el heap limitado (`KAFKA_HEAP_OPTS`), descrita en `compose.yaml`.
- **Alternativas:** Kafka nativo en Windows, ZooKeeper y etiquetas flotantes como `latest`.
- **Motivo:** la serie 4.x ya no trae ZooKeeper ni scripts `.bat`, y una etiqueta flotante (`ubuntu`) rompió la construcción del devcontainer.
- **Consecuencias:** Kafka en reposo usa unos 274 MiB, pero `docker compose down` borra sus datos porque el volumen no está en la ruta que usa la imagen (TROUBLESHOOTING, entrada 13).

## 3. Java 25, Spring Boot 4.1 y Jackson 3
- **Decisión:** Java 25, Spring Boot 4.1.2, Spring Kafka 4.1.2 y Jackson 3 (`JacksonJsonSerializer` y `JacksonJsonDeserializer`).
- **Alternativas:** Spring Boot 4.0 o la serie 3.x.
- **Motivo:** según la búsqueda de octubre de 2026, Boot 4.0 pierde el soporte gratuito el 31/12/2026 y la 3.x ya no lo tiene.
- **Consecuencias:** Spring Kafka 4 eliminó Spring Retry y dejó obsoletas `JsonSerializer` y `JsonDeserializer`, así que hubo que verificar la documentación en vez de copiar ejemplos antiguos.

## 4. Un repositorio por micro app
- **Decisión:** un repositorio por micro app. `crypto-price-tracker` es el primero.
- **Alternativas:** un monorepo y un repositorio de pruebas (`kafka-lab`).
- **Motivo:** cada app se termina, se publica y se enseña por separado en el portafolio, y el entorno se validó dentro de la propia app.
- **Consecuencias:** cada repositorio necesita su README, su CI y su documentación.

## 5. Versiones fijas en todo
- **Decisión:** versiones fijas: imagen base `ubuntu-24.04`, `apache/kafka:4.3.1`, runner `ubuntu-24.04` y JDK 25 de Microsoft.
- **Alternativas:** `latest`, `ubuntu` y `ubuntu-latest`.
- **Motivo:** una etiqueta flotante dejó el codespace en modo recuperación, y el CI avisó de que `ubuntu-latest` migraría a Ubuntu 26 desde el 19/10/2026.
- **Consecuencias:** las versiones se actualizan a mano; la 24.04 sigue mantenida según la documentación consultada (deducido de que solo se mantienen las dos últimas versiones, sin calendario de retirada visto).

## 6. El símbolo es la clave del mensaje
- **Decisión:** el símbolo es la clave en `prices.raw` y en `prices.processed`, con 3 particiones cada uno.
- **Alternativas:** mensajes sin clave.
- **Motivo:** la clave decide la partición, así que los precios de un símbolo conservan su orden y los lee un solo hilo del grupo.
- **Consecuencias:** BTC y ETH comparten la partición 1, SOL usa la 0 y la 2 queda vacía (verificado); con tres símbolos el reparto es desigual.

## 7. Tres consumer groups independientes
- **Decisión:** tres grupos sobre `prices.raw`: `price-processor` (registro y Dead Letter Topic), `price-stream` (panel) y `price-analytics` (variaciones).
- **Alternativas:** un solo grupo con varios listeners.
- **Motivo:** cada grupo recibe todos los mensajes y lleva su propio progreso, así que un fallo o un retraso en uno no afecta a los otros.
- **Consecuencias:** cada mensaje se procesa tres veces, y un mensaje ilegible genera un registro en el DLT por grupo (tres, comprobado en un test).

## 8. Server-Sent Events para el panel
- **Decisión:** el panel recibe los precios por Server-Sent Events (`/api/prices/stream`), con `/api/prices/latest` para el estado inicial.
- **Alternativas:** polling y WebSocket.
- **Motivo:** el flujo va solo del servidor al navegador, y SSE lo resuelve con HTTP y reconexión automática (`EventSource`).
- **Consecuencias:** las conexiones y los últimos precios viven en la memoria de cada instancia, un límite para escalar (ver `escalabilidad.md`).

## 9. Reintentos bloqueantes y Dead Letter Topic
- **Decisión:** `DefaultErrorHandler` con 4 intentos separados por 1 s y publicación en `prices.raw-dlt` (3 particiones).
- **Alternativas:** reintentos no bloqueantes con topics de reintento.
- **Motivo:** es lo más simple y basta para esperas de segundos; las excepciones no reintentables, como `DeserializationException`, van directas al DLT.
- **Consecuencias:** mientras se reintenta, la partición espera: se midió un retraso de unos 3,6 s en el mensaje siguiente.

## 10. Deserializador envuelto y DLT con dos plantillas
- **Decisión:** `ErrorHandlingDeserializer` envolviendo a `JacksonJsonDeserializer`, y un DLT con dos plantillas: bytes para los mensajes ilegibles y JSON para el resto.
- **Alternativas:** dejar el deserializador de Jackson sin envolver y usar una sola plantilla.
- **Motivo:** un mensaje que no era JSON dejó al consumidor en bucle y el log creció 26 MB; con una sola plantilla, el DLT guardaba el texto en base64.
- **Consecuencias:** el test del mensaje ilegible lo protege, y se comprobó que falla al romper esa configuración.

## 11. Estado del procesador confirmado después de publicar
- **Decisión:** el procesador lee el precio previo sin modificarlo, publica y espera la confirmación del broker (hasta 10 s) y solo entonces confirma el estado.
- **Alternativas:** actualizar el estado antes de publicar y un almacén externo.
- **Motivo:** con la actualización antes del envío, un reintento publicaría `previousPrice` igual al precio y `changePercent` 0, sin ningún error.
- **Consecuencias:** el estado y la guarda de offset viven en memoria y se pierden al reiniciar, así que el primer precio de cada símbolo sale con `null`.

## 12. `eventId` determinista, entrega al menos una vez y sin transacciones
- **Decisión:** `eventId` determinista (`prices.raw:<partición>:<offset>`), entrega al menos una vez y una guarda que salta un mensaje solo si la partición coincide y el offset es menor o igual al confirmado.
- **Alternativas:** un UUID aleatorio y las transacciones de Kafka.
- **Motivo:** un UUID no sirve para deduplicar, y las transacciones exigen más piezas mientras el estado en memoria no es transaccional (el efecto de las transacciones sobre el simulador y los reintentos se razonó, sin verificar).
- **Consecuencias:** puede haber duplicados en `prices.processed` (los consumidores deben deduplicar por `eventId`) y no se detecta un productor que envía el mismo dato dos veces; las transacciones se retomarán con el patrón Outbox.

## 13. `latest` y descarte de precios no positivos
- **Decisión:** `price-analytics` lee con `latest` y el procesador ignora los precios no positivos.
- **Alternativas:** `earliest`.
- **Motivo:** con `earliest`, un grupo nuevo releería los mensajes ilegibles antiguos y los duplicaría en el DLT; descartar es seguro porque `price-processor` ya los envía al DLT.
- **Consecuencias:** lo enviado antes de que el grupo tenga particiones asignadas se pierde, y por eso los tests esperan la asignación (la carrera no se llegó a observar: 6 de 6 ejecuciones pasaron sin esa espera).

## 14. Contrato del evento de salida
- **Decisión:** `PriceProcessedEvent` versión 1 con `BigDecimal`, `previousPrice` y `changePercent` en `null` en el primer precio de cada símbolo, y variación con 4 decimales (`HALF_UP`).
- **Alternativas:** `double` y un cero en el primer precio.
- **Motivo:** los decimales binarios acumulan errores, y un cero afirmaría «no cambió», que es falso.
- **Consecuencias:** los consumidores deben tolerar `null`, y el campo `version` permite evolucionar el esquema.

## 15. Tests de integración con un Kafka efímero
- **Decisión:** los tests de integración usan un Kafka efímero (`apache/kafka:4.3.1` con Testcontainers y `@ServiceConnection`), con el contexto compartido entre clases y un símbolo único por test.
- **Alternativas:** seguir usando el Kafka de desarrollo.
- **Motivo:** el test de contexto antiguo dejó seis mensajes sin consumir en el broker de desarrollo; con uno propio los tests son repetibles y usan la imagen de producción.
- **Consecuencias:** necesitan Docker, y cada test se probó rompiendo a propósito lo que protege (mutaciones): un test que nunca ha fallado no demuestra nada.

## 16. CI con GitHub Actions
- **Decisión:** un flujo en cada push a `main` y en cada pull request, con `ubuntu-24.04`, JDK 25 de Microsoft, caché de Maven, `./mvnw -B -ntp verify`, permisos `contents: read` y un límite de 15 minutos.
- **Alternativas:** `ubuntu-latest`.
- **Motivo:** reproduce el entorno de desarrollo y ejecuta los mismos tests, incluidos los que necesitan Docker.
- **Consecuencias:** cada ejecución dura alrededor de un minuto, y el CI no tiene acceso a Kraken, así que el cliente va apagado por defecto.

## 17. Precios reales de Kraken, opcionales
- **Decisión:** precios reales opcionales del canal `ticker` del WebSocket v2 de Kraken (BTC/USD, ETH/USD y SOL/USD), apagados por defecto (`app.kraken.enabled=false`), con el simulador como origen predeterminado.
- **Alternativas:** CoinGecko por REST y Binance.
- **Motivo:** el plan gratuito de CoinGecko da 10.000 llamadas al mes (una cada 4,3 minutos), insuficiente para un panel en vivo; `api.binance.com` bloquea las IP de EE. UU. y el codespace está en US East (no se probó); el canal público de Kraken no necesita clave.
- **Consecuencias:** antes de una demo pública con datos reales hay que leer los términos de Kraken (fuentes secundarias dicen que la redistribución comercial requiere aprobación), y solo se aceptan pares en USD.

## 18. Diseño del cliente de Kraken
- **Decisión:** un cliente WebSocket del JDK que reconecta con una espera que se duplica hasta 60 s, un vigilante que fuerza la reconexión tras 60 s sin mensajes y un contador de intentos que se reinicia con el primer ticker válido; sin limitar el ritmo.
- **Alternativas:** un `ping` periódico y limitar el ritmo por símbolo.
- **Motivo:** el formato del `ping` no se verificó, y un silencio de 60 s con BTC operando indica una conexión muerta; el ritmo medido fue de unos 88 mensajes por minuto, con el procesador al día (lag de 0 a 1).
- **Consecuencias:** no se ha probado reconectar con una conexión viva que se rompe (con una dirección inválida sí se comprobó la espera hasta el tope), y el ritmo es una muestra de un minuto.
