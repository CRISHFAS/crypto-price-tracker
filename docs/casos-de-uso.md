# Casos de uso

Qué se puede hacer con la aplicación y qué capacidad de Kafka demuestra cada caso.

## Casos

| Caso | Qué demuestra | Cómo comprobarlo |
|---|---|---|
| Ver precios en vivo | Un flujo continuo de eventos con el símbolo como clave y Server-Sent Events hacia el navegador | `docker compose up -d` y `./mvnw spring-boot:run`, y abrir el puerto 8080 |
| Usar precios reales | Integrar una fuente externa por WebSocket, con reconexión | `APP_SIMULATOR_ENABLED=false APP_KRAKEN_ENABLED=true ./mvnw spring-boot:run` |
| Calcular la variación porcentual | Procesamiento con estado y publicación en un segundo topic | Leer `prices.processed` con la consola de Kafka (el comando está en el README) |
| Resistir un dato inválido | Reintentos con espera y Dead Letter Topic | Enviar un precio negativo (el README explica cómo) y leer `prices.raw-dlt` |
| Resistir un mensaje ilegible | Un deserializador que no se atasca y un DLT que conserva los bytes originales | `PriceFailureIntegrationTest` lo comprueba |
| Reproducir el entorno desde cero | Devcontainer y Docker Compose con versiones fijas | Abrir el repositorio en un codespace y ejecutar `docker compose up -d` |
| Comprobar que nada se rompe | Pruebas unitarias y de integración con un Kafka efímero, y CI en cada push | `./mvnw test` (necesita Docker) o la insignia del CI del README |

## Cómo leer este repositorio

1. El README: qué es, cómo ejecutarlo y cómo probar los fallos.
2. `docs/arquitectura.md`: el diagrama, los topics, los grupos y los paquetes.
3. `docs/flujo-de-eventos.md`: el recorrido de un precio, los dos eventos y los casos de fallo.
4. `docs/decisiones.md`: qué se eligió, qué alternativas había y por qué.
5. `docs/TROUBLESHOOTING.md`: los problemas reales y cómo se resolvieron.
6. `docs/escalabilidad.md`: qué limita hoy al sistema y qué haría falta en producción.

## Lo que no es

- No es un sistema de trading: muestra precios, no opera con ellos.
- Los precios simulados son un paseo aleatorio y derivan.
- El estado del procesador vive en memoria y se pierde al reiniciar.
- Los precios reales de Kraken son opcionales: antes de publicar una demo con ellos hay que leer los términos de uso de Kraken.
