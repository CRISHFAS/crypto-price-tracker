# Cómo escalarlo

Qué limita hoy al sistema, cómo se resolvería y qué se ha medido de verdad. «Medido» significa que se observó en las pruebas; «razonado» significa que sale del diseño, sin probarlo.

## Qué limita hoy al sistema

| Límite | Por qué | Cómo se resolvería | Estado |
|---|---|---|---|
| Paralelismo de consumo | Hay 3 particiones y 3 símbolos: BTC y ETH comparten la partición 1, SOL usa la 0 y la 2 está vacía. Con más instancias que particiones, las sobrantes no hacen nada | Más particiones y más símbolos distintos como clave | Medido el reparto |
| Panel con varias instancias | Las conexiones SSE y los últimos precios viven en la memoria de cada instancia, y con el mismo grupo se repartirían las particiones | Un grupo propio por instancia, o un intermediario de mensajes | Razonado |
| Estado del procesador | Está en memoria y se pierde al reiniciar: el primer precio de cada símbolo sale con `null` | Un almacén externo | Medido el efecto del reinicio |
| Entrega al menos una vez | Un fallo entre publicar y confirmar el offset, o un envío tardío tras un tiempo de espera, puede duplicar un evento en `prices.processed` | Consumidores que deduplican por `eventId`; transacciones o patrón Outbox | Razonado |
| Reintentos bloqueantes | Mientras se reintenta un mensaje, su partición espera: el siguiente salió unos 3,6 s después | Reintentos no bloqueantes con topics de reintento | Medido |
| Cliente de Kraken | Cada instancia con el feed abierto abriría su propia conexión y publicaría los mismos precios. Kraken limita las reconexiones por IP (unos 150 intentos cada 10 minutos, según una fuente secundaria) | Un único proceso con el feed | Razonado |
| Durabilidad de Kafka | Un solo nodo con 1 réplica, y `docker compose down` borra los datos porque el volumen no está en la ruta que usa la imagen | 3 nodos con 3 réplicas, `min.insync.replicas=2`, creación automática de topics desactivada y la configuración completa de la imagen | Medido el borrado |
| Rendimiento del procesador | Espera la confirmación de cada envío (hasta 10 s) antes de pasar al siguiente mensaje de su partición | Más particiones, o enviar sin esperar cada confirmación con otra estrategia de errores | No probado con carga: con 1,5 mensajes por segundo el retraso fue de 0 a 1 mensaje |
| Observabilidad | Solo Actuator, y `/actuator/health` responde `UP` aunque no haya broker | Métricas del retraso por grupo y comprobar la conexión con Kafka | Medido |

## Qué se midió y qué no

- **Medido:** unos 88 mensajes por minuto con tres símbolos (una muestra de un minuto), retraso de 0 a 1 mensaje en el procesador, reparto por particiones y el efecto de los reintentos.
- **Sin probar:** carga alta, varias instancias, la caída de un broker en caliente y la reconexión con una conexión viva que se rompe.

## Camino hacia producción

1. Kafka de 3 nodos con `replication.factor=3` y `min.insync.replicas=2`, y creación automática de topics desactivada.
2. Persistir el estado del procesador y deduplicar por `eventId` en los consumidores.
3. Panel con un grupo por instancia, o un intermediario entre Kafka y los navegadores.
4. Un único proceso con el cliente de Kraken.
5. Métricas del retraso por grupo y alertas.
