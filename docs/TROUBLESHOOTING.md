# Problemas encontrados y cómo se resolvieron

Cada entrada sigue el formato síntoma, causa, solución y prevención. Lo que no se pudo comprobar está marcado como «sin verificar».

## 1. «Rebuild Container» falla con `command not found`
- **Síntoma:** al reconstruir el codespace, el comando no se encontraba.
- **Causa:** la extensión de Codespaces aún no estaba cargada (modo restringido).
- **Solución:** confiar en la carpeta, recargar y reintentar.
- **Prevención:** confiar en la carpeta al abrir el codespace.

## 2. El codespace arranca en modo recuperación, sin Java ni Docker
- **Síntoma:** tras reconstruir, el entorno no tenía Java ni Docker.
- **Causa:** la etiqueta flotante `base:ubuntu` pasó a una versión nueva de Ubuntu en la que la característica `docker-in-docker` no puede instalar `moby-cli`.
- **Solución:** fijar `base:ubuntu-24.04` y reconstruir.
- **Prevención:** usar siempre etiquetas con versión fija.

## 3. `git push` falla con un aviso de git-lfs
- **Síntoma:** el push se interrumpía con un aviso de Git LFS.
- **Causa:** había un hook de Git LFS configurado y `git-lfs` no estaba instalado.
- **Solución:** `sudo apt-get install -y git-lfs` y reintentar.
- **Prevención:** añadir la característica `ghcr.io/devcontainers/features/git-lfs:1` al devcontainer.

## 4. `unzip -o` sobrescribe el `.gitignore`
- **Síntoma:** al descomprimir el proyecto generado desapareció el `.gitignore` del repositorio.
- **Causa:** `-o` pisa archivos sin preguntar.
- **Solución:** fusionar los dos archivos.
- **Prevención:** usar `unzip -n`.

## 5. Un `sed` con comentarios deja la terminal esperando comillas
- **Síntoma:** la terminal no devolvía el control al pegar el comando.
- **Causa:** caracteres especiales al pegar.
- **Solución:** cancelar con `Ctrl+C`.
- **Prevención:** escribir los archivos con `cat > archivo <<'EOF'` en vez de `sed` complejos.

## 6. `./mvnw -q dependency:tree` no imprime nada
- **Síntoma:** el comando terminaba sin salida.
- **Causa:** `-q` oculta el árbol.
- **Solución:** `-DoutputFile=` y filtrar el archivo.
- **Prevención:** no combinar `-q` con comandos cuya salida es el resultado.

## 7. El Dead Letter Topic no recibe los mensajes fallidos
- **Síntoma:** el mensaje fallido no llegaba a `prices.raw.DLT`; el log decía `Destination resolver returned non-existent partition`.
- **Causa:** el sufijo por defecto del recuperador es `-dlt`, y el topic se había creado con otro nombre. Kafka creó automáticamente un topic con una sola partición y ocultó el error.
- **Solución:** renombrar a `prices.raw-dlt`, borrar los topics y recrearlos.
- **Prevención:** comprobar el nombre real en el log y, en producción, desactivar la creación automática de topics.

## 8. Un mensaje que no es JSON atasca al consumidor y llena el log
- **Síntoma:** un mensaje ilegible detuvo al grupo (lag creciente también en otra partición) y el log creció 26 MB en menos de un minuto.
- **Causa:** el fallo de deserialización ocurre dentro del cliente de Kafka, antes de que Spring pueda actuar, y se repite en cada `poll`.
- **Solución:** `ErrorHandlingDeserializer` envolviendo a `JacksonJsonDeserializer`. El error va directo al DLT, sin reintentos.
- **Prevención:** el test de integración del mensaje ilegible lo protege.

## 9. El DLT guarda el mensaje ilegible en base64
- **Síntoma:** el DLT tenía el texto original como una cadena en base64 entre comillas.
- **Causa:** el `KafkaTemplate` serializa con Jackson, que representa un `byte[]` en base64.
- **Solución:** dos plantillas en un `LinkedHashMap` (la de bytes para `byte[].class`, que va primero, y la de JSON para el resto).
- **Prevención:** el test del mensaje ilegible falla si se rompe esa configuración; se comprobó con una mutación.

## 10. 404 en el navegador aunque `curl` da 200
- **Síntoma:** la página daba 404 en el navegador y `curl localhost:8080/` devolvía 200.
- **Causa:** caché del navegador, que guardó el 404 de cuando la aplicación aún no estaba corriendo.
- **Solución:** recarga forzada con `Ctrl+Shift+R`.
- **Prevención:** diagnosticar de dentro hacia fuera: primero `curl`, después el navegador.

## 11. Un `grep` con un patrón amplio da cifras erróneas
- **Síntoma:** `grep "= -1"` daba 14 coincidencias en vez de un múltiplo de 4.
- **Causa:** el patrón también coincidía con líneas de configuración de Kafka (`acks = -1`).
- **Solución:** patrones precisos que incluyan contexto, como `BTC = -1`.
- **Prevención:** comprobar el patrón con un control positivo y uno negativo.

## 12. Al reanudar el codespace, Kafka no está en marcha
- **Síntoma:** `docker exec` decía `container ... is not running`, y la aplicación arrancada sin broker llenó el log (16,5 MB, 2457 avisos de conexión) mientras `/actuator/health` respondía 200.
- **Causa:** al detener el codespace se detienen los contenedores, y al reanudarlo no arrancan solos.
- **Solución:** `docker compose up -d` y comprobar con `docker ps`.
- **Prevención:** hacer la rutina de reanudación antes de arrancar la aplicación. El health no demuestra que haya broker.

## 13. `docker compose down` borra los datos de Kafka
- **Síntoma:** tras `down` y `up -d` no quedaba ningún topic.
- **Causa:** la imagen `apache/kafka` escribe en `/tmp/kraft-combined-logs`, y el volumen estaba montado en `/var/lib/kafka/data`, que quedó vacío. Antes sobrevivían a los reinicios del contenedor, probablemente en su capa de escritura (sin verificar), y `down` la elimina.
- **Solución actual:** usar `docker compose stop` al cerrar. Los datos de desarrollo son regenerables.
- **Arreglo pendiente:** declarar la configuración completa de la imagen, con `KAFKA_LOG_DIRS=/var/lib/kafka/data`, y probarlo con un volumen nuevo. Una variable de servidor sola no basta: el contenedor salió con `Missing required configuration "process.roles"`, porque la documentación de la imagen indica que cualquier configuración sobrescrita anula las de por defecto.

## 14. Kafka queda en `Exited (255)` tras detener el codespace
- **Síntoma:** el estado del contenedor era 255 al reanudar.
- **Causa:** sin explicar. Ocurrió en los casos en que el contenedor seguía en marcha al detener el codespace.
- **Solución y prevención:** con `docker compose stop` antes de cerrar, el estado es `Exited (143)`, una terminación limpia por SIGTERM, y se mantuvo estable tras detener el codespace.

## 15. Una medición vacía parece válida
- **Síntoma:** una comparación de offsets «sin diferencias» que en realidad comparaba dos archivos vacíos.
- **Causa:** `2>/dev/null` o un `grep` dentro de la función de medición ocultaron el error de Docker.
- **Solución:** quitar `2>/dev/null` y hacer que la función falle a gritos.
- **Prevención:** comprobar el número de líneas esperado con `wc -l`. Una salida vacía no es una confirmación.

## 16. `python3` no está instalado en el codespace
- **Síntoma:** `python3: command not found` al verificar mensajes.
- **Causa:** el devcontainer no lo incluye.
- **Solución:** un verificador en Java de archivo único (`java Archivo.java`).
- **Prevención:** no asumir herramientas: comprobar antes.

## 17. `--property` está obsoleto en las herramientas de Kafka 4.x
- **Síntoma:** advertencia de opción obsoleta, que además acababa en los archivos de salida.
- **Causa:** la opción se renombró.
- **Solución:** `--reader-property` en el productor y `--formatter-property` en el consumidor.
- **Prevención:** filtrar las líneas que no empiezan por `Partition:` al procesar la salida.

## 18. Secuencias de control en los archivos de salida
- **Síntoma:** líneas como `^[]633;...` al comparar archivos con `diff`.
- **Causa:** el terminal integrado de VS Code las inyecta al redirigir bucles (sin verificar el mecanismo exacto).
- **Solución:** filtrar con `grep '^prices'` antes de comparar.
- **Prevención:** revisar con `cat -A` cuando un `diff` marque una línea que parece igual.

## 19. `/tmp` se vacía al detener el codespace
- **Síntoma:** desaparecían los logs, las copias y las funciones de la terminal.
- **Causa:** sin verificar. Se observó en todas las paradas; la hipótesis es que `/tmp` se limpia en cada arranque.
- **Solución y prevención:** guardar lo que importa en el repositorio o en la carpeta personal, que sí sobrevivió.

## 20. Al reescribir un archivo se pierde su última línea
- **Síntoma:** el README perdió la línea `MIT`.
- **Causa:** el original no terminaba en salto de línea y `cat -n` no la mostraba.
- **Solución:** restaurarla con `printf`.
- **Prevención:** editar con `sed` en vez de reescribir, y mirar antes cómo termina el archivo con `tail -c` y `od -c`.

## 21. `Ctrl+C` no deja ver el cierre de la aplicación en el log
- **Síntoma:** el log termina con un mensaje normal, sin líneas de cierre.
- **Causa:** `tee` también recibe la interrupción y muere (explicación plausible, sin verificar).
- **Solución:** `tee -i`, que ignora la interrupción (sin probar).
- **Prevención:** medir el cierre con la hora de la última línea del log.

## 22. Los tests de contexto dejaban mensajes en el Kafka de desarrollo
- **Síntoma:** seis mensajes pendientes en `prices.raw` tras ejecutar `./mvnw test`.
- **Causa:** el test de contexto arrancaba la aplicación contra `localhost:9092`.
- **Solución:** un Kafka efímero con Testcontainers y `@ServiceConnection`.
- **Prevención:** los tests de integración nunca usan el broker de desarrollo; se comprobó con los offsets.
