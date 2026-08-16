# Práctica 3 — Protocolo de Transporte Confiable sobre UDP

Implementación en C de un protocolo de transporte propio sobre `SOCK_DGRAM`
(UDP) que garantiza entrega **confiable, en orden y sin duplicados** de un
archivo entre un `emisor` y un `receptor`, tolerando pérdida, duplicidad y
retardo de paquetes (simulables con `netem`).

## Tabla de contenidos

1. [Compilación y ejecución](#compilación-y-ejecución)
2. [Diseño del protocolo](#diseño-del-protocolo)
3. [Garantías: orden, no-duplicación, confiabilidad](#garantías-orden-no-duplicación-confiabilidad)
4. [Concurrencia y sincronización](#concurrencia-y-sincronización)
5. [Manejo de archivos](#manejo-de-archivos)
6. [Pruebas con netem](#pruebas-con-netem)
7. [Parámetros y límites](#parámetros-y-límites)
8. [Limitaciones y trabajo futuro](#limitaciones-y-trabajo-futuro)

---

## Compilación y ejecución

```bash
make          # compila ./emisor y ./receptor (gcc -Wall -Wextra -pthread)
make clean    # borra los binarios
```

### Uso

```bash
./receptor <puerto>
./emisor <host_destino> <puerto_destino> <nombre_archivo>
```

Ejemplo:

```bash
# Terminal 1
./receptor 5000

# Terminal 2
./emisor 127.0.0.1 5000 archivo_prueba.bin
```

El receptor escribe el archivo recibido en su directorio de trabajo con el
nombre `recibido_<nombre_original>` (ver [Manejo de archivos](#manejo-de-archivos)
para la justificación del prefijo y el saneo del nombre).

---

## Diseño del protocolo

### Formato de paquete

Header fijo de **15 bytes**, serializado a mano en network byte order (sin
depender de `struct` empaquetado, para evitar problemas de padding/endianness
entre plataformas):

```
+----------+----------------+-----------+--------+------------+
| seq_num  | total_chunks   | data_len  | type   | checksum   |
| 4 bytes  | 4 bytes        | 2 bytes   |1 byte  | 4 bytes    |
+----------+----------------+-----------+--------+------------+
|              payload (0..1024 bytes de datos del archivo)   |
+---------------------------------------------------------------+
```

- **`seq_num`**: número de chunk (0-indexado) para `DATA`/`ACK`.
- **`total_chunks`**: total de chunks de la transferencia (0 si el archivo
  está vacío).
- **`data_len`**: bytes válidos de payload en este paquete puntual (el
  último chunk suele medir menos que `CHUNK_SIZE` si el archivo no es
  múltiplo exacto de 1024).
- **`type`**: `INIT`, `INIT_ACK`, `DATA`, `ACK`, `FIN`, `FIN_ACK`.
- **`checksum`**: CRC32 sobre el header (con este campo en 0) + payload.
  Detecta corrupción a nivel de aplicación — un paquete cuyo checksum no
  coincide se descarta silenciosamente (se trata como si se hubiera
  perdido, disparando el mecanismo de retransmisión ya existente).

`CHUNK_SIZE = 1024` bytes → paquete máximo de 1039 bytes, por debajo del
MTU típico de 1500 bytes, para evitar fragmentación IP.

### Identificación de sesión

Se usa un esquema **estilo TFTP (RFC 1350)**:

1. El emisor envía `INIT` al puerto bien conocido del receptor (el que se
   pasa como argumento a `./receptor`). El payload de `INIT` lleva el
   tamaño del archivo (8 bytes, partido en dos mitades de 32 bits) y el
   nombre del archivo (sin ruta).
2. El receptor, al recibir un `INIT` nuevo, crea un **socket UDP con
   puerto efímero** (`bind` a puerto 0) y lanza un **hilo dedicado**
   (`session_thread`) para esa transferencia. Responde `INIT_ACK` **desde
   ese socket efímero**.
3. El emisor detecta el puerto de origen del `INIT_ACK` y hace `connect()`
   a esa dirección: el resto de la conversación (`DATA`/`ACK`/`FIN`/`FIN_ACK`)
   ocurre en ese par de sockets dedicado.

Esto identifica la sesión implícitamente por el par `(IP, puerto)` del
socket efímero, sin necesitar un campo de sesión adicional en el header, y
resuelve la concurrencia de paso: cada emisor termina hablando con un
socket y un hilo distintos del lado del receptor.

### Tipos de mensaje

| Tipo       | Dirección          | Propósito                                        |
|------------|--------------------|---------------------------------------------------|
| `INIT`     | emisor → receptor  | Solicita iniciar transferencia (tamaño + nombre)   |
| `INIT_ACK` | receptor → emisor  | Acepta e indica el puerto de sesión                |
| `DATA`     | emisor → receptor  | Chunk de datos con su `seq_num`                    |
| `ACK`      | receptor → emisor  | Confirma recepción de `seq_num`                    |
| `FIN`      | emisor → receptor  | Fin de archivo                                     |
| `FIN_ACK`  | receptor → emisor  | Confirma cierre                                    |

### Entrega en orden y retransmisión: stop-and-wait

Se eligió **stop-and-wait** (un solo paquete en vuelo) en vez de ventana
deslizante:

- El emisor solo avanza al chunk `N+1` después de recibir `ACK(N)`, así
  que la red nunca puede entregarle al receptor un chunk fuera de orden
  salvo por duplicados (retransmisiones), que se filtran aparte.
- Es más simple de razonar y depurar correctamente bajo pérdida,
  duplicidad y reordenamiento — justamente los tres fenómenos que hay que
  demostrar que el protocolo tolera.
- El costo es rendimiento: con RTT alto (por `delay` de netem) el
  throughput cae porque solo hay una confirmación en vuelo a la vez (ver
  [limitaciones](#limitaciones-y-trabajo-futuro)).

Cada envío de `INIT`/`DATA`/`FIN` usa un timeout (`TIMEOUT_MS = 300 ms`)
con `SO_RCVTIMEO` sobre el socket; si no llega la confirmación esperada a
tiempo, se reenvía el mismo paquete, hasta `MAX_RETRIES = 30` intentos
(~9 s) antes de abortar con error. Una respuesta que no corresponde al
`seq_num`/tipo esperado (por ejemplo, un `ACK` duplicado de un paquete
viejo) se ignora sin resetear el temporizador ni contar como fallo.

### Detección de duplicados

- **Lado receptor**: por sesión se mantiene un arreglo `received[total_chunks]`
  de booleanos. Al llegar un `DATA` con `seq_num` ya marcado, **no se
  vuelve a escribir** el chunk (evita duplicados en el archivo final), pero
  sí se reenvía el `ACK` correspondiente — necesario porque un duplicado
  típicamente ocurre cuando el `ACK` original se perdió y el emisor
  retransmitió el `DATA`; si no se reenviara el `ACK`, el emisor nunca se
  enteraría de que ya fue recibido y seguiría reintentando indefinidamente.
- **Lado emisor**: al ser stop-and-wait, solo hay un `seq_num` esperado a
  la vez; cualquier `ACK` con otro `seq_num` se descarta sin efecto.
- El establecimiento de sesión también es idempotente ante duplicados: si
  llega un `INIT` repetido para una sesión ya activa (porque el
  `INIT_ACK` original se perdió), el receptor reenvía el `INIT_ACK`
  cacheado en vez de crear una segunda sesión/hilo para el mismo cliente.

### Cierre confiable (FIN / FIN_ACK)

El cierre reutiliza el mismo mecanismo de retransmisión por timeout: el
emisor reenvía `FIN` hasta recibir `FIN_ACK` o agotar los reintentos. Del
lado del receptor, el `FIN` solo se acepta si `chunks_done == total_chunks`
(protección contra un cierre prematuro). Tras enviar el primer `FIN_ACK`,
la sesión entra en un estado análogo a **TIME_WAIT** de TCP: se queda
escuchando ese socket efímero por `TIME_WAIT_MS = 2000 ms` adicionales,
reenviando `FIN_ACK` si llega otro `FIN` duplicado (porque el `FIN_ACK`
anterior se perdió), antes de cerrar el archivo y liberar el hilo. Esto
evita el problema clásico de "último ACK perdido" en protocolos de cierre
de dos vías.

---

## Garantías: orden, no-duplicación, confiabilidad

| Garantía          | Mecanismo                                                                 |
|--------------------|----------------------------------------------------------------------------|
| **Confiabilidad**  | Retransmisión por timeout (`SO_RCVTIMEO` + reintentos) en `INIT`, `DATA` y `FIN`. |
| **Orden**          | Stop-and-wait: el emisor nunca envía `seq_num+1` sin el ACK de `seq_num`; el receptor además escribe cada chunk con `pwrite()` en `seq_num * CHUNK_SIZE`, así que aunque llegara desordenado terminaría en la posición correcta del archivo. |
| **Sin duplicados** | Arreglo `received[]` en el receptor evita reescribir un chunk ya recibido; los `ACK` se reenvían ante un duplicado pero no se vuelve a procesar como "dato nuevo". |
| **Corrupción**     | CRC32 en cada paquete; un paquete corrupto se descarta y se trata como pérdida (dispara la retransmisión ya existente). |
| **Cierre limpio**  | Handshake `FIN`/`FIN_ACK` con retransmisión + estado tipo TIME_WAIT en el receptor. |

---

## Concurrencia y sincronización

**Modelo**: un hilo por sesión de transferencia (`pthread_create` +
`pthread_detach`), igual que el modelo de un socket dedicado por sesión
descrito arriba. El hilo principal del receptor solo hace una cosa:
escuchar `INIT` en el puerto bien conocido y despachar sesiones nuevas.

**Recurso compartido y por qué se protege**:

- `g_sessions[]` (tabla de sesiones activas, arreglo estático de tamaño
  fijo `MAX_SESSIONS`): la escribe el hilo principal (al crear una sesión
  nueva, al buscar si un `INIT` repetido corresponde a una sesión ya en
  curso) y los hilos de sesión (guardan su socket recién creado, y al
  finalizar marcan su slot como inactivo). Se protege con
  `g_sessions_mutex`. Sin este mutex, dos hilos podrían pisarse leyendo o
  escribiendo el mismo slot — por ejemplo, una condición de carrera entre
  "buscar sesión existente" y "el hilo de esa sesión terminando y
  liberando el slot" podría llevar a leer un slot en un estado
  inconsistente, o a crear dos sesiones para el mismo cliente.

No hace falta proteger el archivo de salida, el socket de sesión, ni el
buffer `received[]` de cada sesión porque **cada hilo tiene los suyos
propios** — no hay recurso de transferencia compartido entre sesiones,
solo la tabla de control.

`MAX_SESSIONS = 10` (arreglo estático en `receptor.c`) — suficiente para
la práctica (mínimo 2 exigido) sin la complejidad de una tabla dinámica.
Si se alcanza el límite, un `INIT` nuevo se descarta silenciosamente y el
emisor lo reintenta más tarde (por si para entonces se liberó algún slot).

---

## Manejo de archivos

- El emisor lee el archivo en bloques de `CHUNK_SIZE = 1024` bytes
  secuencialmente con `fread`.
- El receptor abre el archivo de salida con `O_WRONLY | O_CREAT | O_TRUNC`
  (esto ya vacía cualquier archivo previo con el mismo nombre) y escribe
  cada chunk con `pwrite(fd, payload, data_len, seq_num * CHUNK_SIZE)`.
  Escribir por posición absoluta (en vez de ir agregando secuencialmente)
  permite:
  - Manejar paquetes fuera de orden sin bufferear todo el archivo en
    memoria (relevante si en el futuro se cambia a ventana deslizante).
  - Que el archivo termine con el tamaño exacto sin necesidad de
    preasignarlo: como el `FIN` solo se acepta cuando **todos** los
    chunks (incluido el último, que tiene el offset más alto) ya se
    escribieron, `pwrite()` extiende el archivo a su tamaño final de
    forma natural.
  - Que un `DATA` duplicado no requiera lógica especial de "descartar
    payload" — simplemente no se vuelve a escribir esa posición (aunque
    escribirla de nuevo sería igual de inofensivo, ya que el contenido es
    idéntico).
- El nombre de archivo recibido por red se sanea con `basename()` (evita
  *path traversal* del estilo `../../etc/passwd`) y se guarda con el
  prefijo `recibido_` para no colisionar con el archivo original si
  emisor y receptor corren en el mismo directorio.

---

## Pruebas con netem

`tc`/`netem` requiere privilegios de root. Comandos de referencia sobre
`lo` (loopback, para cuando emisor y receptor corren en la misma
máquina — usar la interfaz real, ej. `eth0`, si corren en máquinas
distintas):

```bash
# Agregar retardo de 100ms +/- 20ms de jitter
sudo tc qdisc add dev lo root netem delay 100ms 20ms

# Agregar pérdida de paquetes del 5%
sudo tc qdisc change dev lo root netem loss 5%

# Agregar duplicidad del 2%
sudo tc qdisc change dev lo root netem duplicate 2%

# Combinar todo
sudo tc qdisc change dev lo root netem delay 100ms 20ms loss 5% duplicate 2%

# Ver configuración actual
tc qdisc show dev lo

# Limpiar configuración (importante al terminar las pruebas)
sudo tc qdisc del dev lo root
```

### Generación de archivo de prueba y verificación de integridad

```bash
head -c 5M /dev/urandom > archivo_prueba.bin
./receptor 5000 &
./emisor 127.0.0.1 5000 archivo_prueba.bin
diff archivo_prueba.bin recibido_archivo_prueba.bin && echo "OK: archivos identicos"
```

### Escenario con 2 emisores concurrentes

```bash
# Terminal 1: receptor
./receptor 5000

# Terminal 2: emisor 1
./emisor 127.0.0.1 5000 archivo1.bin

# Terminal 3: emisor 2 (simultáneo)
./emisor 127.0.0.1 5000 archivo2.bin
```

Cada uno debería recibirse en un `recibido_archivoN.bin` idéntico al
original, atendidos por hilos y sockets efímeros independientes.

### Casos límite recomendados

- Archivo vacío (0 bytes, 0 chunks).
- Archivo de un solo chunk (menor a `CHUNK_SIZE`).
- Archivo cuyo tamaño no es múltiplo exacto de `CHUNK_SIZE`.
- Pérdida alta (ej. `loss 30%`) para confirmar que el protocolo converge
  (no se cuelga) aun en condiciones extremas — solo tarda más por los
  reintentos de `send_with_retry`.

> Nota: como el protocolo es stop-and-wait, con `delay` de netem activo
> el throughput baja considerablemente (ver
> [limitaciones](#limitaciones-y-trabajo-futuro)); conviene usar archivos
> chicos (1-2 MB) para las pruebas con retardo simulado.

---

## Parámetros y límites

Definidos en `src/protocolo.h` (protocolo) y `src/receptor.c` (receptor):

| Constante           | Valor  | Significado                                              |
|----------------------|--------|-------------------------------------------------------------|
| `CHUNK_SIZE`          | 1024   | Bytes de payload por chunk de datos.                       |
| `PKT_HEADER_LEN`      | 15     | Tamaño del header serializado, en bytes.                    |
| `MAX_FILENAME`        | 900    | Largo máximo aceptado para el nombre de archivo en `INIT`.  |
| `TIMEOUT_MS`          | 300    | Espera de respuesta antes de retransmitir.                  |
| `MAX_RETRIES`         | 30     | Reintentos máximos antes de abortar (~9 s).                 |
| `TIME_WAIT_MS`        | 2000   | Ventana de TIME_WAIT del receptor tras el primer `FIN_ACK`. |
| `IDLE_TIMEOUT_MS`     | 5000   | Timeout de inactividad del socket de sesión.                |
| `MAX_IDLE_RETRIES`    | 12     | Reintentos de inactividad antes de abortar sesión (~60 s).  |
| `MAX_SESSIONS`        | 10     | Sesiones concurrentes máximas soportadas por el receptor.   |

---

## Limitaciones y trabajo futuro

- **Rendimiento con RTT alto**: al ser stop-and-wait, con `delay 100ms` el
  throughput teórico máximo es ~`CHUNK_SIZE / RTT` ≈ 1024 bytes / 0.2s ≈
  5 KB/s — impracticable para archivos grandes bajo netem. La mejora
  natural es evolucionar a **ventana deslizante (Go-Back-N o Selective
  Repeat)**; el uso de `pwrite()` por posición absoluta en el receptor ya
  deja el terreno preparado para aceptar chunks fuera de orden sin
  cambios estructurales grandes.
- **Límite de sesiones concurrentes**: `MAX_SESSIONS = 10` (arreglo
  estático), suficiente para la práctica pero fijo en tiempo de
  compilación.
- **Timeout de inactividad**: si un emisor desaparece sin enviar `FIN`
  (crash, corte de red), el hilo de sesión del receptor lo detecta tras
  `IDLE_TIMEOUT_MS * MAX_IDLE_RETRIES` (~60 s) sin actividad y libera sus
  recursos (archivo, socket, slot de la tabla).
- **Sin logging**: el receptor y el emisor no imprimen el detalle de cada
  retransmisión/duplicado/sesión (salvo los mensajes de progreso del
  emisor) — para depurar en profundidad durante las pruebas con netem
  conviene agregar impresiones puntuales temporalmente.
