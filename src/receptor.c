#define _POSIX_C_SOURCE 200809L

#include "protocolo.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <unistd.h>
#include <errno.h>
#include <signal.h>
#include <pthread.h>
#include <fcntl.h>
#include <libgen.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <arpa/inet.h>

#define MAX_SESSIONS        64
#define IDLE_TIMEOUT_MS      5000   /* espera de socket de sesion */
#define MAX_IDLE_RETRIES     12     /* ~60s de inactividad -> aborta sesion */

typedef struct {
    int active;
    struct sockaddr_in addr;
    int sock;
    uint8_t init_ack_buf[PKT_HEADER_LEN];
    size_t init_ack_len;
} session_slot_t;

typedef struct {
    struct sockaddr_in cli_addr;
    int slot;
    uint64_t file_size;
    uint32_t total_chunks;
    char filename[MAX_FILENAME + 1];
} session_arg_t;

static session_slot_t g_sessions[MAX_SESSIONS];
static pthread_mutex_t g_sessions_mutex = PTHREAD_MUTEX_INITIALIZER;
static volatile sig_atomic_t g_shutdown = 0;

static void on_sigint(int signo)
{
    (void)signo;
    g_shutdown = 1;
}

static void send_ack(int sock, const struct sockaddr_in *to, uint32_t seq_num,
                      uint8_t type)
{
    pkt_header_t hdr = {0};
    hdr.seq_num = seq_num;
    hdr.type = type;
    uint8_t buf[PKT_HEADER_LEN];
    size_t len = pkt_pack(buf, &hdr, NULL);
    sendto(sock, buf, len, 0, (const struct sockaddr *)to, sizeof(*to));
}

static void clean_filename(const char *in, char *out, size_t outlen)
{
    char tmp[MAX_FILENAME + 1];
    strncpy(tmp, in, MAX_FILENAME);
    tmp[MAX_FILENAME] = '\0';
    char *bn = basename(tmp);
    if (bn[0] == '\0' || strcmp(bn, "/") == 0) {
        bn = (char *)"archivo_recibido";
    }
    snprintf(out, outlen, "recv_%s", bn);
}

static void *session_thread(void *arg_ptr)
{
    session_arg_t *arg = (session_arg_t *)arg_ptr;

    int sock = socket(AF_INET, SOCK_DGRAM, 0);
    if (sock < 0) {
        goto cleanup_slot;
    }
    struct sockaddr_in local = {0};
    local.sin_family = AF_INET;
    local.sin_addr.s_addr = htonl(INADDR_ANY);
    local.sin_port = 0;
    if (bind(sock, (struct sockaddr *)&local, sizeof(local)) < 0) {
        close(sock);
        goto cleanup_slot;
    }

    pkt_header_t ack_hdr = {0};
    ack_hdr.seq_num = 0;
    ack_hdr.total_chunks = arg->total_chunks;
    ack_hdr.type = PKT_INIT_ACK;
    uint8_t ack_buf[PKT_HEADER_LEN];
    size_t ack_len = pkt_pack(ack_buf, &ack_hdr, NULL);

    pthread_mutex_lock(&g_sessions_mutex);
    g_sessions[arg->slot].sock = sock;
    memcpy(g_sessions[arg->slot].init_ack_buf, ack_buf, ack_len);
    g_sessions[arg->slot].init_ack_len = ack_len;
    pthread_mutex_unlock(&g_sessions_mutex);

    sendto(sock, ack_buf, ack_len, 0, (struct sockaddr *)&arg->cli_addr,
           sizeof(arg->cli_addr));

    char safe_name[MAX_FILENAME + 16];
    clean_filename(arg->filename, safe_name, sizeof(safe_name));

    int fd = open(safe_name, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) {
        close(sock);
        goto cleanup_slot;
    }
    if (arg->file_size > 0) {
        int rc = ftruncate(fd, (off_t)arg->file_size);
        (void)rc;
    }

    bool *received = NULL;
    if (arg->total_chunks > 0) {
        received = calloc(arg->total_chunks, sizeof(bool));
    }
    size_t chunks_done = 0;
    int idle_retries = 0;
    int finished = 0;

    set_recv_timeout(sock, IDLE_TIMEOUT_MS);

    while (!finished && !g_shutdown) {
        uint8_t buf[PKT_MAX_LEN];
        struct sockaddr_in from;
        socklen_t fromlen = sizeof(from);
        ssize_t n = recvfrom(sock, buf, sizeof(buf), 0,
                              (struct sockaddr *)&from, &fromlen);
        if (n < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                idle_retries++;
                if (idle_retries >= MAX_IDLE_RETRIES) {
                    break;
                }
                continue;
            }
            break;
        }
        if (!sockaddr_equal(&from, &arg->cli_addr)) {
            continue; /* paquete de otro origen  */
        }
        idle_retries = 0;

        pkt_header_t hdr;
        const uint8_t *payload;
        if (pkt_unpack(buf, (size_t)n, &hdr, &payload) != 0) {
            continue;
        }

        switch (hdr.type) {
        case PKT_INIT:
            sendto(sock, ack_buf, ack_len, 0, (struct sockaddr *)&arg->cli_addr,
                   sizeof(arg->cli_addr));
            break;

        case PKT_DATA:
            if (hdr.seq_num < arg->total_chunks) {
                if (received && !received[hdr.seq_num]) {
                    ssize_t w = pwrite(fd, payload, hdr.data_len,
                                       (off_t)hdr.seq_num * CHUNK_SIZE);
                    if (w >= 0) {
                        received[hdr.seq_num] = true;
                        chunks_done++;
                    }
                }
                send_ack(sock, &arg->cli_addr, hdr.seq_num, PKT_ACK);
            }
            break;

        case PKT_FIN:
            if (chunks_done == arg->total_chunks) {
                if (fd >= 0) {
                    fsync(fd);
                    close(fd);
                    fd = -1;
                }
                send_ack(sock, &arg->cli_addr, 0, PKT_FIN_ACK);

                set_recv_timeout(sock, 200);
                long long deadline = now_ms() + TIME_WAIT_MS;
                while (now_ms() < deadline) {
                    ssize_t n2 = recvfrom(sock, buf, sizeof(buf), 0,
                                          (struct sockaddr *)&from, &fromlen);
                    if (n2 > 0 && sockaddr_equal(&from, &arg->cli_addr)) {
                        pkt_header_t hdr2;
                        const uint8_t *pl2;
                        if (pkt_unpack(buf, (size_t)n2, &hdr2, &pl2) == 0 &&
                            hdr2.type == PKT_FIN) {
                            send_ack(sock, &arg->cli_addr, 0, PKT_FIN_ACK);
                        }
                    }
                }
                finished = 1;
            }
            break;

        default:
            break;
        }
    }

    if (fd >= 0) {
        close(fd);
    }
    free(received);
    close(sock);

cleanup_slot:
    pthread_mutex_lock(&g_sessions_mutex);
    g_sessions[arg->slot].active = 0;
    pthread_mutex_unlock(&g_sessions_mutex);
    free(arg);
    return NULL;
}

int main(int argc, char *argv[])
{
    if (argc != 2) {
        fprintf(stderr, "Uso: %s <puerto>\n", argv[0]);
        return 1;
    }
    int port = atoi(argv[1]);
    if (port <= 0 || port > 65535) {
        fprintf(stderr, "Puerto invalido: %s\n", argv[1]);
        return 1;
    }

    signal(SIGINT, on_sigint);
    signal(SIGTERM, on_sigint);

    int main_sock = socket(AF_INET, SOCK_DGRAM, 0);
    if (main_sock < 0) {
        return 1;
    }
    int reuse = 1;
    setsockopt(main_sock, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));

    struct sockaddr_in local = {0};
    local.sin_family = AF_INET;
    local.sin_addr.s_addr = htonl(INADDR_ANY);
    local.sin_port = htons((uint16_t)port);
    if (bind(main_sock, (struct sockaddr *)&local, sizeof(local)) < 0) {
        close(main_sock);
        return 1;
    }

    set_recv_timeout(main_sock, 1000);

    while (!g_shutdown) {
        uint8_t buf[PKT_MAX_LEN];
        struct sockaddr_in cli_addr;
        socklen_t cli_len = sizeof(cli_addr);
        ssize_t n = recvfrom(main_sock, buf, sizeof(buf), 0,
                              (struct sockaddr *)&cli_addr, &cli_len);
        if (n < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) continue;
            if (errno == EINTR) continue;
            continue;
        }

        pkt_header_t hdr;
        const uint8_t *payload;
        if (pkt_unpack(buf, (size_t)n, &hdr, &payload) != 0) {
            continue;
        }
        if (hdr.type != PKT_INIT) {
            continue;
        }
        if (hdr.data_len < 8) {
            continue;
        }

        pthread_mutex_lock(&g_sessions_mutex);
        int existing = -1;
        for (int i = 0; i < MAX_SESSIONS; i++) {
            if (g_sessions[i].active && sockaddr_equal(&g_sessions[i].addr, &cli_addr)) {
                existing = i;
                break;
            }
        }
        if (existing >= 0) {
            int wsock = g_sessions[existing].sock;
            uint8_t ackbuf[PKT_HEADER_LEN];
            size_t acklen = g_sessions[existing].init_ack_len;
            memcpy(ackbuf, g_sessions[existing].init_ack_buf, acklen);
            pthread_mutex_unlock(&g_sessions_mutex);
            if (wsock >= 0 && acklen > 0) {
                sendto(wsock, ackbuf, acklen, 0, (struct sockaddr *)&cli_addr, sizeof(cli_addr));
            }
            continue;
        }

        int slot = -1;
        for (int i = 0; i < MAX_SESSIONS; i++) {
            if (!g_sessions[i].active) { slot = i; break; }
        }
        if (slot < 0) {
            pthread_mutex_unlock(&g_sessions_mutex);
            continue;
        }
        g_sessions[slot].active = 1;
        g_sessions[slot].addr = cli_addr;
        g_sessions[slot].sock = -1;
        g_sessions[slot].init_ack_len = 0;
        pthread_mutex_unlock(&g_sessions_mutex);

        uint32_t hi, lo;
        memcpy(&hi, payload, 4);
        memcpy(&lo, payload + 4, 4);
        hi = ntohl(hi);
        lo = ntohl(lo);
        uint64_t file_size = ((uint64_t)hi << 32) | lo;

        size_t name_len = hdr.data_len - 8;
        if (name_len > MAX_FILENAME) name_len = MAX_FILENAME;

        session_arg_t *arg = calloc(1, sizeof(session_arg_t));
        arg->cli_addr = cli_addr;
        arg->slot = slot;
        arg->file_size = file_size;
        arg->total_chunks = hdr.total_chunks;
        memcpy(arg->filename, payload + 8, name_len);
        arg->filename[name_len] = '\0';

        pthread_t tid;
        if (pthread_create(&tid, NULL, session_thread, arg) != 0) {
            pthread_mutex_lock(&g_sessions_mutex);
            g_sessions[slot].active = 0;
            pthread_mutex_unlock(&g_sessions_mutex);
            free(arg);
            continue;
        }
        pthread_detach(tid);
    }

    close(main_sock);
    return 0;
}
