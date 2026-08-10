
#define _POSIX_C_SOURCE 200809L

#include "protocolo.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <libgen.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <netdb.h>
#include <arpa/inet.h>

static int resolve_addr(const char *host, const char *port_str, struct sockaddr_in *out)
{
    struct addrinfo hints, *res;
    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_DGRAM;

    int rc = getaddrinfo(host, port_str, &hints, &res);
    if (rc != 0) {
        return -1;
    }
    *out = *(struct sockaddr_in *)res->ai_addr;
    freeaddrinfo(res);
    return 0;
}

static int send_with_retry(int sock, const uint8_t *pkt, size_t pkt_len,
                            uint8_t expected_type, uint32_t expected_seq,
                            pkt_header_t *out_hdr)
{
    uint8_t buf[PKT_MAX_LEN];
    for (int attempt = 0; attempt < MAX_RETRIES; attempt++) {
        if (send(sock, pkt, pkt_len, 0) < 0) {
            return -1;
        }

        long long deadline = now_ms() + TIMEOUT_MS;
        while (1) {
            long long remaining = deadline - now_ms();
            if (remaining <= 0) break;
            set_recv_timeout(sock, (int)remaining);
            ssize_t n = recv(sock, buf, sizeof(buf), 0);
            if (n < 0) {
                break; 
            }
            const uint8_t *payload;
            if (pkt_unpack(buf, (size_t)n, out_hdr, &payload) != 0) {
                continue; 
            }
            if (out_hdr->type == expected_type && out_hdr->seq_num == expected_seq) {
                return 0;
            }
        }
        fprintf(stderr, "EMISOR demoro mucho %s(seq=%u), reintento %d/%d\n",
                pkt_type_name(expected_type), expected_seq, attempt + 1, MAX_RETRIES);
    }
    return -1;
}

static int connection_init(int sock, const struct sockaddr_in *dest,
                              const uint8_t *pkt, size_t pkt_len,
                              struct sockaddr_in *from_out, pkt_header_t *out_hdr)
{
    uint8_t buf[PKT_MAX_LEN];
    for (int attempt = 0; attempt < MAX_RETRIES; attempt++) {
        if (sendto(sock, pkt, pkt_len, 0, (const struct sockaddr *)dest, sizeof(*dest)) < 0) {
            perror("EMISOR, error al mandar el paquete");
            return -1;
        }
        set_recv_timeout(sock, TIMEOUT_MS);
        struct sockaddr_in from;
        socklen_t fromlen = sizeof(from);
        ssize_t n = recvfrom(sock, buf, sizeof(buf), 0, (struct sockaddr *)&from, &fromlen);
        if (n < 0) {
            fprintf(stderr, "EMISOR, timeout superado, reintento %d/%d\n",
                    attempt + 1, MAX_RETRIES);
            continue;
        }
        const uint8_t *payload;
        if (pkt_unpack(buf, (size_t)n, out_hdr, &payload) != 0) continue;
        if (out_hdr->type == PKT_INIT_ACK) {
            *from_out = from;
            return 0;
        }
    }
    return -1;
}

int main(int argc, char *argv[]) //<host_destino> <puerto_destino> <nombre_archivo>
{
    if (argc != 4) {
        fprintf(stderr, "Uso: %s <host_destino> <puerto_destino> <nombre_archivo>\n", argv[0]);
        return 1;
    }
    const char *host = argv[1];
    const char *port_str = argv[2];
    const char *filepath = argv[3];

    struct sockaddr_in dest_addr;
    if (resolve_addr(host, port_str, &dest_addr) != 0) {
        return 1;
    }

    FILE *f = fopen(filepath, "rb");
    if (!f) {
        return 1;
    }
    struct stat st;
    if (stat(filepath, &st) != 0) {
        perror("EMISOR, archivo no valido");
        fclose(f);
        return 1;
    }
    uint64_t file_size = (uint64_t)st.st_size;
    uint32_t total_chunks = (file_size == 0) ? 0
        : (uint32_t)((file_size + CHUNK_SIZE - 1) / CHUNK_SIZE);

    char namebuf[MAX_FILENAME + 1];
    strncpy(namebuf, filepath, MAX_FILENAME);
    namebuf[MAX_FILENAME] = '\0';
    char *bn = basename(namebuf);
    size_t bn_len = strlen(bn);

    int sock = socket(AF_INET, SOCK_DGRAM, 0);
    if (sock < 0) {
        perror("EMISOR, socket no valido");
        fclose(f);
        return 1;
    }

    uint8_t init_payload[8 + MAX_FILENAME];
    uint32_t hi = htonl((uint32_t)(file_size >> 32));
    uint32_t lo = htonl((uint32_t)(file_size & 0xFFFFFFFFu));
    memcpy(init_payload, &hi, 4);
    memcpy(init_payload + 4, &lo, 4);
    memcpy(init_payload + 8, bn, bn_len);

    pkt_header_t init_hdr = {0};
    init_hdr.seq_num = 0;
    init_hdr.total_chunks = total_chunks;
    init_hdr.data_len = (uint16_t)(8 + bn_len);
    init_hdr.type = PKT_INIT;

    uint8_t init_buf[PKT_MAX_LEN];
    size_t init_len = pkt_pack(init_buf, &init_hdr, init_payload);

    printf("EMISOR, enviando INIT a %s:%s (archivo='%s', %lu bytes, %u chunks)\n",
           host, port_str, bn, (unsigned long)file_size, total_chunks);

    struct sockaddr_in session_addr;
    pkt_header_t resp_hdr;
    if (connection_init(sock, &dest_addr, init_buf, init_len, &session_addr, &resp_hdr) != 0) {
        fprintf(stderr, "EMISOR, el receptor no respondio al INIT, abortando\n");
        close(sock);
        fclose(f);
        return 1;
    }
    if (connect(sock, (struct sockaddr *)&session_addr, sizeof(session_addr)) < 0) {
        perror("EMISOR, no se puede conectar a este socker");
        close(sock);
        fclose(f);
        return 1;
    }
    char sess_str[32];
    sockaddr_to_str(&session_addr, sess_str, sizeof(sess_str));
    printf("EMISOR, ajustado al nombre %s\n", sess_str);



    uint8_t chunk_buf[CHUNK_SIZE];
    uint8_t pkt_buf[PKT_MAX_LEN];
    long long t_start = now_ms();

    for (uint32_t seq = 0; seq < total_chunks; seq++) {
        size_t n = fread(chunk_buf, 1, CHUNK_SIZE, f);
        if (n == 0 && ferror(f)) {
            close(sock);
            fclose(f);
            return 1;
        }

        pkt_header_t data_hdr = {0};
        data_hdr.seq_num = seq;
        data_hdr.total_chunks = total_chunks;
        data_hdr.data_len = (uint16_t)n;
        data_hdr.type = PKT_DATA;

        size_t pkt_len = pkt_pack(pkt_buf, &data_hdr, chunk_buf);

        pkt_header_t ack_hdr;
        if (send_with_retry(sock, pkt_buf, pkt_len, PKT_ACK, seq, &ack_hdr) != 0) {
            close(sock);
            fclose(f);
            return 1;
        }

        if (total_chunks > 0 && (seq % 200 == 0 || seq == total_chunks - 1)) {
            printf("\rEMISOR progreso: %u/%u chunks (%.1f%%)\n", seq + 1, total_chunks,
                   100.0 * (seq + 1) / total_chunks);
            fflush(stdout);
        }
    }
    fclose(f);

    

		pkt_header_t fin_hdr = {0};
    fin_hdr.seq_num = 0;
    fin_hdr.total_chunks = total_chunks;
    fin_hdr.type = PKT_FIN;
    uint8_t fin_buf[PKT_HEADER_LEN];
    size_t fin_len = pkt_pack(fin_buf, &fin_hdr, NULL);

    pkt_header_t finack_hdr;
    if (send_with_retry(sock, fin_buf, fin_len, PKT_FIN_ACK, 0, &finack_hdr) != 0) {
        fprintf(stderr, "EMISOR no se puede cerrar el socket tras %d intentos\n",
                MAX_RETRIES);
    } else {
        printf("EMISOR se desconecto al socket \n");
    }

    close(sock);
    return 0;
}
