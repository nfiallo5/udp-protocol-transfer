#include "protocolo.h"

#include <string.h>
#include <stdio.h>
#include <time.h>
#include <arpa/inet.h>

/* ---- CRC32 (implementacion sin tabla, suficiente para chunks de 1KB) --- */

static uint32_t crc32_update(uint32_t crc, const uint8_t *buf, size_t len)
{
    crc = ~crc;
    for (size_t i = 0; i < len; i++) {
        crc = crc ^ buf[i];
        for (int k = 0; k < 8; k++) {
            uint32_t mask = -(crc & 1u);
            crc = (crc >> 1) ^ (0xEDB88320u & mask);
        }
    }
    return ~crc;
}

uint32_t pkt_checksum(const pkt_header_t *hdr, const void *payload)
{
    uint8_t tmp[PKT_HEADER_LEN];
    pkt_header_t h = *hdr;
    h.checksum = 0;

    uint32_t seq_n = htonl(h.seq_num);
    uint32_t tot_n = htonl(h.total_chunks);
    uint16_t len_n = htons(h.data_len);

    memcpy(tmp, &seq_n, 4);
    memcpy(tmp + 4, &tot_n, 4);
    memcpy(tmp + 8, &len_n, 2);
    tmp[10] = h.type;
    memset(tmp + 11, 0, 4); /* chksum se deja en 0 durante el calculo */

    uint32_t crc = crc32_update(0, tmp, PKT_HEADER_LEN);
    if (h.data_len > 0 && payload != NULL) {
        crc = crc32_update(crc, (const uint8_t *)payload, h.data_len);
    }
    return crc;
}

size_t pkt_pack(uint8_t *buf, const pkt_header_t *hdr, const void *payload)
{
    pkt_header_t h = *hdr;
    h.checksum = pkt_checksum(hdr, payload);

    uint32_t seq_n = htonl(h.seq_num);
    uint32_t tot_n = htonl(h.total_chunks);
    uint16_t len_n = htons(h.data_len);
    uint32_t chk_n = htonl(h.checksum);

    memcpy(buf, &seq_n, 4);
    memcpy(buf + 4, &tot_n, 4);
    memcpy(buf + 8, &len_n, 2);
    buf[10] = h.type;
    memcpy(buf + 11, &chk_n, 4);

    if (h.data_len > 0 && payload != NULL) {
        memcpy(buf + PKT_HEADER_LEN, payload, h.data_len);
    }
    return PKT_HEADER_LEN + h.data_len;
}

int pkt_unpack(const uint8_t *buf, size_t len, pkt_header_t *hdr,
               const uint8_t **payload)
{
    if (len < PKT_HEADER_LEN) {
        return -1;
    }

    uint32_t seq_n, tot_n, chk_n;
    uint16_t len_n;
    memcpy(&seq_n, buf, 4);
    memcpy(&tot_n, buf + 4, 4);
    memcpy(&len_n, buf + 8, 2);
    memcpy(&chk_n, buf + 11, 4);

    hdr->seq_num = ntohl(seq_n);
    hdr->total_chunks = ntohl(tot_n);
    hdr->data_len = ntohs(len_n);
    hdr->type = buf[10];
    hdr->checksum = ntohl(chk_n);

    if (len < (size_t)(PKT_HEADER_LEN + hdr->data_len)) {
        return -1; 
    }

    *payload = buf + PKT_HEADER_LEN;

    uint32_t expected = pkt_checksum(hdr, *payload);
    if (expected != hdr->checksum) {
        return -1; /* corrupto */
    }
    return 0;
}

int set_recv_timeout(int sockfd, int timeout_ms)
{
    struct timeval tv;
    tv.tv_sec = timeout_ms / 1000;
    tv.tv_usec = (timeout_ms % 1000) * 1000;
    return setsockopt(sockfd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
}

int sockaddr_equal(const struct sockaddr_in *a, const struct sockaddr_in *b)
{
    return a->sin_addr.s_addr == b->sin_addr.s_addr &&
           a->sin_port == b->sin_port;
}

void sockaddr_to_str(const struct sockaddr_in *addr, char *out, size_t outlen)
{
    char ip[INET_ADDRSTRLEN];
    inet_ntop(AF_INET, &addr->sin_addr, ip, sizeof(ip));
    snprintf(out, outlen, "%s:%d", ip, ntohs(addr->sin_port));
}

long long now_ms(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (long long)ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}

const char *pkt_type_name(uint8_t type)
{
    switch (type) {
        case PKT_INIT:     return "INIT";
        case PKT_INIT_ACK: return "INIT_ACK";
        case PKT_DATA:     return "DATA";
        case PKT_ACK:      return "ACK";
        case PKT_FIN:      return "FIN";
        case PKT_FIN_ACK:  return "FIN_ACK";
        default:           return "UNKNOWN";
    }
}
