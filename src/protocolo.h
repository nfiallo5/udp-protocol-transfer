
#ifndef PROTOCOLO_H
#define PROTOCOLO_H

#include <stdint.h>
#include <stddef.h>
#include <sys/socket.h>
#include <netinet/in.h>



#define CHUNK_SIZE      1024
#define PKT_HEADER_LEN  15
#define PKT_MAX_LEN     (PKT_HEADER_LEN + CHUNK_SIZE)

#define MAX_FILENAME    900

#define TIMEOUT_MS      300     /* espera de ACK antes de reenviar */
#define MAX_RETRIES     30      /* Alrededor de9 segundos */

#define TIME_WAIT_MS    2000


typedef enum {
    PKT_INIT     = 1,  /* emisor -> receptor: solicita iniciar conversaion */
    PKT_INIT_ACK = 2,  /* receptor -> emisor: acepta, da el ok para el puerto */
    PKT_DATA     = 3,  /* emisor -> receptor: chunk de datos */
    PKT_ACK      = 4,  /* receptor -> emisor: confirma datos */
    PKT_FIN      = 5,  /* emisor -> receptor: fin de archivo */
    PKT_FIN_ACK  = 6   /* receptor -> emisor: confirma cierre */
} pkt_type_t;


typedef struct {
    uint32_t seq_num;
    uint32_t total_chunks;
    uint16_t data_len;
    uint8_t  type;
    uint32_t checksum;
} pkt_header_t;

size_t pkt_pack(uint8_t *buf, const pkt_header_t *hdr, const void *payload);

int pkt_unpack(const uint8_t *buf, size_t len, pkt_header_t *hdr,
               const uint8_t **payload);

uint32_t pkt_checksum(const pkt_header_t *hdr, const void *payload);

int set_recv_timeout(int sockfd, int timeout_ms);

int sockaddr_equal(const struct sockaddr_in *a, const struct sockaddr_in *b);

void sockaddr_to_str(const struct sockaddr_in *addr, char *out, size_t outlen);

long long now_ms(void);

const char *pkt_type_name(uint8_t type);

#endif /* PROTOCOLO_H */
