CC      = gcc
CFLAGS  = -Wall -Wextra -pthread -O2 -g
LDFLAGS = -pthread

SRC_DIR = src
BIN     = emisor receptor

.PHONY: all clean

all: $(BIN)

emisor: $(SRC_DIR)/emisor.c $(SRC_DIR)/protocolo.c $(SRC_DIR)/protocolo.h
	$(CC) $(CFLAGS) -o $@ $(SRC_DIR)/emisor.c $(SRC_DIR)/protocolo.c $(LDFLAGS)

receptor: $(SRC_DIR)/receptor.c $(SRC_DIR)/protocolo.c $(SRC_DIR)/protocolo.h
	$(CC) $(CFLAGS) -o $@ $(SRC_DIR)/receptor.c $(SRC_DIR)/protocolo.c $(LDFLAGS)

clean:
	rm -f $(BIN)
