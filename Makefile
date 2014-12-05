BIN=mediator

CC := gcc

CFLAGS := -g -Wall -O3 -std=c99 -D_POSIX_SOURCE -D_GNU_SOURCE
CFLAGS += `pkg-config glib-2.0 --cflags`
CFLAGS += `pkg-config --cflags gthread-2.0`
CFLAGS += `mysql_config --cflags`
CFLAGS += -pthread
#CFLAGS += -DWITH_TIME_CALC

LDFLAGS := -pthread
LDFLAGS += `pkg-config glib-2.0 --libs`
LDFLAGS += `pkg-config --libs gthread-2.0`
LDFLAGS += `mysql_config --libs_r`

CFILES := $(wildcard *.c)
OFILES := $(CFILES:.c=.o)

.PHONY: $(BIN) all

all: $(BIN)

$(BIN): $(OFILES)
	$(CC) -o $@ $^ $(CFLAGS) $(LDFLAGS)

%.o: %.c
	$(CC) $(CFLAGS) -c -o $@ $<

clean:
	rm -f $(OFILES)
	rm -f core*
	rm -f $(BIN)

