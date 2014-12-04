BIN=mediator

CC := gcc

GLIB_CFLAGS := `pkg-config glib-2.0 --cflags` `pkg-config --cflags gthread-2.0`
CFLAGS := -I/usr/include/mysql $(GLIB_CFLAGS) -g -Wall -O3
CFLAGS += -pthread
#CFLAGS += -DWITH_TIME_CALC

GLIB_LDFLAGS := `pkg-config glib-2.0 --libs` `pkg-config --libs gthread-2.0`
LDFLAGS := -lmysqlclient

CFILES := $(wildcard *.c)
OFILES := $(CFILES:.c=.o)

.PHONY: $(BIN) all

all: $(BIN)

$(BIN): $(OFILES)
	$(CC) -o $@ $^ $(CFLAGS) $(LDFLAGS) $(GLIB_LDFLAGS)

%.o: %.c
	$(CC) $(CFLAGS) -c -o $@ $<

clean:
	rm -f *.o
	rm -f core*
	rm -f $(BIN)

