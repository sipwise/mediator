BIN=mediator

CC := gcc

#CFLAGS := -I. -I/usr/include/mysql -I/usr/local/include -g -Wall -DWITH_TIME_CALC

GLIB_CFLAGS := `pkg-config glib-2.0 --cflags`
CFLAGS := -I. -I/usr/include/mysql $(GLIB_CFLAGS) -g -Wall -O3

GLIB_LDFLAGS := `pkg-config glib-2.0 --libs`
LDFLAGS := -lmysqlclient -O3

CFILES := $(wildcard *.c)
OFILES := $(CFILES:.c=.o)

.PHONY: $(BIN) all

all: $(BIN)

$(BIN): $(OFILES)
	$(CC) -o $@ $^ $(LDFLAGS) $(GLIB_LDFLAGS)

%.o: %.c
	$(CC) $(CFLAGS) -c -o $@ $<

clean:
	rm -f *.o
	rm -f core*
	rm -f $(BIN)

