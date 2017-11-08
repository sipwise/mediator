BIN=mediator

CC := gcc

GLIB_CFLAGS := $(shell pkg-config glib-2.0 --cflags)
CFLAGS := -I/usr/include/mysql $(GLIB_CFLAGS) -g -Wall -O2
#CFLAGS += -DWITH_TIME_CALC

GLIB_LDFLAGS := $(shell pkg-config glib-2.0 --libs)
LDFLAGS := -lmysqlclient

CFILES := $(wildcard *.c)
OFILES := $(CFILES:.c=.o)

.PHONY: $(BIN) all clean coverity

all: $(BIN)

$(BIN): $(OFILES)
	$(CC) -o $@ $^ $(CFLAGS) $(LDFLAGS)

%.o: %.c
	$(CC) $(CPPFLAGS) $(CFLAGS) -c -o $@ $<

clean:
	rm -f *.o
	rm -f core*
	rm -f $(BIN)
	rm -rf project.tgz cov-int

coverity:
	cov-build --dir cov-int $(MAKE)
	tar -czf project.tgz cov-int
	curl --form token=$(COVERITY_MEDIATOR_TOKEN) \
		--form email=$(DEBEMAIL) \
		--form file=@project.tgz \
		--form version="$(MEDIATOR_VERSION)" \
		--form description="automatic upload" \
		https://scan.coverity.com/builds?project=$(COVERITY_MEDIATOR_PROJECT)

