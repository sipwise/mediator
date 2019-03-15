BIN=ngcp-mediator

VERSION := $(shell sed -n -e '1s/^.*(\(.*\)).*$$/\1/p' debian/changelog)

CC := gcc

CPPFLAGS := -DMEDIATOR_VERSION="\"$(VERSION)\""

GLIB_CFLAGS := $(shell pkg-config glib-2.0 --cflags)

# mariadb/mysql support
ifneq ($(shell which mariadb_config),)
MYSQL_CFLAGS := $(shell mariadb_config --cflags)
MYSQL_LDFLAGS := $(shell mariadb_config --libs)
else ifneq ($(shell which mysql_config),)
MYSQL_CFLAGS := $(shell mysql_config --cflags)
MYSQL_LDFLAGS := $(shell mysql_config --libs)
else
MYSQL_CFLAGS := -I/usr/include/mysql
MYSQL_LDFLAGS := -lmysqlclient
endif

CFLAGS := $(GLIB_CFLAGS) -g -Wall -Wextra -O2 -D_GNU_SOURCE
CFLAGS += $(MYSQL_CFLAGS)
#CFLAGS += -DWITH_TIME_CALC
CFLAGS += $(shell pkg-config json-c --cflags)
CFLAGS += $(shell pkg-config hiredis --cflags)
CFLAGS += $(shell pkg-config libsystemd --cflags)

GLIB_LDFLAGS := $(shell pkg-config glib-2.0 --libs)
LDFLAGS := $(GLIB_LDFLAGS)
LDFLAGS += $(MYSQL_LDFLAGS)
LDFLAGS += $(shell pkg-config json-c --libs)
LDFLAGS += $(shell pkg-config hiredis --libs)
LDFLAGS += $(shell pkg-config libsystemd --libs)

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


