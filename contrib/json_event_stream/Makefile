# contrib/json_event_stream/Makefile

PGFILEDESC = "json_event_httpd - HTTP ingestion for JSON event streams"
PGAPPICON = win32

PROGRAM = json_event_httpd
OBJS = \
	$(WIN32RES) \
	json_event_httpd.o

EXTENSION = json_event_stream
DATA = json_event_stream--1.0.sql json_event_stream--1.0--1.1.sql \
	json_event_stream--1.1.sql
SCRIPTS = json_event_bench json_event_ci json_event_package \
	json_event_remote_guard json_event_smoke
REGRESS = json_event_stream_upgrade json_event_stream

PG_CPPFLAGS = -I$(libpq_srcdir)
PG_LIBS_INTERNAL = $(libpq_pgport)

all: json_event_mqttd
install: install-json-event-mqttd
uninstall: uninstall-json-event-mqttd
clean distclean: clean-json-event-mqttd

all: json_event_ctl
install: install-json-event-ctl
uninstall: uninstall-json-event-ctl
clean distclean: clean-json-event-ctl

json_event_ctl: json_event_ctl.o $(WIN32RES) | submake-libpq submake-libpgport
	$(CC) $(CFLAGS) json_event_ctl.o $(WIN32RES) $(PG_LIBS_INTERNAL) $(LDFLAGS) $(LDFLAGS_EX) $(PG_LIBS) $(LIBS) -o $@$(X)

json_event_mqttd: json_event_mqttd.o $(WIN32RES) | submake-libpq submake-libpgport
	$(CC) $(CFLAGS) json_event_mqttd.o $(WIN32RES) $(PG_LIBS_INTERNAL) $(LDFLAGS) $(LDFLAGS_EX) $(PG_LIBS) $(LIBS) -o $@$(X)

install-json-event-ctl: json_event_ctl installdirs
	$(INSTALL_PROGRAM) json_event_ctl$(X) '$(DESTDIR)$(bindir)'

install-json-event-mqttd: json_event_mqttd installdirs
	$(INSTALL_PROGRAM) json_event_mqttd$(X) '$(DESTDIR)$(bindir)'

uninstall-json-event-ctl:
	rm -f '$(DESTDIR)$(bindir)/json_event_ctl$(X)'

uninstall-json-event-mqttd:
	rm -f '$(DESTDIR)$(bindir)/json_event_mqttd$(X)'

clean-json-event-ctl:
	rm -f json_event_ctl$(X) json_event_ctl.o

clean-json-event-mqttd:
	rm -f json_event_mqttd$(X) json_event_mqttd.o

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/json_event_stream
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
