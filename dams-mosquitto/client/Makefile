R=..
include ${R}/config.mk

.PHONY: all install uninstall reallyclean clean static static_pub static_sub static_rr

ifeq ($(WITH_SHARED_LIBRARIES),yes)
SHARED_DEP:=${R}/lib/libmosquitto.so.${SOVERSION}
endif

LOCAL_CFLAGS+=
LOCAL_CPPFLAGS+=
LOCAL_LDFLAGS+=
LOCAL_LDADD+=-lcjson ${SHARED_DEP}
STATIC_LDADD+=-lcjson

ifeq ($(WITH_SOCKS),yes)
	LOCAL_CPPFLAGS+=-DWITH_SOCKS
endif

ifeq ($(WITH_THREADING),yes)
	LOCAL_CFLAGS+=-pthread
	LOCAL_CPPFLAGS+=-DWITH_THREADING
	LOCAL_LDFLAGS+=-pthread
	STATIC_LDADD+=-pthread
endif

ifeq ($(WITH_TLS),yes)
	LOCAL_LDADD+=-lssl -lcrypto
	STATIC_LDADD+= -lssl -lcrypto
endif

ifeq ($(WITH_SHARED_LIBRARIES),yes)
ALL_DEPS:= mosquitto_pub mosquitto_sub mosquitto_rr
else
ifeq ($(WITH_STATIC_LIBRARIES),yes)
ALL_DEPS:= static_pub static_sub static_rr
endif
endif

all : ${ALL_DEPS}

static : static_pub static_sub static_rr
	# This makes mosquitto_pub/sub/rr versions that are statically linked with
	# libmosquitto only.

static_pub : pub_client.o pub_shared.o client_props.o client_shared.o ${R}/lib/libmosquitto.a
	${CROSS_COMPILE}${CC} $^ -o mosquitto_pub ${LOCAL_LDFLAGS} ${STATIC_LIB_DEPS} ${STATIC_LDADD}

static_sub : sub_client.o sub_client_output.o client_props.o client_shared.o ${R}/lib/libmosquitto.a
	${CROSS_COMPILE}${CC} $^ -o mosquitto_sub ${LOCAL_LDFLAGS} ${STATIC_LIB_DEPS} ${STATIC_LDADD}

static_rr : rr_client.o client_props.o client_shared.o pub_shared.o sub_client_output.o ${R}/lib/libmosquitto.a
	${CROSS_COMPILE}${CC} $^ -o mosquitto_rr ${LOCAL_LDFLAGS} ${STATIC_LIB_DEPS} ${STATIC_LDADD}

mosquitto_pub : pub_client.o pub_shared.o client_shared.o client_props.o
	${CROSS_COMPILE}${CC} $(LOCAL_LDFLAGS) $^ -o $@ $(LOCAL_LDADD)

mosquitto_sub : sub_client.o sub_client_output.o client_shared.o client_props.o
	${CROSS_COMPILE}${CC} $(LOCAL_LDFLAGS) $^ -o $@ $(LOCAL_LDADD)

mosquitto_rr : rr_client.o client_shared.o client_props.o pub_shared.o sub_client_output.o
	${CROSS_COMPILE}${CC} $(LOCAL_LDFLAGS) $^ -o $@ $(LOCAL_LDADD)

pub_client.o : pub_client.c ${SHARED_DEP}
	${CROSS_COMPILE}${CC} $(LOCAL_CPPFLAGS) $(LOCAL_CFLAGS) -c $< -o $@

pub_shared.o : pub_shared.c ${SHARED_DEP}
	${CROSS_COMPILE}${CC} $(LOCAL_CPPFLAGS) $(LOCAL_CFLAGS) -c $< -o $@

sub_client.o : sub_client.c ${SHARED_DEP}
	${CROSS_COMPILE}${CC} $(LOCAL_CPPFLAGS) $(LOCAL_CFLAGS) -c $< -o $@

sub_client_output.o : sub_client_output.c sub_client_output.h ${SHARED_DEP}
	${CROSS_COMPILE}${CC} $(LOCAL_CPPFLAGS) $(LOCAL_CFLAGS) -c $< -o $@

rr_client.o : rr_client.c ${SHARED_DEP}
	${CROSS_COMPILE}${CC} $(LOCAL_CPPFLAGS) $(LOCAL_CFLAGS) -c $< -o $@

client_shared.o : client_shared.c client_shared.h
	${CROSS_COMPILE}${CC} $(LOCAL_CPPFLAGS) $(LOCAL_CFLAGS) -c $< -o $@

client_props.o : client_props.c client_shared.h
	${CROSS_COMPILE}${CC} $(LOCAL_CPPFLAGS) $(LOCAL_CFLAGS) -c $< -o $@

# The "testing" target is intended to make it easy to compile a quick client
# for testing purposes. testing.c should not be committed as a file.
testing : testing.o
	${CROSS_COMPILE}${CC} $() $^ -o $@ $(LOCAL_LDADD) $(LOCAL_LDFLAGS)

testing.o : testing.c
	${CROSS_COMPILE}${CC} $(LOCAL_CPPFLAGS) $(LOCAL_CFLAGS) -c $< -o $@


${R}/lib/libmosquitto.so.${SOVERSION} :
	$(MAKE) -C ${R}/lib

${R}/lib/libmosquitto.a :
	$(MAKE) -C ${R}/lib libmosquitto.a

install : all
	$(INSTALL) -d "${DESTDIR}$(prefix)/bin"
	$(INSTALL) ${STRIP_OPTS} mosquitto_pub "${DESTDIR}${prefix}/bin/mosquitto_pub"
	$(INSTALL) ${STRIP_OPTS} mosquitto_sub "${DESTDIR}${prefix}/bin/mosquitto_sub"
	$(INSTALL) ${STRIP_OPTS} mosquitto_rr "${DESTDIR}${prefix}/bin/mosquitto_rr"

uninstall :
	-rm -f "${DESTDIR}${prefix}/bin/mosquitto_pub"
	-rm -f "${DESTDIR}${prefix}/bin/mosquitto_sub"
	-rm -f "${DESTDIR}${prefix}/bin/mosquitto_rr"

reallyclean : clean

clean :
	-rm -f *.o mosquitto_pub mosquitto_sub mosquitto_rr *.gcda *.gcno
