R=../..
include ${R}/config.mk

LOCAL_CFLAGS+=
LOCAL_CPPFLAGS+=-I${R}/lib
LOCAL_LDFLAGS+=
LOCAL_LDADD+=

.PHONY: all install uninstall clean reallyclean

OBJS= \
	mosquitto_signal.o \
	signal_unix.o \

all : mosquitto_signal

mosquitto_signal : ${OBJS} ${OBJS_EXTERNAL}
	${CROSS_COMPILE}${CC} ${LOCAL_LDFLAGS} $^ -o $@ $(LOCAL_LDADD)

${OBJS} : %.o: %.c
	${CROSS_COMPILE}${CC} $(LOCAL_CPPFLAGS) $(LOCAL_CFLAGS) -c $< -o $@

install : all
ifeq ($(WITH_TLS),yes)
	$(INSTALL) -d "${DESTDIR}$(prefix)/bin"
	$(INSTALL) ${STRIP_OPTS} mosquitto_signal "${DESTDIR}${prefix}/bin/mosquitto_signal"
endif

uninstall :
	-rm -f "${DESTDIR}${prefix}/bin/mosquitto_signal"

clean :
	-rm -f *.o *.a mosquitto_signal *.gcda *.gcno

reallyclean : clean
	-rm -rf *.orig *.db
