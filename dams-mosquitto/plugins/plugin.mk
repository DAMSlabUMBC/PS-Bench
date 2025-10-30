.PHONY : all binary check clean reallyclean test test-compile install uninstall


LOCAL_CFLAGS+=-fPIC
LOCAL_CPPFLAGS+=
LOCAL_LIBADD+=
LOCAL_LDFLAGS+=-fPIC -shared

binary : ${PLUGIN_NAME}.so ${PLUGIN_NAME}.a

${PLUGIN_NAME}.a : ${OBJS} ${OBJS_EXTERNAL}
	${CROSS_COMPILE}$(AR) cr $@ $^

${PLUGIN_NAME}.so : ${OBJS} ${OBJS_EXTERNAL}
	${CROSS_COMPILE}${CC} $(LOCAL_LDFLAGS) $^ -o $@ ${LOCAL_LIBADD}

${OBJS} : %.o: %.c ${EXTRA_DEPS}
	${CROSS_COMPILE}${CC} $(LOCAL_CPPFLAGS) $(LOCAL_CFLAGS) -c $< -o $@

reallyclean : clean
clean:
	-rm -f *.o ${PLUGIN_NAME}.a ${PLUGIN_NAME}.so *.gcda *.gcno

test-compile:

check: test
test: test-compile

ifeq ($(PLUGIN_NOINST),)
install: all
	$(INSTALL) -d "${DESTDIR}$(libdir)"
	$(INSTALL) ${STRIP_OPTS} ${PLUGIN_NAME}.so "${DESTDIR}${libdir}/${PLUGIN_NAME}.so"

uninstall :
	-rm -f "${DESTDIR}${libdir}/${PLUGIN_NAME}.so"
endif
