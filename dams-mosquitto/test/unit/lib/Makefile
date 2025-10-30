R=../../..
include ${R}/config.mk

.PHONY: all check test test-compile clean coverage

LOCAL_CFLAGS+=-coverage
LOCAL_CPPFLAGS+=-I${R}/src -I${R}/test -I${R}/lib -I${R}/libcommon -DTEST_SOURCE_DIR='"$(realpath .)"'
LOCAL_LDFLAGS+=-coverage
LOCAL_LDADD+=-lcunit ${LIBMOSQ_COMMON}

ifeq ($(WITH_TLS),yes)
	LOCAL_LDADD+=-lssl -lcrypto
endif

TEST_OBJS = \
	test.o \
	datatype_read.o \
	datatype_write.o \
	property_read.o \
	property_user_read.o \
	property_write.o \
	stubs.o \

LIB_OBJS = \
	${R}/lib/packet_datatypes.o \
	${R}/lib/packet_mosq.o \
	${R}/lib/property_mosq.o \
	${R}/lib/util_mosq.o


all : test-compile

check : test

lib_test : ${TEST_OBJS} ${LIB_OBJS}
	$(CROSS_COMPILE)$(CC) $(LOCAL_LDFLAGS) -o $@ $^ $(LOCAL_LDADD)

${TEST_OBJS} : %.o: %.c
	${CROSS_COMPILE}${CC} $(LOCAL_CPPFLAGS) $(LOCAL_CFLAGS) -c $< -o $@

lib_stubs.o : stubs.c
	${CROSS_COMPILE}$(CC) $(LIB_LOCAL_CPPFLAGS) $(LIB_LOCAL_CFLAGS) $(CFLAGS) $(CPPFLAGS) -c $< -o $@

${R}/lib/misc_mosq.o : ${R}/common/misc_mosq.c
	$(MAKE) -C ${R}/lib/ misc_mosq.o

${R}/lib/packet_datatypes.o : ${R}/lib/packet_datatypes.c
	$(MAKE) -C ${R}/lib/ packet_datatypes.o

${R}/lib/packet_mosq.o : ${R}/lib/packet_mosq.c
	$(MAKE) -C ${R}/lib/ packet_mosq.o

${R}/lib/property_mosq.o : ${R}/lib/property_mosq.c
	$(MAKE) -C ${R}/lib/ property_mosq.o

${R}/lib/util_mosq.o : ${R}/lib/util_mosq.c
	$(MAKE) -C ${R}/lib/ util_mosq.o

${R}/lib/utf8_mosq.o : ${R}/common/utf8_mosq.c
	$(MAKE) -C ${R}/lib/ utf8_mosq.o

build : lib_test

test : build
	./lib_test

test-compile: build

clean :
	-rm -rf lib_test
	-rm -rf *.o *.gcda *.gcno coverage.info out/

coverage :
	lcov --capture --directory . --output-file coverage.info
	genhtml coverage.info --output-directory out
