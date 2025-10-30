R=../..
include ${R}/config.mk

CC=cc
LOCAL_CFLAGS=-I${R}/src -I${R}/include -I. -I${R} -Wall -ggdb -DDEBUG -DWITH_CLIENT
LOCAL_LDFLAGS=
SOVERSION=1

.PHONY: all test clean

all : msgsps_pub msgsps_sub

msgsps_pub : msgsps_pub.o
	${CC} $^ -o $@ ${LIBMOSQ}

msgsps_pub.o : msgsps_pub.c msgsps_common.h
	${CC} $(LOCAL_CFLAGS) -c $< -o $@

msgsps_sub : msgsps_sub.o
	${CC} $^ -o $@ ${LIBMOSQ}

msgsps_sub.o : msgsps_sub.c msgsps_common.h
	${CC} $(LOCAL_CFLAGS) -c $< -o $@

clean :
	-rm -f *.o msgsps_pub msgsps_sub
