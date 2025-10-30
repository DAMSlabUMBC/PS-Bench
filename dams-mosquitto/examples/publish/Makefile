R=../..
include ${R}/config.mk

.PHONY: all

all : basic-1 basic-websockets-1

basic-1 : basic-1.o
	${CROSS_COMPILE}${CC} $^ -o $@ ${LIBMOSQ}

basic-websockets-1 : basic-websockets-1.o
	${CROSS_COMPILE}${CC} $^ -o $@ ${LIBMOSQ}

basic-1.o : basic-1.c ${LIBMOSQ}
	${CROSS_COMPILE}${CC} -c $< -o $@ -I${R}/include ${LOCAL_CPPFLAGS} ${LOCAL_CFLAGS}

basic-websockets-1.o : basic-websockets-1.c ${LIBMOSQ}
	${CROSS_COMPILE}${CC} -c $< -o $@ -I${R}/include ${LOCAL_CPPFLAGS} ${LOCAL_CFLAGS}

${LIBMOSQ} :
	$(MAKE) -C ${R}/lib

clean :
	-rm -f *.o sub_single sub_multiple
