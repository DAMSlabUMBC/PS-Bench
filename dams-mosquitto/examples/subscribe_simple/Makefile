R=../..
include ${R}/config.mk

.PHONY: all

all : sub_callback sub_single sub_multiple

sub_callback : callback.o
	${CROSS_COMPILE}${CC} $^ -o $@ ${LIBMOSQ}

sub_single : single.o
	${CROSS_COMPILE}${CC} $^ -o $@ ${LIBMOSQ}

sub_multiple : multiple.o
	${CROSS_COMPILE}${CC} $^ -o $@ ${LIBMOSQ}

callback.o : callback.c ${LIBMOSQ}
	${CROSS_COMPILE}${CC} -c $< -o $@ -I${R}/lib ${LOCAL_CFLAGS}

single.o : single.c ${LIBMOSQ}
	${CROSS_COMPILE}${CC} -c $< -o $@ -I${R}/lib ${LOCAL_CFLAGS}

multiple.o : multiple.c ${LIBMOSQ}
	${CROSS_COMPILE}${CC} -c $< -o $@ -I${R}/lib ${LOCAL_CFLAGS}

${LIBMOSQ}:
	$(MAKE) -C ${R}/lib

clean :
	-rm -f *.o sub_single sub_multiple
