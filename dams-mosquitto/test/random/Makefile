R=../..
include ${R}/config.mk

.PHONY: all test

all : auth_plugin.so

auth_plugin.so : auth_plugin.c
	$(CC) ${LOCAL_CFLAGS} -fPIC -shared $< -o $@ -I${R}/lib -I${R}/src

${R}/lib/libmosquitto.so.${SOVERSION} :
	$(MAKE) -C ${R}/lib

${R}/lib/libmosquitto.a :
	$(MAKE) -C ${R}/lib libmosquitto.a

clean :
	-rm -f *.o random_client *.gcda *.gcno

test : all
	./test.py
