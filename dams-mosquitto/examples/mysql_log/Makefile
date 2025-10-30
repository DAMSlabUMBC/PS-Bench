R=../..
LOCAL_CFLAGS=-Wall -ggdb
LOCAL_LDFLAGS=${LIBMOSQ} -lmysqlclient

.PHONY: all clean

all : mosquitto_mysql_log

mosquitto_mysql_log : mysql_log.o
	${CC} $^ -o $@ ${LOCAL_LDFLAGS}

mysql_log.o : mysql_log.c
	${CC} -c $^ -o $@ ${LOCAL_CFLAGS} -I${R}/lib

clean :
	-rm -f *.o mosquitto_mysql_log
