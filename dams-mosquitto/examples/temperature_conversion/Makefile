R=../..
LOCAL_CFLAGS=-Wall -ggdb -I${R}/lib -I${R}/lib/cpp
LOCAL_LDFLAGS=-L${R}/lib ${R}/lib/cpp/libmosquittopp.so.1 ${LIBMOSQ}

.PHONY: all clean

all : mqtt_temperature_conversion

mqtt_temperature_conversion : main.o temperature_conversion.o
	${CXX} $^ -o $@ ${LOCAL_LDFLAGS}

main.o : main.cpp
	${CXX} -c $^ -o $@ ${LOCAL_CFLAGS}

temperature_conversion.o : temperature_conversion.cpp
	${CXX} -c $^ -o $@ ${LOCAL_CFLAGS}

clean :
	-rm -f *.o mqtt_temperature_conversion
