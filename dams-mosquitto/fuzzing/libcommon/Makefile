R=../..
include ${R}/fuzzing/config.mk

.PHONY: all clean

FUZZERS:= \
	libcommon_fuzz_pub_topic_check2 \
	libcommon_fuzz_sub_topic_check2 \
	libcommon_fuzz_topic_tokenise \
	libcommon_fuzz_utf8

LOCAL_CPPFLAGS+=-I${R}/include/
LOCAL_CXXFLAGS+=-g -Wall -Werror -pthread
LOCAL_LDFLAGS+=
LOCAL_LIBADD+=$(LIB_FUZZING_ENGINE) -lssl -lcrypto ${R}/libcommon/libmosquitto_common.a -Wl,-Bstatic -largon2 -Wl,-Bdynamic

all: $(FUZZERS)

libcommon_fuzz_pub_topic_check2 : libcommon_fuzz_pub_topic_check2.cpp
	$(CXX) $(LOCAL_CXXFLAGS) $(LOCAL_CPPFLAGS) $(LOCAL_LDFLAGS) -o $@ $^ $(LOCAL_LIBADD)
	install $@ ${OUT}/$@

libcommon_fuzz_sub_topic_check2 : libcommon_fuzz_sub_topic_check2.cpp
	$(CXX) $(LOCAL_CXXFLAGS) $(LOCAL_CPPFLAGS) $(LOCAL_LDFLAGS) -o $@ $^ $(LOCAL_LIBADD)
	install $@ ${OUT}/$@

libcommon_fuzz_topic_tokenise : libcommon_fuzz_topic_tokenise.cpp
	$(CXX) $(LOCAL_CXXFLAGS) $(LOCAL_CPPFLAGS) $(LOCAL_LDFLAGS) -o $@ $^ $(LOCAL_LIBADD)
	install $@ ${OUT}/$@

libcommon_fuzz_utf8 : libcommon_fuzz_utf8.cpp
	$(CXX) $(LOCAL_CXXFLAGS) $(LOCAL_CPPFLAGS) $(LOCAL_LDFLAGS) -o $@ $^ $(LOCAL_LIBADD)
	install $@ ${OUT}/$@

clean:
	rm -f *.o $(FUZZERS)
