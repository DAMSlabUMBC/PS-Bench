R=../..
include ${R}/config.mk

PLUGIN_NAME=mosquitto_sparkplug_aware
LOCAL_CFLAGS+=
LOCAL_CPPFLAGS+=
LOCAL_LDFLAGS+=
LOCAL_LDADD+=

# Objects for this plugin only, built from source in this directory
OBJS = \
	on_message.o \
	plugin.o

# Objects from e.g. the common directory that are not in this directory
OBJS_EXTERNAL =

all : binary

include ${R}/plugins/plugin.mk
