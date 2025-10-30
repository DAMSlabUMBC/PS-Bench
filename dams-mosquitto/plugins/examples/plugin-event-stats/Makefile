R=../../..
include ${R}/config.mk

PLUGIN_NAME=mosquitto_plugin_event_stats
LOCAL_CFLAGS+=
LOCAL_CPPFLAGS+=
LOCAL_LDFLAGS+=
LOCAL_LIBADD+=

all : binary

OBJS:=${PLUGIN_NAME}.o

PLUGIN_NOINST:=1
include ${R}/plugins/plugin.mk
