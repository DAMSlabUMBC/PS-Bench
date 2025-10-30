/*
Copyright (c) 2023 Cedalo Gmbh
*/
#ifndef PLUGIN_GLOBAL_H
#define PLUGIN_GLOBAL_H

#include "config.h"

/* PLUGIN_NAME and PLUGIN_VERSION reported to the broker */
#define PLUGIN_NAME "sparkplug-aware"
#define PLUGIN_VERSION "1.0"

int plugin__message_in_callback(int event, void *event_data, void *user_data);

#endif
