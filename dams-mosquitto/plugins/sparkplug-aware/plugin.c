/*
Copyright (c) 2023 Cedalo Gmbh
*/

#include "config.h"

#include <stdlib.h>
#include <string.h>

#include "mosquitto/broker.h"
#include "mosquitto/broker_plugin.h"
#include "plugin_global.h"

MOSQUITTO_PLUGIN_DECLARE_VERSION(5);

static mosquitto_plugin_id_t *mosq_pid = NULL;

int mosquitto_plugin_init(mosquitto_plugin_id_t *identifier, void **user_data, struct mosquitto_opt *options, int option_count)
{
	int rc;

	UNUSED(user_data);
	UNUSED(options);
	UNUSED(option_count);

	mosq_pid = identifier;
	mosquitto_plugin_set_info(identifier, PLUGIN_NAME, PLUGIN_VERSION);

	rc = mosquitto_callback_register(mosq_pid, MOSQ_EVT_MESSAGE_IN, plugin__message_in_callback, NULL, NULL);
	if(rc) return rc;

	return MOSQ_ERR_SUCCESS;
}
