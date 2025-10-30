/*
Copyright (c) 2023 Cedalo Gmbh
*/

#include "config.h"

#include <stdio.h>
#include <string.h>

#include "mosquitto.h"
#include "plugin_global.h"

int plugin__message_in_callback(int event, void *event_data, void *user_data)
{
	struct mosquitto_evt_message *ed = event_data;
	bool match;

	UNUSED(event);
	UNUSED(user_data);

	mosquitto_topic_matches_sub("spBv1.0/+/NBIRTH/+", ed->topic, &match);
	if(!match){
		mosquitto_topic_matches_sub("spBv1.0/+/DBIRTH/+/+", ed->topic, &match);
	}
	if(match){
		size_t len = strlen("$sparkplug/certificates/") + strlen(ed->topic) + 1;
		char *topic = mosquitto_malloc(len);
		if(topic){
			snprintf(topic, len, "$sparkplug/certificates/%s", ed->topic);
			mosquitto_broker_publish_copy(NULL, topic, (int)ed->payloadlen, ed->payload, ed->qos, true, ed->properties);
			mosquitto_free(topic);
		}
	}

	return MOSQ_ERR_SUCCESS;
}
