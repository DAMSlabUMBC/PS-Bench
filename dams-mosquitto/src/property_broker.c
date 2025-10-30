/*
Copyright (c) 2018-2021 Roger Light <roger@atchoo.org>

All rights reserved. This program and the accompanying materials
are made available under the terms of the Eclipse Public License 2.0
and Eclipse Distribution License v1.0 which accompany this distribution.

The Eclipse Public License is available at
   https://www.eclipse.org/legal/epl-2.0/
and the Eclipse Distribution License is available at
  http://www.eclipse.org/org/documents/edl-v10.php.

SPDX-License-Identifier: EPL-2.0 OR BSD-3-Clause

Contributors:
   Roger Light - initial implementation and documentation.
*/

#include "config.h"

#include <stdio.h>
#include <string.h>

#include "mosquitto_broker_internal.h"
#include "mosquitto/mqtt_protocol.h"
#include "property_mosq.h"
#include "property_common.h"

/* Process the incoming properties, we should be able to assume that only valid
 * properties for CONNECT are present here. */
int property__process_connect(struct mosquitto *context, mosquitto_property **props)
{
	mosquitto_property *p;

	p = *props;

	while(p){
		switch(mosquitto_property_identifier(p)){
			case MQTT_PROP_SESSION_EXPIRY_INTERVAL:
				context->session_expiry_interval = mosquitto_property_int32_value(p);
				break;

			case MQTT_PROP_RECEIVE_MAXIMUM:
				context->msgs_out.inflight_maximum = mosquitto_property_int16_value(p);
				if(context->msgs_out.inflight_maximum == 0){
					return MOSQ_ERR_PROTOCOL;
				}
				context->msgs_out.inflight_quota = context->msgs_out.inflight_maximum;
				break;

			case MQTT_PROP_MAXIMUM_PACKET_SIZE:
				context->maximum_packet_size = mosquitto_property_int32_value(p);
				if(context->maximum_packet_size == 0){
					return MOSQ_ERR_PROTOCOL;
				}
				break;

			case MQTT_PROP_TOPIC_ALIAS_MAXIMUM:
				context->alias_max_l2r = mosquitto_property_int16_value(p);
				if(context->alias_max_l2r > context->listener->max_topic_alias_broker){
					context->alias_max_l2r = context->listener->max_topic_alias_broker;
				}
				break;

			default:
				break;
		}
		p = mosquitto_property_next(p);
	}

	return MOSQ_ERR_SUCCESS;
}


int property__process_will(struct mosquitto *context, struct mosquitto_message_all *msg, mosquitto_property **props)
{
	mosquitto_property *p, *p_prev;
	mosquitto_property *msg_properties, *msg_properties_last;

	p = *props;
	p_prev = NULL;
	msg_properties = NULL;
	msg_properties_last = NULL;
	msg->expiry_interval = MSG_EXPIRY_INFINITE;
	while(p){
		switch(mosquitto_property_identifier(p)){
			case MQTT_PROP_CONTENT_TYPE:
			case MQTT_PROP_CORRELATION_DATA:
			case MQTT_PROP_PAYLOAD_FORMAT_INDICATOR:
			case MQTT_PROP_RESPONSE_TOPIC:
			case MQTT_PROP_USER_PROPERTY:
				/* We save these properties for transmission with the PUBLISH */

				/* Add this property to the end of the list */
				if(msg_properties){
					msg_properties_last->next = p;
					msg_properties_last = p;
				}else{
					msg_properties = p;
					msg_properties_last = p;
				}

				/* And remove it from *props */
				if(p_prev){
					p_prev->next = mosquitto_property_next(p);
					p = mosquitto_property_next(p_prev);
				}else{
					*props = mosquitto_property_next(p);
					p = *props;
				}
				msg_properties_last->next = NULL;
				break;

			case MQTT_PROP_WILL_DELAY_INTERVAL:
				/* Leave this in *props, to be freed */
				context->will_delay_interval = mosquitto_property_int32_value(p);
				p_prev = p;
				p = mosquitto_property_next(p);
				break;

			case MQTT_PROP_MESSAGE_EXPIRY_INTERVAL:
				/* Leave this in *props, to be freed */
				msg->expiry_interval = mosquitto_property_int32_value(p);
				p_prev = p;
				p = mosquitto_property_next(p);
				break;

			default:
				msg->properties = msg_properties;
				return MOSQ_ERR_PROTOCOL;
				break;
		}
	}

	msg->properties = msg_properties;
	return MOSQ_ERR_SUCCESS;
}


int property__process_publish(struct mosquitto__base_msg *base_msg, mosquitto_property **props, int *topic_alias, uint32_t *message_expiry_interval)
{
	mosquitto_property *p, *p_prev;
	mosquitto_property *msg_properties_last;

	p = *props;
	p_prev = NULL;
	base_msg->data.properties = NULL;
	msg_properties_last = NULL;
	while(p){
		switch(mosquitto_property_identifier(p)){
			case MQTT_PROP_CONTENT_TYPE:
			case MQTT_PROP_CORRELATION_DATA:
			case MQTT_PROP_PAYLOAD_FORMAT_INDICATOR:
			case MQTT_PROP_RESPONSE_TOPIC:
			case MQTT_PROP_USER_PROPERTY:
				if(base_msg->data.properties){
					msg_properties_last->next = p;
					msg_properties_last = p;
				}else{
					base_msg->data.properties = p;
					msg_properties_last = p;
				}
				if(p_prev){
					p_prev->next = mosquitto_property_next(p);
					p = mosquitto_property_next(p_prev);
				}else{
					*props = mosquitto_property_next(p);
					p = *props;
				}
				msg_properties_last->next = NULL;
				break;

			case MQTT_PROP_TOPIC_ALIAS:
				*topic_alias = mosquitto_property_int16_value(p);
				p_prev = p;
				p = mosquitto_property_next(p);
				break;

			case MQTT_PROP_MESSAGE_EXPIRY_INTERVAL:
				*message_expiry_interval = mosquitto_property_int32_value(p);
				p_prev = p;
				p = mosquitto_property_next(p);
				break;

			case MQTT_PROP_SUBSCRIPTION_IDENTIFIER:
				if(mosquitto_property_varint_value(p) == 0){
					return MOSQ_ERR_PROTOCOL;
				}
				p_prev = p;
				p = mosquitto_property_next(p);
				break;

			default:
				p = mosquitto_property_next(p);
				break;
		}
	}

	return MOSQ_ERR_SUCCESS;
}

/* Process the incoming properties, we should be able to assume that only valid
 * properties for DISCONNECT are present here. */
int property__process_disconnect(struct mosquitto *context, mosquitto_property **props)
{
	mosquitto_property *p;

	p = *props;

	while(p){
		if(mosquitto_property_identifier(p) == MQTT_PROP_SESSION_EXPIRY_INTERVAL){
			uint32_t session_expiry_interval = mosquitto_property_int32_value(p);
			if(context->session_expiry_interval == 0 && session_expiry_interval != 0){
				return MOSQ_ERR_PROTOCOL;
			}
			context->session_expiry_interval = session_expiry_interval;
		}
		p = mosquitto_property_next(p);
	}
	return MOSQ_ERR_SUCCESS;
}
