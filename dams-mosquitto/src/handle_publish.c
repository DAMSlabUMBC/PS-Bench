/*
Copyright (c) 2009-2021 Roger Light <roger@atchoo.org>

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
#include "alias_mosq.h"
#include "mosquitto/mqtt_protocol.h"
#include "packet_mosq.h"
#include "mp_registry.h" 
#include "ri_registry.h" 
#include "dr_registry.h" 
#include "rights_registry.h" 
#include "rights_broker.h" 
#include "sp_registry.h" 
#include "property_common.h"
#include "property_mosq.h"
#include "read_handle.h"
#include "send_mosq.h"
#include "sys_tree.h"
#include "util_mosq.h"


int handle__publish(struct mosquitto *context)
{
	uint8_t dup;
	int rc = 0;
	int rc2;
	uint8_t header = context->in_packet.command;
	int res = 0;
	struct mosquitto__base_msg *base_msg, *stored = NULL;
	struct mosquitto__client_msg *cmsg_stored = NULL;
	size_t len;
	uint16_t slen;
	char *topic_mount;
	mosquitto_property *properties = NULL;
	uint32_t message_expiry_interval = MSG_EXPIRY_INFINITE;
	int topic_alias = -1;
	uint8_t reason_code = 0;
	uint16_t mid = 0;

	// For operations
	bool found_op = false;
	char *op_id = NULL;
	char *op_info = NULL; 
	char *correlation_data = NULL;
	uint16_t correlation_data_len = 0;
	char *response_topic = NULL;

	bool found_purpose_filter = false;
	char* purpose_filter = NULL;

	if(context->state != mosq_cs_active){
		return MOSQ_ERR_PROTOCOL;
	}

	context->stats.messages_received++;

	base_msg = mosquitto_calloc(1, sizeof(struct mosquitto__base_msg));
	if(base_msg == NULL){
		return MOSQ_ERR_NOMEM;
	}

	dup = (header & 0x08)>>3;
	base_msg->data.qos = (header & 0x06)>>1;
	if(dup == 1 && base_msg->data.qos == 0){
		log__printf(NULL, MOSQ_LOG_INFO,
				"Invalid PUBLISH (QoS=0 and DUP=1) from %s, disconnecting.", context->id);
		db__msg_store_free(base_msg);
		return MOSQ_ERR_MALFORMED_PACKET;
	}
	if(base_msg->data.qos == 3){
		log__printf(NULL, MOSQ_LOG_INFO,
				"Invalid QoS in PUBLISH from %s, disconnecting.", context->id);
		db__msg_store_free(base_msg);
		return MOSQ_ERR_MALFORMED_PACKET;
	}
	if(base_msg->data.qos > context->max_qos){
		log__printf(NULL, MOSQ_LOG_INFO,
				"Too high QoS in PUBLISH from %s, disconnecting.", context->id);
		db__msg_store_free(base_msg);
		return MOSQ_ERR_QOS_NOT_SUPPORTED;
	}
	base_msg->data.retain = (header & 0x01);

	if(base_msg->data.retain && db.config->retain_available == false){
		db__msg_store_free(base_msg);
		return MOSQ_ERR_RETAIN_NOT_SUPPORTED;
	}

	if(packet__read_string(&context->in_packet, &base_msg->data.topic, &slen)){
		db__msg_store_free(base_msg);
		return MOSQ_ERR_MALFORMED_PACKET;
	}
	if(!slen && context->protocol != mosq_p_mqtt5){
		/* Invalid publish topic, disconnect client. */
		db__msg_store_free(base_msg);
		return MOSQ_ERR_MALFORMED_PACKET;
	}

	if(base_msg->data.qos > 0){
		if(packet__read_uint16(&context->in_packet, &mid)){
			db__msg_store_free(base_msg);
			return MOSQ_ERR_MALFORMED_PACKET;
		}
		if(mid == 0){
			db__msg_store_free(base_msg);
			return MOSQ_ERR_PROTOCOL;
		}
		/* It is important to have a separate copy of mid, because msg may be
		 * freed before we want to send a PUBACK/PUBREC. */
		base_msg->data.source_mid = mid;
	}

	/* Handle properties */
	if(context->protocol == mosq_p_mqtt5){
		rc = property__read_all(CMD_PUBLISH, &context->in_packet, &properties);
		if(rc){
			db__msg_store_free(base_msg);
			return rc;
		}

		/* Only run the below if using the framework */
		if(db.config->use_protection_framework)
		{
			/* Immediately check for consent and disallow if not given */
			const mosquitto_property *curr_prop_ptr = properties;
			bool consent_given = false;
			while(curr_prop_ptr)
			{
				/* Parse the current property name/value */
				char *name, *value;

				curr_prop_ptr = mosquitto_property_read_string_pair(curr_prop_ptr, MQTT_PROP_USER_PROPERTY, &name, &value, false );
				if(curr_prop_ptr)
				{
					/* Check if this is the consent property */
					if(!strcmp(name, MOSQ_DAP_CONSENT_KEY))
					{
						/* If value is "1", consent given, keep processing. Otherwise reject */
						if(!strcmp(value, "1"))
						{
							consent_given = true;
							break;
						}
						else
						{
							log__printf(NULL, MOSQ_LOG_INFO,
								"Consent not given for packet from %s, rejecting.",
								context->id);
							mosquitto_property_free_all(&properties);
							return MOSQ_ERR_MALFORMED_PACKET;
						}
					}
					/* Move to the next property */
					curr_prop_ptr = curr_prop_ptr->next;
				}
			}

			if(!consent_given)
			{
				log__printf(NULL, MOSQ_LOG_INFO,
					"Consent not given for packet from %s, rejecting.",
					context->id);
				mosquitto_property_free_all(&properties);
				return MOSQ_ERR_MALFORMED_PACKET;
			}

			/* Check if purpose filtering is enabled */
			if(db.config->purpose_filtering)
			{
				/* (1) Per-Message Declaration */
				if(db.config->purpose_filter_method == MOSQ_DAP_PER_MSG)
				{
					/* Since there can be multiple user properties, loop through them */
					const mosquitto_property *curr_prop_ptr = properties;
					while(curr_prop_ptr)
					{
						/* Parse the current property name/value */
						char *name, *value;
						curr_prop_ptr = mosquitto_property_read_string_pair(curr_prop_ptr, MQTT_PROP_USER_PROPERTY, &name, &value, false );
						if(curr_prop_ptr)
						{
							/* Check if this is a DAP-MP property */
							if(!strcmp(name, MOSQ_DAP_MP_KEY))
							{
								/* Message cannot have multiple purpose filters */
								if(found_purpose_filter)
								{
									log__printf(NULL, MOSQ_LOG_INFO,
										"More than one purpose filter from %s, rejecting.",
										context->id);
									mosquitto_property_free_all(&properties);
									return MOSQ_ERR_MALFORMED_PACKET;
								}
		
								/* Allocate memory for the filter */
								char *filter = mosquitto_malloc(strlen(value) + 1);
								if(!filter)
								{
									mosquitto_property_free_all(&properties);
									return MOSQ_ERR_NOMEM;
								}
								strcpy(filter, value);
								purpose_filter = filter;
								found_purpose_filter = true;
							}
							/* Move to the next property */
							curr_prop_ptr = curr_prop_ptr->next;
						}
					}
				}
				/* (2) Registration by Message */
				else if(db.config->purpose_filter_method == MOSQ_DAP_MSG_REG)
				{
					/* Check if this is a registration message on $DAP/purpose_management */
					if(!strcmp(base_msg->data.topic, MOSQ_DAP_PM_TOPIC))
					{
						/* Since there can be multiple user properties, loop through them */
						const mosquitto_property *curr_prop_ptr = properties;
						while(curr_prop_ptr)
						{
							/* Parse the current property name/value */
							char *name, *value;
							curr_prop_ptr = mosquitto_property_read_string_pair(curr_prop_ptr, MQTT_PROP_USER_PROPERTY, &name, &value, false );
							if(curr_prop_ptr)
							{
								/* Check if this is a DAP-MP property */
								if(!strcmp(name, MOSQ_DAP_MP_KEY))
								{
									char *temp, *filter, *topic = NULL;

									/* In Registration by Message, MP is of the form '<MP>:<topic> */
									temp = strchr(value, ':');

									if (temp == NULL)
									{
										mosquitto_property_free_all(&properties);
										return MOSQ_ERR_MALFORMED_PACKET;
									}

									uint32_t index = (uint32_t)(temp - value);
									temp++; /* Skip the ':' */
									
									// Allocate memory for topic and purpose
									filter = mosquitto_malloc(index + 1);
									topic = mosquitto_malloc(strlen(temp) + 1);
									if(!filter || !topic)
									{
										mosquitto_property_free_all(&properties);
										return MOSQ_ERR_NOMEM;
									}

									// Copy
									filter = strncpy(filter, value, index);
									filter[index] = '\0';
									topic = strcpy(topic, temp);

									/* Register the topic to purpose filter mapping */
									mp__register_topic(context->id, topic, filter);
								}
								/* Move to the next property */
								curr_prop_ptr = curr_prop_ptr->next;
							}
						}

						/* Do not forward this registration message */
						mosquitto_property_free_all(&properties);
						return MOSQ_ERR_SUCCESS;
					}
					else
					{
						/* Normal data publish: lookup stored purpose filter */
						char *stored = mp__lookup_topic(context->id, base_msg->data.topic);
						if(stored)
						{
							base_msg->data.purpose_filter = mosquitto_strdup(stored);
							base_msg->data.has_purpose_filter = true;
						}
						else
						{
							/* Copy "deny all" filter*/
							base_msg->data.purpose_filter = mosquitto_strdup("");
							base_msg->data.has_purpose_filter = true;
						}
					}
				}
				/* (3) Registration by Topic */
				else if(db.config->purpose_filter_method == MOSQ_DAP_TOPIC_REG)
				{
					/* Check if this is a registration topic starting with $DAP/MP_reg/ */
					if(strlen(base_msg->data.topic) >= strlen(MOSQ_DAP_MP_REG_TOPIC) && !strncmp(base_msg->data.topic, MOSQ_DAP_MP_REG_TOPIC, strlen(MOSQ_DAP_MP_REG_TOPIC)))
					{
						/* Parse out real_topic and mp_value from the bracketed suffix */
						const char *rest = base_msg->data.topic + strlen(MOSQ_DAP_MP_REG_TOPIC);
						char rt[256] = {0}, mp[256] = {0};
		
						const char *b = strchr(rest, '[');
						if(!b)
						{
							mosquitto_property_free_all(&properties);
							return MOSQ_ERR_PROTOCOL;
						}
		
						size_t rlen = b - rest;
						if(rlen > 255) rlen = 255;
						memcpy(rt, rest, rlen);
						rt[rlen] = '\0';
		
						const char *eb = strrchr(b, ']');
						if(!eb) return MOSQ_ERR_PROTOCOL;
						size_t mlen = eb - (b + 1);
						if(mlen > 255) mlen = 255;
						memcpy(mp, b + 1, mlen);
						mp[mlen] = '\0';
		
						mp__register_topic(context->id, rt, mp);

						/* Do not forward registration */
						mosquitto_property_free_all(&properties);
						return MOSQ_ERR_SUCCESS;
					}
					/* Check if the subscription topic begins with "$DAP/SP_reg/" */
					else if(strlen(base_msg->data.topic) >= strlen(MOSQ_DAP_SP_REG_TOPIC) && !strncmp(base_msg->data.topic, MOSQ_DAP_SP_REG_TOPIC, strlen(MOSQ_DAP_SP_REG_TOPIC)))
					{
						/*  Parse the special subscription topic of the form */
						const char *rest = base_msg->data.topic + strlen(MOSQ_DAP_SP_REG_TOPIC);

						/* SP can contain replacement terms for wildcards */
						char* curr_string = malloc(strlen(rest) + 1);
						strcpy(curr_string, rest);

						int found = 1;
						while(found)
						{
							found = 0;
							
							// Check for HASH first
							char* replace_start = strstr(curr_string, "HASH");
							if(replace_start != NULL)
							{
								size_t start_index = (size_t)(replace_start - curr_string);
								char* hash_end = replace_start + 4;
								size_t len_after_hash = strlen(hash_end);
								
								curr_string[start_index] = '#'; // Replace next part with literal '#'
								strncpy(&curr_string[start_index + 1], hash_end, len_after_hash); // Fill in the rest
								curr_string[start_index + len_after_hash + 1] = '\0';
								found = 1;
							}
							
							// Now check for PLUS
							replace_start = strstr(curr_string, "PLUS");
							if(replace_start != NULL)
							{
								size_t start_index = (size_t)(replace_start - curr_string);
								char* hash_end = replace_start + 4;
								size_t len_after_hash = strlen(hash_end);
								
								curr_string[start_index] = '+'; // Replace next part with literal '#'
								strncpy(&curr_string[start_index + 1], hash_end, len_after_hash); // Fill in the rest
								curr_string[start_index + len_after_hash + 1] = '\0';
								found = 1;
							}
						}

						rest = curr_string;
						
						/* Buffers for the real topic and the subscriber SP */
						char rt[256] = {0};
						char sp_val[256] = {0};
						const char *b = strchr(rest, '[');
						if (!b)
						{
							log__printf(NULL, MOSQ_LOG_INFO, 
								"SP registration error: missing '[' in %s", base_msg->data.topic);
							mosquitto_property_free_all(&properties);
							return MOSQ_ERR_INVAL;
						}
				
						/* Calculate and copy the real topic */
						size_t rlen = b - rest;
						if (rlen > 255)
							rlen = 255;
						memcpy(rt, rest, rlen);
						rt[rlen] = '\0';
				
						/* Find the closing bracket that ends the SP value */
						const char *eb = strrchr(b, ']');
						if (!eb)
						{
							log__printf(NULL, MOSQ_LOG_INFO, 
								"SP registration error: missing ']' in %s", base_msg->data.topic);
							return MOSQ_ERR_INVAL;
						}
				
						/* Copy the subscriber SP */
						size_t splen = eb - (b + 1);
						if (splen > 255)
							splen = 255;
						memcpy(sp_val, b + 1, splen);
						sp_val[splen] = '\0';
				
						/* Register this SP for the real topic in the sp_registry */
						sp__register_topic(context->id, rt, sp_val);

						/* Return success so that this is not forwarded as a normal subscription */
						mosquitto_property_free_all(&properties);
						return MOSQ_ERR_SUCCESS;
					}
					else
					{
						/* Normal data publish */
						char *stored = mp__lookup_topic(context->id, base_msg->data.topic);
						if(stored)
						{
							base_msg->data.purpose_filter = mosquitto_strdup(stored);
							base_msg->data.has_purpose_filter = true;
						}
						else
						{
							/* Copy "deny all" filter*/
							base_msg->data.purpose_filter = mosquitto_strdup("");
							base_msg->data.has_purpose_filter = true;
						}
					}
				}
			}

			/* Read all potential operational properties for later */
			if(db.config->metadata_operation_handling)
			{
				/* Look through the user properties for DAP-Operation */
				const mosquitto_property *p = properties;
				while(p){
					if(p->identifier == MQTT_PROP_USER_PROPERTY){
						char *name=NULL, *value=NULL;
						mosquitto_property_read_string_pair(p, MQTT_PROP_USER_PROPERTY, &name, &value, false);

						if(name && value){
							/* If we find DAP-Operation then note it. */
							if(!strcmp(name, MOSQ_DAP_OP_KEY)){
								found_op = true;
								op_id  = mosquitto_strdup(value);
							} else if(!strcmp(name, MOSQ_DAP_OP_INFO_KEY)){
								op_info = mosquitto_strdup(value);
							}
						}
					}
					else if (p->identifier == MQTT_PROP_CORRELATION_DATA)
					{
						mosquitto_property_read_binary(p, MQTT_PROP_CORRELATION_DATA, (void **)&correlation_data, &correlation_data_len, false);
					}
					else if(p->identifier == MQTT_PROP_RESPONSE_TOPIC)
					{
						mosquitto_property_read_string(p, MQTT_PROP_RESPONSE_TOPIC, &response_topic, false);
					}

					/* Move to next property. */
					p = p->next;
				}
			}
		}

		rc = property__process_publish(base_msg, &properties, &topic_alias, &message_expiry_interval);
		if(rc){
			mosquitto_property_free_all(&properties);
			db__msg_store_free(base_msg);
			return MOSQ_ERR_PROTOCOL;
		}
	}
	mosquitto_property_free_all(&properties);

	if(topic_alias == 0 || (context->listener && topic_alias > context->listener->max_topic_alias)){
		db__msg_store_free(base_msg);
		return MOSQ_ERR_TOPIC_ALIAS_INVALID;
	}else if(topic_alias > 0){
		if(base_msg->data.topic){
			rc = alias__add_r2l(context, base_msg->data.topic, (uint16_t)topic_alias);
			if(rc){
				db__msg_store_free(base_msg);
				return rc;
			}
		}else{
			rc = alias__find_by_alias(context, ALIAS_DIR_R2L, (uint16_t)topic_alias, &base_msg->data.topic);
			if(rc){
				db__msg_store_free(base_msg);
				return MOSQ_ERR_PROTOCOL;
			}
		}
	}

#ifdef WITH_BRIDGE
	rc = bridge__remap_topic_in(context, &base_msg->data.topic);
	if(rc){
		db__msg_store_free(base_msg);
		return rc;
	}

#endif
	if(mosquitto_pub_topic_check(base_msg->data.topic) != MOSQ_ERR_SUCCESS){
		/* Invalid publish topic, just swallow it. */
		db__msg_store_free(base_msg);
		return MOSQ_ERR_MALFORMED_PACKET;
	}

	if(db.config->use_protection_framework && db.config->purpose_filtering)
	{
		/* Purpose filter must exist if filtering type is MOSQ_DAP_PER_MSG */
		if(db.config->purpose_filter_method == MOSQ_DAP_PER_MSG)
		{
			if(found_purpose_filter)
			{
				base_msg->data.purpose_filter = purpose_filter;
				base_msg->data.has_purpose_filter = true;
			}
			else if (db.config->purpose_filtering && db.config->purpose_filter_method == MOSQ_DAP_PER_MSG)
			{
				log__printf(NULL, MOSQ_LOG_INFO,
					"Purpose filter not specified by publication from %s, rejecting.",
					context->id);
					db__msg_store_free(base_msg);
					return MOSQ_ERR_MALFORMED_PACKET;
			}
		}
		else if(db.config->purpose_filter_method == MOSQ_DAP_NONE)
		{
			base_msg->data.has_purpose_filter = false;
		}
	}

	base_msg->data.payloadlen = context->in_packet.remaining_length - context->in_packet.pos;
	metrics__int_inc(mosq_counter_pub_bytes_received, base_msg->data.payloadlen);
	if(context->listener && context->listener->mount_point){
		len = strlen(context->listener->mount_point) + strlen(base_msg->data.topic) + 1;
		topic_mount = mosquitto_malloc(len+1);
		if(!topic_mount){
			db__msg_store_free(base_msg);
			return MOSQ_ERR_NOMEM;
		}
		snprintf(topic_mount, len, "%s%s", context->listener->mount_point, base_msg->data.topic);
		topic_mount[len] = '\0';

		mosquitto_FREE(base_msg->data.topic);
		base_msg->data.topic = topic_mount;
	}

	if(base_msg->data.payloadlen){
		if(db.config->message_size_limit && base_msg->data.payloadlen > db.config->message_size_limit){
			log__printf(NULL, MOSQ_LOG_DEBUG, "Dropped too large PUBLISH from %s (d%d, q%d, r%d, m%d, '%s', ... (%ld bytes))", context->id, dup, base_msg->data.qos, base_msg->data.retain, base_msg->data.source_mid, base_msg->data.topic, (long)base_msg->data.payloadlen);
			reason_code = MQTT_RC_PACKET_TOO_LARGE;
			goto process_bad_message;
		}
		base_msg->data.payload = mosquitto_malloc(base_msg->data.payloadlen+1);
		if(base_msg->data.payload == NULL){
			db__msg_store_free(base_msg);
			return MOSQ_ERR_NOMEM;
		}
		/* Ensure payload is always zero terminated, this is the reason for the extra byte above */
		((uint8_t *)base_msg->data.payload)[base_msg->data.payloadlen] = 0;

		if(packet__read_bytes(&context->in_packet, base_msg->data.payload, base_msg->data.payloadlen)){
			db__msg_store_free(base_msg);
			return MOSQ_ERR_MALFORMED_PACKET;
		}
	}

	/* Check for topic access */
	rc = mosquitto_acl_check(context, base_msg->data.topic, base_msg->data.payloadlen, base_msg->data.payload, base_msg->data.qos, base_msg->data.retain, MOSQ_ACL_WRITE);
	if(rc == MOSQ_ERR_ACL_DENIED){
		log__printf(NULL, MOSQ_LOG_DEBUG,
				"Denied PUBLISH from %s (d%d, q%d, r%d, m%d, '%s', ... (%ld bytes))",
				context->id, dup, base_msg->data.qos, base_msg->data.retain, base_msg->data.source_mid, base_msg->data.topic,
				(long)base_msg->data.payloadlen);
		reason_code = MQTT_RC_NOT_AUTHORIZED;
		goto process_bad_message;
	}else if(rc != MOSQ_ERR_SUCCESS){
		db__msg_store_free(base_msg);
		return rc;
	}

	log__printf(NULL, MOSQ_LOG_DEBUG, "Received PUBLISH from %s (d%d, q%d, r%d, m%d, '%s', ... (%ld bytes))", context->id, dup, base_msg->data.qos, base_msg->data.retain, base_msg->data.source_mid, base_msg->data.topic, (long)base_msg->data.payloadlen);

	if(!strncmp(base_msg->data.topic, "$CONTROL/", 9)){
#ifdef WITH_CONTROL
		rc = control__process(context, base_msg);
		db__msg_store_free(base_msg);
		return rc;
#else
		reason_code = MQTT_RC_IMPLEMENTATION_SPECIFIC;
		goto process_bad_message;
#endif
	}

	{
		rc = plugin__handle_message_in(context, &base_msg->data);
		if(rc == MOSQ_ERR_ACL_DENIED){
			log__printf(NULL, MOSQ_LOG_DEBUG,
					"Denied PUBLISH from %s (d%d, q%d, r%d, m%d, '%s', ... (%ld bytes))",
					context->id, dup, base_msg->data.qos, base_msg->data.retain, base_msg->data.source_mid, base_msg->data.topic,
					(long)base_msg->data.payloadlen);

			reason_code = MQTT_RC_NOT_AUTHORIZED;
			goto process_bad_message;
		}else if(rc == MOSQ_ERR_QUOTA_EXCEEDED){
			log__printf(NULL, MOSQ_LOG_DEBUG,
					"Rejected PUBLISH from %s, quota exceeded.", context->id);

			reason_code = MQTT_RC_QUOTA_EXCEEDED;
			goto process_bad_message;
		}else if(rc != MOSQ_ERR_SUCCESS){
			db__msg_store_free(base_msg);
			return rc;
		}
	}

	if(base_msg->data.qos > 0){
		db__message_store_find(context, base_msg->data.source_mid, &cmsg_stored);
	}

	if(cmsg_stored && cmsg_stored->base_msg && base_msg->data.source_mid != 0 &&
			(cmsg_stored->base_msg->data.qos != base_msg->data.qos
			 || cmsg_stored->base_msg->data.payloadlen != base_msg->data.payloadlen
			 || strcmp(cmsg_stored->base_msg->data.topic, base_msg->data.topic)
			 || memcmp(cmsg_stored->base_msg->data.payload, base_msg->data.payload, base_msg->data.payloadlen) )){

		log__printf(NULL, MOSQ_LOG_WARNING, "Reused message ID %u from %s detected. Clearing from storage.", base_msg->data.source_mid, context->id);
		db__message_remove_incoming(context, base_msg->data.source_mid);
		cmsg_stored = NULL;
	}

	if(!cmsg_stored){
		if(base_msg->data.qos > 0 && context->msgs_in.inflight_quota == 0){
			/* Client isn't allowed any more incoming messages, so fail early */
			db__msg_store_free(base_msg);
			return MOSQ_ERR_RECEIVE_MAXIMUM_EXCEEDED;
		}

		if(base_msg->data.qos == 0
				|| db__ready_for_flight(context, mosq_md_in, base_msg->data.qos)
				){

			dup = 0;
			rc = db__message_store(context, base_msg, &message_expiry_interval, mosq_mo_client);
			if(rc) return rc;
		}else{
			/* Client isn't allowed any more incoming messages, so fail early */
			reason_code = MQTT_RC_QUOTA_EXCEEDED;
			goto process_bad_message;
		}
		stored = base_msg;
		base_msg = NULL;
		dup = 0;
	}else{
		db__msg_store_free(base_msg);
		base_msg = NULL;
		stored = cmsg_stored->base_msg;
		cmsg_stored->data.dup++;
		dup = cmsg_stored->data.dup;
	}

	if(stored->data.retain)
	{
		dr__record_retained_publisher(context->id, stored->data.topic);
	}

	if(db.config->use_protection_framework)
	{
		/* Read all potential operational properties for later */
		if(db.config->metadata_operation_handling && found_op)
		{
			if(!strncmp(stored->data.topic, MOSQ_DAP_TOPIC_OSYS, 5))
			{
				/* C1 Operations */
				if(!strcmp(op_id, MOSQ_DAP_RIGHT_INFORMED))
				{
					subscription_list *subs = find_subscriptions_for_publisher(context->id);
					while(subs){
						const char *info = ri__lookup_info(subs->subscriber_id);
						if(info){
							broker_send_response_success(context->id, op_id, correlation_data, correlation_data_len, info, response_topic);
							ri__mark_sent_to_pub(context->id, subs->subscriber_id);
						}
						subs = subs->next;
					}
				}

				/* C1 Registration Operations */
				else if(!strcmp(op_id, MOSQ_DAP_RIGHT_INFORMED_REG))
				{
					ri__register_info(context->id, stored->data.payload);
				}						

				/* C2/C3 Operations */
				else if (!strcmp(op_id, MOSQ_DAP_RIGHT_ACCESS) || !strcmp(op_id, MOSQ_DAP_RIGHT_PORTABILITY) || !strcmp(op_id, MOSQ_DAP_RIGHT_RECTIFICATION) 
				|| !strcmp(op_id, MOSQ_DAP_RIGHT_ERASURE) || !strcmp(op_id, MOSQ_DAP_RIGHT_RESTRICTION)   || !strcmp(op_id, MOSQ_DAP_RIGHT_OBJECT) 
				|| !strcmp(op_id, MOSQ_DAP_RIGHT_AUTODECISION))
				{
					/* Foward requests only to subs that have data */
					subscriber_list *sub_list = find_subscribers_with_data(context->id, op_info);
					subscriber_list *offline = forward_request_to_connected(sub_list, &stored->data, NULL, op_id, op_info, correlation_data, correlation_data_len);
					if(offline){
						broker_send_response_failure(context->id, op_id, correlation_data, correlation_data_len, "Subscriber not connected", offline);
					}
					else
					{
						broker_send_response_success(context->id, op_id, correlation_data, correlation_data_len, NULL, response_topic);
					}

					/* Erasure has an extra consideration */
					if(!strcmp(op_id, MOSQ_DAP_RIGHT_ERASURE))
					{
						handle_remove_stored_messages(context->id);
					}
				}

				else
				{
					/* Unrecognized right. */
					broker_send_response_failure(context->id, op_id, correlation_data, correlation_data_len, "Unknown right", NULL);
				}
			}
		}
	}

	switch(stored->data.qos){
		case 0:
			rc2 = sub__messages_queue(context->id, stored->data.topic, stored->data.qos, stored->data.retain, &stored);
			if(rc2 > 0) rc = 1;
			break;
		case 1:
			util__decrement_receive_quota(context);
			rc2 = sub__messages_queue(context->id, stored->data.topic, stored->data.qos, stored->data.retain, &stored);
			/* stored may now be free, so don't refer to it */
			if(rc2 == MOSQ_ERR_SUCCESS || context->protocol != mosq_p_mqtt5){
				if(send__puback(context, mid, 0, NULL)) rc = 1;
			}else if(rc2 == MOSQ_ERR_NO_SUBSCRIBERS){
				if(send__puback(context, mid, MQTT_RC_NO_MATCHING_SUBSCRIBERS, NULL)) rc = 1;
			}else{
				rc = rc2;
			}
			break;
		case 2:
			if(dup == 0){
				res = db__message_insert_incoming(context, 0, stored, true);
			}else{
				res = 0;
			}

			/* db__message_insert() returns 2 to indicate dropped message
			 * due to queue. This isn't an error so don't disconnect them. */
			/* FIXME - this is no longer necessary due to failing early above */
			if(!res){
				if(dup == 0 || dup == 1){
					rc2 = send__pubrec(context, stored->data.source_mid, 0, NULL);
					if(rc2) rc = rc2;
				}else{
					return MOSQ_ERR_PROTOCOL;
				}
			}else if(res == 1){
				rc = 1;
			}
			break;
	}

	db__message_write_queued_in(context);
	return rc;
process_bad_message:
	rc = 1;
	if(base_msg){
		switch(base_msg->data.qos){
			case 0:
				rc = MOSQ_ERR_SUCCESS;
				break;
			case 1:
				rc = send__puback(context, base_msg->data.source_mid, reason_code, NULL);
				break;
			case 2:
				rc = send__pubrec(context, base_msg->data.source_mid, reason_code, NULL);
				break;
		}
		db__msg_store_free(base_msg);
	}
	if(context->out_packet_count >= db.config->max_queued_messages){
		rc = MQTT_RC_QUOTA_EXCEEDED;
	}
	return rc;
}
