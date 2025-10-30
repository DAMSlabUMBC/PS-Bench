/*
Copyright (c) 2015-2021 Roger Light <roger@atchoo.org>

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

#ifndef SYS_TREE_H
#define SYS_TREE_H

#if defined(WITH_SYS_TREE) && defined(WITH_BROKER)

/* This ordering *must* match the metrics array in sys_tree.c. */
enum mosq_metric_type{
	mosq_gauge_clients_total = 0,
	mosq_counter_clients_maximum = 1,
	mosq_gauge_clients_disconnected = 2,
	mosq_gauge_clients_connected = 3,
	mosq_counter_clients_expired = 4,
	mosq_gauge_message_store_count = 5,
	mosq_gauge_message_store_bytes = 6,
	mosq_gauge_subscriptions = 7,
	mosq_gauge_shared_subscriptions = 8,
	mosq_gauge_retained_messages = 9,
	mosq_gauge_heap_current = 10,
	mosq_counter_heap_maximum = 11,
	mosq_counter_messages_received = 12,
	mosq_counter_messages_sent = 13,
	mosq_counter_bytes_received = 14,
	mosq_counter_bytes_sent = 15,
	mosq_counter_pub_bytes_received = 16,
	mosq_counter_pub_bytes_sent = 17,
	mosq_gauge_out_packets = 18,
	mosq_gauge_out_packet_bytes = 19,
	mosq_counter_socket_connections = 20,
	mosq_counter_mqtt_connect_received = 21,
	mosq_counter_mqtt_connect_sent = 22,
	mosq_counter_mqtt_connack_received = 23,
	mosq_counter_mqtt_connack_sent = 24,
	mosq_counter_mqtt_publish_dropped = 25,
	mosq_counter_mqtt_publish_received = 26,
	mosq_counter_mqtt_publish_sent = 27,
	mosq_counter_mqtt_puback_received = 28,
	mosq_counter_mqtt_puback_sent = 29,
	mosq_counter_mqtt_pubrec_received = 30,
	mosq_counter_mqtt_pubrec_sent = 31,
	mosq_counter_mqtt_pubrel_received = 32,
	mosq_counter_mqtt_pubrel_sent = 33,
	mosq_counter_mqtt_pubcomp_received = 34,
	mosq_counter_mqtt_pubcomp_sent = 35,
	mosq_counter_mqtt_subscribe_received = 36,
	mosq_counter_mqtt_subscribe_sent = 37,
	mosq_counter_mqtt_suback_received = 38,
	mosq_counter_mqtt_suback_sent = 39,
	mosq_counter_mqtt_unsubscribe_received = 40,
	mosq_counter_mqtt_unsubscribe_sent = 41,
	mosq_counter_mqtt_unsuback_received = 42,
	mosq_counter_mqtt_unsuback_sent = 43,
	mosq_counter_mqtt_pingreq_received = 44,
	mosq_counter_mqtt_pingreq_sent = 45,
	mosq_counter_mqtt_pingresp_received = 46,
	mosq_counter_mqtt_pingresp_sent = 47,
	mosq_counter_mqtt_disconnect_received = 48,
	mosq_counter_mqtt_disconnect_sent = 49,
	mosq_counter_mqtt_auth_received = 50,
	mosq_counter_mqtt_auth_sent = 51,

	mosq_metric_max,
};

/* This ordering *must* match the metrics_load array in sys_tree.c. */
enum mosq_metric_load_type{
	mosq_load_messages_received_1min = 0,
	mosq_load_messages_received_5min = 1,
	mosq_load_messages_received_15min = 2,
	mosq_load_messages_sent_1min = 3,
	mosq_load_messages_sent_5min = 4,
	mosq_load_messages_sent_15min = 5,
	mosq_load_pub_messages_dropped_1min = 6,
	mosq_load_pub_messages_dropped_5min = 7,
	mosq_load_pub_messages_dropped_15min = 8,
	mosq_load_pub_messages_received_1min = 9,
	mosq_load_pub_messages_received_5min = 10,
	mosq_load_pub_messages_received_15min = 11,
	mosq_load_pub_messages_sent_1min = 12,
	mosq_load_pub_messages_sent_5min = 13,
	mosq_load_pub_messages_sent_15min = 14,
	mosq_load_bytes_received_1min = 15,
	mosq_load_bytes_received_5min = 16,
	mosq_load_bytes_received_15min = 17,
	mosq_load_bytes_sent_1min = 18,
	mosq_load_bytes_sent_5min = 19,
	mosq_load_bytes_sent_15min = 20,
	mosq_load_sockets_1min = 21,
	mosq_load_sockets_5min = 22,
	mosq_load_sockets_15min = 23,
	mosq_load_connections_1min = 24,
	mosq_load_connections_5min = 25,
	mosq_load_connections_15min = 26,

	mosq_metric_load_max,
};

void metrics__int_inc(enum mosq_metric_type m, int64_t value);
void metrics__int_dec(enum mosq_metric_type m, int64_t value);

#else
#  define metrics__int_inc(A, B)
#  define metrics__int_dec(A, B)

#endif

#endif
