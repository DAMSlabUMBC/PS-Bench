/*
Copyright (c) 2023 Roger Light <roger@atchoo.org>
Copyright (c) 2023 Cedalo Gmbh

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

#include "mosquitto_broker_internal.h"
#include "utlist.h"


static void plugin__handle_client_offline_single(struct mosquitto__security_options *opts, struct mosquitto *context, int reason)
{
	struct mosquitto_evt_client_offline event_data;
	struct mosquitto__callback *cb_base, *cb_next;

	if(context->id == NULL) return;

	memset(&event_data, 0, sizeof(event_data));
	event_data.client = context;
	event_data.reason = reason;
	DL_FOREACH_SAFE(opts->plugin_callbacks.client_offline, cb_base, cb_next){
		cb_base->cb(MOSQ_EVT_CLIENT_OFFLINE, &event_data, cb_base->userdata);
	}
}


void plugin__handle_client_offline(struct mosquitto *context, int reason)
{
	/* Global plugins */
	plugin__handle_client_offline_single(&db.config->security_options, context, reason);

	/* Per listener plugins */
	if(db.config->per_listener_settings && context->listener){
		plugin__handle_client_offline_single(context->listener->security_options, context, reason);
	}
}
