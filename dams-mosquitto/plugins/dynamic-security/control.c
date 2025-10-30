/*
Copyright (c) 2020-2021 Roger Light <roger@atchoo.org>

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

#include <cjson/cJSON.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

#include "dynamic_security.h"
#include "json_help.h"

#define RESPONSE_TOPIC "$CONTROL/dynamic-security/v1/response"

static int dynsec__handle_command(struct mosquitto_control_cmd *cmd, void *userdata)
{
	struct dynsec__data *data = userdata;
	int rc = MOSQ_ERR_SUCCESS;

	/* Plugin */
	if(!strcasecmp(cmd->command_name, "setDefaultACLAccess")){
		rc = dynsec__process_set_default_acl_access(data, cmd);
	}else if(!strcasecmp(cmd->command_name, "getDefaultACLAccess")){
		rc = dynsec__process_get_default_acl_access(data, cmd);

	/* Clients */
	}else if(!strcasecmp(cmd->command_name, "createClient")){
		rc = dynsec_clients__process_create(data, cmd);
	}else if(!strcasecmp(cmd->command_name, "deleteClient")){
		rc = dynsec_clients__process_delete(data, cmd);
	}else if(!strcasecmp(cmd->command_name, "getClient")){
		rc = dynsec_clients__process_get(data, cmd);
	}else if(!strcasecmp(cmd->command_name, "listClients")){
		rc = dynsec_clients__process_list(data, cmd);
	}else if(!strcasecmp(cmd->command_name, "modifyClient")){
		rc = dynsec_clients__process_modify(data, cmd);
	}else if(!strcasecmp(cmd->command_name, "setClientPassword")){
		rc = dynsec_clients__process_set_password(data, cmd);
	}else if(!strcasecmp(cmd->command_name, "setClientId")){
		rc = dynsec_clients__process_set_id(data, cmd);
	}else if(!strcasecmp(cmd->command_name, "addClientRole")){
		rc = dynsec_clients__process_add_role(data, cmd);
	}else if(!strcasecmp(cmd->command_name, "removeClientRole")){
		rc = dynsec_clients__process_remove_role(data, cmd);
	}else if(!strcasecmp(cmd->command_name, "enableClient")){
		rc = dynsec_clients__process_enable(data, cmd);
	}else if(!strcasecmp(cmd->command_name, "disableClient")){
		rc = dynsec_clients__process_disable(data, cmd);

	/* Groups */
	}else if(!strcasecmp(cmd->command_name, "addGroupClient")){
		rc = dynsec_groups__process_add_client(data, cmd);
	}else if(!strcasecmp(cmd->command_name, "createGroup")){
		rc = dynsec_groups__process_create(data, cmd);
	}else if(!strcasecmp(cmd->command_name, "deleteGroup")){
		rc = dynsec_groups__process_delete(data, cmd);
	}else if(!strcasecmp(cmd->command_name, "getGroup")){
		rc = dynsec_groups__process_get(data, cmd);
	}else if(!strcasecmp(cmd->command_name, "listGroups")){
		rc = dynsec_groups__process_list(data, cmd);
	}else if(!strcasecmp(cmd->command_name, "modifyGroup")){
		rc = dynsec_groups__process_modify(data, cmd);
	}else if(!strcasecmp(cmd->command_name, "removeGroupClient")){
		rc = dynsec_groups__process_remove_client(data, cmd);
	}else if(!strcasecmp(cmd->command_name, "addGroupRole")){
		rc = dynsec_groups__process_add_role(data, cmd);
	}else if(!strcasecmp(cmd->command_name, "removeGroupRole")){
		rc = dynsec_groups__process_remove_role(data, cmd);
	}else if(!strcasecmp(cmd->command_name, "setAnonymousGroup")){
		rc = dynsec_groups__process_set_anonymous_group(data, cmd);
	}else if(!strcasecmp(cmd->command_name, "getAnonymousGroup")){
		rc = dynsec_groups__process_get_anonymous_group(data, cmd);

	/* Roles */
	}else if(!strcasecmp(cmd->command_name, "createRole")){
		rc = dynsec_roles__process_create(data, cmd);
	}else if(!strcasecmp(cmd->command_name, "getRole")){
		rc = dynsec_roles__process_get(data, cmd);
	}else if(!strcasecmp(cmd->command_name, "listRoles")){
		rc = dynsec_roles__process_list(data, cmd);
	}else if(!strcasecmp(cmd->command_name, "modifyRole")){
		rc = dynsec_roles__process_modify(data, cmd);
	}else if(!strcasecmp(cmd->command_name, "deleteRole")){
		rc = dynsec_roles__process_delete(data, cmd);
	}else if(!strcasecmp(cmd->command_name, "addRoleACL")){
		rc = dynsec_roles__process_add_acl(data, cmd);
	}else if(!strcasecmp(cmd->command_name, "removeRoleACL")){
		rc = dynsec_roles__process_remove_acl(data, cmd);

	/* Unknown */
	}else{
		mosquitto_control_command_reply(cmd, "Unknown command");
		rc = MOSQ_ERR_INVAL;
	}

	return rc;
}


int dynsec_control_callback(int event, void *event_data, void *userdata)
{
	struct mosquitto_evt_control *ed = event_data;
	struct dynsec__data *data = userdata;
	int rc;

	UNUSED(event);

	data->need_save = false;
	rc = mosquitto_control_generic_callback(ed, RESPONSE_TOPIC, userdata, dynsec__handle_command);
	if(rc == MOSQ_ERR_SUCCESS && data->need_save){
		dynsec__config_save(data);
	}
	return rc;
}
