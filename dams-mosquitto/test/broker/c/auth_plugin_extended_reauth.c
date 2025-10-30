#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <mosquitto.h>
#include <mosquitto/broker.h>
#include <mosquitto/broker_plugin.h>

#define UNUSED(A) (void)(A)

static int auth_count = 0;

int mosquitto_auth_plugin_version(void)
{
	return 4;
}

int mosquitto_auth_plugin_init(void **user_data, struct mosquitto_opt *auth_opts, int auth_opt_count)
{
	UNUSED(user_data);
	UNUSED(auth_opts);
	UNUSED(auth_opt_count);

	return MOSQ_ERR_SUCCESS;
}

int mosquitto_auth_plugin_cleanup(void *user_data, struct mosquitto_opt *auth_opts, int auth_opt_count)
{
	UNUSED(user_data);
	UNUSED(auth_opts);
	UNUSED(auth_opt_count);

	return MOSQ_ERR_SUCCESS;
}

int mosquitto_auth_security_init(void *user_data, struct mosquitto_opt *auth_opts, int auth_opt_count, bool reload)
{
	UNUSED(user_data);
	UNUSED(auth_opts);
	UNUSED(auth_opt_count);
	UNUSED(reload);

	return MOSQ_ERR_SUCCESS;
}

int mosquitto_auth_security_cleanup(void *user_data, struct mosquitto_opt *auth_opts, int auth_opt_count, bool reload)
{
	UNUSED(user_data);
	UNUSED(auth_opts);
	UNUSED(auth_opt_count);
	UNUSED(reload);

	return MOSQ_ERR_SUCCESS;
}

int mosquitto_auth_acl_check(void *user_data, int access, struct mosquitto *client, const struct mosquitto_acl_msg *msg)
{
	UNUSED(user_data);
	UNUSED(access);
	UNUSED(client);
	UNUSED(msg);

	return MOSQ_ERR_PLUGIN_DEFER;
}


int mosquitto_auth_start(void *user_data, struct mosquitto *client, const char *method, bool reauth, const void *data, uint16_t data_len, void **data_out, uint16_t *data_out_len)
{
	UNUSED(user_data);
	UNUSED(client);
	UNUSED(method);
	UNUSED(reauth);
	UNUSED(data);
	UNUSED(data_len);
	UNUSED(data_out);
	UNUSED(data_out_len);

	if(auth_count == 0){
		auth_count++;
		return MOSQ_ERR_SUCCESS;
	}else{
		return MOSQ_ERR_AUTH;
	}
}

int mosquitto_auth_continue(void *user_data, struct mosquitto *client, const char *method, const void *data, uint16_t data_len, void **data_out, uint16_t *data_out_len)
{
	UNUSED(user_data);
	UNUSED(client);
	UNUSED(method);
	UNUSED(data);
	UNUSED(data_len);
	UNUSED(data_out);
	UNUSED(data_out_len);

	return MOSQ_ERR_AUTH;
}
