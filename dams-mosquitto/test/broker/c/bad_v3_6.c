#include <stdio.h>
#include <string.h>
#include <mosquitto.h>
#include <mosquitto/broker.h>
#include <mosquitto/broker_plugin.h>

int mosquitto_auth_plugin_version(void)
{
	return 3;
}

int mosquitto_auth_plugin_init(void **user_data, struct mosquitto_opt *auth_opts, int auth_opt_count)
{
	(void)user_data;
	(void)auth_opts;
	(void)auth_opt_count;

	return MOSQ_ERR_SUCCESS;
}
