#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <mosquitto.h>
#include <mosquitto/broker.h>
#include <mosquitto/broker_plugin.h>

#define UNUSED(A) (void)(A)

static int handle_tick(int event, void *event_data, void *user_data);

static mosquitto_plugin_id_t *plg_id;


int mosquitto_plugin_version(int supported_version_count, const int *supported_versions)
{
	UNUSED(supported_version_count);
	UNUSED(supported_versions);

	return 5;
}

int mosquitto_plugin_init(mosquitto_plugin_id_t *identifier, void **user_data, struct mosquitto_opt *auth_opts, int auth_opt_count)
{
	UNUSED(user_data);
	UNUSED(auth_opts);
	UNUSED(auth_opt_count);

	plg_id = identifier;

	mosquitto_callback_register(plg_id, MOSQ_EVT_TICK, handle_tick, NULL, NULL);

	return MOSQ_ERR_SUCCESS;
}

int mosquitto_plugin_cleanup(void *user_data, struct mosquitto_opt *auth_opts, int auth_opt_count)
{
	UNUSED(user_data);
	UNUSED(auth_opts);
	UNUSED(auth_opt_count);

	mosquitto_callback_unregister(plg_id, MOSQ_EVT_TICK, handle_tick, NULL);

	return MOSQ_ERR_SUCCESS;
}

int handle_tick(int event, void *event_data, void *user_data)
{
	UNUSED(event);
	UNUSED(event_data);
	UNUSED(user_data);

	mosquitto_broker_publish_copy("plugin-tick-test", "topic/tick", strlen("test-message"), "test-message", 0, false, NULL);
	return MOSQ_ERR_SUCCESS;
}
