#include <stdio.h>
#include <string.h>
#include <mosquitto.h>
#include <mosquitto/broker.h>
#include <mosquitto/broker_plugin.h>

int mosquitto_auth_plugin_version(void)
{
	return 3;
}
