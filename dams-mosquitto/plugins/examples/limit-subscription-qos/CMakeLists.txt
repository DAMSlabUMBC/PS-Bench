set (PLUGIN_NAME mosquitto_limit_subscription_qos)

add_mosquitto_plugin_no_install("${PLUGIN_NAME}" "${PLUGIN_NAME}.c" "" "")
