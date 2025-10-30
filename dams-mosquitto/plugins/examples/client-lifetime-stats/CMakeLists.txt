set (PLUGIN_NAME mosquitto_client_lifetime_stats)

add_mosquitto_plugin_no_install("${PLUGIN_NAME}" "${PLUGIN_NAME}.c" "" "")
