set (PLUGIN_NAME mosquitto_connection_state)

add_mosquitto_plugin_no_install("${PLUGIN_NAME}" "${PLUGIN_NAME}.c" "" "")
