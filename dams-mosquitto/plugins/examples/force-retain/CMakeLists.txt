set (PLUGIN_NAME mosquitto_force_retain)

add_mosquitto_plugin_no_install("${PLUGIN_NAME}" "${PLUGIN_NAME}.c" "" "")
