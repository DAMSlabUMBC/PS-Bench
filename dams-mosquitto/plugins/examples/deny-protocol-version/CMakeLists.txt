set (PLUGIN_NAME mosquitto_deny_protocol_version)

add_mosquitto_plugin_no_install("${PLUGIN_NAME}" "${PLUGIN_NAME}.c" "" "")
