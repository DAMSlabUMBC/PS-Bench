% Environment vars
-define(ENV_NODE_NAME, node_name).
-define(ENV_DEVICE_DEF_DIR, device_definitions_directory).
-define(ENV_DEPLOYMENT_DEF_DIR, deployment_definitions_directory).
-define(ENV_SCENARIO_DEF_DIR, scenario_definitions_directory).
-define(ENV_SELECTED_SCENARIO, selected_scenario).
-define(ENV_REQ_KEY_LIST, [?ENV_NODE_NAME, ?ENV_DEVICE_DEF_DIR, ?ENV_DEPLOYMENT_DEF_DIR, ?ENV_SCENARIO_DEF_DIR, ?ENV_SELECTED_SCENARIO]).

% Wildcard string for device file searching
-define(DEVICE_FILE_EXT, "*.device").
-define(DEPLOYMENT_FILE_EXT, "*.deployment").
-define(SCENARIO_FILE_EXT, "*.scenario").

% Device file fields
-define(DEVICE_TYPE_PROP, type).
-define(DEVICE_PUB_FREQ_PROP, publication_frequency_ms).
-define(DEVICE_SIZE_MEAN_PROP, payload_bytes_mean).
-define(DEVICE_SIZE_VARIANCE_PROP, payload_bytes_variance).
-define(DEVICE_DISCON_CHECK_MS_PROP, disconnect_check_period_ms).
-define(DEVICE_DISCON_PCT_PROP, disconnect_chance_pct).
-define(DEVICE_RECON_CHECK_MS_PROP, reconnect_check_period_ms).
-define(DEVICE_RECON_PCT_PROP, reconnect_chance_pct).
-define(DEVICE_KEY_LIST, [?DEVICE_TYPE_PROP, ?DEVICE_PUB_FREQ_PROP, ?DEVICE_SIZE_MEAN_PROP, ?DEVICE_SIZE_VARIANCE_PROP,
                            ?DEVICE_DISCON_CHECK_MS_PROP, ?DEVICE_DISCON_PCT_PROP, ?DEVICE_RECON_CHECK_MS_PROP, ?DEVICE_RECON_PCT_PROP]).

% Deployment file fields
-define(DEPLOYMENT_NAME_PROP, name).
-define(DEPLOYMENT_NODES_PROP, nodes).
-define(DEPLOYMENT_DEVICES_PROP, devices).
-define(DEPLOYMENT_REQ_KEYS, [?DEPLOYMENT_NAME_PROP, ?DEPLOYMENT_NODES_PROP]).
-define(DEPLOYMENT_REQ_NODE_KEYS, [?DEPLOYMENT_DEVICES_PROP]).

% Scenario file fields
-define(SCENARIO_NAME_PROP, name).
-define(SCENARIO_PROTOCOL_PROP, protocol).
-define(SCENARIO_PROTOCOL_CONFIG_PROP, protocol_config).
-define(SCENARIO_INTERFACE_TYPE_PROP, client_interface_type).
-define(SCENARIO_INTERFACE_NAME_PROP, client_interface_name).
-define(SCENARIO_DEPLOYMENT_NAME_PROP, deployment_name).
% NOTE: We do not require a default type or interface since we fall back to the builtin erlang interface
-define(SCENARIO_REQ_KEY_LIST, [?SCENARIO_NAME_PROP, ?SCENARIO_PROTOCOL_PROP, ?SCENARIO_PROTOCOL_CONFIG_PROP, ?SCENARIO_DEPLOYMENT_NAME_PROP]).
-define(SCENARIO_STORAGE_CONSTANT, scenario).

% Supported protocol types
-define(MQTT_V5_PROTOCOL, mqttv5).
-define(MQTT_V311_PROTOCOL, mqttv311).
-define(DDS_PROTOCOL, dds).
-define(SUPPORTED_PROTOCOLS, [?MQTT_V5_PROTOCOL, ?MQTT_V311_PROTOCOL, ?DDS_PROTOCOL]).

% MQTT protocol_config fields
-define(MQTT_BROKER_IP_PROP, broker).
-define(MQTT_BROKER_PORT_PROP, port).
-define(MQTT_QOS_PROP, qos).
-define(MQTT_REQ_KEY_LIST, [?MQTT_BROKER_IP_PROP, ?MQTT_BROKER_PORT_PROP, ?MQTT_QOS_PROP]).
% NOTE: We do not require a default MQTT QoS as we fall back to 0
-define(MQTT_DEFAULT_QOS_PROP, default_qos).

% MQTT topic constants
-define(MQTT_TOPIC_PREFIX, <<"ps_bench/device/">>).

% Supported interfaces types
-define(PYTHON_INTERFACE, python).
-define(ERLANG_INTERFACE, erlang).
-define(DEFAULT_INTERFACE_TYPE, ?ERLANG_INTERFACE).
-define(DEFAULT_INTERFACE_NAME, "ps_bench_default_client").
-define(SUPPORTED_INTERFACES, [?PYTHON_INTERFACE, ?ERLANG_INTERFACE]).