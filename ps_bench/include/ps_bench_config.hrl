% Environment vars
-define(ENV_NODE_NAME, node_name).
-define(ENV_DEVICE_DEF_DIR, device_definitions_directory).
-define(ENV_TEST_CONFIG_FILE, test_configuration_file).
-define(ENV_REQ_KEY_LIST, [?ENV_NODE_NAME, ?ENV_DEVICE_DEF_DIR, ?ENV_TEST_CONFIG_FILE]).

% Wildcard string for device file searching
-define(DEVICE_FILE_EXT, "*.def").

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

% Test file fields
-define(TEST_CONFIG_GLOBAL_SECTION_PROP, global_config).
-define(TEST_NAME_PROP, test_name).
-define(TEST_NODE_LIST_PROP, node_list).
-define(TEST_PROTOCOL_PROP, protocol).
-define(TEST_INTERFACE_TYPE_PROP, client_interface_type).
-define(TEST_INTERFACE_NAME_PROP, client_interface_name).
-define(TEST_NODE_DEVICES_PROP, devices).
% NOTE: We do not require a default type or interface since we fall back to the builtin erlang interface
-define(TEST_CONFIG_GLOBAL_REQ_KEY_LIST, [?TEST_NAME_PROP, ?TEST_NODE_LIST_PROP, ?TEST_PROTOCOL_PROP]).
-define(TEST_CONFIG_NODE_REQ_KEY_LIST, [?TEST_NODE_DEVICES_PROP]).

% Supported protocol types
-define(MQTT_V5_PROTOCOL, mqttv5).
-define(MQTT_V311_PROTOCOL, mqttv311).
-define(DDS_PROTOCOL, dds).
-define(SUPPORTED_PROTOCOLS, [?MQTT_V5_PROTOCOL, ?MQTT_V311_PROTOCOL, ?DDS_PROTOCOL]).

% MQTT configuration fields
-define(MQTT_BROKER_IP_PROP, broker).
-define(MQTT_BROKER_PORT_PROP, port).
-define(MQTT_REQ_KEY_LIST, [?MQTT_BROKER_IP_PROP, ?MQTT_BROKER_PORT_PROP]).

% MQTT topic constants
-define(MQTT_TOPIC_PREFIX, <<"ps_bench/device/">>).

% Supported interfaces types
-define(PYTHON_INTERFACE, python).
-define(ERLANG_INTERFACE, erlang).
-define(DEFAULT_INTERFACE_TYPE, ?ERLANG_INTERFACE).
-define(DEFAULT_INTERFACE_NAME, "ps_bench_default_client").
-define(SUPPORTED_INTERFACES, [?PYTHON_INTERFACE, ?ERLANG_INTERFACE]).

%% metrics and runtime knobs 
-define(WINDOW_MS_PROP, window_ms).
-define(ROLLUP_SECS_PROP, rollup_secs).
-define(METRICS_PLUGINS_PROP, metrics_plugins).
-define(PYTHON_PATH_PROP, python_path).

%% future: multiple transports keep existing TEST_PROTOCOL_PROP for now
-define(TRANSPORTS_PROP, transports).

%% sane defaults
-define(DEFAULT_WINDOW_MS, 1000).
-define(DEFAULT_ROLLUP_SECS, 5).

%% seq header (8B seq + 8B t_pub_ns)
-define(SEQ_HDR_BYTES, 16).
