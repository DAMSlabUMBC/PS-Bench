% Environment vars
-define(ENV_NODE_NAME, node_name).
-define(ENV_DEVICE_DEF_DIR, device_definitions_directory).
-define(ENV_DEPLOYMENT_DEF_DIR, deployment_definitions_directory).
-define(ENV_SCENARIO_DEF_DIR, scenario_definitions_directory).
-define(ENV_SELECTED_SCENARIO, selected_scenario).
-define(ENV_REQ_KEY_LIST, [?ENV_NODE_NAME, ?ENV_DEVICE_DEF_DIR, ?ENV_DEPLOYMENT_DEF_DIR, ?ENV_SCENARIO_DEF_DIR, ?ENV_SELECTED_SCENARIO]).

% Cookie for connections
-define(PS_BENCH_COOKIE, ps_bench_cookie).

% Wildcard string for device file searching
-define(DEVICE_FILE_EXT, "**/*.device").
-define(DEPLOYMENT_FILE_EXT, "**/*.deployment").
-define(SCENARIO_FILE_EXT, "**/*.scenario").

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
-define(SCENARIO_DURATION_PROP, duration).
-define(SCENARIO_PROTOCOL_PROP, protocol).
-define(SCENARIO_DEPLOYMENT_NAME_PROP, deployment_name).
-define(SCENARIO_HOSTS_PROP, hosts).
-define(SCENARIO_HOST_HOSTNAME_PROP, hostname).
-define(SCENARIO_HOST_RNG_SEED_PROP, rng_seed).
-define(SCENARIO_PROTOCOL_CONFIG_PROP, protocol_config).
-define(SCENARIO_METRIC_CONFIG_PROP, metric_config).
-define(SCENARIO_REQ_KEY_LIST, [?SCENARIO_NAME_PROP, ?SCENARIO_DURATION_PROP, ?SCENARIO_PROTOCOL_PROP, ?SCENARIO_DEPLOYMENT_NAME_PROP, 
                                    ?SCENARIO_HOSTS_PROP, ?SCENARIO_PROTOCOL_CONFIG_PROP, ?SCENARIO_METRIC_CONFIG_PROP]).

% Supported protocol types
-define(MQTT_V5_PROTOCOL, mqttv5).
-define(MQTT_V311_PROTOCOL, mqttv311).
-define(DDS_PROTOCOL, dds).
-define(SUPPORTED_PROTOCOLS, [?MQTT_V5_PROTOCOL, ?MQTT_V311_PROTOCOL, ?DDS_PROTOCOL]).

% MQTT protocol_config fields
-define(MQTT_CLIENT_INTERFACE_MODULE_PROP, client_interface_module).
-define(MQTT_BROKER_IP_PROP, broker).
-define(MQTT_BROKER_PORT_PROP, port).
-define(MQTT_QOS_PROP, qos).
% NOTE: We do not require a client interface module as we fall back to a default one
% NOTE: We do not require a default MQTT QoS as we fall back to 0
-define(MQTT_REQ_KEY_LIST, [?MQTT_BROKER_IP_PROP, ?MQTT_BROKER_PORT_PROP]).

% MQTT constants
-define(MQTT_DEFAULT_CLIENT_INTERFACE_MODULE, ps_bench_default_mqtt_interface).
-define(MQTT_DEFAULT_QOS_PROP, default_qos).
-define(MQTT_TOPIC_PREFIX, <<"ps_bench/device/">>).

% DDS protocol_config fields
-define(DDS_NIF_MODULE_PROP, nif_module).
-define(DDS_NIF_FULL_PATH_PROP, nif_full_path).
-define(DDS_CONFIG_FILE_PATH_PROP, config_file).
-define(DDS_DOMAIN_ID_PROP, domain_id).
-define(DDS_QOS_PROFILE_PROP, qos_profile).
% NOTE: We do not require a qos_xml_file as DDS defines sensible defaults
% NOTE: We do not require a nif module as we fall back to the default one
-define(DDS_REQ_KEY_LIST, [?DDS_DOMAIN_ID_PROP]).

% DDS constants
-define(DDS_DEFAULT_NIF_MODULE, ps_bench_default_dds_interface).
-define(DDS_DEFAULT_NIF_FULL_PATH, "priv/dds_cplusplus/lib/libps_bench_default_dds_interface").
-define(DDS_DEFAULT_CONFIG_FILE_PATH, "configs/dds_configs/ps_bench_default_dds_interface.ini").
-define(DDS_DEFAULT_PROFILE, ""). % Intentionally empty
-define(DDS_TOPIC, "PS_BENCH_TOPIC").

% Interface types for reference
-define(PYTHON_INTERFACE, python).
-define(ERLANG_INTERFACE, erlang).
-define(NIF_INTERFACE, nif).

% metrics and runtime props 
-define(METRIC_STORAGE_CONSTANT, metrics).
-define(METRIC_RESULTS_DIR_PROP, output_dir).
-define(METRIC_HW_STATS_POLL_PERIOD, hw_stats_poll_period_ms).
-define(METRIC_PYTHON_ENGINE_PATH, python_metric_engine_path).
-define(METRIC_PLUGINS_PROP, metric_plugins).
% NOTE: We do not require a window, rollup period, or python path to be defined
-define(METRIC_REQ_KEY_LIST, [?METRIC_PLUGINS_PROP]).

% Set some defaults for metric calculation
-define(DEFAULT_OUT_DIR, "results").
-define(DEFAULT_PYTHON_ENGINE_PATH, "priv/py_engine").
-define(DEFAULT_PYTHON_EXECUTABLE, "python3").

% Message Atoms
-define(CONNECTED_MSG, connected).
-define(DISCONNECTED_MSG, disconnected).
-define(PUBLISH_RECV_MSG, publish_recv).
-define(SUBSCRIBED_MSG, subscribed).
-define(UNSUBSCRIBED_MSG, unsubscribed).

% Records for metric storage
-define(PUB_AGG_RECORD_NAME, pub_aggregate).
-record(?PUB_AGG_RECORD_NAME, {node_name, binary_aggregate}).