% Wildcard string for device file searching
-define(DEVICE_FILE_EXT, "*.def").

% Device file fields
-define(DEVICE_TYPE_FIELD, type).
-define(DEVICE_PUB_FREQ_FIELD, publication_frequency_ms).
-define(DEVICE_SIZE_MEAN_FIELD, payload_bytes_mean).
-define(DEVICE_SIZE_VARIANCE_FIELD, payload_bytes_variance).
-define(DEVICE_DISCON_CHECK_MS_FIELD, disconnect_check_period_ms).
-define(DEVICE_DISCON_PCT_FIELD, disconnect_chance_pct).
-define(DEVICE_RECON_CHECK_MS_FIELD, reconnect_check_period_ms).
-define(DEVICE_RECON_PCT_FIELD, reconnect_chance_pct).
-define(DEVICE_KEY_LIST, [?DEVICE_TYPE_FIELD, ?DEVICE_PUB_FREQ_FIELD, ?DEVICE_SIZE_MEAN_FIELD, ?DEVICE_SIZE_VARIANCE_FIELD,
                            ?DEVICE_DISCON_CHECK_MS_FIELD, ?DEVICE_DISCON_PCT_FIELD, ?DEVICE_RECON_CHECK_MS_FIELD, ?DEVICE_RECON_PCT_FIELD]).