# PSMark Custom Protocol Interfaces

PSMark separates workload orchestration from protocol client implementations via adapters. You can provide custom interfaces for MQTT (Erlang) and DDS (NIF), and select them per-scenario.

## MQTT: Custom Erlang Client Interface
Configure your module under `protocol_config.client_interface_module` in the scenario. Your module should be a `gen_server` with this contract (see `ps_bench_default_mqtt_interface` for a full reference implementation):

### Required API
- `start_link(ScenarioName, ClientName, OwnerPid) -> {ok, Pid}`
  - Register the process under `ClientName` (atom) for subsequent calls: `gen_server:start_link({local, ClientName}, ...)`
- Handle calls (forwarded by `ps_bench_erlang_mqtt_adapter`):
  - `connect` → establish connection; reply `{ok, new_connection}` or `{ok, already_connected}`
  - `connect_clean` → force clean-session connect (fresh)
  - `reconnect` → reconnect using existing session if present
  - `{subscribe, Properties, Topics}` → subscribe; OK to be no-op if not connected
  - `{publish, Properties, TopicBin, PayloadBin, PubOpts}` → publish; reply `{ok, published}` or `{ok, not_connected}`
  - `{unsubscribe, Properties, Topics}`
  - `disconnect` → gracefully disconnect if connected
  - `stop` → internal cleanup (optional)

### Events to send to the adapter (OwnerPid)
- `{?CONNECTED_MSG, {TimeNs}, ClientName}` when connection is established
- `{?DISCONNECTED_MSG, {TimeNs, Reason}, ClientName}` on disconnect (`Reason = normal | econnrefused | ...`)
- `{?PUBLISH_RECV_MSG, {TimeNs, TopicBin, PayloadBin}, ClientName}` on incoming publish

### Payload format for publishes
- Outgoing publishes should prepend an 8-byte `TimeNs` (publish time) to the payload.
- Use `ps_bench_utils:generate_mqtt_payload_data/3` to produce `{SeqId, Payload}` which embeds sequence and publisher ID. The adapter extracts metrics via `decode_seq_header_with_publisher/1`.

### Selecting your module in a scenario
```erlang
{protocol_config,
 [
   {client_interface_module, my_mqtt_interface},
   {broker, "10.0.0.5"},
   {port, 1883},
   {qos, [{default_qos, 0}]}
 ]}
```

## DDS: Custom NIF Interface Module
The DDS adapter (`ps_bench_nif_dds_adapter`) calls a NIF module to interact with the DDS runtime. Provide a module that exports these functions (see `ps_bench/src/protocol_clients/default_interfaces/ps_bench_default_dds_interface.erl`):

### Required exports
- `init(NifFullPath)` → `ok = erlang:load_nif(NifFullPath, 0)`
- `create_participant(DomainId, ConfigPath, QoSProfile) -> {ok, Participant}`
- `create_subscriber_on_topic(Topic, ClientNameStr, Participant, ListenerPid, QoSProfile) -> {ok, Subscriber}`
- `create_publisher_on_topic(Topic, Participant, QoSProfile, PublishingNodeNameStr) -> {ok, Publisher}`
- `publish_message(SeqId, DataBin, Publisher) -> ok | not_connected`
- `delete_subscriber(Participant, Subscriber) -> ok`
- `delete_publisher(Participant, Publisher) -> ok`

### Selecting your module in a scenario
```erlang
{protocol_config,
 [
   {nif_module, my_dds_interface},
   {nif_full_path, "priv/dds_cplusplus/lib/libmy_dds_interface"},
   {domain_id, 42},
   {qos_profile, ""},
   {config_file, "configs/dds_configs/ps_bench_default_dds_interface.ini"}
 ]}
```
## Advanced: Adding a New Protocol
If you want to add an entirely new protocol:
- Extend `?SUPPORTED_PROTOCOLS` in `ps_bench/include/ps_bench_config.hrl`.
- Teach `ps_bench_config_manager` how to parse your protocol’s `protocol_config` and provide accessor functions.
- Add a new adapter module analogous to `ps_bench_erlang_mqtt_adapter` or `ps_bench_nif_dds_adapter`.
- Update `ps_bench_client_sup` with a `process_<yourproto>_children/1` to supervise your client workers.
