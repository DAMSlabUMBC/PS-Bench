# PS-Bench

Pub/Sub Benchmark for Large-Scale IoT Deployments. PS-Bench orchestrates synthetic device workloads across one or more Erlang nodes, publishes and subscribes via pluggable protocol clients (MQTT v5/v3.1.1 and DDS included), and calculates metrics via a simple plugin system.

This repo contains the Erlang application, default protocol interfaces, example scenarios/devices/deployments, and Docker tooling to run locally or in distributed mode.

## Key Features
- Multi-node orchestration with deterministic seeds per node
- Protocol adapters: MQTT (Erlang client) and DDS (NIF)
- Config-driven devices, deployments, and scenarios
- Metric plugins (currently Erlang-only) writing CSV outputs per run
- Optional HW stats polling (Prometheus node_exporter)

## Repository Layout
- `ps_bench/src/core`: configuration, lifecycle, node management, storage
- `ps_bench/src/protocol_clients`: adapters and default protocol implementations
- `ps_bench/src/metrics`: metric manager, HW stats reader, plugins
- `ps_bench/configs`: templates, built-in examples, and initial test configs
- `ps_bench/Dockerfile` + `docker-compose.*.yml`: local/dev orchestration
- `docs/`: how-tos for plugins and interfaces

## Quick Start (5-Node)
- Prereqs: Docker + Docker Compose
- Build runner image (from `ps_bench/`):
  - `cd ps_bench && docker build -t ps_bench-runner:latest -f Dockerfile .`
- MQTT (EMQX broker) 5-node:
  - `docker compose -f ps_bench/docker-compose.mqtt.emqx.yml up --build`
- MQTT (Mosquitto broker) 5-node:
  - `docker compose -f ps_bench/docker-compose.mqtt.mosquitto.yml up --build`
- DDS (no broker) 5-node:
  - `docker compose -f ps_bench/docker-compose.dds.yml up --build`

### Tear Down
- Stop and remove containers: append `-d` to run detached, then `docker compose -f <compose-file> down`
- Remove images if needed: `docker rmi ps_bench-runner:latest emqx-with-exporter mosquitto-with-exporter`

### Notes
- Compose files mount `./results` and per-runner `./out/<runner>` for artifacts.
- Default scenarios used by the compose files are the built-in scalability test suites. Override with `SCENARIO=<name>` or edit `ps_bench/.env`.
- Distribution uses long names (`-name`) and a bridge network `benchnet` compose handles hostname wiring.

There are no single-node compose experiments in this repo. All provided compose stacks run 5 benchmark runners.

## Configuration Model (files under `ps_bench/configs`)
- Devices (`*.device`): defines device type payload size, frequency, disconnect/reconnect behavior.
- Deployments (`*.deployment`): per-node device counts by device type.
- Scenarios (`*.scenario`): selects protocol, deployment, node hosts, metrics, and protocol-specific config.

### Scenario Key Sections
- `protocol`: one of `mqttv5`, `mqttv311`, or `dds`.
- `protocol_config`:
  - MQTT: `{client_interface_module, Module}`, `{broker, "IP"}`, `{port, 1883}`, `{qos, [{default_qos, 0} | {device_type, 1..2}]}`
  - DDS: `{nif_module, Module}`, `{nif_full_path, "priv/..."}`, `{domain_id, Int}`, `{qos_profile, String}`, `{config_file, "configs/..."}`
- `metric_config`:
  - `{output_dir, "results"}` (base dir)
  - Optional `{hw_stats_poll_period_ms, 1000}` to enable node_exporter polling
  - `{metric_plugins, [{PluginModule, erlang}, ...]}`

## Adding Metric Plugins (Erlang)
- Implement a module with the following callbacks:
  - `init(OutDir) -> ok` called once per run with the timestamped run directory
  - `calc() -> ok` perform calculations and write outputs (e.g., CSV to `OutDir`)
- Register the plugin in your scenario: add `{my_plugin_module, erlang}` to `metric_plugins`.
- Access data via `ps_bench_store`:
  - `fetch_recv_events/0`, `fetch_publish_events/0`, `fetch_connect_events/0`, `fetch_disconnect_events/0`
  - Filter helpers like `fetch_recv_events_by_filter/1`
  - HW stats if enabled: `fetch_cpu_usage/0`, `fetch_memory_usage/0` (and broker variants)
- See `ps_bench/src/metrics/plugins/*.erl` for working examples. A complete how-to is in `docs/metrics-plugins.md`.

## Custom Protocol Interfaces
- MQTT (Erlang): provide a module and set `protocol_config.client_interface_module = your_module`.
  - Your module should be a `gen_server` implementing:
    - `start_link(ScenarioName, ClientName, OwnerPid)` registers under `ClientName`
    - handle calls: `connect | connect_clean | reconnect | {subscribe, Props, Topics} | {publish, Props, Topic, Payload, PubOpts} | {unsubscribe, Topics} | disconnect | stop`
    - send events to `OwnerPid`:
      - `{?CONNECTED_MSG, {TimeNs}, ClientName}`
      - `{?DISCONNECTED_MSG, {TimeNs, Reason}, ClientName}`
      - `{?PUBLISH_RECV_MSG, {TimeNs, Topic, Payload}, ClientName}` with payload header as in `ps_bench_utils:generate_mqtt_payload_data/3`
  - The default implementation is `ps_bench_default_mqtt_interface` (see source for a solid reference).
- DDS (NIF): provide a NIF module with the same exported functions as `ps_bench_default_dds_interface` and set `protocol_config.nif_module` and `nif_full_path` appropriately. The adapter expects: `init/1`, `create_participant/3`, `create_subscriber_on_topic/5`, `create_publisher_on_topic/4`, `publish_message/3`, `delete_subscriber/2`, `delete_publisher/2`.
- Full details and minimal examples are in `docs/interfaces.md`.

## Local Development (without Docker)
- Install Erlang/OTP 25+ and `rebar3`
- Build: `cd ps_bench && rebar3 compile`
- Run shell with config: `rebar3 shell --apps mnesia,ps_bench` (uses `configs/ps_bench.config`)
- Release build: `rebar3 release`

## Results
- Metric CSVs are written by plugins to the run directory under `metric_config.output_dir` with a run-specific subfolder.
- If HW stats polling is enabled, `local_hw_stats.csv` is always written; when primary, `broker_hw_stats.csv` is also written.

## Environment Overrides
- Edit `ps_bench/.env` to adjust common variables like `SCENARIO`, `RUN_TAG`, `ADD_NODE_TO_SUFFIX`, and `RELEASE_COOKIE`.
- One-off overrides: prefix the compose command, e.g. `SCENARIO=scalabilitysuite_smart_home_mqttv5 docker compose -f ps_bench/docker-compose.mqtt.emqx.yml up --build`.

## Submission Cleanup Checklist
- Remove initial test configs not intended for submission:
  - `ps_bench/configs/initial_tests/**`
- Keep `ps_bench/configs/templates/**` and any curated examples you want to include.
- Ensure README and docs reflect your final plugin/interface guidance.

## More Docs
- Metrics plugins (Erlang-only for now): `docs/metrics-plugins.md`
- Custom protocol interfaces: `docs/interfaces.md`
