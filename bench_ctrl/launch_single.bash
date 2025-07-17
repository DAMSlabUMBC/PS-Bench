ERL_FLAGS="-kernel inet_dist_listen_min 15000 inet_dist_listen_max 15010" rebar3 shell --name bench@127.0.0.1 --setcookie bench --config ./ps_bench.config
