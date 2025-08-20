FROM erlang:24-alpine                                

# Install build deps
RUN apk add --no-cache bash git python3 py3-pip py3-paho-mqtt

# Copy and fetch deps
WORKDIR /app
COPY . /app
RUN rebar3 release                                   

# Expose the epmd+dist ports 
EXPOSE 4369 15000-15010

# Container entry that will start a non‑interactive node
CMD ["sh","-c","cd /app && _build/default/rel/bench_ctrl/bin/bench_ctrl foreground --name bench@bench_ctrl --setcookie bench"]
