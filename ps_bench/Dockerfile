FROM erlang:27-alpine

# Install all dependencies for both MQTT and DDS
RUN apk add --no-cache \
      bash git build-base cmake perl curl \
      python3 py3-pip linux-headers numactl-dev openssl-dev pkgconf \
      netcat-openbsd \
      g++ xerces-c-dev make wget
      
# Install OpenDDS
RUN wget https://github.com/OpenDDS/OpenDDS/releases/download/v3.33.0/OpenDDS-3.33.0.tar.gz && \
    tar xzf OpenDDS-3.33.0.tar.gz -C /opt && \
    cd /opt/OpenDDS-3.33.0 && ./configure --xerces3 \
    && make -j 4 && source /opt/OpenDDS-3.33.0/setenv.sh

# Install node_exporter for CPU/Memory stats
RUN cd /tmp && \
    wget https://github.com/prometheus/node_exporter/releases/download/v1.8.2/node_exporter-1.8.2.linux-amd64.tar.gz && \
    tar -xzf node_exporter-1.8.2.linux-amd64.tar.gz && \
    mv node_exporter-1.8.2.linux-amd64/node_exporter /usr/local/bin/ && \
    rm -rf /tmp/node_exporter-*
    
# Set OpenDDS environment variables
ENV DDS_ROOT=/opt/OpenDDS-3.33.0
ENV ACE_ROOT=$DDS_ROOT/ACE_wrappers
ENV TAO_ROOT=$ACE_ROOT/TAO
ENV MPC_ROOT=$ACE_ROOT/MPC
ENV PATH=$DDS_ROOT/bin:$ACE_ROOT/bin:$PATH
ENV LD_LIBRARY_PATH=$DDS_ROOT/lib:$ACE_ROOT/lib:$LD_LIBRARY_PATH

# isolate python packages 
RUN python3 -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"
RUN pip install --no-cache-dir erlport paho-mqtt

WORKDIR /app
COPY . /app

# clean any host artifacts, ensure output dir exists
RUN rm -rf _build deps && mkdir -p /app/out

# build the release
RUN rebar3 release

# make configs/out visible to the release
RUN set -eux; rel="/app/_build/default/rel/ps_bench"; \
    ln -s /app/configs "$rel/configs" || true; \
    ln -s /app/out     "$rel/out"     || true; \
    ln -s /app/results "$rel/results" || true

# ports for epmd + distribution 
EXPOSE 4369 15000-15010

# clean ERL flags, set cookie + metrics dir default
ENV ERL_AFLAGS="-kernel inet_dist_listen_min 15000 inet_dist_listen_max 15010" \
    RELEASE_COOKIE="ps_bench_cookie" \
    METRICS_DIR="/app/out" \
    PATH="/opt/venv/bin:$PATH"

# run the release - use the modified entrypoint that supports both MQTT and DDS
COPY /entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]