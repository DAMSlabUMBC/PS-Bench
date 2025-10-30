FROM ubuntu:20.04

# Disable interactive prompts
ENV DEBIAN_FRONTEND=noninteractive

# Install required packages including the missing dependencies
RUN apt-get update && apt-get install -y \
    cmake \
    build-essential \
    libssl-dev \
    ca-certificates \
    libcjson-dev \
    libargon2-dev \
    libsqlite3-dev \
    pkg-config \
    libcunit1-dev \
    xsltproc \
    docbook-xsl \
    iproute2 \
    iputils-ping \
 && rm -rf /var/lib/apt/lists/*

# Set working directory to project root
WORKDIR /opt/mqtt_brokers/dams-mosquitto

# Copy the entire repository into the container
COPY . /opt/mqtt_brokers/dams-mosquitto

# Build the broker with LTO disabled and ENGINE_cleanup disabled
RUN cmake -DWITH_LTO=OFF \
          -DWITH_CLIENTS=OFF \
          -DWITH_BROKER=ON \
          -DWITH_TESTS=OFF \
          -DCMAKE_C_FLAGS="-DDISABLE_ENGINE_CLEANUP" . \
 && make

# Copy the built Mosquitto binary and library to system directories, then update the linker cache
RUN cp src/mosquitto /usr/local/sbin/mosquitto && \
    cp lib/libmosquitto.so.1 /usr/local/lib/ && \
    ldconfig

# Create mosquitto configuration directory and copy the default config
RUN mkdir -p /etc/mosquitto && \
    cp mosquitto.conf /etc/mosquitto/mosquitto.conf

# Expose the default MQTT port
EXPOSE 1883

# Run the Mosquitto broker using the configuration file
CMD ["/usr/local/sbin/mosquitto", "-c", "/etc/mosquitto/mosquitto.conf"]
