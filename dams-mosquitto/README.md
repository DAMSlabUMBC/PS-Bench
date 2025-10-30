MQTT-DAP: Data Protection Extension for Eclipse Mosquitto
==========================================================

This repository contains a modified implementation of Eclipse Mosquitto that implements **MQTT-DAP (MQTT for Data Protection)**, a framework for extending the MQTT protocol to facilitate the protection of sensitive data in IoT systems.

## About MQTT-DAP

MQTT-DAP extends MQTT v5 to provide protocol-level data protection mechanisms, including:

- **Purpose-Based Access Control (PBAC)**: Ensures that sensitive data is only delivered to subscribers with explicitly permitted purposes for processing that data
- **Data Linkability**: Tracks data provenance and enables auditing of data flows through the system
- **Data Protection Operations**: Supports standardized operations for data access requests, corrections, deletions, and purpose updates
- **GDPR Compliance**: Facilitates compliance with privacy regulations like GDPR through structured handling of data subject requests

### Key Features

- Purpose-based message filtering to prevent unauthorized data access
- Support for hierarchical purpose definitions (e.g., "billing/electricity")
- Protocol-level enforcement of explicit consent for data processing
- Standardized request/response patterns for data protection operations
- Backward compatibility with standard MQTT topics and payloads
- Designed for resource-constrained IoT environments

### Use Cases

MQTT-DAP is designed for IoT deployments in privacy-sensitive domains:

- **Industrial IoT**: Protecting proprietary manufacturing data while allowing selective sharing with suppliers and partners
- **Healthcare**: Ensuring patient data is only accessed for legitimate medical purposes
- **Smart Cities**: Managing citizen data with transparent purpose tracking and consent management

## How MQTT-DAP Works

MQTT-DAP uses MQTT v5 user properties to embed data protection metadata in control packets:

1. **Purpose Filters**: Publishers specify allowed purposes using the `DAP-MP` (Message Purpose) property; subscribers declare their intended purposes using `DAP-SP` (Subscription Purpose)
2. **Purpose Matching**: The broker ensures messages are only delivered to subscribers whose stated purposes are explicitly permitted by the publisher
3. **Data Tracking**: The `DAP-ClientID` property links each message to its publisher, enabling auditing and data subject requests
4. **Explicit Consent**: The `DAP-Allow` property serves as an unambiguous indicator that the publisher consents to data collection

### Example

```bash
# Subscriber registers interest in temperature data for billing purposes
mosquitto_sub -t 'sensors/temperature' -D subscribe user-property DAP-SP "billing/electricity:sensors/temperature"

# Publisher sends temperature data allowed for billing purposes
mosquitto_pub -t 'sensors/temperature' -m '22.5' \
  -D publish user-property DAP-MP "billing/electricity" \
  -D publish user-property DAP-ClientID "sensor-01" \
  -D publish user-property DAP-Allow "1"
```

## Original Mosquitto Implementation

This implementation is based on Eclipse Mosquitto, an open source implementation of a server for version 5.0, 3.1.1,
and 3.1 of the MQTT protocol. It also includes a C and C++ client library, and
the `mosquitto_pub` and `mosquitto_sub` utilities for publishing and
subscribing.

## Links

### MQTT-DAP Resources

- Research paper: "MQTT-DAP: A Data Protection Extension of the MQTT Protocol" 

### MQTT Protocol Information

- Community page: <http://mqtt.org/>
- MQTT v5.0 standard: <https://docs.oasis-open.org/mqtt/mqtt/v5.0/mqtt-v5.0.html>

### Original Mosquitto Project

- Main homepage: <https://mosquitto.org/>
- Source code repository: <https://github.com/eclipse/mosquitto>
- Bug tracker: <https://github.com/eclipse/mosquitto/issues>

## Installing

See <https://mosquitto.org/download/> for details on installing binaries for
various platforms.

## Quick start

If you have installed a binary package the broker should have been started
automatically. If not, it can be started with a very basic configuration:

    mosquitto

Then use `mosquitto_sub` to subscribe to a topic:

    mosquitto_sub -t 'test/topic' -v

And to publish a message:

    mosquitto_pub -t 'test/topic' -m 'hello world'

Note that starting the broker like this allows anonymous/unauthenticated access
but only from the local computer, so it's only really useful for initial testing.

If you want to have clients from another computer connect, you will need to
provide a configuration file. If you have installed from a binary package, you
will probably already have a configuration file at somewhere like
`/etc/mosquitto/mosquitto.conf`. If you've compiled from source, you can write
your config file then run as `mosquitto -c /path/to/mosquitto.conf`.

To start your config file you define a listener and you will need to think
about what authentication you require. It is not advised to run your broker
with anonymous access when it is publically available.

For details on how to do this, look at the
[authentication methods](https://mosquitto.org/documentation/authentication-methods/)
available and the [dynamic security plugin](https://mosquitto.org/documentation/dynamic-security/).

## Documentation

Documentation for the broker, clients and client library API can be found in
the man pages, which are available online at <https://mosquitto.org/man/>. There
are also pages with an introduction to the features of MQTT, the
`mosquitto_passwd` utility for dealing with username/passwords, and a
description of the configuration file options available for the broker.

Detailed client library API documentation can be found at <https://mosquitto.org/api/>

## Building from source

To build from source the recommended route for end users is to download the
archive from <https://mosquitto.org/download/>.

On Windows and Mac, use `cmake` to build. On other platforms, just run `make`
to build. For Windows, see also `README-windows.md`.

If you are building from the git repository then the documentation will not
already be built. Use `make binary` to skip building the man pages, or install
`docbook-xsl` on Debian/Ubuntu systems.

### Build Dependencies

* c-ares (libc-ares-dev on Debian based systems) - only when compiled with `make WITH_SRV=yes`
* cJSON - required for dynsec plugin, broker control plugin, and for client JSON output support.
* libwebsockets (libwebsockets-dev) - enable with `make WITH_WEBSOCKETS=lws`
* openssl (libssl-dev on Debian based systems) - disable with `make WITH_TLS=no`
* pthreads - for client library thread support. This is required to support the
  `mosquitto_loop_start()` and `mosquitto_loop_stop()` functions. If compiled
  without pthread support, the library isn't guaranteed to be thread safe.
* uthash / utlist - bundled versions of these headers are provided, disable their use with `make WITH_BUNDLED_DEPS=no`
* xsltproc (xsltproc and docbook-xsl on Debian based systems) - only needed when building from git sources - disable with `make WITH_DOCS=no`

Equivalent options for enabling/disabling features are available when using the CMake build.


## Credits

Mosquitto was written by Roger Light <roger@atchoo.org>
