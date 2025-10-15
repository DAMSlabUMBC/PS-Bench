# PSMark Built In Test Cases

PSMark provides built in test cases which benchmark four real-world datasets across three categories of scenarios. Each test case is designed to distribute the dataset across five benchmark runners (representing data aggregation gateways).

One scenario and five configuration files are provided for each test case described in [Scenarios](#scenarios). For each test case, the hostnames of the runners must be configured in the scenario file. If using MQTT, the broker IP or hostname must also be configured. Each runner should be started with the config files provided in the test case and executed from the top level directory in this repository.

## Datasets

### Smart Factory
TBD

### Smart Healthcare
TBD

### Smart City
TBD

### Smart Home
TBD

## Scenarios

### Scalability
The scalability scenarios are intended to benchmark how varying the quantites of devices within the datasets affects the performance of the pub/sub system. Scenarios are provided to benchmark the baseline dataset with scaling factors of x0.5, x1, x2, x10. Scenarios are provided for the MQTT 5 and DDS protocols.

### QoS Variation
The QoS variation scenarios are intended to benchmark how altering the required quality-of-service parameters for device communication affects the performance of the pub/sub system. Scenarios are provided to benchmark each dataset with both weak and strong service quality guarentees. Scenarios are provided for the MQTT 5 and DDS protocols.

MQTT 5 uses QoS 0 for low guarentee and 2 for high guarentee.
DDS uses the quality of service parameters given in the PsBenchHighProfile and PsBenchLowProfile profiles in the qos.xml file found within `configs/dds_configs`.

### Cross-Protocol
The cross-protocol scenarios are intended to benchmark performance differences in the system across different pub/sub protocols. Scenarios are provided to benchmark each dataset with MQTT 3.1.1, MQTT 5, and DDS.
