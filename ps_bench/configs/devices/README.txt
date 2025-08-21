This file contains information about how the devices's specifications were derived.

--- Smart home scenario ---
Dataset link: https://iotanalytics.unsw.edu.au/
Dataset paper: https://ieeexplore.ieee.org/abstract/document/8116438
Dataset analysis paper: https://ieeexplore.ieee.org/document/9590566

I've used the above paper, which analyzes the characteristics of the devices, to create the device specifications based on the templates that we have.
For the deployment, I used the same setup which the paper presented.


--- Smart meter dataset (for the smart city scenario) ---
Dataset link: https://www.kaggle.com/datasets/pythonafroz/electricity-smart-meter-data-from-india?select=CEEW+-+Smart+meter+data+Mathura+2019.csv
Dataset link for citation: https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/GOCHJH

This dataset provides high-frequency three minutes interval smart meter data including information on urban household electricity consumption patterns from nearly 100 smart meters installed in two cities in India from May 2019 to October 2021.
I used the dataset to aggregate power consumption information in JSON format, based on the assumption that in a smart city we would have smart meters sending data periodically using MQTT. I think calculated the size of the payload based on the JSON data and considered a ~10% variance in the payload.

I considered the following data format based on the information provided in the dataset:
{
"meter_id": "MH01",
"timestamp": "2019-05-01 00:00:00",
"t_kWh": 0.017,
"z_Avg Voltage": 252.42,
"z_Avg Current": 1.32,
"y_freq": 50.02
}


--- Smart mobility dataset (for the smart city scenario) ---
Dataset link: https://www.kaggle.com/datasets/ziya07/smart-mobility-traffic-dataset

This dataset provides real-time traffic patterns, vehicle telemetry, ride-sharing demand, public transport efficiency, social media sentiment, and environmental factors. Again, data here may not be collected using MQTT, but I assumed the each metric is provided by a sensor, and used a JSON format to calculate the size of the payload.
I also assumed that processing of video data is done on-device (since MQTT is not the best protocol to transmit video frames).
The dataset shows that data is collected every 5 minutes.
I considered the following sensors in this scenario (with their associated payload):

* Vehicle counter device:
{
"camera_id": "C01",
"timestamp": "2024-03-01 00:00:00",
"latitude": 40.842275292891834,
"longitude": -73.70314869323049,
"vehicle_count": 205
}

* Traffic speed calculator:
{
"camera_id": "C01",
"timestamp": "2024-03-01 00:00:00",
"latitude": 40.842275292891834,
"longitude": -73.70314869323049,
"traffic_speed": 49.89343479610332
}

* Road occupancy calculator:
{
"camera_id": "C01",
"timestamp": "2024-03-01 00:00:00",
"latitude": 40.842275292891834,
"longitude": -73.70314869323049,
"road_occupancy": 82.65277992850861
}

* Traffic light state:
{
"light_id": "L01",
"timestamp": "2024-03-01 00:00:00",
"latitude": 40.842275292891834,
"longitude": -73.70314869323049,
"state": "Yellow"
}

* Weather station:
{
"station_id": "S01",
"timestamp": "2024-03-01 00:00:00",
"latitude": 40.842275292891834,
"longitude": -73.70314869323049,
"condition": "Clear"
}

* Parking sensor:
{
"sensor_id": "P01",
"timestamp": "2024-03-01 00:00:00",
"latitude": 40.842275292891834,
"longitude": -73.70314869323049,
"available_spots": 50
}

* CO2 level:
{
"sensor_id": "P01",
"timestamp": "2024-03-01 00:00:00",
"latitude": 40.842275292891834,
"longitude": -73.70314869323049,
"emission_level": 450.7600550695179
}

* Energy consumption:
{
"sensor_id": "P01",
"timestamp": "2024-03-01 00:00:00",
"latitude": 40.842275292891834,
"longitude": -73.70314869323049,
"energy_consumption": 19.57433652960039
}


--- Pune Smart City Dataset ---
Dataset link: https://www.kaggle.com/datasets/akshman/pune-smartcity-test-dataset/data
The dataset includes various air quality indicators in the city of Pune, India. Updates are provided every 15 minutes. I considered that we would have one sensor for recording each metric.
The payload size of each sensor is based on the following format:

* Humidity:
{
"sensor_id": "T01",
"timestamp": "2024-03-01 00:00:00",
"location": "PMPML_Bus_Depot_Deccan_15",
"latitude": 18.5594267,
"longitude": 73.8286556,
"humidity": 20.73
}

* Light sensor:
{
"sensor_id": "L01",
"timestamp": "2024-03-01 00:00:00",
"location": "PMPML_Bus_Depot_Deccan_15",
"latitude": 18.5594267,
"longitude": 73.8286556,
"light": 3762.914
}

* NO sensor:
{
"sensor_id": "NO01",
"timestamp": "2024-03-01 00:00:00",
"location": "PMPML_Bus_Depot_Deccan_15",
"latitude": 18.5594267,
"longitude": 73.8286556,
"no_min": 0,
"no_max": 0
}

* NO2 sensor:
{
"sensor_id": "NO201",
"timestamp": "2024-03-01 00:00:00",
"location": "PMPML_Bus_Depot_Deccan_15",
"latitude": 18.5594267,
"longitude": 73.8286556,
"no2_min": 77,
"no2_max": 82
}

* Ozone sensor:
{
"sensor_id": "O01",
"timestamp": "2024-03-01 00:00:00",
"location": "PMPML_Bus_Depot_Deccan_15",
"latitude": 18.5594267,
"longitude": 73.8286556,
"ozone_min": 0,
"ozone_max": 25
}

* PM10 sensor:
{
"sensor_id": "PM1001",
"timestamp": "2024-03-01 00:00:00",
"location": "PMPML_Bus_Depot_Deccan_15",
"latitude": 18.5594267,
"longitude": 73.8286556,
"pm10_min": 19,
"pm10_max": 23
}

* CO2 sensor:
{
"sensor_id": "CO201",
"timestamp": "2024-03-01 00:00:00",
"location": "PMPML_Bus_Depot_Deccan_15",
"latitude": 18.5594267,
"longitude": 73.8286556,
"co2_min": 401,
"co2_max": 448
}

* Sound sensor:
{
"sensor_id": "S01",
"timestamp": "2024-03-01 00:00:00",
"location": "PMPML_Bus_Depot_Deccan_15",
"latitude": 18.5594267,
"longitude": 73.8286556,
"sound_level": 66.133
}

* Temperature sensor:
{
"sensor_id": "S01",
"timestamp": "2024-03-01 00:00:00",
"location": "PMPML_Bus_Depot_Deccan_15",
"latitude": 18.5594267,
"longitude": 73.8286556,
"temperature_min": 25,
"temperature_max": 40
}

* UV sensor:
{
"sensor_id": "UV01",
"timestamp": "2024-03-01 00:00:00",
"location": "PMPML_Bus_Depot_Deccan_15",
"latitude": 18.5594267,
"longitude": 73.8286556,
"uv_min": 0.2,
"uv_max": 5.3
}

* Air pressure sensor:
{
"sensor_id": "AP01",
"timestamp": "2024-03-01 00:00:00",
"location": "PMPML_Bus_Depot_Deccan_15",
"latitude": 18.5594267,
"longitude": 73.8286556,
"air_pressure": 0.933,
}


--- Wearable IoT dataset (for the smart healthcare scenario) --- 
Dataset link: https://www.kaggle.com/datasets/dcsavinod/iot-in-healthcare-and-well-being?select=Wearable+IoT+Health+Dataset.csv
Hospital statistics (France): https://tradingeconomics.com/france/hospital-beds

The dataset includes 10 devices which seem to be sending data at 5 minutes intervals. 
I assumed here that a single device would record multiple parameters (e.g., a smart watch). Based on the provided data, we assume that the payload looks like the following:
{
"device_id": "Device_5",
"timestamp": "02:47.1",
"heart_rate": 57,
"steps": 102,
"activity_label": "Sedentary",
"activity_confidence": 0.94,
"temperature": 22.6,
"humidity": 35,
"location": "office",
"battery_level": 77,
"calories_burned": 13.18
}

To create the smart healthcare scenario, I considered the average number of hospital beds in a hospital in France, and assumed that ~20% of beds would require constant monitoring (e.g., in ICUs). I then assumed that we would have 5 hospitals in a given region that would be connected to a local healthcare system for remotely monitoring patients.



--- Smart manufaturing dataset (for the smart factory scenario) ---
Smart manufacturing dataset link: https://www.kaggle.com/datasets/programmer3/smart-manufacturing-process-data
Robot dataset: https://github.com/fraunhoferhhi/ai4mobile-industrial?tab=readme-ov-file
The smart manufacturing dataset contains reading at 1-minute intervals from an industrial machine. This is not explicitly stated but I assumed that the machine is equiped with several sensors, each one providing one parameter.

* Temperature sensor:
{
"sensor_id": "Device_5",
"timestamp": "2025-04-01 08:00:00",
"temperature": 78.92
}

* Speed sensor:
{
"sensor_id": "Device_5",
"timestamp": "2025-04-01 08:00:00",
"speed": 1461
}

* Production quality sensor:
{
"sensor_id": "Device_5",
"timestamp": "2025-04-01 08:00:00",
"production_quality": 8.49
}

* Vibration level:
{
"sensor_id": "Device_5",
"timestamp": "2025-04-01 08:00:00",
"vibration_level": 0.07
}

* Energy consumption:
{
"sensor_id": "Device_5",
"timestamp": "2025-04-01 08:00:00",
"energy_consumption": 1.97
}

To construct the smart factory scenario, I relied on the smart factory setup presented in (https://www.uni-trier.de/index.php?id=69689) to decide the number of machines and robots in a single factory floor. I then assumed that we would have 3 factory floors in a small/medium factory, but we can scale the setup as needed. 