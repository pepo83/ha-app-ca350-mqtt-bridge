# CA350 MQTT Bridge

This add-on runs a Python bridge between a Zehnder ComfoAir 350 with connected CC Ease or Comfosense control unit and MQTT.
It provides Home Assistant MQTT Auto-Discovery (Climate + Sensors).

# OPTIONS:

DEBUG = False -->debug mode on/off, to see all commands/messages

PcMode = 0 -->0 default; 1 PC only; 3 PC Logmode

COMFOAIR_HOST = "192.168.40.130" -->IP of the RS232 TCP Adapter

COMFOAIR_PORT = 8899 -->Port of the RS232 TCP Adapter

mqtt_base_topic = "comfoair"

ha_prefix = "homeassistant"





