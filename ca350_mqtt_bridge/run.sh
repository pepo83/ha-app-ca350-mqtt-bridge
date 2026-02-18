#!/usr/bin/with-contenv bashio
set -e

bashio::log.info "Starting CA350 MQTT Bridge..."

# MQTT from Supervisor
export MQTT_HOST=$(bashio::services mqtt | jq -r '.host')
export MQTT_PORT=$(bashio::services mqtt | jq -r '.port')
export MQTT_USER=$(bashio::services mqtt | jq -r '.username')
export MQTT_PASS=$(bashio::services mqtt | jq -r '.password')

python3 /app/ca350.py
