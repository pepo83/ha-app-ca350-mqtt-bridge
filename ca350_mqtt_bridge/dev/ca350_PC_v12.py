# -*- coding: utf-8 -*-
"""
Created on Tue Feb 17 11:33:26 2026

@author: pepo83
"""

import socket
import threading
import time
import logging
import json
import paho.mqtt.client as mqtt

# ================== CONFIG ==================

DEBUG = False

PcMode = 0  # 0,1,4 allowed

COMFOAIR_HOST = "192.168.40.130"
COMFOAIR_PORT = 8899

mqtt_host = "192.168.40.100"
mqtt_port = 1883
mqtt_user = "PCtest"
mqtt_pass = "PCtest"

mqtt_base_topic = "comfoair"
ha_prefix = "homeassistant"

DEVICE_INFO = {
    "identifiers": ["ca350"],
    "name": "CA350",
    "manufacturer": "Zehnder",
    "model": "Comfoair 350"
}


# ================== LOGGING ==================

logging.basicConfig(
    level=logging.DEBUG if DEBUG else logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%d-%m-%Y %H:%M:%S",
    force=True
)
log = logging.getLogger("CA350")
log.setLevel(logging.DEBUG if DEBUG else logging.INFO)

# ================== MQTT MANAGER ==================

class MqttManager:
    def __init__(self):
        self.ca = None
        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, "CA350")
        self.client.username_pw_set(mqtt_user, mqtt_pass)
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        self.client.on_disconnect = self.on_disconnect
        self.client.reconnect_delay_set(min_delay=2, max_delay=60)

        # Last Will (shows HA if script dies)
        self.client.will_set(
            f"{mqtt_base_topic}/status",
            payload="offline",
            qos=1,
            retain=True
        )

    def connect(self):
        log.info("Connecting to MQTT broker...")
        self.client.connect(mqtt_host, mqtt_port, 60)
        self.client.loop_start()

    def stop(self):
        log.info("Stopping MQTT...")
        try:
            self.publish("status", "offline", retain=True)
            time.sleep(1)
            self.client.loop_stop()
            self.client.disconnect()
        except Exception as e:
            log.warning(f"MQTT shutdown error: {e}")

    def publish(self, topic, payload, retain=True):
        full_topic = f"{mqtt_base_topic}/{topic}"
        self.client.publish(full_topic, payload, retain=retain)

    # ---------- Callbacks ----------

    def on_connect(self, client, userdata, flags, reason_code, properties):
        if reason_code == 0:
            log.info("MQTT connected Success")

            # online state
            self.publish("status", "online", retain=True)

            # subscribe to command topics
            self.subscribe_commands()

            # Home Assistant discovery
            self.publish_discovery()

        else:
            log.error(f"MQTT connect failed: {reason_code}")

    def on_disconnect(self, client, userdata, flags, reason_code, properties):
        log.warning(f"MQTT disconnected: {reason_code}")
        if reason_code != 0:
            time.sleep(5)
            try:
                log.info("Trying MQTT reconnect...")
                client.reconnect()
            except Exception as e:
                log.error(f"MQTT reconnect failed: {e}")

    def on_message(self, client, userdata, msg):
        if not self.ca:
            return

        topic = msg.topic.replace(f"{mqtt_base_topic}/set/", "")
        payload = msg.payload.decode().strip()

        log.info(f"MQTT CMD {topic} = {payload}")

        try:
            # --- Climate entity topics ---
            if topic == "climate/mode":
                # HA sends: off / fan_only
                if payload == "off":
                    self.ca.set_fan_level(1)  # off = minimal
                else:
                    # fan_only -> keep current, but ensure at least 2 if None
                    if self.ca.current_fan_level is None:
                        self.ca.set_fan_level(2)

            elif topic == "climate/fan_mode":
                MAP = {
                    "low": 2,
                    "medium": 3,
                    "high": 4,
                    "away": 1,
                }
                self.ca.set_fan_level(MAP.get(payload, 2))

            elif topic == "climate/temperature":
                self.ca.set_temperature(float(payload))

            elif topic == "pc_mode":
                self.ca.set_pc_mode(int(payload))
                
            elif topic == "airflow_mode":
                mode = payload
                self.ca.set_airflow_mode(mode)

        except Exception as e:
            log.warning(f"MQTT command error: {e}")

    def subscribe_commands(self):
        self.client.subscribe(f"{mqtt_base_topic}/set/#")
        log.info("Subscribed to MQTT command topics")

    # ---------- HOME ASSISTANT DISCOVERY ----------

    def publish_discovery(self):

        availability = {
            "availability_topic": f"{mqtt_base_topic}/status",
            "payload_available": "online",
            "payload_not_available": "offline",
        }

        # --------- CLIMATE ENTITY ---------

        climate_cfg = {
            "name": "CA350",
            "unique_id": "ca350_climate",
            "device": DEVICE_INFO,

            "mode_state_topic": f"{mqtt_base_topic}/status/hvac_mode",
            "mode_command_topic": f"{mqtt_base_topic}/set/climate/mode",
            "modes": ["off", "fan_only"],

            "fan_mode_state_topic": f"{mqtt_base_topic}/status/fan_mode",
            "fan_mode_command_topic": f"{mqtt_base_topic}/set/climate/fan_mode",
            "fan_modes": ["away", "low", "medium", "high"],

            "temperature_state_topic": f"{mqtt_base_topic}/status/comfort_temp",
            "temperature_command_topic": f"{mqtt_base_topic}/set/climate/temperature",
            "min_temp": 15,
            "max_temp": 27,
            "temp_step": 0.5,
            "temperature_unit": "C",
            **availability,
        }
        
        air_cfg = {
            "name": "CA350 Airflow Mode",
            "unique_id": "ca350_airflow_mode",
            "state_topic": f"{mqtt_base_topic}/status/airflow_mode",
            "command_topic": f"{mqtt_base_topic}/set/airflow_mode",
            "options": ["In", "Out", "In and Out"],
            "device": DEVICE_INFO,
            **availability,
        }
        
        self.client.publish(
            f"{ha_prefix}/select/ca350/airflow_mode/config",
            json.dumps(air_cfg),
            retain=True
        )


        self.client.publish(
            f"{ha_prefix}/climate/ca350/main/config",
            json.dumps(climate_cfg),
            retain=True
        )

        # --------- SENSORS ---------

        sensors = [
            ("outside_temp", "Outside air", "°C"),
            ("supply_temp", "Supply air", "°C"),
            ("extract_temp", "Extract air", "°C"),
            ("exhaust_temp", "Exhaust air", "°C"),
            ("fan_level", "Fan level", None),
            ("intake_fan", "Intake fan %", "%"),
            ("exhaust_fan", "Exhaust fan %", "%"),
            ("bypass", "Bypass", None),
            ("rs232_mode", "RS232 Mode", None),
            ("preheater_flap", "Preheater flap", None),
            ("frost_protection", "Frost protection", None),
            ("preheater", "Preheater active", None),
            ("frost_minutes", "Frost minutes", "min"),

        ]

        for key, name, unit in sensors:
            cfg = {
                "name": f"CA350 {name}",
                "unique_id": f"ca350_{key}",
                "state_topic": f"{mqtt_base_topic}/status/{key}",
                "device": DEVICE_INFO,
                **availability
            }
            if unit:
                cfg["unit_of_measurement"] = unit

            self.client.publish(
                f"{ha_prefix}/sensor/ca350/{key}/config",
                json.dumps(cfg),
                retain=True
            )


# ================== CA350 CLIENT ==================

class CA350Client:
    START = b"\x07\xF0"
    END = b"\x07\x0F"

    @staticmethod
    def calc_checksum(cmd: bytes, length: int, data: bytes) -> int:
        total = sum(cmd) + length
        skip_next_07 = False

        for b in data:
            if b == 0x07:
                if skip_next_07:
                    skip_next_07 = False
                    continue
                total += b
                skip_next_07 = True
            else:
                skip_next_07 = False
                total += b

        total += 173
        return total & 0xFF

    @classmethod
    def build_frame(cls, cmd: bytes, data: bytes = b"") -> bytes:
        ln = len(data)
        checksum = cls.calc_checksum(cmd, ln, data)
        return (
            cls.START +
            cmd +
            bytes([ln]) +
            data +
            bytes([checksum]) +
            cls.END
        )

    def __init__(self, host, port, mqtt_client):
        self.host = host
        self.port = port
        self.mqtt = mqtt_client
        self.sock = None
        self.running = False
        self.rx_thread = None
        self.buffer = bytearray()
        self.lock = threading.Lock()
        self.seen_commands = set()
        self.current_fan_level = None
        self.current_comfo_temp_c = None
        self.current_comfo_temp_raw = None
        self.current_RS232_mode = None
        self.current_airflow_mode = None

    # ---------- CONNECTION ----------

    def connect(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect((self.host, self.port))
        self.running = True
        self.rx_thread = threading.Thread(target=self.rx_loop, daemon=True)
        self.rx_thread.start()
        log.info("Connected to CA350")

    def stop(self):
        log.info("Stopping CA350 client...")
        self.running = False
        try:
            if self.sock:
                self.sock.close()
        except:
            pass

    # ---------- RX LOOP ----------

    def rx_loop(self):
        while self.running:
            try:
                data = self.sock.recv(256)
                if not data:
                    break
                self.buffer.extend(data)
                self.process_buffer()
            except socket.timeout:
                continue
            except Exception as e:
                if self.running:
                    log.error(f"RX error: {e}")
                break

    # ---------- FRAME PARSER ----------

    def process_buffer(self):
        while True:

            start = self.buffer.find(self.START)
            if start == -1:
                self.buffer.clear()
                return

            if start > 0:
                del self.buffer[:start]

            if len(self.buffer) < 5:
                return

            cmd = bytes(self.buffer[2:4])
            length = self.buffer[4]

            i = 5
            decoded = bytearray()

            while len(decoded) < length:

                if i >= len(self.buffer):
                    return

                b = self.buffer[i]

                if b == 0x07:
                    if i + 1 >= len(self.buffer):
                        return

                    nxt = self.buffer[i + 1]

                    if nxt == 0x07:
                        decoded.append(0x07)
                        i += 2
                    else:
                        log.debug("Invalid escape sequence, resync")
                        del self.buffer[0]
                        break
                else:
                    decoded.append(b)
                    i += 1

            if i + 3 > len(self.buffer):
                return

            checksum = self.buffer[i]
            end = self.buffer[i + 1:i + 3]

            if end != self.END:
                log.debug("Frame sync lost, dropping 1 byte")
                del self.buffer[0]
                continue

            raw_frame = bytes(self.buffer[:i + 3])
            del self.buffer[:i + 3]

            self.handle_frame(cmd, length, bytes(decoded), checksum, raw_frame)

    # ---------- FRAME HANDLER ----------

    def handle_frame(self, cmd, length, data, checksum, raw):

        self.seen_commands.add(cmd)

        calc = self.calc_checksum(cmd, length, data)
        if calc != checksum:
            log.warning(f"Checksum error: {raw.hex(' ')}")
            return

        self.decode_frame(cmd, data)

    # ---------- STATUS FRAMES ----------

    def decode_frame(self, cmd: bytes, data: bytes):
        log.debug(f"RX {cmd.hex(' ')} DATA={data.hex(' ')}")

        # Ventilation Status
        if cmd == b"\x00\xCE" and len(data) >= 14:
            
            exhaust = data[6]
            intake = data[7]
            fan = data[8]
            
            log.info(f"Exhaust(%) = {exhaust}")
            log.info(f"Intake(%) = {intake}")
            log.info(f"Fan = {fan}")

            self.current_fan_level = fan

            self.publish("fan_level", str(fan))
            self.publish("intake_fan", str(intake))
            self.publish("exhaust_fan", str(exhaust))

            # Derive fan_mode string for HA climate
            if fan == 1:
                fan_mode = "away"
            elif fan == 2:
                fan_mode = "low"
            elif fan == 3:
                fan_mode = "medium"
            else:
                fan_mode = "high"

            self.publish("fan_mode", fan_mode)

            # Derive hvac_mode for HA
            # off = fan level 1
            self.publish("hvac_mode", "off" if fan == 1 else "fan_only")

        # Temperature Status
        elif cmd == b"\x00\xD2" and len(data) >= 9:
            def temp(x): return (x / 2) - 20

            comfosense_raw = data[0]
            comfosense_c = temp(comfosense_raw)

            outside = temp(data[1])
            supply = temp(data[2])
            extract = temp(data[3])
            exhaust = temp(data[4])
            
            log.info(f"Comfosense Temp. = {comfosense_c} °C")
            log.info(f"Outside Temp. = {outside} °C")
            log.info(f"Supply Temp. = {supply} °C")
            log.info(f"Extract Temp. = {extract} °C")
            log.info(f"Exhaust Temp. = {exhaust} °C")

            self.current_comfo_temp_raw = comfosense_raw
            self.current_comfo_temp_c = comfosense_c

            self.publish("comfort_temp", str(round(comfosense_c, 1)))
            self.publish("outside_temp", str(round(outside, 1)))
            self.publish("supply_temp", str(round(supply, 1)))
            self.publish("extract_temp", str(round(extract, 1)))
            self.publish("exhaust_temp", str(round(exhaust, 1)))

        # Bypass
        elif cmd == b"\x00\xE0" and len(data) >= 7:
            bypass = data[6]
            self.publish("bypass", "ON" if bypass == 1 else "OFF")
            log.info(f"Bypass = {bypass}")

        # RS232 mode
        elif cmd == b"\x00\x9C" and len(data) >= 1:
            RS232_mode = data[0]
            self.current_RS232_mode = RS232_mode
            self.publish("rs232_mode", str(RS232_mode))
            log.info(f"RS232 mode = {RS232_mode}")
          
        # Airflow mode from Display commands
        elif cmd == b"\x00\x3C" and len(data) >= 10:
        
            flags = data[9]
            In = bool(flags & 0x40)
            Out = bool(flags & 0x80)
        
            if In and Out:
                mode = "In and Out"
            elif In:
                mode = "In"
            elif Out:
                mode = "Out"
            else:
                mode = "unknown"
        
            self.current_airflow_mode = mode
        
            log.info(f"Airflow mode = {mode}")
            self.publish("airflow_mode", mode)
        
        # Preheater / Frost protection status
        elif cmd == b"\x00\xE2" and len(data) >= 6:
        
            flap_status = data[0]   # 1=open / 0=closed / 2=unknown
            frost_protection = data[1]   # 1=active / 0=inactive
            preheat = data[2]    # 1=active / 0=inactive
        
            frost_minutes = (data[3] << 8) | data[4]   # Bytes 4-5
        
            flap_txt = {0: "closed", 1: "open", 2: "unknown"}.get(flap_status, str(flap_status))
        
            log.info(f"Preheater flap = {flap_txt}")
            log.info(f"Frost protection = {'ON' if frost_protection == 1 else 'OFF'}")
            log.info(f"Preheater active = {'ON' if preheat == 1 else 'OFF'}")
            log.info(f"Frost minutes = {frost_minutes} min")
        
            # MQTT publish
            self.publish("preheater_flap", flap_txt)
            self.publish("frost_protection", "ON" if frost_protection == 1 else "OFF")
            self.publish("preheater", "ON" if preheat == 1 else "OFF")
            self.publish("frost_minutes", str(frost_minutes))


    # ---------- MQTT PUBLISH ----------

    def publish(self, key, value):
        self.mqtt.publish(f"status/{key}", value)

    # ---------- VERIFIED SEND ----------

    def send_verified(self, frame, check_fn, name):
        for attempt in range(1, 4):
            self.sock.send(frame)
            log.info(f"Sent {name} (try {attempt})")

            for _ in range(20):
                if check_fn():
                    log.info(f"{name} verified")
                    return True
                time.sleep(0.2)

        log.warning(f"{name} failed after 3 tries")
        return False

    # ---------- COMMANDS ----------

    def set_fan_level(self, level: int):
        if level not in [1, 2, 3, 4]:
            return

        data = bytes([level])
        frame = self.build_frame(b"\x00\x99", data)

        self.send_verified(
            frame,
            lambda: self.current_fan_level == level,
            f"Fan level {level}"
        )

    def set_temperature(self, temp_c: float):
        if not (15 <= temp_c <= 27):
            log.warning(f"Wrong temperature provided: {temp_c}. No changes made")
            return

        # Gerät erwartet raw = temp*2 + 40
        val = int(round(temp_c * 2 + 40))
        data = bytes([val])
        frame = self.build_frame(b"\x00\xD3", data)

        self.send_verified(
            frame,
            lambda: self.current_comfo_temp_raw == val,
            f"Temperature {temp_c}"
        )

    def set_pc_mode(self, nr: int):
        if nr not in [0, 1, 3, 4]:
            return

        data = bytes([nr])
        frame = self.build_frame(b"\x00\x9B", data)

        # Map requested mode to expected status response
        expected_mode = 2 if nr == 0 else nr

        self.send_verified(
            frame,
            lambda: self.current_RS232_mode == expected_mode,
            f"RS232 mode {nr}"
        )
        
       
    def set_airflow_mode(self, mode: str):   
        mode = (mode or "").strip().lower()
        MAP = {
          "in": "In",
          "out": "Out",
          "in and out": "In and Out"
        }
        mode = MAP.get(mode)
        if mode not in ["In", "Out", "In and Out"]:
            log.warning("Invalid airflow mode!")
            return False
        for i in range(6):
            self.press_airmode_button()
            time.sleep(0.8)
            if self.current_airflow_mode == mode:
                break
        if self.current_airflow_mode != mode:
            log.warning(f"Airflow mode change failed: {mode}")
        return True
    
    def press_airmode_button(self):
        data = bytes([0x00, 0x06, 0x00, 0x00, 0x00, 0x00, 0x02])
        frame = self.build_frame(b"\x00\x37", data)
        self.sock.send(frame)
        log.info(f"Sent airflow_mode_button {frame.hex(' ')}")
        data = bytes([0x00, 0x0C, 0x00, 0x00, 0x00, 0x00, 0x03])
        frame = self.build_frame(b"\x00\x37", data)
        self.sock.send(frame)
        log.info(f"Sent airflow_mode_button {frame.hex(' ')}")
        # data = bytes([0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02])
        # frame = self.build_frame(b"\x00\x37", data)
        # self.sock.send(frame)
        # log.info(f"Sent airflow_mode_button {frame.hex(' ')})")

    def print_seen_commands(self):
        log.debug("Seen protocol commands:")
        for c in sorted(self.seen_commands):
            log.debug(f"  CMD {' '.join(f'{b:02X}' for b in c)}")

# ================== MAIN ==================

def main():
    mqtt_mgr = MqttManager()
    mqtt_mgr.connect()

    ca = CA350Client(COMFOAIR_HOST, COMFOAIR_PORT, mqtt_mgr)
    mqtt_mgr.ca = ca

    try:
        ca.connect()
        log.info("System running (CTRL+C to exit)")

        # set RS232 mode
        if PcMode in (0, 1, 4):
            ca.set_pc_mode(PcMode)
        else:
            log.warning(f"Invalid PC mode: {PcMode}")
            ca.set_pc_mode(0)

        while True:
            time.sleep(1)

    except KeyboardInterrupt:
        log.info("CTRL+C received")

    finally:
        ca.stop()
        mqtt_mgr.stop()
        log.info("Shutdown complete")
        ca.print_seen_commands()
        log.info("~~~ Close Program ~~~")

if __name__ == "__main__":
    main()
