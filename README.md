


import time
import json
import smbus2
import paho.mqtt.client as mqtt
from paho.mqtt.client import CallbackAPIVersion
import socket
import threading
import queue
from collections import deque

# === CONFIGURATIONS ===
MQTT_BROKER = '172.20.10.14'
MQTT_PORT = 1883
MQTT_TOPIC = 'sensor/acceleration'
SAMPLE_RATE = 200  # Target 200 Hz
SAMPLE_INTERVAL = 1.0 / SAMPLE_RATE  # 5 ms per sample
MQTT_BUFFER_SIZE = 20  # 0.1s buffer (~20 samples at 200 Hz)
I2C_BUS = 1
SENSOR_ADDRESS = 0x19  # LIS2DW12 I2C address

# === SENSOR SETUP ===
def init_sensor(bus):
    try:
        # Soft reset
        bus.write_byte_data(SENSOR_ADDRESS, 0x21, 0x40)  # CTRL2: SOFT_RESET
        time.sleep(0.01)
        # Set range Â±2g, High-Performance, 200 Hz
        bus.write_byte_data(SENSOR_ADDRESS, 0x20, 0x44)  # CTRL1: ODR=200Hz, MODE=High-Performance
        bus.write_byte_data(SENSOR_ADDRESS, 0x23, 0x00)  # CTRL6: FS=Â±2g, Low-pass filter
        # Enable continuous data update
        bus.write_byte_data(SENSOR_ADDRESS, 0x25, 0x00)  # CTRL3: Block Data Update disabled
        print("LIS2DW12 initialized")
    except Exception as e:
        raise RuntimeError(f"Failed to initialize LIS2DW12: {e}")

def read_acceleration(bus):
    try:
        # Read 6 bytes from OUT_X_L (0x28) to OUT_Z_H (0x2D)
        data = bus.read_i2c_block_data(SENSOR_ADDRESS, 0x28, 6)
        # Convert 16-bit two's complement
        x = (data[1] << 8 | data[0]) if data[1] & 0x80 == 0 else -((~data[1] << 8 | ~data[0]) + 1)
        y = (data[3] << 8 | data[2]) if data[3] & 0x80 == 0 else -((~data[3] << 8 | ~data[2]) + 1)
        z = (data[5] << 8 | data[4]) if data[5] & 0x80 == 0 else -((~data[5] << 8 | ~data[4]) + 1)
        return x, y, z
    except Exception as e:
        raise Exception(f"Read error: {e}")

# === MQTT SETUP ===
def is_broker_alive(host, port, timeout=2):
    try:
        sock = socket.create_connection((host, port), timeout)
        sock.close()
        return True
    except Exception:
        return False

def on_disconnect(client, userdata, rc, properties=None):
    print("Disconnected from MQTT broker. Reconnecting...")
    while userdata['running']:
        try:
            client.reconnect()
            print("Reconnected to MQTT broker")
            break
        except Exception as e:
            print(f"Reconnect failed: {e}")
            time.sleep(5)

def init_mqtt():
    client = mqtt.Client(callback_api_version=CallbackAPIVersion.VERSION2, userdata={'running': True})
    client.on_disconnect = on_disconnect
    client.enable_logger()
    return client

# === THREADS ===
def sensor_read_loop(bus, sensor_queue, running):
    sample_count = 0
    start_time = time.time()
    next_sample_time = time.time()
    while running.is_set():
        try:
            read_start = time.time()
            x, y, z = read_acceleration(bus)
            read_duration = time.time() - read_start
            timestamp = time.time()
            print(f'x = {x}, y = {y}, z = {z}')
            try:
                sensor_queue.put_nowait((x, y, z, timestamp, read_duration))
            except queue.Full:
                print("Sensor queue full, dropping sample")
                

            sample_count += 1
            if time.time() - start_time >= 1.0:
                avg_read_duration = sum([item[4] for item in list(sensor_queue.queue)]) / max(1, sensor_queue.qsize())
                print(f"Sample rate: {sample_count / (time.time() - start_time):.2f} Hz, Avg read time: {avg_read_duration*1000:.2f} ms")
                sample_count = 0
                start_time = time.time()

            next_sample_time += SAMPLE_INTERVAL
            sleep_duration = next_sample_time - time.time()
            if sleep_duration > 0:
                time.sleep(sleep_duration)
        except Exception as e:
            print(f"Read error: {e}, retrying...")
            time.sleep(0.0001)

def mqtt_publish_loop(client, mqtt_buffer, lock):
    while True:
        try:
            buffer = []
            with lock:
                while mqtt_buffer['x']:
                    buffer.append((
                        mqtt_buffer['x'].popleft(),
                        mqtt_buffer['y'].popleft(),
                        mqtt_buffer['z'].popleft(),
                        mqtt_buffer['timestamps'].popleft()
                    ))
            for x, y, z, timestamp in buffer:
                payload = json.dumps({
                    "timestamp": time.strftime("%Y-%m-%d %H:%M:%S.%f", time.localtime(timestamp)),
                    "x": x,
                    "y": y,
                    "z": z
                })
                client.publish(MQTT_TOPIC, payload)
            time.sleep(0.1)
        except Exception as e:
            print(f"MQTT publish error: {e}")
            time.sleep(0.1)

# === MAIN LOOP ===
def main_loop():
    if is_broker_alive(MQTT_BROKER, MQTT_PORT):
        print("Broker is alive")
    else:
        print("Broker is not reachable")
        return

    bus = smbus2.SMBus(I2C_BUS)
    init_sensor(bus)
    mqtt_client = init_mqtt()
    mqtt_client.connect(MQTT_BROKER, MQTT_PORT, 60)

    running = threading.Event()
    running.set()
    sensor_queue = queue.Queue(maxsize=1000)
    lock = threading.Lock()

    mqtt_buffer = {
        'timestamps': deque(maxlen=MQTT_BUFFER_SIZE),
        'x': deque(maxlen=MQTT_BUFFER_SIZE),
        'y': deque(maxlen=MQTT_BUFFER_SIZE),
        'z': deque(maxlen=MQTT_BUFFER_SIZE)
    }

    sensor_thread = threading.Thread(target=sensor_read_loop, args=(bus, sensor_queue, running), daemon=True)
    mqtt_thread = threading.Thread(target=mqtt_publish_loop, args=(mqtt_client, mqtt_buffer, lock), daemon=True)
    sensor_thread.start()
    mqtt_thread.start()

    try:
        while True:
            try:
                x, y, z, timestamp, _ = sensor_queue.get_nowait()
                with lock:
                    mqtt_buffer['x'].append(x)
                    mqtt_buffer['y'].append(y)
                    mqtt_buffer['z'].append(z)
                    mqtt_buffer['timestamps'].append(timestamp)
            except queue.Empty:
                time.sleep(0.001)
            except Exception as e:
                print(f"Main loop error: {e}")
                time.sleep(0.01)

    except KeyboardInterrupt:
        print("Exiting...")
    finally:
        running.clear()
        mqtt_client.user_data_set({'running': False})
        mqtt_client.disconnect()
        bus.close()

if __name__ == '__main__':
    main_loop()
