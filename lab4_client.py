from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTClient
import time
import json
import pandas as pd

# -------- CONFIG --------
device_id = "Vehicle0"

endpoint = "a1ffmj0yhmq3ry-ats.iot.us-east-2.amazonaws.com"

root_ca = "C:/Users/kyles/Downloads/thing0/AmazonRootCA1.pem"
cert = "C:/Users/kyles/Downloads/thing0/08ec2bd1090a223b3d8641d7a162d45201edcd357e86f6a697e23fb986acbf78-certificate.pem.crt"
private_key = "C:/Users/kyles/Downloads/thing0/08ec2bd1090a223b3d8641d7a162d45201edcd357e86f6a697e23fb986acbf78-private.pem.key"

data_path = "C:/Users/kyles/Downloads/vehicle0.csv"

# MUST match main.py
PUBLISH_TOPIC = "vehicle/data"         # input to the Pi
RESULT_TOPIC_1 = "vehicle/result"      # output from main.py
RESULT_TOPIC_2 = "iot/Vehicle_0"       # output from process_emission


# -------- MQTT CLIENT CLASS --------
class MQTTClient:
    def __init__(self, device_id):
        self.client = AWSIoTMQTTClient(device_id)

        # Configure endpoint + certs
        self.client.configureEndpoint(endpoint, 8883)
        self.client.configureCredentials(root_ca, private_key, cert)

        self.client.configureOfflinePublishQueueing(-1)
        self.client.configureDrainingFrequency(2)
        self.client.configureConnectDisconnectTimeout(10)
        self.client.configureMQTTOperationTimeout(5)

    def connect(self):
        print("[INFO] Connecting to AWS IoT...")
        self.client.connect()
        print("[INFO] Connected.")

        # Subscribe to result topics
        self.client.subscribe(RESULT_TOPIC_1, 1, self.on_message)
        self.client.subscribe(RESULT_TOPIC_2, 1, self.on_message)

        print(f"[INFO] Subscribed to return topics:\n  - {RESULT_TOPIC_1}\n  - {RESULT_TOPIC_2}")

    def on_message(self, client, userdata, message):
        print(f"\n[RETURN MESSAGE] Topic: {message.topic}")
        print(f"Payload: {message.payload.decode()}\n")

    def send_csv(self):
        df = pd.read_csv(data_path)
        print(f"[INFO] Loaded {len(df)} rows")

        for idx, row in df.iterrows():
            # IMPORTANT — must include these keys:
            #   vehicle_CO2
            #   vehicle_id
            # Because process_emission.py expects them!
            payload = {
                "vehicle_CO2": float(row["vehicle_CO2"]),
                "vehicle_id": str(row["vehicle_id"])
            }

            j = json.dumps(payload)
            print(f"[PUBLISH] {j}")

            # Publish to Greengrass input topic
            self.client.publish(PUBLISH_TOPIC, j, 1)
            time.sleep(0.3)


# -------- MAIN --------
client = MQTTClient(device_id)
client.connect()

print("Press 's' to send CSV rows, 'q' to quit.")

while True:
    cmd = input("> ")

    if cmd == "s":
        client.send_csv()
    elif cmd == "q":
        print("Disconnecting...")
        client.client.disconnect()
        break
