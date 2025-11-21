import json
import sys
import awsiot.greengrasscoreipc
from awsiot.greengrasscoreipc.model import (
    PublishToTopicRequest,
    SubscribeToTopicRequest,
    SubscriptionResponseMessage
)

import process_emission 

ipc_client = awsiot.greengrasscoreipc.connect()

# TOPICS FOR THE LAB
INPUT_TOPIC = "vehicle/data"
OUTPUT_TOPIC = "vehicle/result"

class CO2Subscriber:
    def __init__(self):
        pass

    def on_stream_event(self, event: SubscriptionResponseMessage):
        try:
            message_str = event.json_message.message
            row = json.loads(message_str)

            # Call the provided lab function
            result = process_emission.lambda_handler([row], None)

            # Publish result back
            out = json.dumps(result)
            pub_req = PublishToTopicRequest()
            pub_req.topic = OUTPUT_TOPIC
            pub_req.payload = bytes(out, 'utf-8')
            ipc_client.publish_to_topic(pub_req)
            print("Published:", out)

        except Exception as e:
            print("Processing error:", e)

    def on_stream_error(self, error):
        print("Stream error:", error)

    def on_stream_closed(self):
        print("Stream closed.")


def main():
    # Subscribe to vehicle data topic
    request = SubscribeToTopicRequest()
    request.topic = INPUT_TOPIC

    handler = CO2Subscriber()
    _, operation = ipc_client.subscribe_to_topic(request, handler)
    print("Subscribed to:", INPUT_TOPIC)

    try:
        while True:
            pass  # keep component alive
    except KeyboardInterrupt:
        operation.close()


if __name__ == "__main__":
    main()
