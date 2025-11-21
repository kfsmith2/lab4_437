import json
import time

import awsiot.greengrasscoreipc
from awsiot.greengrasscoreipc.model import (
    SubscribeToTopicRequest,
    SubscriptionResponseMessage,
    KinesisFirehosePutRecordBatchRequest,
    KinesisFirehosePutRecordBatchEntry
)

ipc = awsiot.greengrasscoreipc.connect()

INPUT_TOPIC = "vehicle/result"
FIREHOSE_STREAM = "vehicle-firehose"


class Subscriber:
    def on_stream_event(self, event: SubscriptionResponseMessage):
        try:
            msg = event.json_message.message
            row = json.loads(msg)

            entry = KinesisFirehosePutRecordBatchEntry()
            entry.data = bytes(json.dumps(row) + "\n", "utf-8")

            req = KinesisFirehosePutRecordBatchRequest()
            req.records = [entry]
            req.stream_name = FIREHOSE_STREAM

            ipc.invoke_async("aws.greengrass.KinesisFirehose.PutRecordBatch", req)

            print("Sent to Firehose:", row)

        except Exception as e:
            print("Error sending to Firehose:", e)

    def on_stream_error(self, error):
        print("Stream error:", error)

    def on_stream_closed(self):
        print("Stream closed.")


def main():
    req = SubscribeToTopicRequest()
    req.topic = INPUT_TOPIC

    handler = Subscriber()
    _, op = ipc.subscribe_to_topic(req, handler)

    print("Subscribed to:", INPUT_TOPIC)

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        op.close()


if __name__ == "__main__":
    main()
