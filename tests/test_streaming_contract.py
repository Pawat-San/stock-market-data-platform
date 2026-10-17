"""Run with: python -m unittest discover -s tests -v"""

import importlib.util
import json
import sys
import types
import unittest
from pathlib import Path
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]


class FakeKinesis:
    def __init__(self):
        self.records = []

    def put_record(self, **kwargs):
        self.records.append(kwargs)
        return {"ShardId": "shard-000"}


class FakeS3:
    def __init__(self):
        self.objects = []

    def put_object(self, **kwargs):
        self.objects.append(kwargs)


class FakeSession:
    def __init__(self, kinesis, s3):
        self.kinesis = kinesis
        self.s3 = s3

    def client(self, service, **_kwargs):
        return {"kinesis": self.kinesis, "s3": self.s3}[service]


def load_streaming_module(name, path, session):
    boto3 = types.SimpleNamespace(Session=lambda: session)
    websocket = types.SimpleNamespace(WebSocketApp=object)
    spec = importlib.util.spec_from_file_location(name, ROOT / path)
    module = importlib.util.module_from_spec(spec)
    with patch.dict(sys.modules, {"boto3": boto3, "websocket": websocket}):
        spec.loader.exec_module(module)
    return module


class StreamingContractTests(unittest.TestCase):
    def setUp(self):
        self.kinesis = FakeKinesis()
        self.s3 = FakeS3()
        session = FakeSession(self.kinesis, self.s3)
        self.producer = load_streaming_module(
            "test_finnhub_producer", "src/streaming/finnhub_producer.py", session
        )
        self.consumer = load_streaming_module(
            "test_kinesis_consumer", "src/streaming/kinesis_consumer.py", session
        )

    def test_identical_trade_values_keep_distinct_event_ids(self):
        trade = {"s": "AAPL", "p": 150.25, "t": 1760000000000, "v": 10}
        self.producer.on_message(
            None, json.dumps({"type": "trade", "data": [trade, trade]})
        )

        records = [json.loads(item["Data"]) for item in self.kinesis.records]
        self.assertEqual(len(records), 2)
        self.assertNotEqual(records[0]["event_id"], records[1]["event_id"])
        for record in records:
            self.assertEqual(record["symbol"], "AAPL")
            self.assertEqual(record["price"], 150.25)
            self.assertEqual(record["timestamp"], 1760000000000)
            self.assertEqual(record["volume"], 10)

    def test_jsonl_preserves_each_event_id(self):
        records = [
            {"event_id": "first", "symbol": "AAPL", "price": 1},
            {"event_id": "second", "symbol": "AAPL", "price": 1},
        ]
        self.consumer.upload_batch_to_s3(records)

        self.assertEqual(len(self.s3.objects), 1)
        uploaded = self.s3.objects[0]
        self.assertTrue(uploaded["Key"].endswith(".jsonl"))
        self.assertEqual(uploaded["ContentType"], "application/x-ndjson")
        self.assertEqual(
            [json.loads(line) for line in uploaded["Body"].decode().splitlines()],
            records,
        )


if __name__ == "__main__":
    unittest.main()
