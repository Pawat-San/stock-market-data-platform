import json
from urllib import response
import boto3
import time
from datetime import datetime, timezone
import uuid


AWS_PROFILE = "stock-de"
AWS_REGION = "ap-southeast-7"
KINESIS_STREAM_NAME = "stock-trades-stream"
S3_BUCKET = "stock-market-data-platform-pait"
BATCH_SIZE = 100
FLUSH_INTERVAL_SECONDS = 30  # seconds


session = boto3.Session(profile_name=AWS_PROFILE)

kinesis_client = session.client(
    "kinesis",
    region_name=AWS_REGION
)

s3_client = session.client(
    "s3",
    region_name=AWS_REGION
)



def get_shard_ids():
    response = kinesis_client.list_shards(
        StreamName=KINESIS_STREAM_NAME
    )

    return [
        shard["ShardId"]
        for shard in response["Shards"]
    ]


def get_shard_iterator(shard_id):
    response = kinesis_client.get_shard_iterator(
        StreamName=KINESIS_STREAM_NAME,
        ShardId=shard_id,
        ShardIteratorType="TRIM_HORIZON"
    )

    return response["ShardIterator"]


def consume_records():
    shard_ids = get_shard_ids()

    print(f"Found {len(shard_ids)} shards")

    shard_iterators = {}

    for shard_id in shard_ids:
        shard_iterators[shard_id] = get_shard_iterator(shard_id)
        print(f"Reading shard: {shard_id}")

    batch = []
    last_flush_time = time.time()

    try:
        while True:
            for shard_id, shard_iterator in list(shard_iterators.items()):

                if shard_iterator is None:
                    continue

                response = kinesis_client.get_records(
                    ShardIterator=shard_iterator,
                    Limit=10
                )

                for record in response["Records"]:
                    raw_data = record["Data"].decode("utf-8")

                    try:
                        data = json.loads(raw_data)

                        batch.append(data)

                        print(
                            f"{shard_id} | "
                            f"{data.get('symbol')} | "
                            f"{data.get('price')} | "
                            f"batch={len(batch)}/{BATCH_SIZE}"
                        )

                    except json.JSONDecodeError:
                        print(
                            f"{shard_id} | Skipping invalid JSON: "
                            f"{raw_data}"
                        )

                shard_iterators[shard_id] = response.get(
                    "NextShardIterator"
                )

            # สำคัญ: ส่วนนี้ต้องอยู่นอก for shard
            current_time = time.time()
            elapsed = current_time - last_flush_time

            print(
                f"Waiting... batch={len(batch)} "
                f"elapsed={elapsed:.1f}s"
            )

            should_flush_by_size = len(batch) >= BATCH_SIZE

            should_flush_by_time = (
                len(batch) > 0
                and elapsed >= FLUSH_INTERVAL_SECONDS
            )

            if should_flush_by_size or should_flush_by_time:
                upload_batch_to_s3(batch)

                batch.clear()
                last_flush_time = time.time()

            time.sleep(1)

    except KeyboardInterrupt:
        print("\nStopping consumer...")

        if batch:
            print(
                f"Flushing remaining {len(batch)} "
                f"records before shutdown..."
            )

            upload_batch_to_s3(batch)

        print("Consumer stopped safely.")

def upload_batch_to_s3(records):
    if not records:
        return

    now = datetime.now(timezone.utc)

    s3_key = (
        f"raw/trades/"
        f"year={now.year}/"
        f"month={now.month:02d}/"
        f"day={now.day:02d}/"
        f"hour={now.hour:02d}/"
        f"trades-{uuid.uuid4()}.jsonl"
    )

    body = "\n".join(
        json.dumps(record)
        for record in records
    )

    s3_client.put_object(
        Bucket=S3_BUCKET,
        Key=s3_key,
        Body=body.encode("utf-8"),
        ContentType="application/x-ndjson"
    )

    print(
        f"Uploaded {len(records)} records "
        f"to s3://{S3_BUCKET}/{s3_key}"
    )


if __name__ == "__main__":
        consume_records()
