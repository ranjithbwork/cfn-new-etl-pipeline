import sys
import io
import uuid

from decimal import Decimal, ROUND_HALF_UP
from urllib.parse import urlparse

import boto3
import pandas as pd

from awsglue.utils import getResolvedOptions


glue = boto3.client("glue")

# Glue automatically passes these two when a job runs inside a workflow
args = getResolvedOptions(sys.argv, ["WORKFLOW_NAME", "WORKFLOW_RUN_ID"])

workflow_run_props = glue.get_workflow_run_properties(
    Name=args["WORKFLOW_NAME"],
    RunId=args["WORKFLOW_RUN_ID"]
)["RunProperties"]

input_path = workflow_run_props["INPUT_PATH"]
output_path = workflow_run_props["OUTPUT_PATH"]

print(f"Resolved INPUT_PATH: {input_path}")
print(f"Resolved OUTPUT_PATH: {output_path}")

s3 = boto3.client("s3")


def split_s3_uri(uri):
    parsed = urlparse(uri)
    return parsed.netloc, parsed.path.lstrip("/")


def list_input_keys(bucket, key):
    if key and not key.endswith("/"):
        return [key]

    keys = []
    paginator = s3.get_paginator("list_objects_v2")

    for page in paginator.paginate(Bucket=bucket, Prefix=key):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith("/"):
                continue
            keys.append(obj["Key"])

    return keys


def read_csv(bucket, key):
    body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
    return pd.read_csv(io.BytesIO(body))


def round_half_up(value):
    if pd.isna(value):
        return value

    return float(
        Decimal(str(value)).quantize(
            Decimal("0.01"),
            rounding=ROUND_HALF_UP
        )
    )


input_bucket, input_key = split_s3_uri(input_path)

print(f"Input bucket: {input_bucket}, Input key/prefix: {input_key}")

input_keys = list_input_keys(input_bucket, input_key)

print(f"Resolved input keys: {input_keys}")

if not input_keys:
    raise Exception(f"No input files found at {input_path}")

df = pd.concat(
    [read_csv(input_bucket, key) for key in input_keys],
    ignore_index=True
)

df = df.dropna(how="all")

df["customer_name"] = df["customer_name"].astype("string").str.upper()

df["discounted_amount"] = (df["amount"] * 0.90).map(round_half_up)

output_bucket, output_prefix = split_s3_uri(output_path)

if output_prefix and not output_prefix.endswith("/"):
    output_prefix += "/"

output_key = f"{output_prefix}part-{uuid.uuid4().hex}.csv"

buffer = io.StringIO()
df.to_csv(buffer, index=False, header=True)

s3.put_object(
    Bucket=output_bucket,
    Key=output_key,
    Body=buffer.getvalue().encode("utf-8")
)

print(f"Read {len(input_keys)} input file(s) from {input_path}")
print(f"Wrote {len(df)} rows to s3://{output_bucket}/{output_key}")