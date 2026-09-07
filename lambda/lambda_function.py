import boto3
import os
import urllib.parse

from botocore.exceptions import ClientError

glue = boto3.client("glue")

GLUE_WORKFLOW_NAME = os.environ["GLUE_WORKFLOW_NAME"]


def lambda_handler(event, context):
    print(f"Received event: {event}")

    record = event["Records"][0]

    bucket_name = record["s3"]["bucket"]["name"]
    object_size = record["s3"]["object"].get("size", 0)
    object_key = urllib.parse.unquote_plus(record["s3"]["object"]["key"])

    print(f"Bucket: {bucket_name}")
    print(f"Object key: {object_key}")
    print(f"Object size: {object_size} bytes")

    if not object_key.startswith("input/"):
        print("Ignored: object is outside input/")
        return {"statusCode": 200, "message": "Ignored object outside input/"}

    if not object_key.lower().endswith(".csv"):
        print("Ignored: object is not a CSV file")
        return {"statusCode": 200, "message": "Ignored non-CSV file"}

    if object_size == 0:
        print("Ignored: CSV file is empty")
        return {"statusCode": 200, "message": "Ignored empty CSV file"}

    input_path = f"s3://{bucket_name}/{object_key}"
    file_name = object_key.split("/")[-1]
    base_name = file_name.rsplit(".", 1)[0]
    output_path = f"s3://cfn-new-pipeline-output/output/{base_name}/"

    print(f"Input path: {input_path}")
    print(f"Output path: {output_path}")
    print(f"Starting Glue workflow: {GLUE_WORKFLOW_NAME}")

    try:
        response = glue.start_workflow_run(
            Name=GLUE_WORKFLOW_NAME,
            RunProperties={
                "INPUT_PATH": input_path,
                "OUTPUT_PATH": output_path
            }
        )

        print("Glue workflow started successfully")
        print(f"Workflow RunId: {response['RunId']}")

        return {
            "statusCode": 200,
            "message": "Glue workflow started successfully",
            "runId": response["RunId"],
            "inputPath": input_path,
            "outputPath": output_path
        }

    except ClientError as error:
        print(f"Unexpected AWS error: {error}")
        raise