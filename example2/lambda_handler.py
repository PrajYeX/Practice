import json
import os
import boto3
from botocore.exceptions import ClientError
from datetime import datetime

# DynamoDB client
dynamodb = boto3.resource("dynamodb")

# Environment variable for table name
TABLE_NAME = os.environ.get("DYNAMODB_TABLE", "CloudFormationTable")

def lambda_handler(event, context):
  
    try:
        # Validate DynamoDB table
        table = dynamodb.Table(TABLE_NAME)

        # Extract fields safely with defaults
        event_time = event.get("time", datetime.utcnow().isoformat())
        event_source = event.get("source", "UnknownSource")
        event_name = event.get("detail-type", "UnknownEvent")
        aws_region = event.get("region", "UnknownRegion")

        # Extract resource name if available
        resources = event.get("resources", [])
        resource_name = resources[0] if resources else "UnknownResource"

        # Extract username from detail.userIdentity if present
        username = (
            event.get("detail", {})
                 .get("userIdentity", {})
                 .get("userName", "UnknownUser")
        )

        # Prepare item for DynamoDB
        item = {
            "Event_id": context.aws_request_id,  # Unique ID for each Lambda invocation
            "EventTime": event_time,
            "EventSource": event_source,
            "EventName": event_name,
            "ResourceName": resource_name,
            "AWSRegion": aws_region,
            "Username": username
        }

        # Store in DynamoDB
        table.put_item(Item=item)

        print(f" Event stored successfully: {item}")
        return {
            "statusCode": 200,
            "body": json.dumps({"message": "Event stored successfully", "data": item})
        }

    except ClientError as e:
        print(f" DynamoDB error: {e}")
        return {
            "statusCode": 500,
            "body": json.dumps({"error": "DynamoDB operation failed", "details": str(e)})
        }
    except Exception as e:
        print(f" Unexpected error: {e}")
        return {
            "statusCode": 500,
            "body": json.dumps({"error": "Unexpected error", "details": str(e)})
        }
