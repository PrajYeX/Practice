import json
import boto3
from botocore.exceptions import ClientError


TABLE_NAME = "Emp_Master"

dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(TABLE_NAME)


def lambda_handler(event, context):
    

   
    http_info = event.get("requestContext", {}).get("http", {})

    http_method = http_info.get("method")
    path = http_info.get("path")

    
    print("HTTP Method:", http_method)
    print("Path:", path)

    try:
        if http_method == "POST" and path.endswith("/employee"):
            return create_employee(event)

        if http_method == "GET" and path.endswith("/employee"):
            return get_employee(event)

        return response(
            400,
            {"message": "Unsupported method or path"}
        )

    except Exception as exc:
        print("Unhandled error:", str(exc))
        return response(
            500,
            {"message": "Internal server error"}
        )


def create_employee(event):
    body = json.loads(event.get("body", "{}"))

    required_fields = [
        "Emp_Id",
        "First_Name",
        "Last_Name",
        "Date_Of_Joining"
    ]

    for field in required_fields:
        if field not in body:
            return response(
                400,
                {"message": f"Missing field: {field}"}
            )

    item = {
        "Emp_Id": body["Emp_Id"],
        "First_Name": body["First_Name"],
        "Last_Name": body["Last_Name"],
        "Date_Of_Joining": body["Date_Of_Joining"]
    }

    table.put_item(Item=item)

    
    print("Inserted employee record:", item)

    return response(
        201,
        {"message": "Employee created successfully"}
    )


def get_employee(event):
    params = event.get("queryStringParameters") or {}
    emp_id = params.get("emp_id")

    if not emp_id:
        return response(
            400,
            {"message": "emp_id query parameter is required"}
        )

    try:
        result = table.get_item(
            Key={"Emp_Id": emp_id}
        )
    except ClientError as error:
        print("DynamoDB error:", error)
        return response(
            500,
            {"message": "Error retrieving employee"}
        )

    item = result.get("Item")

    if not item:
        return response(
            404,
            {"message": "Employee not Found"}
        )

   
    print("Retrieved employee record:", item)

    return response(200, item)


def response(status_code, body):
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json"
        },
        "body": json.dumps(body)
    }
