import json
import boto3

def lambda_handler(event, context):
    sqs = boto3.client('sqs', region_name='us-east-1')
    queue_url = "https://sqs.us-east-1.amazonaws.com/009923585255/cs5260-requests"
    
    response = sqs.send_message(
    QueueUrl=queue_url,
    MessageBody=json.dumps(event),
    )

    return {
        'statusCode': 200,
        'body': response
    }

# invoke url for api
# https://tqqj1wobug.execute-api.us-east-1.amazonaws.com/prod
