import boto3
import logging
import json
from requests import createRequest, deleteRequest, updateRequest


logging.basicConfig(filename="actionlog.log", level=logging.INFO)
sqs = boto3.client('sqs', region_name='us-east-1')
queue_url = "https://sqs.us-east-1.amazonaws.com/009923585255/cs5260-requests"


def getNumberOfMessages():
  response = sqs.get_queue_attributes(
    QueueUrl=queue_url,
    AttributeNames=['ApproximateNumberOfMessages']
  )
  numberOfWidgets = response['Attributes']['ApproximateNumberOfMessages']

  return numberOfWidgets


def deleteMessage(receipt_handle):
  logging.info("Deleting message from SQS")
  sqs.delete_message(
    QueueUrl = queue_url,
    ReceiptHandle = receipt_handle
  )


def getMessage():
  response = sqs.receive_message(
    QueueUrl = queue_url,
    AttributeNames = [
        'SentTimestamp'
    ],
    MaxNumberOfMessages = 1,
    MessageAttributeNames = [
        'All'
    ],
    VisibilityTimeout = 100,
    WaitTimeSeconds = 0
  )

  message = response['Messages'][0]
  receipt_handle = message['ReceiptHandle']
  deleteMessage(receipt_handle)
  
  return message


def processRequest(type):
  logging.info("Retrieving widgets from SQS")
  numberOfMessages = getNumberOfMessages()

  for i in range(int(numberOfMessages)):
    try: 
      message = getMessage()
      message = json.loads(message['Body'])
      
      # Process the request
      if (message['type'] == 'create'):
        print("Create request")
        createRequest(message, type)
      elif (message['type'] == 'delete'):
        print("Delete request")
        deleteRequest(type, message)
      elif (message['type'] == 'update'):
        print("Update request")
        updateRequest(type, message)
      else:
        print("Invalid request type: ", message['type'])
    except Exception as e:
      logging.info("Error caught in the try except block while processing the request in bucket 2")
      print("Error: ", e)
      continue

  print("Finished processing all requests in the sqs")
  logging.info("Finished processing all requests in the sqs")
