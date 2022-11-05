import boto3
import logging

logging.basicConfig(filename="actionlog.log", level=logging.INFO)
sqs = boto3.client('sqs', region_name='us-east-1')
queue_url = "https://sqs.us-east-1.amazonaws.com/009923585255/cs5260-requests"


class Message:
  def __init__(self, message, receipt_handle):
    self.message = message
    self.receipt_handle = receipt_handle


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
    VisibilityTimeout = 0,
    WaitTimeSeconds = 0
  )

  return response['Messages']


def getNumberOfMessages():
  response = sqs.get_queue_attributes(
    QueueUrl=queue_url,
    AttributeNames=['ApproximateNumberOfMessages']
  )
  numberOfWidgets = response['Attributes']['ApproximateNumberOfMessages']

  # print("Number of widgets: " + str(numberOfWidgets))
  return numberOfWidgets


def deleteMessage(client, message, queue_url):
  client.delete_message(
    QueueUrl = queue_url,
    ReceiptHandle = message['ReceiptHandle']
  )


def processRequest(storageDestination):
  if (storageDestination == "s3"):
    useS3 = True
  else:
    useS3 = False

  numberOfMessages = getNumberOfMessages()

  for i in range(numberOfMessages):
    message = getMessage()
    print(message)
      
 