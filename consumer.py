import boto3
import time
import json
import sys
import logging
from utils import *

s3 = boto3.resource('s3')
client = boto3.client("s3")
sqs = boto3.client('sqs', region_name='us-east-1')
dynamo = boto3.resource('dynamodb', region_name='us-east-1')
bucket2Name = 'jread-bucket-2'
bucket3Name = 'jread-bucket-3'
bucket2 = s3.Bucket(bucket2Name)
queue_url = "https://sqs.us-east-1.amazonaws.com/009923585255/cs5260-requests"

logging.basicConfig(filename="actionlog.log", level=logging.INFO)

class Message:
  def __init__(self, message, receipt_handle):
    self.message = message
    self.receipt_handle = receipt_handle


def storage(useS3):
  if (useS3):
    return "s3"
  else:
    return "dynamo"


def retrieval(param):
  if (param == "sqs"):
    response = sqs.receive_message(
      QueueUrl = queue_url,
      AttributeNames = [
          'SentTimestamp'
      ],
      MaxNumberOfMessages = 10,
      MessageAttributeNames = [
          'All'
      ],
      VisibilityTimeout = 0,
      WaitTimeSeconds = 0
    )

    # print("Response length: ", len(response))
    # print()
    # print(response)
    # print()

    message = response['Messages'][0]
    receipt_handle = message['ReceiptHandle']

    # Delete received message from queue
    sqs.delete_message(
        QueueUrl=queue_url,
        ReceiptHandle=receipt_handle
    )

    messageBody = json.loads(message['Body'])

    return Message(messageBody, receipt_handle)
  
  else:
    keys = getAllKeys(bucket2)

    return keys


def getRequest(storageDestination, retrievalLocation):
  logging.info("Received correct command line arguments")

  widgets = retrieval(retrievalLocation)
  return

  if (storageDestination == "s3"):
    useS3 = True
  else:
    useS3 = False

  for i in range(len(widgets)):
    try:
      smallestKey, keys, fileName = getSmallestKey(widgets)

      # Check if there is an object in bucket 2
      if (fileName == None):
        print("No objects in bucket 2")
        time.sleep(0.1)
        continue

      fileLocation = "./jsonFileName/" + fileName
      
      # Download the object with the smallest key
      client.download_file(bucket2Name, smallestKey, fileLocation) 
      s3.Object(bucket2Name, smallestKey).delete() # delete the file from bucket 2
      logging.info("Deleted object from bucket 2")

      # Convert json file to python dictionary
      jsonFileReference = open(fileLocation)
      widgetDictionaryObject = json.load(jsonFileReference)

      # Process the request
      if (widgetDictionaryObject['type'] == 'create'):
        print("Create request")
        createRequest(widgetDictionaryObject, useS3)
      elif (widgetDictionaryObject['type'] == 'delete'):
        print("Delete request")
        deleteRequest()
      elif (widgetDictionaryObject['type'] == 'update'):
        print("Change request")
        changeRequest()
      else:
        print("Invalid request type: ", widgetDictionaryObject['type'])
    
    except Exception as e:
      logging.info("Error caught in the try except block")
      print("Error: ", e)
      continue

  print("Finished processing all requests in the bucket")
  logging.info("Finished processing all requests in the bucket")


###################################
# Widget request functions
###################################

def createRequest(widgetDictionaryObject, useS3):
  logging.info('Began widget creation request')

  try:
    # Check json for all required fields
    if (widgetDictionaryObject['owner'] == ""):
      print("Owner field is empty")
      logging.info('Owner field in the json is empty')
      return

    if (widgetDictionaryObject['widgetId'] == ""):
      print("widgetID field is empty")
      logging.info('widgetID field in the json is empty')
      return

    if (useS3):
      # widgets/{owner}/{widget id}
      newNameFormat = "widgets/" + widgetDictionaryObject['owner'].replace(" ", "-").lower() + "/" + widgetDictionaryObject['widgetId']
      print("New name format: " + newNameFormat)
      s3.Object(bucket3Name, newNameFormat).put()
      logging.info('Put widget in S3')
    else:
      table = dynamo.Table('dynamo_table')
      flattenedDictionary = flattenDictionary(widgetDictionaryObject)
      table.put_item(Item=flattenedDictionary)
      print("I put the item in the table")
      logging.info('Put widget in DynamoDB table')

  except:
    logging.info('Error in createRequest caught by the try except block')
    print("There was an error creating the request")


def deleteRequest():
  logging.info('Began widget delete request')
  print("Write the code for deleting requests")


def changeRequest():
  logging.info('Began widget change request')
  print("Write the code for changing requests")


def main():
  if (len(sys.argv) == 3):
    if (sys.argv[1] == "s3" or sys.argv[1] == "dynamodb"):
      if (sys.argv[2] == "sqs" or sys.argv[2] == "bucket"):
        getRequest(sys.argv[1], sys.argv[2])
      else:
        print("Invalid storage command line argument")
        logging.info('Invalid storage command line argument')
    else:
      print("Invalid retrieval command line argument")
      logging.info('Invalid retrieval command line argument')
  else:
    print("To store using S3, use 's3' as your third argument")
    print("To store using DynamoDB, use 'dynamodb' as your third argument")
    print("To retrieve using SQS, use 'sqs' as your fourth argument")
    print("To retrieve using bucket, use 'bucket' as your fourth argument")
    sys.exit()


if __name__ == "__main__":
  main()