import time
import json
import sys
import logging
import requests
import boto3

class AWS:
  def __init__(self):
    self.s3 = boto3.resource('s3')
    self.client = boto3.client("s3")
    self.bucket2Name = 'jread-bucket-2'
    self.bucket3Name = 'jread-bucket-3'
    self.bucket2 = self.s3.Bucket(self.bucket2Name)
    self.dynamo = boto3.resource('dynamodb', region_name='us-east-1')

aws = AWS()
logging.basicConfig(filename="actionlog.log", level=logging.INFO)


def getAllKeys(bucket):
  keys = []
  for object in bucket.objects.all():
    keys.append(object.key)

  return keys


def getSmallestKey(keys):
  desiredKey = "99999999999999999"
  for key in keys:
    if (key < desiredKey):
      desiredKey = key

  if (key == "99999999999999999"):
    logging.info('No objects in bucket 2')
    return None, None
  else:
    logging.info('Getting the object with key: ' + desiredKey)
    keys.remove(desiredKey)
    return desiredKey, keys, ''.join([desiredKey, ".json"])


def getRequest(useS3):
  logging.info("Received correct command line arugments")

  # TODO: figure out how to get all the keys in the bucket and then stop the loop
  gotRequest = False
  while not gotRequest:
    keys = getAllKeys(aws.bucket2)
    smallestKey, keys, fileName = getSmallestKey(keys)
    print(keys.count)

    # Check if there is an object in bucket 2
    if (fileName == None):
      print("No objects in bucket 2")
      time.sleep(0.1)
      continue
    elif (keys.count == 0):
      gotRequest = True
    else:
      gotRequest = True

    fileLocation = "./jsonFileName/" + fileName
    
    # Download the object with the smallest key
    aws.client.download_file(aws.client, smallestKey, fileLocation) 
    aws.s3.Object(aws.bucket2Name, smallestKey).delete() # delete the file from bucket 2
    logging.info("Deleted object from bucket 2")

    # Convert json file to python dictoinary
    jsonFileReference = open(fileLocation)
    widgetDictionaryObject = json.load(jsonFileReference)

    # Process the request
    if (widgetDictionaryObject['type'] == 'create'):
      print("Create request")
      requests.createRequest(widgetDictionaryObject, aws, useS3)
    elif (widgetDictionaryObject['type'] == 'delete'):
      print("Delete request")
      requests.deleteRequest(aws)
    elif (widgetDictionaryObject['type'] == 'update'):
      print("Change request")
      requests.changeRequest(aws)
    else:
      print("Invalid request type: ", widgetDictionaryObject['type'])


def flattenDictionary(dictionary):
  logging.info('Flattening dictionary')

  flattenedDictionary = {}
  for key in dictionary:
    flattenedDictionary[key] = dictionary[key]

  # print("Key: ", key)
  # print("Value: ", dictionary[key][0])
  # if isinstance(dictionary[key][0], collections.abc.Sequence):
  #   flattenDictionary(dictionary[key])
  # else:
  #   flattenedDictionary[key] = dictionary[key]

  return flattenedDictionary


def main():
  if (len(sys.argv) == 1):
    print("Defaulting to s3")
    getRequest(True)
  elif (len(sys.argv) == 2):
    if (sys.argv[1] == "s3"):
      print("Using S3")
      getRequest(True)
    elif (sys.argv[1] == "dynamodb"):
      print("Using DynamoDB")
      getRequest(False)
    else:
      print("To store using S3, use the command: python3 consumer.py s3")
      print("To store using DynamoDB, use the command: python3 consumer.py dynamodb")
      sys.exit()
  else:
    print("To store using S3, use the command: python3 consumer.py s3")
    print("To store using DynamoDB, use the command: python3 consumer.py dynamodb")
    sys.exit()

main()
