from collections.abc import MutableMapping
import collections.abc
import boto3
import time
import json
import os
import sys

s3 = boto3.resource('s3')
client = boto3.client("s3")
bucket2Name = 'jread-bucket-2'
bucket3Name = 'jread-bucket-3'
bucket2 = s3.Bucket(bucket2Name)
dynamo = boto3.resource('dynamodb', region_name='us-east-1')


def getSmallestKey(bucket):
  # Get the smallest key in bucket 2
  desiredKey = "99999999999999999"
  for object in bucket.objects.all():
      if (object.key < desiredKey):
          desiredKey = object.key

  if (desiredKey == "99999999999999999"):
      return None, None
  else:
    return desiredKey, ''.join([desiredKey, ".json"])


def getRequest(useS3):
  gotRequest = False
  while not gotRequest:
    smallestKey, fileName = getSmallestKey(bucket2)

    # Check if there is an object in bucket 2
    if (fileName == None):
      print("No objects in bucket 2")
      time.sleep(0.1)
      continue
    else:
      gotRequest = True

    fileLocation = "./jsonFileName/" + fileName
    
    # Download the object with the smallest key
    client.download_file(bucket2Name, smallestKey, fileLocation) 
    s3.Object(bucket2Name, smallestKey).delete() # delete the file from bucket 2

    # Convert json file to python dictoinary
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


def flattenDictionary(dictionary):
  flattenedDictionary = {}
  print("Dictionary: ", dictionary['otherAttributes'])
  print()
  print("Other Attributes: ", dictionary['otherAttributes'])
  print()

  for key in dictionary:
    print("Key: ", key)
    print("Value: ", dictionary[key][0])
    if isinstance(dictionary[key][0], collections.abc.Sequence):
      flattenDictionary(dictionary[key])
    else:
      flattenedDictionary[key] = dictionary[key]

  return flattenedDictionary
    # items = []
    # for key, value in dictionary.items(): 
    #   new_key = key
    #   if isinstance(value, MutableMapping):
    #     items.extend(flattenDictionary(value).items())
    #   else:
    #     items.append((new_key, value))
    # return dict(items)


def createRequest(widgetDictionaryObject, useS3):
  # try:
    # Check json for all required fields
    if (widgetDictionaryObject['owner'] == ""):
      print("Owner field is empty")
      return

    if (widgetDictionaryObject['widgetId'] == ""):
      print("widgetID field is empty")
      return

    if (useS3): # Store in S3
      # widgets/{owner}/{widget id}
      newNameFormat = "widgets/" + widgetDictionaryObject['owner'].replace(" ", "-").lower() + "/" + widgetDictionaryObject['widgetId']
      print("New name format: " + newNameFormat)
      s3.Object(bucket3Name, newNameFormat).put()
    else: # Store in dynamodb
      table = dynamo.Table('dynamodb_table')

      print("Fix the code to flatten the dictionary")
      # flattenedDictionary = flattenDictionary(widgetDictionaryObject)
      # print("I flattened the dictionary: \n", str(flattenedDictionary))
      # print()
      # print("I am about to put the item in the table")
      # table.put_item(Item=flattenedDictionary)

  # except:
  #   print("There was an error creating the request")


def deleteRequest():
  print("Write the code fore deleting requests")


def changeRequest():
  print("Write the code fore changing requests")


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
