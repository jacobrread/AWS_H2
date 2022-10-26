import boto3
import time
import json
import sys
import logging

s3 = boto3.resource('s3')
client = boto3.client("s3")
bucket2Name = 'jread-bucket-2'
bucket3Name = 'jread-bucket-3'
bucket2 = s3.Bucket(bucket2Name)
dynamo = boto3.resource('dynamodb', region_name='us-east-1')
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
    logging.info('Getting the object from bucket 2 with key: ' + desiredKey)
    keys.remove(desiredKey)
    return desiredKey, keys, ''.join([desiredKey, ".json"])


def getRequest(useS3):
  logging.info("Received correct command line arugments")
  gotRequest = False
  while not gotRequest:
    keys = getAllKeys(bucket2)
    # TODO: figure out how to get all the keys in the bucket and then stop the loop
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
    client.download_file(bucket2Name, smallestKey, fileLocation) 
    s3.Object(bucket2Name, smallestKey).delete() # delete the file from bucket 2
    logging.info("Deleted object from bucket 2")

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
