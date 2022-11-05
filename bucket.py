import json
import boto3
import logging
import time
from requests import createRequest, deleteRequest, updateRequest

logging.basicConfig(filename="actionlog.log", level=logging.INFO)
client = boto3.client("s3")
s3 = boto3.resource('s3')
bucket2Name = 'jread-bucket-2'
bucket2 = s3.Bucket(bucket2Name)


# Gets all the keys from a bucket
def getAllKeys(bucket):
  keys = []
  for object in bucket.objects.all():
    keys.append(object.key)

  return keys


# Gets the smallest key from a list of keys
def getSmallestKey(keys):
  desiredKey = "99999999999999999"
  for key in keys:
    if (key < desiredKey):
      desiredKey = key

  if (key == "99999999999999999"):
    logging.info('No objects in bucket 2')
    return None, None, None
  else:
    logging.info('Getting the object from bucket 2 with key: ' + desiredKey)
    keys.remove(desiredKey)
    return desiredKey, keys, ''.join([desiredKey, ".json"])


# Main function
def processRequest(storageDestination):
  if (storageDestination == "s3"):
    useS3 = True
  else:
    useS3 = False

  keys = getAllKeys(bucket2)

  for i in range(len(keys)):
    try:
      smallestKey, keys, fileName = getSmallestKey(keys)

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
        updateRequest()
      else:
        print("Invalid request type: ", widgetDictionaryObject['type'])
    
    except Exception as e:
      logging.info("Error caught in the try except block while processing the request in bucket 2")
      print("Error: ", e)
      continue

  print("Finished processing all requests in the bucket")
  logging.info("Finished processing all requests in the bucket")


