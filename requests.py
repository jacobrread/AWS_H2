import logging
import boto3

logging.basicConfig(filename="actionlog.log", level=logging.INFO)
s3 = boto3.resource('s3')
client = boto3.client("s3")
dynamo = boto3.resource('dynamodb', region_name='us-east-1')
bucket3Name = 'jread-bucket-3'

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


def deleteRequest(type, widgetId):
  logging.info('Began widget delete request')

  if (type == "dynamodb"):
    try:
      dynamo.delete_item(Key={'widgetId': widgetId})

    except Exception as e:
      logging.info("Error while deleting item from dynamodb table")
      print(e)
      raise

  elif (type == "s3"):
    try:
      s3.Object(bucket3Name, widgetId).delete()

    except Exception as e:
      logging.info("Error while deleting item from s3 bucket")
      print(e)
      raise

  else:
    logging.info("Unrecognized type passed to deleteRequest")
    print("Unrecognized type passed to deleteRequest")


def updateRequest(type, widgetId, updateDictionary):
  logging.info('Began widget update request')

  if (type == "dynamodb"):
    return 

    try:
      response = dynamo.update_item(
        Key={'widgetId': widgetId},
        UpdateExpression="set info.rating=:r, info.plot=:p",
        ExpressionAttributeValues={
            ':r': Decimal(str(rating)), ':p': plot},
        ReturnValues="UPDATED_NEW")

    except Exception as e:
      logging.info("Error while updating item in dynamodb table")
      print(e)
      raise

    else:
      logging.info("Successfully updated item in dynamodb table")
      return response['Attributes']

  elif (type == "s3"):
    try:
      # TODO: Change this to the actual update for s3
      s3.Object(bucket3Name, widgetId).put(Body=updateDictionary)

    except Exception as e:
      logging.info("Error while updating item in s3 bucket")
      print(e)
      raise

    else:
      logging.info("Successfully updated item in s3 bucket")
      return

  else:
    logging.info("Unrecognized type passed to updateRequest")
    print("Unrecognized type passed to updateRequest")

