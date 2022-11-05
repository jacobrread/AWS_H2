import logging
import boto3


logging.basicConfig(filename="actionlog.log", level=logging.INFO)
s3 = boto3.resource('s3')
client = boto3.client("s3")
dynamo = boto3.resource('dynamodb', region_name='us-east-1')
table = dynamo.Table('dynamo_table')
bucket3Name = 'jread-bucket-3'

def flattenDictionary(dictionary):
  logging.info('Flattening dictionary')

  flattenedDictionary = {}
  for key in dictionary:
    flattenedDictionary[key] = dictionary[key]

  return flattenedDictionary


def createRequest(widgetDictionaryObject, type):
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

    if (type == "s3"):
      # widgets/{owner}/{widget id}
      newNameFormat = "widgets/" + widgetDictionaryObject['owner'].replace(" ", "-").lower() + "/" + widgetDictionaryObject['widgetId']
      s3.Object(bucket3Name, newNameFormat).put(Body=str(widgetDictionaryObject))
      logging.info('Put widget in S3')
    else:
      table = dynamo.Table('dynamo_table')
      flattenedDictionary = flattenDictionary(widgetDictionaryObject)
      table.put_item(Item=flattenedDictionary)
      logging.info('Put widget in DynamoDB table')

  except Exception as e:
    logging.info("Error while creating item: ", e)
    print(e)
    raise

def deleteRequest(type, widgetDictionaryObject):
  logging.info('Began widget delete request')

  # Check json for all required fields
  if (widgetDictionaryObject['widgetId'] == ""):
    print("widgetID field is empty")
    logging.info('widgetID field in the json is empty')
    return

  if (type == "dynamo"):
    try:
      table.delete_item(Key={'widgetId': widgetDictionaryObject['widgetId']})

    except Exception as e:
      logging.info("Error while deleting item from dynamodb table: ", e)
      print(e)
      raise

  elif (type == "s3"):
    try:
      filePath = "widgets/" + widgetDictionaryObject['owner'].replace(" ", "-").lower() + "/" + widgetDictionaryObject['widgetId']
      s3.Object(bucket3Name, filePath).delete()

    except Exception as e:
      logging.info("Error while deleting item from s3 bucket: ", e)
      print(e)
      raise

  else:
    logging.info("Unrecognized type passed to deleteRequest")
    print("Unrecognized type passed to deleteRequest")


def getUpdateParams(dictionary):
  update_expression = ["set "]
  update_values = dict()

  for key, val in dictionary.items():
    update_expression.append(f" {key} = :{key},")
    update_values[f":{key}"] = val

  return "".join(update_expression)[:-1], update_values


def updateRequest(type, widgetDictionaryObject):
  logging.info('Began widget update request')

  if (type == "dynamo"):
    try:
      widgetID = widgetDictionaryObject['widgetId']
      del widgetDictionaryObject['type']
      del widgetDictionaryObject['owner']
      del widgetDictionaryObject['widgetId']
      expression, values = getUpdateParams(widgetDictionaryObject)

      response = table.update_item(
        Key={'widgetId': widgetID},
        UpdateExpression = expression,
        ExpressionAttributeValues = values
      )
      
    except Exception as e:
      logging.info("Error while updating item in dynamodb table: ", e)
      print(e)
      raise

    else:
      logging.info("Successfully updated item in dynamodb table")
      return response

  elif (type == "s3"):
    try:
      createRequest(widgetDictionaryObject, True)

    except Exception as e:
      logging.info("Error while updating item in s3 bucket: ", e)
      print(e)
      raise

    else:
      logging.info("Successfully updated item in s3 bucket")
      return

  else:
    logging.info("Unrecognized type passed to updateRequest")
    print("Unrecognized type passed to updateRequest")

