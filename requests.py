import logging
import consumer

logging.basicConfig(filename="actionlog.log", level=logging.INFO)

def createRequest(widgetDictionaryObject, aws, useS3):
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
      aws.s3.Object(aws.bucket3Name, newNameFormat).put()
      logging.info('Put widget in S3')
    else:
      table = aws.dynamo.Table('dynamo_table')
      flattenedDictionary = consumer.flattenDictionary(widgetDictionaryObject)
      table.put_item(Item=flattenedDictionary)
      print("I put the item in the table")
      logging.info('Put widget in DynamoDB table')

  except:
    logging.info('Error in createRequest caught by the try except block')
    print("There was an error creating the request")

def deleteRequest(aws):
  logging.info('Began widget delete request')
  print("Write the code for deleting requests")

def changeRequest(aws):
  logging.info('Began widget change request')
  print("Write the code for changing requests")

