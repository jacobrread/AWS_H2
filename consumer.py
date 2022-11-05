import sys
import logging
from utils import *
import bucket
import sqs

logging.basicConfig(filename="actionlog.log", level=logging.INFO)


def getRequest(storageDestination, retrievalLocation):
  logging.info("Received correct command line arguments")

  if (retrievalLocation == None):
    logging.info("Retrieving widgets from Bucket")
    print("Retrieving widgets from Bucket")
    bucket.processRequest(storageDestination)
  else:
    logging.info("Retrieving widgets from SQS")
    print("Retrieving widgets from SQS")
    sqs.processRequest(storageDestination)
      
  return


def commandLineHelp():
  print("To store using S3, use 's3' as your third argument")
  print("To store using DynamoDB, use 'dynamodb' as your third argument")
  print("To retrieve using SQS, use 'sqs' as your fourth argument")
  print("To retrieve using bucket, use 'bucket' as your fourth argument")
  sys.exit()


def main():
  if (len(sys.argv) == 2):
    if (sys.argv[1] == "s3" or sys.argv[1] == "dynamo"):
      getRequest(sys.argv[1], None)
    else:
      commandLineHelp()
  elif (len(sys.argv) == 3):
    if (sys.argv[1] == "s3" or sys.argv[1] == "dynamo"):
      getRequest(sys.argv[1], sys.argv[2])
    else:
      commandLineHelp()
  else:
    commandLineHelp()


if __name__ == "__main__":
  main()