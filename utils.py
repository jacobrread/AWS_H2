import logging

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
