from consumer import *
import unittest

s3 = boto3.resource('s3')
client = boto3.client("s3")
bucket2Name = 'jread-bucket-2'
bucket3Name = 'jread-bucket-3'
bucket2 = s3.Bucket(bucket2Name)
bucket3 = s3.Bucket(bucket3Name)
dynamo = boto3.resource('dynamodb', region_name='us-east-1')

class TestConsumer(unittest.TestCase):

    def test_getSmallestKey(self):
        keys = ["1612306368338", "1612306368339", "1612306368340"]
        smallestKey, keyList, filename = getSmallestKey(keys)
        self.assertEqual(smallestKey, "1612306368338")

    def test_getAllKeys(self):
        client.upload_file("sample-requests/1612306368338", bucket2Name, "1612306368338")
        client.upload_file("sample-requests/1612306369227", bucket2Name, "1612306369227")
        client.upload_file("sample-requests/1612306369451", bucket2Name, "1612306369451")
        keys = getAllKeys(bucket2)

        for key in keys:
            s3.Object(bucket2Name, key).delete()

        first = keys[0] == "1612306368338"
        second = keys[1] == "1612306369227"
        third = keys[2] == "1612306369451"
        size = len(keys) == 3
        self.assertTrue(first and second and third and size)

    def test_getRequest(self):
        bucket2.objects.all().delete()
        bucket3.objects.filter(Prefix="widgets/").delete()

        client.upload_file("sample-requests/1612306368338", bucket2Name, "1612306368338")
        getRequest(bucket2)
        object = getAllKeys(bucket3)

        if object == None:
            self.fail("No objects in bucket 2")
        
        self.assertEqual(object[0], "widgets/mary-matthews/8123f304-f23f-440b-a6d3-80e979fa4cd6")
    
    def test_createRequest(self):
        bucket2.objects.all().delete()
        bucket3.objects.filter(Prefix="widgets/").delete()
        dictionary = {
            "owner": "Mary Matthews",
            "widgetId": "8123f304-f23f-440b-a6d3-80e979fa4cd6",
        }
        createRequest(dictionary, True)
        object = getAllKeys(bucket3)

        if object == None:
            self.fail("No objects in bucket 3")

        self.assertEqual(object[0], "widgets/mary-matthews/8123f304-f23f-440b-a6d3-80e979fa4cd6")

    # def test_deleteRequest(self):
    #     deleteRequest()
    #     self.assertTrue(True)

    # def test_changeRequest(self):
    #     changeRequest()
    #     self.assertTrue(True)

if __name__ == '__main__':
    unittest.main()

# python3 tests.py