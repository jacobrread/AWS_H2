# Requirements for running consumer.py
1. Must have python3 installed
2. Must have credentials updated (likely stored in the .aws folder on your machine)
3. Must have Widget Requests in the bucket 2

# Command line arguments
Besides the obvious first two arguments (python3 consumer.py) there is an additional parameter
needed by the application to determine where to save the widget requests. There are two options
for this argument:
* s3
* dynamodb

By default the application assumes s3, but the user should specify for consistency and to ensure
the correct storage location is being selected.

An example of a complete command line is as follows:
'python3 consumer.py dynamodb'

If an incorrect argument or an incorrect number of arguments is given then a help dialog will
print to the console and the application will close.

# Log file
For more precise information on what actions were taken while the application was running see the
actionlog.log file. 
