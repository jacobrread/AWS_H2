FROM python:3.10.6
WORKDIR /HW2
COPY consumer.py consumer.py
COPY bucket.py bucket.py
COPY sqs.py sqs.py
COPY requests.py requests.py
COPY /jsonFileName jsonFileName
RUN pip install boto3
CMD ["python3", "consumer.py", "dynamo"]