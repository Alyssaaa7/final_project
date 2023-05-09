import json
import os
import boto3
import sklearn
import io
import pickle
import csv
from datetime import datetime
from sklearn.feature_extraction.text import CountVectorizer

def lambda_handler(event, context):

    print('## ENVIRONMENT VARIABLES')
    print(os.environ['AWS_LAMBDA_LOG_GROUP_NAME'])
    print(os.environ['AWS_LAMBDA_LOG_STREAM_NAME'])
    print('## EVENT')
    print(event)

def lambda_handler(event, context):
    print('ok')

    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('reviews')

    method = event['httpMethod']
    response = {}

    if method == "POST":

        userId = event['queryStringParameters']['userid']
        showId = event['queryStringParameters']['showid']
        reviewText = event['body']

        now = datetime.now()
        
        # SPOILER DETECTION
        
        vectorizer = pickle.load(open('vectorizer','rb'))
        
        # Convert the review text to a bag-of-words format
        review_bow = vectorizer.transform([reviewText]).toarray()
    
        # Convert the bag-of-words format to a CSV string
        csv_data = io.StringIO()
        csv_writer = csv.writer(csv_data)
        csv_writer.writerow(review_bow[0][:30520])
        payload = csv_data.getvalue().strip().encode('utf-8')
        
        runtime = boto3.Session().client('sagemaker-runtime', verify=False)
        response = runtime.invoke_endpoint(EndpointName='xgboost-2023-05-09-01-37-41-964',
                                          ContentType='text/csv',
                                          Body=payload)
        
        result = float(response['Body'].read().decode('utf-8').strip())
        print(result)        
        
        if result > 0.3:
            response = {
                'statusCode': 400,
                'headers': {
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Headers': '*',
                    'Access-Control-Allow-Methods': '*'
                },
                "body": json.dumps('Unsuccessful, review is a spoiler.')
            }
            return response
        
        #END
        

        last_review_id = -1
        response = table.scan(AttributesToGet=['review_id'])
        scanList = response['Items']

        for i in range(len(scanList)):
            if last_review_id <= int(scanList[i]['review_id']):
                last_review_id = int(scanList[i]['review_id'])

        new_review_id = last_review_id + 1

        response = table.put_item(
          Item={
                'review_id': str(new_review_id),
                'reviewText': reviewText,
                'showId': showId,
                'userId': userId,
                'timestamp': now.strftime("%m/%d/%Y, %H:%M:%S")
            }
        )

        response = {
            'statusCode': 200,
            'headers': {
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Headers': '*',
                'Access-Control-Allow-Methods': '*'
            },
            "body": json.dumps('Review recorded')
        }

    elif method == "GET":

        showId = event['queryStringParameters']['showid']

        response = table.scan(AttributesToGet=['showId', 'userId', 'reviewText', 'timestamp'])
        scanList = response['Items']
        show_review_list = []

        for i in range(len(scanList)):
            if showId == scanList[i]['showId']:
                show_review_list.append(scanList[i])

        response = {
            'statusCode': 200,
            'headers': {
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Headers': '*',
                'Access-Control-Allow-Methods': '*'
            },
            "body": json.dumps(show_review_list)
        }

    return response
