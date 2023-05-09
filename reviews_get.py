import json
import os
import boto3
from datetime import datetime

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
    
    if method=="POST":
        
        userId = event['queryStringParameters']['userid']
        showId = event['queryStringParameters']['showid']
        reviewText = event['body']
        
        now = datetime.now()
    
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
    
    elif method=="GET":
        
        showId = event['queryStringParameters']['showid']

        
        response = table.scan(AttributesToGet=['showId','userId','reviewText','timestamp'])
        scanList = response['Items']
        show_review_list=[]

        for i in range(len(scanList)):
            if showId == scanList[i]['showId']:
                show_review_list.append(scanList[i])
        return {
                    'statusCode': 200,
                    "body": json.dumps(show_review_list)
                }

    
    return {
       'statusCode': 200,
       "body": json.dumps('Review recorded')
    }
    

        