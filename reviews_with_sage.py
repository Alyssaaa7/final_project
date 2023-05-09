import json
import os
import boto3
from datetime import datetime
from sklearn.feature_extraction.text import CountVectorizer
import io

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
        
        vectorizer = CountVectorizer()
        
        review_bow = vectorizer.transform([reviewText]).toarray()
        
        # Convert the bag-of-words format to a CSV string
        csv_data = io.StringIO()
        csv_writer = csv.writer(csv_data)
        csv_writer.writerow(review_bow[0])
        payload = csv_data.getvalue().strip().encode('utf-8')
        
        # Call the SageMaker endpoint
        runtime = boto3.Session().client('sagemaker-runtime', verify=False)
        response = runtime.invoke_endpoint(EndpointName='xgboost-2023-05-09-01-37-41-964', ContentType='text/csv', Body=payload)
        
        # Parse the response
        print(response)
        result = json.loads(response['Body'].read().decode())
        prediction = result['predicted_label']
        
        print("prediction:")
        print(json.dumps({'prediction': prediction}))
        
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
    

        
