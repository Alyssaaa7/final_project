import json
import boto3

def lambda_handler(event, context):
    
    userId = event['userId']
    showId = event['showId']
    showName = event['showName']
    
    sqs = boto3.client('sqs')
    
    
    def find_showid(showName):
        foundShowId = ""
        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table('showid_showname')
        response = table.scan(AttributesToGet=['showId','showName'])
        scanList = response['Items']

        for i in range(len(scanList)):
            if showName == scanList[i]['showName']:
                foundShowId = scanList[i]['showId']
                return foundShowId
        
        return ""
    
    def add_to_db_fresh(showId):
        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table('user_watchlist')
        response = table.put_item(
          Item={
                'userId': userId,
                'shows': [showId]
            }
        )
        sqs_message = {'userId':userId, 'showId':showId}
        message = sqs.send_message(
            QueueUrl='https://sqs.us-east-1.amazonaws.com/642748297386/watchlist_add_queue',
            MessageBody=json.dumps(sqs_message)
        )
            
        return response
        
    def add_to_db_append(showId):
        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table('user_watchlist')
        response = table.get_item(Key={"userId": userId})
        showlist = response['Item']['shows']
        if showId not in showlist:
            showlist.append(showId)
            response = table.put_item(
              Item={
                    'userId': userId,
                     'shows': showlist
                }
            )
            sqs_message = {'userId':userId, 'showId':showId}
            message = sqs.send_message(
                QueueUrl='https://sqs.us-east-1.amazonaws.com/642748297386/watchlist_add_queue',
                MessageBody=json.dumps(sqs_message)
            )
            
            return response
        else :
            pass
    
    def check_if_user_exists(userId):
        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table('user_watchlist')
        response = table.get_item(Key={"userId": userId})
        
        if 'Item' not in response:
            add_to_db_fresh(showId)
            print('1')
        else :
            add_to_db_append(showId)
            print('2')
    

    if showId !='' :
        pass
    elif showName !='' :
        showId = find_showid(showName)
        
    print('showId',showId)
    
    if showId!="" :
        check_if_user_exists(userId)
        
        return {
            'statusCode': 200,
            'body': json.dumps('Hello from Lambda!')
        }
        
    
    else:
        return {
                'statusCode': 200,
                'body': json.dumps('That show does not exist in the database!')
            }

        
        
    
