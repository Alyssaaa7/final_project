import json
import boto3

def lambda_handler(event, context):
    
    
    method = event['httpMethod']
    
    if method=="GET":
        
        userId = event['queryStringParameters']['userId']
        
        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table('friends')
        response = table.get_item(Key={"userId": userId})
        friendList = response['Item']['friends']
        
        return {
            'statusCode': 200,
            'body': json.dumps(friendList)
        }
        
    if method=="POST":
        
        userId = event['queryStringParameters']['userId']
        friendId = event['queryStringParameters']['friendId']
    
        def add_to_db_fresh():
            dynamodb = boto3.resource('dynamodb')
            table = dynamodb.Table('friends')
            response = table.put_item(
              Item={
                    'userId': userId,
                    'friends': [friendId]
                }
            )
    
        def add_to_db_append():
            dynamodb = boto3.resource('dynamodb')
            table = dynamodb.Table('friends')
            response = table.get_item(Key={"userId": userId})
            friendList = response['Item']['friends']
            if friendId not in friendList:
                friendList.append(friendId)
                response = table.put_item(
                  Item={
                        'userId': userId,
                        'friends': friendList
                    }
                )
            else :
                pass
        
        def check_if_user_exists(userId):
            dynamodb = boto3.resource('dynamodb')
            table = dynamodb.Table('friends')
            response = table.get_item(Key={"userId": userId})
            
            if 'Item' not in response:
                add_to_db_fresh()
                print('1')
            else :
                add_to_db_append()
                print('2')
        
    
        check_if_user_exists(userId)
    
    return {
            'statusCode': 200,
            'body': json.dumps('Friend added')
    }

        
        
    
