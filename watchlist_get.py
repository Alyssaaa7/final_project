import json
import boto3

def lambda_handler(event, context):
    
    userId = event['userId']
    
    print(userId)
    
    showNameList = []
    
    def get_show_name(id):
        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table('showid_showname')
        
        response = table.get_item(Key={"showId": id})
        print(response)
        return response['Item']['showName']
    
    def find_shows_of_user(userId):
        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table('user_watchlist')
        response = table.get_item(Key={"userId": userId})
        
        showIdlist = response['Item']['shows']
        

        for id in showIdlist:
            showNameList.append(get_show_name(id))
        
        return showIdlist

    find_shows_of_user(userId)

    return {
        'statusCode': 200,
        'body': json.dumps(showNameList)
    }
