import json
import boto3

dynamodb = boto3.resource('dynamodb')
table_name = 'user_info'

def lambda_handler(event, context):
    # Get the user information from the event
    user_email = event['request']['userAttributes']['email']
    user_id = event['userName']
    user_name = event['request']['userAttributes']['name']
    user_info = {
        'user_id': user_id,
        'email': user_email,
        'name': user_name
    }
    
    # Store the user information in DynamoDB
    table = dynamodb.Table(table_name)
    table.put_item(Item=user_info)
    
    # Return a success response
    response = {
        'version': '1.0',
        'response': {
            'autoConfirmUser': True,
            'autoVerifyEmail': True,
            'autoVerifyPhone': False
        }
    }
    
    return response
