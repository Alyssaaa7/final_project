import json
import boto3

dynamodb = boto3.resource('dynamodb')
table_name = 'user_info'

def lambda_handler(event, context):
    # Get all items from the DynamoDB table
    table = dynamodb.Table(table_name)
    response = table.scan()
    items = response['Items']
    
    # Return the items as a JSON object with CORS headers
    return {
        'statusCode': 200,
        'headers': {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Headers': '*',
            'Access-Control-Allow-Methods': '*'
        },
        'body': json.dumps(items)
    }
