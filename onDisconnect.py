import boto3

def lambda_handler(event, context):
    connection_id = event['requestContext']['connectionId']
    dynamodb = boto3.resource('dynamodb')
    connections_table = dynamodb.Table('ChatAppConnections')
    connection_data = connections_table.get_item(Key={'ConnectionId': connection_id}).get('Item')

    if connection_data and connection_data.get('User'):
        user_name = connection_data.get('User')
        forums_table = dynamodb.Table('ChatAppForums')
        scan = forums_table.scan(FilterExpression=Attr('Members').contains(user_name))
        for forum in scan['Items']:
            forums_table.update_item(
                Key={'ForumName': forum['ForumName']},
                UpdateExpression='DELETE Members :user',
                ExpressionAttributeValues={':user': {user_name}},
                ReturnValues='UPDATED_NEW'
            )

    connections_table.delete_item(Key={'ConnectionId': connection_id})
    return {"statusCode": 200}
