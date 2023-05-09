import boto3

def lambda_handler(event, context):
    connection_id = event['requestContext']['connectionId']
    dynamodb = boto3.resource('dynamodb')
    connections_table = dynamodb.Table('ChatAppConnections')
    connections_table.put_item(Item={'ConnectionId': connection_id})
    return {"statusCode": 200}
