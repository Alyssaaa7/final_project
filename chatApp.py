import boto3
import json
from datetime import datetime
from boto3.dynamodb.conditions import Attr
from botocore.exceptions import ClientError

def lambda_handler(event, context):
    body = json.loads(event['body'])
    action = body['action']
    connection_id = event['requestContext']['connectionId']
    api_gateway_endpoint = f"https://{event['requestContext']['domainName']}/{event['requestContext']['stage']}"

    if action == 'createForum':
        return create_forum(body)
    elif action == 'joinForum':
        return join_forum(body, connection_id, api_gateway_endpoint)
    elif action == 'sendMessage':
        return send_messages(body, connection_id, api_gateway_endpoint)  # Change this line
    elif action == 'leaveForum':
        return leave_forum(body)
    else:
        return {"statusCode": 400, "body": "Invalid action"}


def create_forum(body):
    forum_name = body['ForumName']

    dynamodb = boto3.resource('dynamodb')
    forums_table = dynamodb.Table('ChatAppForums')
    forums_table.put_item(Item={'ForumName': forum_name, 'Members': []})
    return {"statusCode": 200, "body": "Forum created successfully"}

def join_forum(body, connection_id, api_gateway_endpoint):
    forum_name = body['ForumName']
    user_name = body['UserName']

    dynamodb = boto3.resource('dynamodb')
    forums_table = dynamodb.Table('ChatAppForums')
    connections_table = dynamodb.Table('ChatAppConnections')

    forums_table.update_item(
        Key={'ForumName': forum_name},
        UpdateExpression='SET Members = list_append(Members, :user)',
        ExpressionAttributeValues={':user': [user_name]},
        ReturnValues='UPDATED_NEW'
    )

    connections_table.update_item(
        Key={'ConnectionId': connection_id},
        UpdateExpression='SET #usr = :user',
        ExpressionAttributeNames={'#usr': 'User'},
        ExpressionAttributeValues={':user': user_name}
    )

    broadcast_message(api_gateway_endpoint, f"{user_name} has joined the forum.", forum_name, connection_id)

    return {"statusCode": 200, "body": "Joined forum successfully"}


def send_messages(body, connection_id, api_gateway_endpoint):
    forum_name = body['ForumName']
    user_name = body['UserName']
    message = body['Message']

    dynamodb = boto3.resource('dynamodb')
    messages_table = dynamodb.Table('ChatAppMessages')
    timestamp = int(datetime.now().timestamp() * 1000)
    messages_table.put_item(
        Item={
            'ForumName': forum_name,
            'Timestamp': timestamp,
            'Sender': user_name,
            'Message': message
        }
    )

    broadcast_message(api_gateway_endpoint, f"{user_name}: {message}", forum_name, connection_id)

    return {"statusCode": 200, "body": "Message sent successfully"}

def leave_forum(body):
    forum_name = body['ForumName']
    user_name = body['UserName']

    dynamodb = boto3.resource('dynamodb')
    forums_table = dynamodb.Table('ChatAppForums')
    forums_table.update_item(
        Key={'ForumName': forum_name},
        UpdateExpression='REMOVE Members[ :index ]',
        ExpressionAttributeValues={':index': forum_data['Members'].index(user_name)},
        ReturnValues='UPDATED_NEW'
    )
    return {"statusCode": 200, "body": "Left forum successfully"}


def broadcast_message(api_gateway_endpoint, message, forum_name, connection_id):
    dynamodb = boto3.resource('dynamodb')
    forums_table = dynamodb.Table('ChatAppForums')
    connections_table = dynamodb.Table('ChatAppConnections')
    api_gateway_management_api = boto3.client('apigatewaymanagementapi', endpoint_url=api_gateway_endpoint)

    response = forums_table.get_item(Key={'ForumName': forum_name})
    if 'Item' in response:
        members = response['Item']['Members']

        for member in members:
            connection_response = connections_table.get_item(Key={'ConnectionId': member})  # Update this line
            if 'Item' in connection_response:
                member_connection_id = connection_response['Item']['ConnectionId']

                try:
                    api_gateway_management_api.post_to_connection(
                        ConnectionId=member_connection_id,
                        Data=message
                    )
                except ClientError as e:
                    if e.response['Error']['Code'] == 'GoneException':
                        connections_table.delete_item(Key={'ConnectionId': member})  # Update this line
                    else:
                        print(e)
    else:
        print("Forum not found")
