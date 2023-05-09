import json
import os
import requests
import boto3
from botocore.exceptions import ClientError


REGION = 'us-east-1'
HOST = 'search-myshows-zmfz4todbf2x2pu6a5oaajngpi.us-east-1.es.amazonaws.com'
INDEX = 'tvshows'
API_KEY= '1c61bfaa656d0222aec46816f8e9eccc'


def lambda_handler(event, context):
    # TODO implement
    print('Received event: ' + json.dumps(event))
    actorName = event.get('queryStringParameters', event).get('actor', None)
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('showid_actor')
    response = table.query(
        IndexName='actor-index',
        KeyConditionExpression=boto3.dynamodb.conditions.Key('actor').eq(actorName))
    print("response")
    print(response)
    result = response['Items']
    result_from_db = []
    
    if (len(result) != 0):
        for each_show in result:
 
            movie_id = each_show['showId']
            result_from_db.append(lookup_data({"MovieID": "{}".format(movie_id)}))
    else:
        result = call_api_search_movies(actorName)
        print(result)
        for movie in result:
            
            data = store_movie_dynamo(movie)
            store_showId_actorName(movie, actorName)
            result_from_db.append(data)
   

    return {
        'statusCode': 200,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': '*',
        },
        'body': json.dumps({'results': result_from_db}, default=str)
    }    
def get_movie_credits(movie_id):
    url = f'https://api.themoviedb.org/3/movie/{movie_id}/credits?api_key={API_KEY}'
    response = requests.get(url)
    data = response.json()
    return data
    
def get_movie_details(movie_id):
    url = f'https://api.themoviedb.org/3/movie/{movie_id}?api_key={API_KEY}'
    response = requests.get(url)
    data = response.json()
    return data

def get_movie_keywords(movie_id):
    url = f'https://api.themoviedb.org/3/movie/{movie_id}/keywords?api_key={API_KEY}'
    response = requests.get(url)
    data = response.json()
    return data
    
def store_movie_dynamo(movie):
    details = get_movie_details(movie['id'])
    credits = get_movie_credits(movie['id'])
    keywords = get_movie_keywords(movie['id'])


    # Extract director and actors
    director = ''
    actors = []
    for person in credits['crew']:
        if person['job'] == 'Director':
            director = person['name']
            break

    for actor in credits['cast'][:5]:
        actors.append(actor['name'])

        
    movie_data = {
        'MovieID': str(movie['id']),
        'MovieTitle': movie['original_title'],
        'ReleaseYear': movie['release_date'],
        'Genres': [genre['name'] for genre in details['genres']],
        'Director': director,
        'Actors': actors,
        'Language': details['spoken_languages'][0]['english_name'],
        'Overview': details['overview'],
        'Keywords': [keyword['name'] for keyword in keywords['keywords']]
    }

    db = boto3.resource('dynamodb')
    table = db.Table('show_data')
    
    table.put_item(Item=movie_data)
    return movie_data
    
 
def store_showId_actorName(movie, actorname):
        movie_data = {
        'showId': str(movie['id']),
        'actor': actorname}

        db = boto3.resource('dynamodb')
        table = db.Table('showid_actor')
        table.put_item(Item=movie_data)
        
     
    
# return top 3 movies with actorName   
# def call_api_search_movies(actorName):
#     actor_search_url = f'https://api.themoviedb.org/3/search/person?api_key={API_KEY}&query={actorName}'

#     response = requests.get(actor_search_url)
#     if response.status_code == 200:
#         results = response.json()['results']
#         return results[:5]
#     else:
#         print("Error retrieving data from TMDB API")    
    
def call_api_search_movies(actorName):
    actor_search_url = f'https://api.themoviedb.org/3/search/person?api_key={API_KEY}&query={actorName}'

    response = requests.get(actor_search_url)
    if response.status_code == 200:
        results = response.json()['results']
        
        if len(results) > 0:
            # Get the first actor's ID and fetch the movies they're in
            actor_id = results[0]['id']
            movies_url = f'https://api.themoviedb.org/3/person/{actor_id}/movie_credits?api_key={API_KEY}'
            movies_response = requests.get(movies_url)
            
            if movies_response.status_code == 200:
                movie_results = movies_response.json()['cast']
                return movie_results[:5]  # Return the top 5 movies

    else:
        print("Error retrieving data from TMDB API")
        
    return []
    
    
def lookup_data(key, db=None, table='show_data'):
    if not db:
        db = boto3.resource('dynamodb')
    table = db.Table(table)
    try:
        response = table.get_item(Key={'MovieID': key['MovieID']})
    except ClientError as e:
        print('Error', e.response['Error']['Message'])
    else:
        print("RESPNSE")
        print(response)
        return response['Item']
        
    
    
    

