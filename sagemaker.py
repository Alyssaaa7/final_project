import boto3, re, sys, math, json, os, sagemaker, urllib.request
from sagemaker import get_execution_role
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import pickle
from IPython.display import Image
from IPython.display import display
from time import gmtime, strftime
from sagemaker.predictor import csv_serializer
from sklearn.feature_extraction.text import CountVectorizer
import nltk
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize

nltk.download('stopwords')
stop_words = stopwords.words('english')

# Define IAM role
role = get_execution_role()
prefix = 'sagemaker/DEMO-xgboost-dm'
my_region = boto3.session.Session().region_name # set the region of the instance

# this line automatically looks for the XGBoost image URI and builds an XGBoost container.
xgboost_container = sagemaker.image_uris.retrieve("xgboost", my_region, "latest")

print("Success - the MySageMakerInstance is in the " + my_region + " region. You will use the " + xgboost_container + " container for your SageMaker endpoint.")

reader = pd.read_json('./IMDB_reviews.json', lines=True, chunksize=10000)

for chunk in reader:
    model_data = chunk
    break

print(model_data.head)

reviews = model_data["review_text"]
labels = model_data["is_spoiler"].astype(int)

preprocessed_reviews = []
for review in reviews:
    tokens = word_tokenize(review.lower())
    preprocessed_review = [token for token in tokens if token not in stop_words and token.isalpha()]
    preprocessed_review = ' '.join(preprocessed_review)
    preprocessed_reviews.append(preprocessed_review)

vectorizer = CountVectorizer()
vectorizer.fit(preprocessed_reviews)
pickle.dump(vectorizer, open("./vectorizer", "wb"))

bucket_name = 'spoiler-detection-bucket'
s3 = boto3.resource('s3')
s3.create_bucket(Bucket=bucket_name)

X = vectorizer.transform(preprocessed_reviews)

X_train, X_test, y_train, y_test = train_test_split(X, labels, test_size=0.2, random_state=42)
X_train = X_train.toarray()
X_test = X_test.toarray()


pd.concat(y_train, X_train, axis=1).to_csv('train.csv', index=False, header=False)

prefix = "data"
boto3.Session().resource('s3').Bucket(bucket_name).Object(os.path.join(prefix, 'train/train.csv')).upload_file('train.csv')
s3_input_train = sagemaker.inputs.TrainingInput(s3_data='s3://{}/{}/train'.format(bucket_name, prefix), content_type='csv')

sess = sagemaker.Session()
xgb = sagemaker.estimator.Estimator(xgboost_container,
                                    role,
                                    train_instance_count=1,
                                    train_instance_type='ml.m4.large',
                                    output_path='s3://{}/{}/output'.format(bucket_name, prefix),
                                    sagemaker_session=sagemaker_session)

xgb.set_hyperparameters(max_depth=5,
                        eta=0.2,
                        gamma=4,
                        min_child_weight=6,
                        subsample=0.8,
                        silent=0,
                        objective='binary:logistic',
                        eval_metric='auc',
                        num_round=100)



xgb.fit({'train': s3_input_train})
xgb_predictor = xgb.deploy(initial_instance_count=1,instance_type='ml.m4.xlarge')

