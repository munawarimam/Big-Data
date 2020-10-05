from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
import datetime as dt
import tweepy
import csv
import json
import re
import pendulum

d = dt.datetime.now()
local = pendulum.timezone("Asia/Jakarta")
default_args = {
    'owner': 'munawarimam',
    'start_date': dt.datetime(2020, 8, 24, tzinfo = local),
    'retries': 1,
    'concurrency': 5
    }
dag_conf = DAG('sparkairflow', default_args=default_args, schedule_interval='0 8,12,17,22 * * *', catchup=False)


def start():
    print("Start the Job!!")

def finish():
    print("Your job is finished.")

def search_hashtags(consumer_key, consumer_secret, access_token, access_token_secret, hashtag_phrase):

    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    
    api = tweepy.API(auth)

    fname = '_'.join(re.findall(r"#(\w+)", hashtag_phrase))
    
    with open(fname, 'w', encoding='utf8') as file:
        w = csv.writer(file)
        w.writerow(['date', 'tweet', 'username', 'all_hashtags', 'followers', 'languange'])
        
        for tweet in tweepy.Cursor(api.search, q=hashtag_phrase+' -filter:retweets', \
                                   tweet_mode='extended').items(1000):
            w.writerow([tweet.created_at, tweet.full_text.replace('\n',' '), tweet.user.screen_name.encode('utf-8'), [e['text'] for e in tweet._json['entities']['hashtags']], tweet.user.followers_count, tweet.lang])
        
def running():   
    access_token = '' 
    access_token_secret = ''
    consumer_key = ''        
    consumer_secret = '' 
    hashtag_phrase = '#covid19'
    
    return (search_hashtags(consumer_key, consumer_secret, access_token, access_token_secret, hashtag_phrase))


start_operator = PythonOperator(task_id='starting_job', python_callable=start, dag=dag_conf)
getdata_operator = PythonOperator(task_id='getdata_job', python_callable=running, dag=dag_conf)
cleansing_operator = BashOperator(task_id='cleansing_job', \
    bash_command='spark-submit --master yarn --class com.imam.datacleansing /home/munawarimam/cleansing.jar', dag=dag_conf)
finish_operator = PythonOperator(task_id='job_finished', python_callable=finish, dag=dag_conf)

start_operator >> getdata_operator >> cleansing_operator >> finish_operator
