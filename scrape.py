import json
import tweepy
# import boto3
import datetime
import os
import time
import glob
from collections import namedtuple

# from boto3.dynamodb.conditions import Key, Attr

# dyn = boto3.client('dynamodb')

with open('client_secret.json') as f:
    credentials = json.load(f)

# Getting the credentials to access the API
auth = tweepy.OAuthHandler(credentials['consumer_key'], credentials['consumer_secret'])
auth.set_access_token(credentials['access_token'], credentials['access_token_secret'])

# Getting the api
api = tweepy.API(auth)


class ComplexEncoder(json.JSONEncoder):
    def default(self, obj):
        if hasattr(obj, '_json'):
            return obj._json()
        else:
            return json.JSONEncoder.default(self, obj)


class Person:
    def __init__(self):
        self.Name = ''
        self.TwitterHandle = ''
        self.Disabled = False
        self.ErrorCode = None
        self.TwitterMaxId = None
        self.TwitterSinceId = None

    def _json(self):
        return self.__dict__


class Politician(Person):
    def __init__(self):
        super().__init__()
        self.Constituency = ''
        self.Party = ''
        self.Position = ''

    def _json(self):
        return self.__dict__


class TweetCollection:
    def __init__(self):
        self.DateScraped = ''
        self.NumberOfTweets = 0
        self.Person = None
        self.Tweets = None

    def _json(self):
        return self.__dict__


class Tweet:
    def __init__(self):
        self.id_str = ''
        self.created_at = ''
        self.entities = {}
        self.favourite_count = ''
        self.in_replay_to_status_id_str = ''
        self.in_replay_to_screen_name = ''
        self.retweet_count = ''
        self.source = ''
        self.text = ''
        self.truncated = ''

    def _json(self):
        return self.__dict__


def get_politicians_from_csv(filename):
    politician_list = []
    with open(filename, 'r') as f:
        temp_politician_list = json.load(f)

    for polit in temp_politician_list:
        p = Politician()
        p.Constituency = polit.get("Constituency", "")
        p.Position = polit.get("Position", "")
        p.TwitterHandle = polit.get("TwitterHandle", "")
        p.Name = polit.get("Name", "")
        p.Party = polit.get("Party", "")
        p.TwitterMaxId = polit.get("TwitterMaxId", None)
        p.TwitterSinceId = polit.get("TwitterSinceId", None)
        p.Disabled = polit.get("Disabled", False)
        politician_list.append(p)

    return politician_list


TWITTER_TOO_MANY_REQUEST_CODE = 429
TWITTER_NOT_FOUND_CODE = 404
TWITTER_PROTECTED_TWEETS_CODE = 401

TWITTER_REQUEST_WAIT = 5

twitter_error_code = None


def grab_tweets(person: Person, num_tweets: int = 10, options: dict = {}) -> TweetCollection:
    print(f"Getting (up to: {num_tweets}) tweets for {person.TwitterHandle}")
    global twitter_error_code
    global TWITTER_TOO_MANY_REQUEST_CODE
    global TWITTER_NOT_FOUND_CODE
    global TWITTER_REQUEST_WAIT

    include_rts = options.get('include_rts', True)
    exclude_replies = options.get('exclude_replies', True)
    trim_user = options.get('trim_user', True)
    retry_try_get_tweets = True
    all_tweets = []

    # Retry grabbing the tweets if we reached the request limit
    while retry_try_get_tweets is True:
        try:
            for page in tweepy.Cursor(api.user_timeline,
                                      screen_name=person.TwitterHandle,
                                      count=num_tweets,
                                      include_rts=include_rts,
                                      exclude_replies=exclude_replies,
                                      trim_user=trim_user,
                                      since_id=person.TwitterMaxId
                                      ).pages():

                person.TwitterMaxId = person.TwitterMaxId or str(page.max_id)
                person.TwitterSinceId = person.TwitterSinceId or str(page.since_id)

                for status in page:
                    new_tweet = Tweet()
                    new_tweet.id_str = status.id_str
                    new_tweet.created_at = str(status.created_at)
                    new_tweet.entities = status.entities
                    new_tweet.favourite_count = status.favorite_count
                    new_tweet.in_replay_to_status_id_str = status.in_reply_to_status_id_str
                    new_tweet.in_replay_to_screen_name = status.in_reply_to_screen_name
                    new_tweet.retweet_count = status.retweet_count
                    new_tweet.source = status.source
                    new_tweet.text = status.text
                    new_tweet.truncated = status.truncated
                    all_tweets.append(new_tweet)
            twitter_error_code = 200
            retry_try_get_tweets = False

        except Exception as e:
            print(f'Could not find twitter for:{person.TwitterHandle}\n{e}')
            twitter_error_code = e.response.status_code

            if twitter_error_code == TWITTER_NOT_FOUND_CODE:
                person.Disabled = True
                retry_try_get_tweets = False
                person.ErrorCode = twitter_error_code
            elif twitter_error_code == TWITTER_TOO_MANY_REQUEST_CODE:
                print(f"Request limit reached, waiting {TWITTER_REQUEST_WAIT} minutes")
                time.sleep(60 * TWITTER_REQUEST_WAIT)
                retry_try_get_tweets = True
                person.ErrorCode = twitter_error_code
            elif twitter_error_code == TWITTER_PROTECTED_TWEETS_CODE:
                print("Protected account, can't get tweets")
                retry_try_get_tweets = False
                person.ErrorCode = twitter_error_code
            else:
                print(f"Unknown issue: {e}")
                retry_try_get_tweets = False
                person.ErrorCode = twitter_error_code


    print(f"Found: {len(all_tweets)} tweets")

    tweet_coll = TweetCollection()
    tweet_coll.DateScraped = str(datetime.datetime.now())
    tweet_coll.NumberOfTweets = len(all_tweets)
    tweet_coll.Tweets = all_tweets
    return tweet_coll


def save_tweet_collection(data_folder_name, person, tweet_coll):
    if not os.path.exists(data_folder_name):
        os.makedirs(data_folder_name)

    filename = f"{data_folder_name}\\{person.TwitterHandle}_{tweet_coll.DateScraped[:10]}.json"
    save_tweets_json(tweet_coll, filename)


def save_person_info_json(parent_folder, politician_list, filename, date):
    with open(f"{parent_folder}\\{filename.split('-')[0]}-{date}.json", "w") as f:
        json.dump(politician_list, f, cls=ComplexEncoder, indent=2)


def chunk_all_person_list():
    chunks = [politician_list[i:i + chunk_size]
              for i in range(0, len(politician_list), chunk_size)]

    for chunk_index, politician_list_chunk in enumerate(chunks):
        # initial chunking
        person_folder_name = os.path.join(parent_dir, person_dir)
        with open(f"{person_folder_name}\\politician_list_{chunk_index}-"
                  f"{str(datetime.datetime.now())[:10]}.json", "w") as fp:
            json.dump(politician_list, fp, cls=ComplexEncoder, indent=2)


def handler(event, context):
    # filename = event['filename']
    # s3 = boto3.resource('s3')
    # content_object = s3.Object('politician-info', f'{filename}')
    # file_content = content_object.get()['Body'].read().decode('utf-8')
    # politician_list = json.loads(file_content)
    global twitter_error_code

    today_date = str(datetime.datetime.now())[:10]
    yesterday_date = f"{today_date[:-1]}{str(int(str(datetime.datetime.now())[:10][-1:])-1)}"

    chunk_size = 100
    parent_dir = "data"
    tweet_dir = "tweets"
    person_dir = "persons"
    # filenames = ["politician_info_all.json"]
    # filenames = ['politicians_info-test.json']

    data_folder_name = os.path.join(parent_dir, person_dir, today_date)
    if not os.path.exists(data_folder_name):
        os.makedirs(data_folder_name)

    # for index, filename in enumerate(filenames):
    for dirpath, dirnames, filenames in os.walk(os.path.join(parent_dir, person_dir, yesterday_date)):
        for filename in filenames:
            print(f"Starting processing on {filename}")
            politician_list = get_politicians_from_csv(os.path.join(dirpath, filename))
            politician_list.sort(key=lambda x: x.TwitterHandle)

            for person in politician_list:
                if person.Disabled is False:
                    tweet_coll = grab_tweets(
                        person=person,
                        num_tweets=3200,
                        options={},
                    )
                    tweet_coll.Person = person
                    data_folder_name = os.path.join(parent_dir, tweet_dir)
                    save_tweet_collection(data_folder_name, person, tweet_coll)

            person_folder_name = os.path.join(parent_dir, person_dir, today_date)
            save_person_info_json(person_folder_name, politician_list, filename, today_date)

        # save_tweets_s3(tweets, item.TwitterHandle)

        # item['TwitterLastGrabbedTweetId'] = max_tweet_id
        # item['Enabled'] = enabled

        # s3 = boto3.resource('s3')
        # object = s3.Object('politician-info', f'{filename}')
        # object.put(Body=json.dumps(politician_list, sort_keys=True, indent=2))

    return {
        'statusCode': 200,
        'body': f"Finished Processing"
    }


# def save_tweets_s3(tweet_coll: TweetCollection, filename: str):
#     s3 = boto3.resource('s3')
#     object = s3.Object('politicians-tweets', f'{twitter_handle}/{twitter_handle}_{str(time_now)}.json')
#     object.put(Body=json.dumps(tweets_json_string))
#     print(f"Done writing.")


def save_tweets_json(tweet_coll: TweetCollection, filename: str):
    with open(filename, "w") as f:
        json.dump(tweet_coll, cls=ComplexEncoder, indent=2, fp=f)


# def save_tweets_s3(tweets: list, twitter_handle: str):
#     print(f"Saving {len(tweets)} tweets for: {twitter_handle}")
#     tweets_json_string = []
#     time_now = datetime.datetime.now().isoformat()
#     for tweet in tweets:
#         add = {
#             "id": tweet.get('id'),
#             "username": tweet.get('username'),
#             'tweet_text': tweet.get('tweet_text'),
#             'retweets': str(tweet.get('retweets')),
#             'users_mentioned': tweet.get('users_mentioned'),
#             'has_urls': tweet.get('has_urls'),
#             'favorite_count': str(tweet.get('favorite_count')),
#             'scraped_time': str(time_now),
#         }
#         tweets_json_string.append(add)
#
#     # Saving tweet to S3 bucket
#
#     if len(tweets_json_string) > 0:
#         s3 = boto3.resource('s3')
#         object = s3.Object('politicians-tweets', f'{twitter_handle}/{twitter_handle}_{str(time_now)}.json')
#         object.put(Body=json.dumps(tweets_json_string))
#     print(f"Done writing.")


handler(None, None)
