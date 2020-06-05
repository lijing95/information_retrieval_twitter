import time

import requests
from bs4 import BeautifulSoup
from twitter_scraper import get_tweets
from twitter_scraper import Profile
import pandas as pd
import twint
import nltk
from nltk.corpus import stopwords
import ssl
ssl._create_default_https_context = ssl._create_unverified_context


"""

Crawling

"""

def get_twitter_username():
    headers = {
        "User-Agent": "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.122 Mobile Safari/537.36"}
    url = "https://www.brandwatch.com/blog/most-twitter-followers/"
    html = requests.get(url, headers=headers)
    soup = BeautifulSoup(html.text, 'html.parser')

    hrefs = soup.find_all('a')

    href_list = []
    for href_ in hrefs:
        h = href_.get('href')
        link_str = "https://twitter.com/"
        if (isinstance(h, str)):
            if (link_str in h):
                href_list.append(h)
    href_list.sort(key=lambda i: len(i), reverse=False)
    twitter_href_list = href_list[:20]

    twitter_username = []
    for t in twitter_href_list:
        twitter_username.append(t[20:])
    return twitter_username


def get_user_info(twitter_username):
    username = []
    tweets_count = []
    followers_count = []
    following_count = []
    for i in twitter_username:
        profile = Profile('{}'.format(i))
        username.append(profile.username)
        tweets_count.append(profile.tweets_count)
        followers_count.append(profile.followers_count)
        following_count.append(profile.following_count)

    dataframe = pd.DataFrame({'username': username, 'tweets_count': tweets_count,
                              'followers_count': followers_count, 'following_count': following_count})
    dataframe.to_csv("user_info.csv", index=False, sep=',')

#
# def get_tweet(twitter_username):
#     tweetId = []
#     isRetweet = []
#     tweet_username = []
#     text = []
#     replies = []
#     retweets = []
#     likes = []
#     time = []
#     for t_username in twitter_username:
#         print(t_username)
#         for tweet in get_tweets('{}'.format(t_username), pages=1000):
#             tweetId.append(tweet['tweetId'])
#             isRetweet.append(tweet['isRetweet'])
#             tweet_username.append(tweet['username'])
#             text.append(tweet['text'])
#             replies.append(tweet['replies'])
#             retweets.append(tweet['retweets'])
#             likes.append(tweet['likes'])
#             time.append(tweet['time'])
#
#     dataframe = pd.DataFrame({'tweetId': tweetId, 'isRetweet': isRetweet,'tweet_username': tweet_username,
#                               'text': text,'replies': replies,'retweets': retweets,'likes': likes, 'time':time})
#     dataframe.to_csv("tweet_info.csv", index=False, sep=',')
#

def get_user_tweet(twitter_username):
    c = twint.Config()
    for t_username in twitter_username:
        c.Username = "{}".format(t_username)
        c.Limit = 10000
        c.Store_csv = True
        c.Output = "tweets_info.csv"
        c.Lang = "en"
        time.sleep(10)
        twint.run.Search(c)


def show_user_tweet():
    pd.set_option('display.max_columns', None)
    tweets = pd.read_csv('tweets_info.csv',usecols=['id','name','tweet','replies_count',
                                                     'retweets_count','likes_count','date','time'])
    print(tweets)


"""
    
Clean
    
"""

def delete_tweet_link():
    pd.set_option('display.max_columns', None)
    df = pd.read_csv('tweets_info.csv', low_memory=False)
    df['clean_tweet'] = ''
    df['clean_tweet'] = df['tweet'].str.replace('http\S+|pic.twitter.com\S+', '', case=False)
    return df['clean_tweet']

def delete_tweet_punctuation_numbers(clean_tweet):
    pd.set_option('display.max_columns', None)
    df = pd.read_csv("tweets_info.csv", low_memory=False)
    df['clean_tweet'] = clean_tweet.str.replace('\W', ' ').str.replace('\d', ' ').str.replace(" +", ' ')
    # print(df['clean_tweet'])
    return df['clean_tweet']


def tweet_to_lower(clean_tweet):

    pd.set_option('display.max_columns', None)
    df = pd.read_csv("tweets_info.csv", low_memory=False)
    df['clean_tweet'] = clean_tweet.str.lower()

    df.to_csv("tweets_info_after_clean.csv",
              columns=['id', 'name', 'clean_tweet', 'replies_count', 'retweets_count',
                       'likes_count', 'date','time'],index=1,header=1)
    # df.to_csv("tweets_info_after_clean.csv",index=1, header=1)
    after_clean = pd.read_csv("tweets_info_after_clean.csv")
    print(after_clean)

def delete_tweet_stopwords():

    nltk.download('stopwords')
    en_stops = stopwords.words('english')
    # print(en_stops)

    pd.set_option('display.max_columns', None)
    df = pd.read_csv("tweets_info_after_clean.csv", low_memory=False)

    df['clean_tweet'] = df['clean_tweet'].apply(lambda words : " ".join([ word  for word in words.split()  if word not in en_stops]))

    return df['clean_tweet']

def delete_tweet_lessThan50(clean_tweet):
    pd.set_option('display.max_columns', None)
    df = pd.read_csv("tweets_info_after_clean.csv", low_memory=False)
    df['clean_tweet'] = clean_tweet

    df = df[~df['clean_tweet'].isin(t for t in clean_tweet if len(t)<50)]
    df = df[~df['clean_tweet'].isin(['null'])]

    # print(df['clean_tweet'])
    # return df['clean_tweet']
    df.to_csv("tweets_info_after_clean.csv",
              columns=['id', 'name', 'clean_tweet', 'replies_count', 'retweets_count',
                       'likes_count', 'date','time'],index=1,header=1)

    # df.to_csv("tweets_info_after_clean.csv",index=1, header=1)

    after_clean = pd.read_csv("tweets_info_after_clean.csv")

    print(after_clean)

if __name__ == '__main__':
    # get_user_info(get_twitter_username())
    # get_tweet(get_twitter_username())
    #get_user_tweet(get_twitter_username())
    #show_user_tweet()
    # delete_tweet_link()
    # clean_tweet = delete_tweet_punctuation_numbers(delete_tweet_link())
    # tweet_to_lower(clean_tweet)
    # delete_tweet_stopwords()
    clean_tweet = delete_tweet_stopwords()
    delete_tweet_lessThan50(clean_tweet)


