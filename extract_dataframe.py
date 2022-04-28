import json
import pandas as pd
from textblob import TextBlob

def read_json(json_file: str)->list:
    """
    json file reader to open and read json files into a list
    Args:
    -----
    json_file: str - path of a json file
    
    Returns
    -------
    length of the json file and a list of json
    """
    
    tweets_data = []
    for tweets in open(json_file,'r'):
        tweets_data.append(json.loads(tweets))
    
    
    return len(tweets_data), tweets_data

class TweetDfExtractor:
    """
    this function will parse tweets json into a pandas dataframe
    
    Return
    ------
    dataframe
    """
    def __init__(self, tweets_list):
        
        self.tweets_list = tweets_list

    # an example function
    def find_statuses_count(self)->list:
        statuses_count = []
        for status in self.tweets_list:
            if status['user'] == 'statuses_count':
                statuses_count.append(status)
        
        
    def find_full_text(self)->list:
    
        text = [] 
        for texts in self.tweets_list:
            if 'retweeted_status' in texts.keys() and 'extended_tweet' in texts['retweeted_status'].keys():
                text.append(texts['retweeted_status']['extended_tweet']['full_text'])
            else:
                text.append('empty')

        return text
    
    def find_sentiments(self, text)->list:
        polarity = []
        subjectivity = []
        
        for i in text:
            blob = TextBlob(i)
            sentiments = blob.sentiment
            polarity.append(sentiments.polarity)
            subjectivity.append(sentiments.subjectivity)
        
        return polarity, subjectivity

    def find_created_time(self)->list:
        created_at = []
        for tweets in self.tweets_list:
            if 'created_at' in tweets.keys():
                created_at.append(tweets['created_at'])
   
        return created_at

    def find_source(self)->list:
        source = []
        for tweets in self.tweets_list:
            if 'source' in tweets.keys():
                source.append(tweets['source'])

        return source

    def find_screen_name(self)->list:
        screen_name = []
        for tweets in self.tweets_list:
            if 'screen_name' in tweets['user'].keys():
                screen_name.append(tweets['user']['screen_name'])
        return screen_name

    def find_followers_count(self)->list:
        followers_count = []
        for tweets in self.tweets_list:
            if 'followers_count' in tweets['user'].keys():
                followers_count.append(tweets['user']['followers_count'])
        return followers_count

    def find_friends_count(self)->list:
        friends_count = []
        for tweets in self.tweets_list:
            if 'friends_count' in tweets['user'].keys():
                friends_count.append(tweets['user']['friends_count'])
                
        return friends_count

    def is_sensitive(self)->list:
        try:
            is_sensitive = []
            for tweets in self.tweets_list:
                if 'possibly_sensitive' in tweets.keys():
                    is_sensitive.append(tweets['possibly_sensitive'])
                else:
                    is_sensitive.append(0)
                    
                    
        except KeyError:
            is_sensitive = None

        return is_sensitive

    def find_favourite_count(self)->list:
        favorite_count = []
        for tweets in self.tweets_list:
            if 'retweeted_status' in tweets.keys():
                favorite_count.append(tweets['retweeted_status']['favorite_count'])
            else: 
                favorite_count.append(0)

        return favorite_count
    
#         favorite_count = []
#         for tweets in self.tweets_list:
#             if tweets.keys() == 'retweeted_status':
#                 favorite_count.append(tweets['retweeted_status']['favorite_count'])
#             else: 
#                 favorite_count.append(0)

#         return favorite_count
        
    
    def find_retweet_count(self)->list:
        retweet_count = []
        for tweets in self.tweets_list:
            if 'retweeted_status' in tweets.keys():
                retweet_count.append(tweets['retweeted_status']['retweet_count'])
            else: 
                retweet_count.append(0)

        return retweet_count
    
#         retweet_count = []
#         for tweets in self.tweets_list:
#             if tweets.keys() == 'retweeted_status':
#                 retweet_count.append(tweets['retweeted_status']['retweet_count'])
#             else:
#                 retweet_count.append(0)

#         return retweet_count
         

    def find_hashtags(self)->list:
        hashtags = []

        for tweets in self.tweets_list:
            hashtags.append(", ".join([hashtag_item['text'] for hashtag_item in tweets['entities']['hashtags']]))
        return hashtags
       

    def find_mentions(self)->list:
        mentions = []
        for tweets in self.tweets_list:
            mentions.append( ", ".join([mention['screen_name'] for mention in tweets['entities']['user_mentions']]))

        return mentions


    def find_location(self)->list:
        try:
            location = self.tweets_list['user']['location']
        except TypeError:
            location = ''
        
        return location
    
    def find_lang(self)->list:
        lang = []
        for tweets in self.tweets_list:
            if 'lang' in tweets.keys():
                lang.append(tweets['lang'])

        return lang
        
        
    def get_tweet_df(self, save=False)->pd.DataFrame:
        """required column to be generated you should be creative and add more features"""
        
        columns = ['created_at', 'source', 'original_text','polarity','subjectivity', 'lang', 'favorite_count',
                   'retweet_count', 'original_author', 'followers_count','friends_count','possibly_sensitive', 'hashtags', 
                   'user_mentions', 'place']
        
        created_at = self.find_created_time()
#         print(len(created_at))
        source = self.find_source()
#         print(len(source))
        text = self.find_full_text()
#         print(len(text))
        polarity, subjectivity = self.find_sentiments(text)
#         print(len(polarity))
#         print(len(subjectivity))
        lang = self.find_lang()
#         print(len(lang))
        fav_count = self.find_favourite_count()
#         print(len(fav_count))
        retweet_count = self.find_retweet_count()
#         print(len(retweet_count))
#         print(retweet_count)
        screen_name = self.find_screen_name()
#         print(len(screen_name))
        follower_count = self.find_followers_count()
#         print(len(follower_count))
        friends_count = self.find_friends_count()
#         print(len(friends_count))
        sensitivity = self.is_sensitive()
#         print(sensitivity)
        hashtags = self.find_hashtags()
#         print(len(hashtags))
        mentions = self.find_mentions()
#         print(len(mentions))
        location = self.find_location()
#         print(len(location))
        
        data = {"created_at":created_at,'source':source,'original_text':text,'polarity':polarity,'subjectivity':subjectivity,
                'lang':lang,'favorite_count':fav_count,'retweet_count':retweet_count,'original_author':screen_name, 
                'followers_count':follower_count,'friends_count':friends_count,'possibly_sensitive':sensitivity,
                'hashtags':hashtags,'user_mentions':mentions}
#         print(data)

        df = pd.DataFrame(data)
        

        if save:
            df.to_csv('processed_tweet_data.csv', index=False)
            print('File Successfully Saved.!!!')
        
        return df

                
if __name__ == "__main__":
    # required column to be generated you should be creative and add more features
    columns = ['created_at', 'source', 'original_text','clean_text', 'sentiment','polarity','subjectivity', 'lang', 'favorite_count', 'retweet_count', 
    'original_author', 'screen_count', 'followers_count','friends_count','possibly_sensitive', 'hashtags', 'user_mentions', 'place', 'place_coord_boundaries']
    _, tweet_list = read_json("Economic_Twitter_Data.json")
    tweet = TweetDfExtractor(tweet_list)
    tweet_df = tweet.get_tweet_df() 
tweet_df

    # use all defined functions to generate a dataframe with the specified columns above

    