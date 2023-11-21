import pandas as pd

# Read the data with specified encoding
data_trump = pd.read_csv('./dataset/hashtag_donaldtrump.csv', encoding='utf-8',engine='python' )
# data_trump = data_trump[["tweet_id","created_at","likes","retweet_count","user_id","user_name","user_join_date","user_followers_count","lat" ,"long","source","tweet"]].head(10000)
data_trump = data_trump.head(10000)
data_trump.to_csv('./partial_dataset/hashtag_donaldtrump_sample.csv', index=False)


# Read the data with specified encoding
data_joebiden = pd.read_csv('./dataset/hashtag_joebiden.csv', encoding='utf-8',engine='python' )
# data_joebiden = data_joebiden[["tweet_id","created_at","likes","retweet_count","user_id","user_name","user_join_date","user_followers_count","lat" ,"long","source","tweet"]].head(10000)
data_joebiden = data_joebiden.head(10000)
data_joebiden.to_csv('./partial_dataset/hashtag_joebiden_sample.csv', index=False)