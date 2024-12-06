import tweepy
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

# Twitter API authentication (OAuth 1.0a)
auth = tweepy.OAuthHandler(
    os.getenv("TWITTER_API_KEY"),
    os.getenv("TWITTER_API_SECRET")
)
auth.set_access_token(
    os.getenv("TWITTER_ACCESS_TOKEN"),
    os.getenv("TWITTER_ACCESS_SECRET")
)
twitter_api = tweepy.API(auth, wait_on_rate_limit=True)

def post_simple_tweet():
    try:
        twitter_api.update_status("🚨 Test Tweet: Liquidation Alert 🚨")
        print("Simple tweet posted successfully.")
    except tweepy.TweepyException as e:
        print(f"Error posting simple tweet: {e}")

post_simple_tweet()



