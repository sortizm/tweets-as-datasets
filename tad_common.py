"""
Common classes, functions and constants of tweets-as-datasets

JSONStorage - storage in json
Tweet - abstraction of a tweet

load_credentials - load twitter credentials
load_topics - load topics from the config file

LANG - language to filter tweets
LOG_LEVELS - logging levels

Author: Steven Ortiz
"""

import logging

from os.path import isfile

try:
    import simplejson as json
except ImportError:
    import json

#LANG = 'es'  # spanish
LANG = 'en'  # english

LOG_LEVELS = {
        'DEBUG': logging.DEBUG,
        'INFO': logging.INFO,
        'WARNING': logging.WARNING,
        'ERROR': logging.ERROR,
        'CRITICAL': logging.CRITICAL
        }


class JSONStorage(object):
    """
    JSON storage class

    store(tweets): store the tweets to the file specified on creation in JSON
                   format
    """

    logger = logging.getLogger('JSONStorage')
    filename = None

    def __init__(self, filename):
        """
        Initialize file storage object

        filename: the name of the file that will be used as a storage unit
        """
        self.filename = filename + '.json'

    def store(self, tweets):
        """
        Store the tweets in JSON format in the file specified at creation

        tweets: the tweets that will be stored
        """
        prev_tweets = dict()
        if isfile(self.filename):
            with open(self.filename, 'r') as storefile:
                self.logger.info('Loading tweets from previous session...')
                prev_tweets = json.load(storefile)
        self.logger.info('Opening {0} to write {1} tweets'.format(
            self.filename, len(tweets)))
        with open(self.filename, 'w') as storefile:
            json.dump(self._tweets_to_dict(tweets, prev_tweets), storefile, indent=3)

    def _tweets_to_dict(self, tweets, prev_tweets_dict):
        """
        Convert a set-like object of Tweet objects to a dictionary representation
        """
        merged_tweets_dict = prev_tweets_dict.copy()
        tweets_dict = dict()
        for twt in tweets:
            twtdict = {'text': twt.text}
            tweets_dict[twt.identifier] = twtdict
        merged_tweets_dict.update(tweets_dict)
        return merged_tweets_dict


class Tweet(object):
    """
    Class that represents a tweet
    """

    logger = logging.getLogger('Tweet')
    identifier = None
    text = None

    def __init__(self, status):
        """
        Create the Tweet object

        status: a Twitter API status
        """
        self.identifier = str(status['id'])
        self.text = status['text']


def load_credentials(credentials_filename):
    """
    Load credentials from credentials_file
    one credential per line, keys and values are space separated

    Go to http://dev.twitter.com/apps/new to create an app and get values
    for these credentials.

    See https://dev.twitter.com/docs/auth/oauth for more information
    on Twitter's OAuth implementation.
    """
    credentials = dict()
    with open(credentials_filename, 'r') as cfile:
        for credential_line in cfile:
            key, val = credential_line.strip().split(' ')
            if key.lower() == 'consumer_key':
                credentials['consumer_key'] = val
            elif key.lower() == 'consumer_secret':
                credentials['consumer_secret'] = val
            elif key.lower() == 'oauth_token':
                credentials['oauth_token'] = val
            elif key.lower() == 'oauth_token_secret':
                credentials['oauth_token_secret'] = val
    return credentials

def load_topics(configuration_filename):
    """Load the topics and their queries from the configuration file"""
    topic_queries = list()
    with open(configuration_filename, 'r') as configfile:
        for line in configfile.readlines():
            if len(line.strip()) == 0 or line.strip()[0] == '#':
                continue  # ignore empty lines or lines starting with '#'
            assignment = line.split('=')
            if len(assignment) == 2:
                topicname = assignment[0].strip()
                queries = tuple(q.strip() for q in assignment[1].split(','))
                topic_queries.append((topicname, queries))
    return tuple(topic_queries)

