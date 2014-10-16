#!/usr/bin/env python3

"""
Twitter Topic Mining

Get tweets matching the queries defined in the configuration file

Retrieve and store the tweets, provided by the GET search/tweets endpoint,
which match the queries defined in the configuration file.
SEE: https://dev.twitter.com/rest/public/search

Author: Steven Ortiz
"""


import twitter
import docopt
import logging
import sys

from os.path import isfile

from tad_common import LOG_LEVELS, LANG, JSONStorage, Tweet, \
        load_credentials, load_topics


CREDENTIALS = dict()

LOG_LEVELS = {
        'DEBUG': logging.DEBUG,
        'INFO': logging.INFO,
        'WARNING': logging.WARNING,
        'ERROR': logging.ERROR,
        'CRITICAL': logging.CRITICAL
        }

USAGE = """
Usage:
    ttm.py [-h] [-l LEVEL] [-o FILE] CONFIGFILE CREDENTIALSFILE

Options:
    -h --help                       show this
    -l --log-level=LEVEL            set the logging level [default: ERROR]
    -o --log-file=FILE              a file to log the output

Loggin levels (from more verbose to less verbose):
    * {0}
    * {1}
    * {2}
    * {3}
    * {4}

Configuration file format:
    topic1 = 'query1', 'query2', ..., 'queryN'
    topic2 = 'query1', 'query2', ..., 'queryN'

    where 'topic' is the the name of the file in which the tweets will be
    stored, and 'query' is a valid Twitter query.
    SEE: https://dev.twitter.com/rest/public/search

""".format(*LOG_LEVELS.keys())


class TopicMiner(object):
    """
    Retrieve all the tweets of a topic

    get_tweets() return a tuple of Tweet objects
    """

    logger = logging.getLogger('TopicMiner')
    queries = None
    tweets = None
    _t_api = None

    def __init__(self, queries):
        """
        Initialize twitter connection objects

        queries: a set-like object of queries (strings)
        """
        self.logger.info('Creating an API connection...')
        auth = twitter.oauth.OAuth(
                CREDENTIALS['oauth_token'],
                CREDENTIALS['oauth_token_secret'],
                CREDENTIALS['consumer_key'],
                CREDENTIALS['consumer_secret'])
        self._t_api = twitter.Twitter(auth=auth)
        self.logger.info('Twitter object received {0}'.format(self._t_api))
        self.queries = queries

    def _get_tweets(self):
        """
        Set the class attribute tweets to the tuple of Tweet objects, returned
        by the twitter API, that match any of the queries
        """
        self.tweets = list()
        for query in self.queries:
            query_tweets = list()
            self.logger.info('Retrieving statuses with query {0}'.format(query))
            results = self._t_api.search.tweets(q=query, lang=LANG, count=100)
            query_tweets += [Tweet(status) for status in results['statuses']]
            while True:
                try:
                    next_results = results['search_metadata']['next_results']
                except KeyError:
                    self.logger.info('Retrieved {0} statuses with query {1}'\
                            ''.format(len(query_tweets), query))
                    break  # No more results when next_results does not exist
                # Create a dictionary from next_results:
                # ?max_id=313519052523986943&q=NCAA&include_entities=1
                kwargs = dict([kv.split('=') for kv in next_results[1:].split("&")])
                results = self._t_api.search.tweets(**kwargs)
                query_tweets += [Tweet(status) for status in results['statuses']]
            self.tweets += query_tweets
        self.logger.info('Retrieved a total of {0} statuses'.format(len(self.tweets)))


    def get_tweets(self):
        """Return a tuple of tweets, that match any of the queries"""
        if self.tweets is None:
            self._get_tweets()
        return self.tweets

    def refresh(self):
        """Reload the tweets attribute"""
        self._get_tweets()


def main():
    """
    Parse arguments, read config file, and initialize logger object,
    and miner objects
    """
    logft = '%(asctime)s [%(levelname)s] - %(name)s.%(funcName)s -- %(message)s'
    arguments = docopt.docopt(USAGE)
    loglevel = arguments['--log-level']
    logfile = arguments['--log-file']
    credentials_file = arguments['CREDENTIALSFILE']

    if loglevel not in LOG_LEVELS:
        print(USAGE, file=sys.stderr)
        sys.exit(1)
    else:
        logging_level = LOG_LEVELS[loglevel]
        if logfile  is not None:
            logging.basicConfig(filename=logfile, level=logging_level, format=logft)
            with open(logfile, 'a') as lfile:
                lfile.write('\n' + '*'*80 + '\n') # to separate different logs
        else:
            logging.basicConfig(level=logging_level, format=logft)

    logger = logging.getLogger()
    logger.info('Reading credentials file {0}'.format(credentials_file))
    CREDENTIALS.update(load_credentials(credentials_file))

    logger.info('Reading configuration file {0}'.format(logfile))
    for topicname, queries in load_topics(arguments['CONFIGFILE']):
        logger.debug('Creating TopicMiner object for {0}: queries={1}'\
                ''.format(topicname, queries))
        topicminer = TopicMiner(queries)
        storage = JSONStorage(topicname)
        storage.store(topicminer.get_tweets())


if __name__ == '__main__':
    main()
