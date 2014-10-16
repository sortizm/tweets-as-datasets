#!/usr/bin/env python3

"""
Twitter Stream Mining

Get tweets using the Twitter streaming API

Author: Steven Ortiz
"""


import twitter
import docopt
import logging
import sys

from tad_common import LOG_LEVELS, LANG, JSONStorage, Tweet, load_credentials

CREDENTIALS = dict()


USAGE = """
Usage:
    tsm.py [-h] [-m LIMIT] [-l LEVEL] [-o FILE] CREDENTIALSFILE

Options:
    -h --help                       show this
    -l --log-level=LEVEL            set the logging level [default: ERROR]
    -o --log-file=FILE              a file to log the output
    -m --max=LIMIT                  maximum number of tweets to download,
                                    a negative number or zero means no limit

Loggin levels (from more verbose to less verbose):
    * {0}
    * {1}
    * {2}
    * {3}
    * {4}
""".format(*LOG_LEVELS.keys())


class StreamMiner(object):
    """
    Retrieve tweets from the Twitter streaming API until the limit is reached,
    an interruption from keyboard is received (^C), or the stream connection is
    closed.

    get_tweets() return a tuple of Tweet objects
    """

    logger = logging.getLogger('StreamMiner')
    limit = None
    tweets = None
    _t_api = None

    def __init__(self, limit):
        """
        Initialize twitter connection objects

        limit = maximum number of tweets to download
        """
        self.logger.info('Creating an API connection...')
        auth = twitter.oauth.OAuth(
                CREDENTIALS['oauth_token'],
                CREDENTIALS['oauth_token_secret'],
                CREDENTIALS['consumer_key'],
                CREDENTIALS['consumer_secret'])
        self._t_api = twitter.TwitterStream(auth=auth)
        self.logger.debug('Twitter object received {0}'.format(self._t_api))
        self.limit = limit if limit > 0 else -1

    def _get_tweets(self):
        """
        Set the class attribute tweets to the list of tweets retrieved from the
        Twitter stream
        """
        all_locations = '-180,-90,180,90'
        self.tweets = list()
        self.logger.info('Retrieving statuses from Twitter stream API')
        count = 0
        try:
            for status in self._t_api.statuses.filter(locations=all_locations, language=LANG):
                self.logger.debug('Status `{0}` received \n{1}'.format(count+1, status))
                if 'text' in status and count != self.limit:
                    self.tweets.append(Tweet(status))
                else:
                    self.logger.info('Retrieved a total of {0} statuses'.format(count))
                    return
                count += 1
        except KeyboardInterrupt:
            self.logger.info('Retrieved a total of {0} statuses'.format(count))

    def get_tweets(self):
        """Return the list of tweets retrieved from the Twitter stream"""
        if self.tweets is None:
            self._get_tweets()
        return self.tweets


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
    try:
        limit = 0 if arguments.get('--max') is None else int(arguments.get('--max'))
    except ValueError:
        print("limit must be a number\n", file=sys.stderr)
        print(USAGE, file=sys.stderr)
        sys.exit(1)

    if loglevel not in LOG_LEVELS:
        print(USAGE, file=sys.stderr)
        sys.exit(1)
    else:
        logging_level = LOG_LEVELS[loglevel]
        if logfile is not None:
            logging.basicConfig(filename=logfile, level=logging_level, format=logft)
            with open(logfile, 'a') as lfile:
                lfile.write('\n' + '*'*80 + '\n') # to separate different logs
        else:
            logging.basicConfig(level=logging_level, format=logft)

    logger = logging.getLogger()
    logger.info('Reading credentials file {0}'.format(credentials_file))
    CREDENTIALS.update(load_credentials(credentials_file))

    streamminer = StreamMiner(limit)
    storage = JSONStorage('tweets')
    storage.store(streamminer.get_tweets())

if __name__ == '__main__':
    main()
