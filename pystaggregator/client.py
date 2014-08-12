from threading import Thread
from six.moves.queue import Queue, Empty
import time
import json
import logging
import requests
import socket

log = logging.getLogger('pystaggregator')

class Client(Thread):
    def __init__(self, url, apikey, timeout=1.00):
        Thread.__init__(self)
        self.url = url                  # url of staggregator
        self.apikey = apikey            # api key for staggregator or None
        self.timeout = timeout          # timeout
        self.q = Queue()                # queue that holds outgoing messages
        self.should_stop = False        # do we keep running
        
        # python will shut down (abruptly) when only daemon thraeads are left
        # this is abrupt, but clients dont have to explicitly close resources
        # and the biggest consequence is that some messages are not sent
        # and that the socket is closed uncleanly.

        # perhaps we will revisit this tradeoff
        self.daemon = True 

        self.headers = {'STAGGREGATOR_KEY':self.apikey}

    def send(self, message):
        self.q.put(message)
        
    def run(self):
        log.info('starting loop')
        while not self.should_stop:
            message, num_stats = self._build_message()
            if len(message) > 0:
                self._send_message(message)
        log.info('stopped loop')

    def stop(self):
        self.should_stop = True

    def _build_message(self):
        m = []
        timeout = 1.00 # seconds
        now = last_time = time.time()
        
        # this loop will get as many messages as the queue will
        # provide in 50 ms.  Assuming that there are no more incoming,
        # the timeout lets us say, "we're only going to wait a bit longer
        # before sending what we have and then waiting on the queue again."
        while last_time - now < timeout:
            try:
                item = self.q.get(True, timeout)
                m.append(item)
            except Empty:
                pass
            last_time = time.time()
        return m, len(m)

    def _send_message(self, message):
        log.info('sending message with {} stats'.format(len(message)))
        log.debug('message is {}'.format(message))
        try:
            res = requests.post(self.url, headers=self.headers, 
                    data=json.dumps(message))
            res.raise_for_status()
        except Exception as e:
            log.error('could not send message {0}'.format(message))
            log.exception(e)
            # put these messages back in the queue, to be revisited when we
            # are connected again.
            # we should probably back off the timeout for sending messages
            # here
            for m in message:
                log.debug('putting {0} back in queue'.format(m))
                self.q.put(m)

# a reference to the Client Thread
_client = None

def start(url, key):
    global _client
    log.info('starting client for {0} with apikey {1}'.format(url, key))
    _client = Client(url, key)
    _client.start()

def send(message):
    """ send a correctly message to staggregator.
    :param message:  dictionary with name, value, and type fields
    """
    _client.send(message)


class Counter(object):
    def __init__(self, name):
        self.name = name

    def count(self, num=1):
        send(dict(name=self.name, value=num, type='c'))


class Timer(object):
    def __init__(self, name=None):
        self.name = name

    def start(self):
        self._start = time.time() * 1000 # in ms.  See docs for time.clock vs time.time

    def end(self, name=None):
        name_to_send = name if name is not None else self.name

        duration = int(round(time.time() * 1000 - self._start))
        send(dict(name=name_to_send, value=duration, type='ms'))


def timer(name):
    def decorator(func):
        t = Timer(name)
        def wrap(*args, **kwargs):
            t.start()
            func(*args, **kwargs)
            t.end()
        return wrap
    return decorator


def counter(name):
    def decorator(func):
        def wrap(*args, **kwargs):
            c = Counter(name)
            func(*args, **kwargs)
            c.count()
        return wrap
    return decorator

    

