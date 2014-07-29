from threading import Thread
from Queue import Queue, Empty
import time
import logging
import socket

log = logging.getLogger('pystaggregator')

class Client(Thread):
    def __init__(self, host, port):
        Thread.__init__(self)
        self.host = host                # pystaggregator hostname/IP
        self.port = port                # pystaggregator port
        self.socket = None              # our connection to pystaggregator
        self.q = Queue()                # queue that holds outgoing messages
        self.should_stop = False        # do we keep running
        self.connected = False          # are we connected to pystaggregator
        
        # python will shut down (abruptly) when only daemon thraeads are left
        # this is abrupt, but clients dont have to explicitly close resources
        # and the biggest consequence is that some messages are not sent
        # and that the socket is closed uncleanly.

        # perhaps we will revisit this tradeoff
        self.daemon = True 

    def _connect(self):
        while not self.connected:
            try:
                self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.socket.connect((self.host, self.port))
                log.info('connected to {0}:{1}'.format(self.host, self.port))
                self.connected = True
            except IOError as e:
                log.debug('could not connect: {0}'.format(e))
                time.sleep(1)

    def send(self, message):
        self.q.put(message)
        
    def run(self):
        while not self.should_stop:
            if not self.connected:
                log.debug('not connected...connecting to {0}:{1}'.format(
                    self.host, self.port))
                # this call will block until we are connected, no sense in continuing
                # if we are not
                self._connect()
            message = self._build_message()
            if len(message) > 0:
                self._send_message(message)

    def stop(self):
        self.should_stop = True

    def _build_message(self):
        m = []
        timeout = 0.05 # 50ms
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

        return '\n'.join(m) 

    def _send_message(self, message):
        log.debug('sending {0}'.format(message))
        try:
            self.socket.sendall(message)
            self.socket.send('\n')
            log.debug('{0} sent.'.format(message))
        except IOError as e:
            log.info('could not send message {0}'.format(message))
            self.connected = False
            # put these messages back in the queue, to be revisited when we
            # are connected again.
            for m in message.split('\n'):
                log.debug('putting {0} back in queue'.format(m))
                self.q.put(m)

# a reference to the Client Thread
_client = None

def connect(host, port):
    """connect to pystaggregator.  This must be called prior to using any
    Timers or Counters.

    :param host:    host of pystaggregator, string
    :param port:    port of pystaggregator, int
    """
    global _client
    _client = Client(host, port)
    _client.start()


class Timer(object):
    def __init__(self, name):
        self.name = name

    def start(self):
        self._start = time.time()

    def end(self):
        duration = int(round((time.time() - self._start) * 1000)) # in ms
        _client.send('{0}:{1}|ms'.format(self.name, duration))


class Counter(object):
    def __init__(self, name):
        self.name = name

    def count(self, num=1):
        _client.send('{0}:{1}|c'.format(self.name, num))


class Timer(object):
    def __init__(self, name):
        self.name = name

    def start(self):
        self._start = time.time()

    def end(self):
        duration = int(round((time.time() - self._start) * 1000)) # in ms
        _client.send('{0}:{1}|ms'.format(self.name, duration))


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
        c = Counter(name)
        def wrap(*args, **kwargs):
            func(*args, **kwargs)
            c.count()
        return wrap
    return decorator

    

