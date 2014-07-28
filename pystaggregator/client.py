from threading import Thread
from Queue import Queue, Empty
import time
import logging
import socket

log = logging.getLogger('pystaggregator')

class Client(Thread):
    def __init__(self, host, port):
        Thread.__init__(self)
        self.host = host
        self.port = port
        self.socket = None
        self.q = Queue()
        self.should_stop = False
        
        # python will shut down (abruptly) when only daemon thraeads are left
        self.daemon = True 

    def _connect(self):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.connect((self.host, self.port))
        log.info('connected to {0}:{1}'.format(self.host, self.port))

    def send(self, message):
        self.q.put(message)
        
    def run(self):
        self._connect()
        while not self.should_stop:
            message = self._build_message()
            if len(message) > 0:
                self._send_message(message)

    def stop(self):
        self.should_stop = True

    def _build_message(self):
        m = []
        timeout = 0.05 # 50ms
        now = last_time = time.time()

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
        self.socket.sendall(message)
        self.socket.send('\n')

_client = None

def connect(host, port):
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

    

