#!/usr/bin/env python

from distutils.core import setup

setup(name='pystaggregator',
      version='0.1.1',
      author='Rob Tandy',
      author_email='rob.tandy@gmail.com',
      url='https://github.com/robtandy/pystaggregator',
      long_description="""
      python client to provide instrumentaiton that will send stats (timers 
      and counters) to staggregator (https://github.com/robtandy/staggregator),
      a stats aggregator for graphite, in the spirit of statsd.""",
      packages=['pystaggregator'],
)
