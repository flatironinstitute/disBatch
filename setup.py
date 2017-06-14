#!/usr/bin/env python
from distutils.core import setup
setup(name='disbatch', version='0',
    description="Distributed processing of a batch of tasks",
    long_description=open("Readme.md").read(),
    requires=['kvsstcp'],
    scripts=['disBatch.py'])
