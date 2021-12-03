#!/usr/bin/env python
import setuptools

setuptools.setup(name='disbatch', version='2.0',
    description="Distributed processing of a batch of tasks",
    long_description=open("Readme.md").read(),
    packages=['disbatch'],
    package_data={'disbatch': ['dbUtil.sh']},
    scripts=['disBatch', 'disBatch.py', 'dbMon.py'])
