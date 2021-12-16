#!/usr/bin/env python
import setuptools

setuptools.setup(name='disbatch', version='2.1',
    description="Distributed processing of a batch of tasks",
    long_description=open("Readme.md").read(),
    packages=setuptools.find_packages(),
    package_data={'disbatch': ['dbMon.py', 'dbUtil.sh']},
    scripts=['disBatch', 'disBatch.py'])
