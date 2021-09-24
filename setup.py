#!/usr/bin/env python
import setuptools

setuptools.setup(name='disbatch', version='2.0-RC3',
    description="Distributed processing of a batch of tasks",
    long_description=open("Readme.md").read(),
    install_requires=['kvsstcp @ git+ssh://git@github.com/flatironinstitute/kvsstcp@1.2#egg=kvsstcp'],
    packages=['disbatch'],
    package_data={'disbatch': ['dbUtil.sh']},
    scripts=['disBatch', 'disBatch.py', 'dbMon.py'])
