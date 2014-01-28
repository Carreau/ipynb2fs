#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys


try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

if sys.argv[-1] == 'publish':
    os.system('python setup.py sdist upload')
    sys.exit()

readme = open('README.rst').read()
history = open('HISTORY.rst').read().replace('.. :changelog:', '')

setup(
    name='ipynb2fs',
    version='0.1.0',
    description='a Fuse filesyteme that expose ipython notebook as folders',
    long_description=readme + '\n\n' + history,
    author='Matthias Bussonnier',
    author_email='bussonniermatthias@gmail.com',
    url='https://github.com/Carreau/ipynb2fs',
    packages=[
        'ipynb2fs',
    ],
    package_dir={'ipynb2fs': 'ipynb2fs'},
    include_package_data=True,
    install_requires=[
    ],
    license="BSD",
    zip_safe=False,
    keywords='ipynb2fs',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Natural Language :: English',
        "Programming Language :: Python :: 2",
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.3',
    ],
    test_suite='tests',
)