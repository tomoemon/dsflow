#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals
import sys
import setuptools

needs_pytest = {'pytest', 'test', 'ptr'}.intersection(sys.argv)
pytest_runner = ['pytest-runner'] if needs_pytest else []

setuptools.setup(
    name='dsflow',
    version='0.2.1',
    author="tomoemon",
    install_requires=[
        "google-api-core",
        "google",
        "protobuf",  # see https://stackoverflow.com/questions/38680593/importerror-no-module-named-google-protobuf
        "apache_beam[gcp]",

        # apache_beam をインストールすると 4系が入ってしまうが、
        # そのあと googledatastore は4系未満を要求するためビルドエラーが起きる
        # 上記問題を回避するために先に3系を入れておく
        # なぜか install_requires に記載した下にあるものからインストールされる
        "oauth2client>=2.0.1,<4",
        "httplib2<0.10,>=0.9.1",
    ],
    setup_requires=[] + pytest_runner,
    tests_require=["pytest"],
    python_requires=">=2.7,<3",
    packages=setuptools.find_packages(),
    scripts=['bin/dsflow', 'bin/dsflowl'],
)
