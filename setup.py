#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import sys
import setuptools

needs_pytest = {'pytest', 'test', 'ptr'}.intersection(sys.argv)
pytest_runner = ['pytest-runner'] if needs_pytest else []

setuptools.setup(
    name='dsflow',
    version='0.4.2',
    author="tomoemon",
    install_requires=[
        "apache_beam[gcp]==2.22.0",
    ],
    setup_requires=[] + pytest_runner,
    tests_require=["pytest"],
    python_requires=">=3.7",
    packages=setuptools.find_packages(),
    scripts=['bin/dsflow', 'bin/dsflowl'],
)
