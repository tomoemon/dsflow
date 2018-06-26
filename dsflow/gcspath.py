# coding: utf-8
from __future__ import absolute_import
from __future__ import unicode_literals
from argparse import ArgumentTypeError
import re


class GCSPath(object):
    # @see https://cloud.google.com/datastore/docs/best-practices
    path_pattern = re.compile(r'^gs://[^\n\r\t]+$')

    def __init__(self, path):
        self._path = path if path else ""

    @property
    def path(self):
        return self._path

    @classmethod
    def validate(cls, string):
        gcs_path = cls.parse(string)
        return gcs_path.path

    @classmethod
    def parse(cls, string):
        match = cls.path_pattern.match(string)
        if not match:
            raise ArgumentTypeError("gcs path must be formatted like gs://*")
        path = match.group(0)
        return cls(path)

    def __str__(self):
        return self.path


if __name__ == '__main__':
    import sys
    print(GCSPath.parse(sys.argv[1]))
