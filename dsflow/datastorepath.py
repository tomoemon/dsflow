# coding: utf-8
from __future__ import absolute_import
from __future__ import unicode_literals
from argparse import ArgumentTypeError
import re


class DatastorePath(object):
    # @see https://cloud.google.com/datastore/docs/best-practices
    path_pattern = re.compile(r'^/([^/]*)/([^/]+)(/([^/]+))?$')

    def __init__(self, project, namespace, kind):
        self.project = project
        self.namespace = namespace
        self.kind = kind

    def is_consistent_with(self, dst):
        if (self.kind and dst.kind) or (not self.kind and not dst.kind):
            return True
        return False

    @property
    def path(self):
        if self.kind:
            return "/{}/{}/{}".format(self.project, self.namespace, self.kind)
        return "/{}/{}".formats(self.project, self.namespace)

    @classmethod
    def validate(cls, string):
        datastore_path = cls.parse(string)
        return datastore_path.path

    @classmethod
    def parse(cls, string):
        match = cls.path_pattern.match(string)
        if not match:
            raise ArgumentTypeError("datastore path must be formatted /{PROJECT}/{NAMESPACE}/{KIND}")
        project = match.group(1)
        namespace = match.group(2)
        kind = match.group(4)
        return cls(project, namespace, kind)

    def __str__(self):
        return self.path


if __name__ == '__main__':
    import sys
    print(DatastorePath.parse(sys.argv[1]))
