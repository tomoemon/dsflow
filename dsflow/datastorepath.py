# coding: utf-8
from __future__ import absolute_import
from __future__ import unicode_literals
from argparse import ArgumentTypeError
import re


class DatastorePath(object):
    # @see https://cloud.google.com/datastore/docs/best-practices

    def __init__(self, project, namespace, kinds):
        self.project = project
        self.namespace = "" if namespace == "@default" else namespace
        self.kinds = kinds

    def is_consistent_with(self, dst):
        if len(self.kinds) == 1:
            # 1つの src_kind は 1つの dst_kind または dst_namespace に割り当て可能
            return True
        else:
            # namespace に含まれるすべての kind、または複数の特定の kind は dst_namespace にのみ割当可能
            if dst.kinds:
                return False
            return True

    def path(self):
        if self.kinds:
            return "/{}/{}/{}".format(self.project, self.namespace, ",".join(self.kinds))
        return "/{}/{}".format(self.project, self.namespace)

    def __str__(self):
        return self.path()


class DatastoreSrcPath(DatastorePath):
    path_pattern = re.compile(
        r'^/([^/,]*)'  # project
        r'/([^/,]*)'  # namespace
        r'(?:/([^/,]+)(?:, *([^/,]+))*)?$'  # kinds
    )

    @property
    def has_multi_kinds(self):
        return not self.kinds or len(self.kinds) > 1

    @classmethod
    def parse(cls, string):
        match = cls.path_pattern.match(string)
        if not match:
            raise ArgumentTypeError(
                '{} must be formatted "/({{PROJECT}})/{{NAMESPACE}}(/{{KIND}}(,{{KIND}}...))"'.format(cls.__name__))
        groups = match.groups()
        project = groups[0]
        namespace = groups[1]
        kinds = [g for g in groups[2:] if g]
        return cls(project, namespace, kinds)


class DatastoreDstPath(DatastorePath):
    path_pattern = re.compile(
        r'^/([^/,]*)'  # project
        r'/([^/,]*)'  # namespace
        r'(?:/([^/,]+))?$'  # kind
    )

    @classmethod
    def parse(cls, string):
        match = cls.path_pattern.match(string)
        if not match:
            raise ArgumentTypeError(
                '{} must be formatted "/({{PROJECT}})/{{NAMESPACE}}(/{{KIND}})"'.format(cls.__name__))
        groups = match.groups()
        project = groups[0]
        namespace = groups[1]
        kind = groups[2]
        return cls(project, namespace, [kind] if kind else [])


if __name__ == '__main__':
    import sys
    print(DatastorePath.parse(sys.argv[1]))
