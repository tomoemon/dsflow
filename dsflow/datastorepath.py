# coding: utf-8
import argparse
import re


class DatastorePath(object):
    # @see https://cloud.google.com/datastore/docs/best-practices
    path_pattern = re.compile(r'^/([^/]*)/([^/]+)(/([^/]+))?$')

    def __init__(self, project, namespace, kind):
        self.project = project if project else None
        self.namespace = namespace if namespace else None
        self.kind = kind if kind else None

    @classmethod
    def is_consistent(cls, src, dst):
        if (src.kind and dst.kind) or (not src.kind and not dst.kind):
            return True
        return False

    @property
    def path(self):
        project = self.project if self.project else ""
        namespace = self.namespace if self.namespace else ""
        if self.kind:
            return "/{}/{}/{}".format(project, namespace, self.kind)
        return "/{}/{}".format(project, namespace)

    @classmethod
    def parse(cls, string):
        match = cls.path_pattern.match(string)
        if not match:
            raise argparse.ArgumentTypeError("datastore path must be formatted /{PROJECT}/{NAMESPACE}/{KIND}")
        project = match.group(1)
        namespace = match.group(2)
        kind = match.group(4)
        return cls(project, namespace, kind)

    def __str__(self):
        return self.path


# >> > def perfect_square(string):
# ...     value = int(string)
# ...     sqrt = math.sqrt(value)
# ... if sqrt != int(sqrt):
# ...         msg = "%r is not a perfect square" % string
# ... raise argparse.ArgumentTypeError(msg)
# ... return value
# ...
# >> > parser = argparse.ArgumentParser(prog='PROG')
# >> > parser.add_argument('foo', type=perfect_square)
# >> > parser.parse_args(['9'])
# Namespace(foo=9)
# >> > parser.parse_args(['7'])
# usage: PROG[-h] foo
# PROG: error: argument foo: '7' is not a perfect square


if __name__ == '__main__':
    import sys
    print(DatastorePath.parse(sys.argv[1]))
