# -*- coding: utf-8 -*-
from __future__ import print_function
from __future__ import absolute_import
import argparse
from google.cloud import datastore


def insert(args):
    client = datastore.Client(args.project)

    for i in range(args.count):
        key = client.key(args.kind, "{}".format(i), namespace=args.namespace)
        entity = datastore.Entity(key)
        entity["Name"] = u"great person {}".format(i * 10)
        entity["Email"] = u"address{}@xyz.com".format(i * 2 * 10)
        client.put(entity)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-P', '--project', required=True)
    parser.add_argument('-N', '--namespace', required=True)
    parser.add_argument('-K', '--kind', required=True)
    parser.add_argument('-C', '--count', type=int, default=5)
    args = parser.parse_args()
    insert(args)
