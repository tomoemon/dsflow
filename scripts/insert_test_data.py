# -*- coding: utf-8 -*-
import argparse
import random
from google.cloud import datastore


def insert(args):
    client = datastore.Client(args.project)
    entities = []
    for i in range(args.count):
        key = client.key(args.kind, "{}".format(i), namespace=args.namespace)
        entity = datastore.Entity(key)
        entity["Name"] = "great person {}".format(i * 10)
        entity["Email"] = "address{}@xyz.com".format(i * 2 * 10)
        entity["Age"] = random.randint(0, 99)
        entities.append(entity)
    client.put_multi(entities)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-P', '--project', required=True)
    parser.add_argument('-N', '--namespace', required=True)
    parser.add_argument('-K', '--kind', required=True)
    parser.add_argument('-C', '--count', type=int, default=5)
    args = parser.parse_args()
    insert(args)
