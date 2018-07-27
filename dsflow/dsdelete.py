# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals
import apache_beam as beam
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.io.gcp.datastore.v1.datastoreio import DeleteFromDatastore
import logging
from dsflow.datastorepath import DatastoreSrcPath
from dsflow.beamutil import create_multi_datasource_reader


class DeleteOptions(GoogleCloudOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('src', type=DatastoreSrcPath.parse)


class EntityToKey(beam.DoFn):
    def process(self, element):
        e = element
        if e.key.path[-1].kind.startswith('__'):
            return []
        return[e.key]


def run():
    from os import path
    import sys

    # DirectRunner で実行した際に datastore パッケージを見つけるため
    sys.path.insert(0, path.dirname(path.abspath(__file__)))

    args = sys.argv[1:]
    options = DeleteOptions(args)

    if not options.src.project:
        options.src.project = options.project

    p = beam.Pipeline(options=options)
    sources = create_multi_datasource_reader(
        p, options.src.project, options.src.namespace, options.src.kinds, keys_only=True)

    sources | beam.Flatten() \
            | 'EntityToKey' >> beam.ParDo(EntityToKey()) \
            | 'DeleteFromDatastore' >> DeleteFromDatastore(options.src.project)
    p.run().wait_until_finish()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
