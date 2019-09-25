# -*- coding: utf-8 -*-
import apache_beam as beam
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.io.gcp.datastore.v1new.datastoreio import DeleteFromDatastore
import logging
from dsflow.lib.datastorepath import DatastoreSrcPath
from dsflow.lib.beamutil import create_multi_datasource_reader


class DeleteOptions(GoogleCloudOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('src', type=DatastoreSrcPath.parse)


class EntityToKey(beam.DoFn):
    def process(self, element):
        k = element.key
        p = list(k.path_elements)
        if len(p) % 2 != 0:
            # incomplete key
            return []
        if p[0].startswith('__'):
            return []
        return [k]


def run():
    from os import path
    import sys

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
