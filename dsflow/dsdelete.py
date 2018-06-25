# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals
import apache_beam as beam
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.io.gcp.datastore.v1.datastoreio import ReadFromDatastore, DeleteFromDatastore
import logging
from dsflow.datastore.query import Query, _pb_from_query
from dsflow.datastorepath import DatastorePath


"""
python dsflow/cmd.py copy \
-P my-dataflow-dev \
-T gs://my-dataflow-dev.appspot.com/temp \
-S gs://my-dataflow-dev.appspot.com/staging \
//experiment/TestStory2 \
//experiment/TestStory3
"""


class DeleteOptions(GoogleCloudOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('src', type=DatastorePath.parse)


class EntityToKey(beam.DoFn):
    def process(self, element):
        return[element.key]


def run():
    from os import path
    import sys

    # DirectRunner で実行した際に datastore パッケージを見つけるため
    sys.path.insert(0, path.dirname(path.abspath(__file__)))

    args = sys.argv[1:]
    options = DeleteOptions(args)
    query = Query(kind=options.src.kind)
    query.keys_only()
    query_pb = _pb_from_query(query)

    if not options.src.project:
        options.src.project = options.project
    if options.src.namespace == "default":
        options.src.namespace = ""

    # namespace を指定しない(==None)と [default] namespace が使われる
    p = beam.Pipeline(options=options)
    p | 'ReadFromDatastore' >> ReadFromDatastore(project=options.src.project,
                                                 query=query_pb,
                                                 namespace=options.src.namespace) \
        | 'EntityToKey' >> beam.ParDo(EntityToKey()) \
        | 'DeleteFromDatastore' >> DeleteFromDatastore(options.src.project)
    p.run().wait_until_finish()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
