# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals
import apache_beam as beam
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions
from apache_beam.io.gcp.datastore.v1.datastoreio import ReadFromDatastore, WriteToDatastore, DeleteFromDatastore
import logging
import itertools
from dsflow.datastore.query import Query, _pb_from_query
from dsflow.datastorepath import DatastorePath
from dsflow.gcspath import GCSPath


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
    import sys
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
    p | 'ReadFromDatastore' >> ReadFromDatastore(project=options.src.project, query=query_pb, namespace=options.src.namespace) \
      | 'EntityToKey' >> beam.ParDo(EntityToKey()) \
      | 'DeleteFromDatastore' >> DeleteFromDatastore(options.src.project)
    p.run().wait_until_finish()


if __name__ == '__main__':
    import logging
    logging.getLogger().setLevel(logging.INFO)
    run()
