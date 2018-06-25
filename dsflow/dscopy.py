# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals
import apache_beam as beam
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.io.gcp.datastore.v1.datastoreio import ReadFromDatastore, WriteToDatastore
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


class ChangeKind(beam.DoFn):
    def __init__(self, to_project, to_namespace, to_kind):
        self.to_project = to_project
        self.to_namespace = to_namespace
        self.to_kind = to_kind

    def process(self, element):
        e = element
        if e.key.path[-1].kind.startswith('__'):
            return []
        e.key.partition_id.project_id = self.to_project
        e.key.partition_id.namespace_id = self.to_namespace
        e.key.path[-1].kind = self.to_kind
        return [e]


class ChangeNamespace(beam.DoFn):
    def __init__(self, to_project, to_namespace):
        self.to_project = to_project
        self.to_namespace = to_namespace

    def process(self, element):
        e = element
        if e.key.path[0].kind.startswith('__'):
            return []
        e.key.partition_id.project_id = self.to_project
        e.key.partition_id.namespace_id = self.to_namespace
        return [e]


class CopyOptions(GoogleCloudOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('src', type=DatastorePath.parse)
        parser.add_argument('dst', type=DatastorePath.parse)
        parser.add_argument('--clear-dst', action="store_true", default=False)


def run():
    from os import path
    import sys

    # DirectRunner で実行した際に datastore パッケージを見つけるため
    sys.path.insert(0, path.dirname(path.abspath(__file__)))

    args = sys.argv[1:]
    options = CopyOptions(args)
    query = Query(kind=options.src.kind)
    query_pb = _pb_from_query(query)

    if not options.src.project:
        options.src.project = options.project
    if options.src.namespace == "default":
        options.src.namespace = ""
    if not options.dst.project:
        options.dst.project = options.project
    if options.dst.namespace == "default":
        options.dst.namespace = ""

    if options.dst.kind:
        changer = ChangeKind(options.dst.project, options.dst.namespace, options.dst.kind)
    else:
        changer = ChangeNamespace(options.dst.project, options.dst.namespace)

    # namespace を指定しない(==None)と [default] namespace が使われる
    p = beam.Pipeline(options=options)
    p | 'ReadFromDatastore' >> ReadFromDatastore(project=options.src.project,
                                                 query=query_pb,
                                                 namespace=options.src.namespace) \
        | 'ChangeKey' >> beam.ParDo(changer) \
        | 'WriteToDatastore' >> WriteToDatastore(options.dst.project)
    p.run().wait_until_finish()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
