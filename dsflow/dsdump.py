# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals
import apache_beam as beam
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions
from apache_beam.io.gcp.datastore.v1.datastoreio import ReadFromDatastore
import logging
from dsflow.datastore.query import Query, _pb_from_query
from dsflow.datastorepath import DatastorePath
from dsflow.gcspath import GCSPath


class RawFormat(beam.DoFn):
    def process(self, element):
        return [element]


class JsonFormat(beam.DoFn):
    @classmethod
    def format_types(cls, obj):
        import datetime
        # ジョブ内で実行される場合は dsflow というパッケージが存在しない
        # パッケージングされるのはこのファイルの階層以下
        from datastore.key import Key

        if isinstance(obj, datetime.datetime):
            return obj.isoformat()
        elif isinstance(obj, Key):
            return {
                "__key__": cls.format_key(obj)
            }

        raise TypeError('Not sure how to serialize %s' % (obj,))

    @classmethod
    def format_key(cls, key):
        return {
            "path": key.path,
            "partition_id": {
                "project_id": key.project,
                "namespace_id": key.namespace,
            }
        }

    def process(self, element):
        import json
        from datastore.helpers import entity_from_protobuf

        entity = entity_from_protobuf(element)
        entity["__key__"] = self.format_key(entity.key)
        return [json.dumps(entity, default=self.format_types)]


class DumpOptions(GoogleCloudOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('src', type=DatastorePath.parse)
        parser.add_argument('dst', type=GCSPath.parse)
        parser.add_argument('--format', choices=["json", "raw"], default="json")
        parser.add_argument('--keys-only', action="store_true", default=False)


def run():
    from os import path
    import sys

    # DirectRunner で実行した際に datastore パッケージを見つけるため
    sys.path.insert(0, path.dirname(path.abspath(__file__)))

    args = sys.argv[1:]

    pipeline_options = PipelineOptions(args)
    options = pipeline_options.view_as(DumpOptions)

    query = Query(kind=options.src.kind)
    if options.keys_only:
        query.keys_only()
    query_pb = _pb_from_query(query)

    if not options.src.project:
        options.src.project = options.project
    if options.src.namespace == "default":
        options.src.namespace = ""

    if options.format == "json":
        formatter = JsonFormat()
    else:
        formatter = RawFormat()

    # namespace を指定しない(==None)と [default] namespace が使われる
    p = beam.Pipeline(options=pipeline_options)
    p | 'ReadFromDatastore' >> ReadFromDatastore(project=options.src.project,
                                                 query=query_pb,
                                                 namespace=options.src.namespace) \
        | 'Format' >> beam.ParDo(formatter) \
        | 'WriteToText' >> WriteToText(options.dst.path)
    p.run().wait_until_finish()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
