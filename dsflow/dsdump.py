# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals
import apache_beam as beam
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import GoogleCloudOptions
import logging
from dsflow.datastore.helpers import entity_from_protobuf
from dsflow.datastorepath import DatastoreSrcPath
from dsflow.beamutil import create_multi_datasource_reader, OptionalProcess


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

        entity = entity_from_protobuf(element)
        entity["__key__"] = self.format_key(entity.key)
        return [json.dumps(entity, default=self.format_types)]


class DumpOptions(GoogleCloudOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('src', type=DatastoreSrcPath.parse)
        parser.add_argument('dst')
        parser.add_argument('--format', choices=["json", "raw"], default="json")
        parser.add_argument('--keys_only', action="store_true", default=False)
        parser.add_argument('--mapper', type=str, default="")


def run():
    from os import path
    import sys

    # DirectRunner で実行した際に datastore パッケージを見つけるため
    sys.path.insert(0, path.dirname(path.abspath(__file__)))

    args = sys.argv[1:]

    options = DumpOptions(args)

    if not options.src.project:
        options.src.project = options.project

    if options.format == "json":
        formatter = JsonFormat()
    else:
        formatter = RawFormat()

    p = beam.Pipeline(options=options)
    sources = create_multi_datasource_reader(
        p, options.src.project, options.src.namespace, options.src.kinds, options.keys_only)

    sources | beam.Flatten() \
            | 'OptionalMapper' >> beam.ParDo(OptionalProcess(options.mapper)) \
            | 'Format' >> beam.ParDo(formatter) \
            | 'WriteToText' >> WriteToText(options.dst)
    p.run().wait_until_finish()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
