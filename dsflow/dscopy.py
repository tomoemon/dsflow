# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals
import apache_beam as beam
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.io.gcp.datastore.v1.datastoreio import WriteToDatastore
import logging
from dsflow.datastorepath import DatastoreSrcPath, DatastoreDstPath
from dsflow.beamutil import create_multi_datasource_reader


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
        parser.add_argument('src', type=DatastoreSrcPath.parse)
        parser.add_argument('dst', type=DatastoreDstPath.parse)


def run():
    from os import path
    import sys

    # DirectRunner で実行した際に datastore パッケージを見つけるため
    sys.path.insert(0, path.dirname(path.abspath(__file__)))

    args = sys.argv[1:]
    options = CopyOptions(args)

    if not options.src.project:
        options.src.project = options.project
    if not options.dst.project:
        options.dst.project = options.project

    if options.dst.kinds:
        changer = ChangeKind(options.dst.project, options.dst.namespace, options.dst.kinds[0])
    else:
        changer = ChangeNamespace(options.dst.project, options.dst.namespace)

    p = beam.Pipeline(options=options)
    sources = create_multi_datasource_reader(
        p, options.src.project, options.src.namespace, options.src.kinds)

    sources | beam.Flatten() \
            | 'ChangeKey' >> beam.ParDo(changer) \
            | 'WriteToDatastore' >> WriteToDatastore(options.dst.project)
    p.run().wait_until_finish()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
