# -*- coding: utf-8 -*-
import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.io.gcp.datastore.v1new.datastoreio import WriteToDatastore
from .lib.datastorepath import DatastoreSrcPath, DatastoreDstPath
from .lib.beamutil import create_multi_datasource_reader, OptionalProcess



class ChangeKey(beam.DoFn):
    def __init__(self, to_project, to_namespace, to_kind):
        self.to_project = to_project
        self.to_namespace = to_namespace
        self.to_kind = to_kind

    def process(self, element):
        return self.change_key(element, self.to_project, self.to_namespace, self.to_kind)

    @classmethod
    def change_key(cls, original_element, to_project, to_namespace, to_kind):
        # see https://beam.apache.org/releases/pydoc/2.14.0/_modules/apache_beam/io/gcp/datastore/v1new/types.html#Key
        k = original_element.key
        p = list(k.path_elements)
        if len(p) % 2 != 0:
            # incomplete key
            return []
        if p[0].startswith('__'):
            return []
        k.project = to_project
        k.namespace = to_namespace
        if to_kind:
            p[-2] = to_kind
            k.path_elements = tuple(p)
        return [original_element]


class CopyOptions(GoogleCloudOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('src', type=DatastoreSrcPath.parse)
        parser.add_argument('dst', type=DatastoreDstPath.parse)
        parser.add_argument('--mapper', type=str, default="")


def run():
    import sys

    args = sys.argv[1:]
    options = CopyOptions(args)

    if not options.src.project:
        options.src.project = options.project
    if not options.dst.project:
        options.dst.project = options.project

    kind = options.dst.kinds[0] if options.dst.kinds else None
    changer = ChangeKey(options.dst.project, options.dst.namespace, kind)

    p = beam.Pipeline(options=options)
    sources = create_multi_datasource_reader(
        p, options.src.project, options.src.namespace, options.src.kinds)

    sources | beam.Flatten() \
            | 'ChangeKey' >> beam.ParDo(changer) \
            | 'OptionalMapper' >> beam.ParDo(OptionalProcess(options.mapper)) \
            | 'WriteToDatastore' >> WriteToDatastore(options.dst.project)
    p.run().wait_until_finish()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
