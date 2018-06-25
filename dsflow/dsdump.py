# -*- coding: utf-8 -*-
import apache_beam as beam
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions
from apache_beam.io.gcp.datastore.v1.datastoreio import ReadFromDatastore, WriteToDatastore, DeleteFromDatastore
import logging
import itertools
from datastore.query import Query, _pb_from_query
from datastorepath import DatastorePath
from gcspath import GCSPath


"""
python dsflow/cmd.py dump \
-P my-dataflow-dev \
-T gs://my-dataflow-dev.appspot.com/temp \
-S gs://my-dataflow-dev.appspot.com/staging \
//experiment/TestStory2 \
gs://my-dataflow-dev.appspot.com/result_dump_.txt
"""


class RawFormat(beam.DoFn):
    def process(self, element):
        return [element]


class JsonFormat(beam.DoFn):
    def process(self, element):
        import json
        from datastore.helpers import entity_from_protobuf
        entity = entity_from_protobuf(element)
        entity["__key__"] = {
            "path": entity.key.path,
            "partition_id": {
                "project_id": entity.key.project,
                "namespace_id": entity.key.namespace,
            }
        }
        return [json.dumps(entity)]


class DumpOptions(GoogleCloudOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('src', type=DatastorePath.parse)
        parser.add_argument('output', type=GCSPath.parse)
        parser.add_argument('--keys-only', action="store_true", default=False)


def run():
    import sys
    args = sys.argv[1:]
    options = DumpOptions(args)
    query = Query(kind=options.src.kind)
    if options.keys_only:
        query.keys_only()
    query_pb = _pb_from_query(query)

    if not options.src.project:
        options.src.project = options.project
    if options.src.namespace == "default":
        options.src.namespace = ""

    # namespace を指定しない(==None)と [default] namespace が使われる
    p = beam.Pipeline(options=options)
    p | 'ReadFromDatastore' >> ReadFromDatastore(project=options.src.project, query=query_pb, namespace=options.src.namespace) \
      | 'Format' >> beam.ParDo(JsonFormat()) \
      | 'WriteToText' >> WriteToText(options.output.path)
    p.run().wait_until_finish()


if __name__ == '__main__':
    import logging
    logging.getLogger().setLevel(logging.INFO)
    run()
