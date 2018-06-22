# -*- coding: utf-8 -*-
import apache_beam as beam
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.datastore.v1.datastoreio import ReadFromDatastore, WriteToDatastore, DeleteFromDatastore
import logging
import itertools
from lib.query import Query


"""
# run on dataflow
python ./dump.py \
 --setup_file ./setup.py \
 --project=my-dataflow-dev \
 --runner=DataflowRunner \
 --staging_location=gs://my-dataflow-dev.appspot.com/staging \
 --temp_location=gs://my-dataflow-dev.appspot.com/temp \
 --source_project=my-dataflow-207812 \
 --namespace=experiment \
 --kind=TestStory \
 --output=gs://my-dataflow-dev.appspot.com/result_dump.txt

# run local
python ./dump.py \
 --source_project=my-dataflow-207812 \
 --namespace=experiment \
 --kind=TestStory \
 --output=gs://my-dataflow-dev.appspot.com/result_dump.txt

# staging
cd templates/datastore/dump
python ./dump.py \
 --project=my-dataflow-dev \
 --output=gs://my-dataflow-dev.appspot.com/result_dump.txt \
 --runner=DataflowRunner \
 --setup_file ./setup.py \
 --staging_location=gs://my-dataflow-dev.appspot.com/staging \
 --template_location=gs://my-dataflow-dev.appspot.com/templates/dump \
 --temp_location=gs://my-dataflow-dev.appspot.com/temp
"""


class Format(beam.DoFn):
    def process(self, element):
        return [element]

 # --setup_file ./setup.py \
 # --staging_location=gs://my-dataflow-dev.appspot.com/staging \
 # --project=my-dataflow-dev \
 # --runner=DataflowRunner \
 # --temp_location=gs://my-dataflow-dev.appspot.com/temp \


"""
python dsflow/cmd.py dump \
-P my-dataflow-dev \
-T gs://my-dataflow-dev.appspot.com/temp \
-S gs://my-dataflow-dev.appspot.com/staging \
-p my-dataflow-207812 \
-n experiment \
-k TestStory \
-o gs://my-dataflow-dev.appspot.com/result_dump.txt
"""


class DumpOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('--source_namespace', required=True)
        parser.add_argument('--source_kind', required=True)
        parser.add_argument('--source_project', required=True)
        parser.add_argument('--output', required=True)

# def run(setup_file, staging_location, temp_location, project,
#        source_namespace, source_kind, source_project, output):
#    _locals = locals()
#    _locals["runner"] = "DataflowRunner"
#options = PipelineOptions.from_dictionary(_locals)


def run():
    import sys
    args = sys.argv + ["--runner", "DataflowRunner"]
    options = DumpOptions(args)

    print(args)
    #source_kind = args[args.index("--source_kind")+1]
    #source_project = args[args.index("--source_project")+1]
    #source_namespace = args[args.index("--source_namespace")+1]
    #output = args[args.index("--output")+1]
    source_kind = options.source_kind
    source_project = options.source_project
    source_namespace = options.source_namespace
    output = options.output

    query = Query(kind=source_kind)
    query.keys_only()
    query_pb = query.pb

    p = beam.Pipeline(options=options)
    # namespace を指定しない(==None)と default の namespace が使われる
    p | 'ReadFromDatastore' >> ReadFromDatastore(project=source_project, query=query_pb, namespace=source_namespace) \
      | 'Format' >> beam.ParDo(Format()) \
      | 'WriteToText' >> WriteToText(output)
    p.run().wait_until_finish()


if __name__ == '__main__':
    run()
