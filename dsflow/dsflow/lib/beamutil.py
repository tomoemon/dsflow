# coding: utf-8
from inspect import signature
import apache_beam as beam
from apache_beam.io.gcp.datastore.v1new.datastoreio import ReadFromDatastore
from google.cloud import datastore
from apache_beam.io.gcp.datastore.v1new.types import Query, Entity


def create_multi_datasource_reader(pipeline, project, namespace, kinds, keys_only=False):
    if not kinds:
        kinds = [None]

    sources = []
    for kind in kinds:
        # namespace を指定しない(==None)と [default] namespace が使われる
        query = Query(project=project, namespace=namespace, kind=kind)
        if keys_only:
            # see 
            # https://beam.apache.org/releases/pydoc/2.14.0/_modules/apache_beam/io/gcp/datastore/v1new/types.html#Query
            # https://google-cloud-python.readthedocs.io/en/0.32.0/_modules/google/cloud/datastore/query.html#Query.keys_only
            query.projection = ['__key__']
        if not kind:
            # kind を指定しない場合は明示的に __key__ asc でソートしないとエラーになる
            query.order = ['__key__']

        description = 'ReadFromDatastore kind={}'.format(kind if kind else "*")

        s = pipeline | description >> ReadFromDatastore(query=query)
        sources.append(s)
    return sources


class OptionalProcess(beam.DoFn):
    def __init__(self, mapper_statements):
        self.func_name = ""
        self.local_dict = {}
        self.mapper_statements = mapper_statements
        # validation only
        if mapper_statements:
            self.make_callable(mapper_statements)

    def make_callable(self, mapper_statements):
        local_dict = {}
        exec(mapper_statements, local_dict)
        callables = list(v for v in local_dict.values() if callable(v))
        if len(callables) != 1:
            raise Exception(
                "mapper code must have only 1 function in global namespace: "
                "{} functions defined".format(len(callables)))
        args = signature(callables[0]).parameters
        if len(args) != 1:
            raise Exception("mapper func must receive only 1 argument: receives {}".format(args))
        return callables[0], local_dict

    def process(self, element):
        if self.mapper_statements:
            # __init__ で下記の処理を行って mapper_statements から関数を生成し、
            # 関数の実体と local_dict をインスタンス変数に格納して、その後関数を process 関数内で実行すると、
            # なぜかその関数の中から str 等の __builtins__ 関数にアクセスできないため、
            # process の中で最初に初期化処理も実行している
            if not self.func_name:
                func, local_dict = self.make_callable(self.mapper_statements)
                self.func_name = func.__name__
                self.local_dict = local_dict

            self.local_dict["__dsflow_element__"] = element.to_client_entity()
            result = eval("{}(__dsflow_element__)".format(self.func_name), self.local_dict)
            return [Entity.from_client_entity(e) for e in result]
        return [element]
