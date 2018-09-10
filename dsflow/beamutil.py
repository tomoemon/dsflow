# coding: utf-8
from __future__ import absolute_import
from __future__ import unicode_literals
import apache_beam as beam
from apache_beam.io.gcp.datastore.v1.datastoreio import ReadFromDatastore
try:
    from inspect import signature
except ImportError:
    from funcsigs import signature
from dsflow.datastore.helpers import entity_from_protobuf, entity_to_protobuf
from dsflow.datastore.query import Query, _pb_from_query


def create_multi_datasource_reader(pipeline, project, namespace, kinds, keys_only=False):
    if not kinds:
        kinds = [None]

    sources = []
    for kind in kinds:
        query = Query(kind=kind)
        if (keys_only):
            query.keys_only()
        query_pb = _pb_from_query(query)

        description = 'ReadFromDatastore kind={}'.format(kind if kind else "*")

        # namespace を指定しない(==None)と [default] namespace が使われる
        s = pipeline | description >> ReadFromDatastore(project=project,
                                                        query=query_pb,
                                                        namespace=namespace)
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

            self.local_dict["__dsflow_element__"] = entity_from_protobuf(element)
            result = eval("{}(__dsflow_element__)".format(self.func_name), self.local_dict)
            return [entity_to_protobuf(e) for e in result]
        return [element]
