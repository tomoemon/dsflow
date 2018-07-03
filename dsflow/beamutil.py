# coding: utf-8
from __future__ import absolute_import
from __future__ import unicode_literals
from apache_beam.io.gcp.datastore.v1.datastoreio import ReadFromDatastore
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
