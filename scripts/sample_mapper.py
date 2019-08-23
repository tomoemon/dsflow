# coding: utf-8


def process(entity):
    # entity: google.cloud.datastore.entity.Entity
    # see https://googleapis.github.io/google-cloud-python/latest/datastore/entities.html
    # 任意のプロパティの値を書き換え、追加、または削除してリスト形式で返してください
    # 複数の entity を返した場合は、それらがすべて copy, dump 対象になります

    entity["HogeProperty"] = "hogehoge"

    if "HogeTag" in entity:
        del entity["HogeTag"]

    return [entity]
