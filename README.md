# dsflowとは

Datastore の一括コピー、削除、リネームを行うためのコマンドラインユーティリティです。
Dataflow 上で各種処理を実行することで、大量のデータ操作に必要なスケールアウトやジョブ管理が簡単になります。

Datastore は1エンティティ単位の読み書きで費用が発生します。大量のデータ操作を行う場合は注意してください。


# 実行前に必要なもの

- Google Cloud SDK (`gcloud command`)
- Application default credential (`gcloud auth application-default login`)

# インストール

```sh
pip install git+https://github.com/tomoemon/dsflow.git
```

# 使い方

## 共通パラメータ
- `-P --job-project (環境変数 DS_JOB_PROJECT でも指定可能)`
  - 実行プロジェクトID。このプロジェクト内で Dataflow ジョブが立ち上がる。
- `-T --temp-location  (環境変数 DS_TEMP_LOCATION でも指定可能)`
  - GCS のパス (`gs://{BUCKET}/{TEMP_PREFIX}`)。Dataflow のジョブ実行中の一時ファイル置き場
- `-S --staging-location  (環境変数 DS_STAGING_LOCATION でも指定可能)`
  - GCS のパス (`gs://{BUCKET}/{STAGING_PREFIX}`)。Dataflow のジョブ実行時に使用するパッケージのアップロード先
- `src` / `dst` 
  - `/{PROJECT}/{NAMESPACE}/{KIND}` で表現する Datastore のパス。
  - `//{NAMESPACE}/{KIND}` と表現した場合は `-P` で指定したジョブ実行プロジェクトと同様のプロジェクトが使用される
  - `//{NAMESPACE}` のように KIND を省略して名前空間全体を指定することも可能（名前空間ごとのコピーや削除を行う場合）

## copyコマンド

```sh
dsflow copy \
-P {PROJECT_NAME} \
-T gs://{BUCKET}/{TEMPORARY_PREFIX} \
-S gs://{BUCKET}/{STAGING_PREFIX} \
{src_datastore_path} {dst_datastore_path}
```

例：ジョブを実行するプロジェクトと同じプロジェクト内で、
`default` namespace に存在する `User` kind を `default` namespace の `User2` kind にコピーする場合

```
dsflow copy \
-P {PROJECT_NAME} \
-T gs://{BUCKET}/{TEMPORARY_PREFIX} \
-S gs://{BUCKET}/{STAGING_PREFIX} \
//default/User //default/User2
```

例：ジョブを実行するプロジェクトと同じプロジェクト内で、
`default` namespace に含まれるすべての kind を `staging` namespace にコピーする場合

```
dsflow copy \
-P {PROJECT_NAME} \
-T gs://{BUCKET}/{TEMPORARY_PREFIX} \
-S gs://{BUCKET}/{STAGING_PREFIX} \
//default //staging
```

例：ジョブを実行するプロジェクトと同じプロジェクト内で、
`default` namespace に含まれるすべての kind を `staging` namespace にコピーする場合
※すでに存在する `staging` namespace を先にクリアしたい場合

```
dsflow copy \
-P {PROJECT_NAME} \
-T gs://{BUCKET}/{TEMPORARY_PREFIX} \
-S gs://{BUCKET}/{STAGING_PREFIX} \
//default //staging
--clear-dst
```

## deleteコマンド

```sh
dsflow delete \
-P {PROJECT_NAME} \
-T gs://{BUCKET}/{TEMPORARY_PREFIX} \
-S gs://{BUCKET}/{STAGING_PREFIX} \
{datastore_path}
```

## renameコマンド

```
dsflow rename \
-P {PROJECT_NAME} \
-T gs://{BUCKET}/{TEMPORARY_PREFIX} \
-S gs://{BUCKET}/{STAGING_PREFIX} \
{src_datastore_path} {dst_datastore_path}
```

# Roadmap

- confirm parameters before starting
