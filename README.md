# dsflowとは

Datastore を namespace/kind 単位で一括コピー、削除、リネームを行うためのコマンドラインツールです。
Apache Beam フレームワーク上で構築されており、ローカルマシンで動かすことも、Google Cloud Dataflow のジョブとして実行することもできます。

# インストール手順

下記、Google Cloud Shell での操作を前提としています。
Python 2.7, Google Cloud SDK がインストールされている環境であればどのマシンでも実行することができますが、Cloud Shell であれば環境依存等がほぼない状態で実行できます。

1. 処理を行いたい Datastore と同じプロジェクト内で Google Cloud Shell を開きます
1. 下記のコマンドを実行します
```sh
sudo pip install git+https://github.com/tomoemon/dsflow.git
```

※ Google Cloud Shell ではなく、個人PC 等で実行する場合に事前に必要なもの
- python 2.7.x
- Google Cloud SDK (`gcloud` command)
- application default credential (`gcloud auth application-default login`)


## インストールされるコマンド

PATH が通った場所に `dsflow`, `dsflowl` という2つのコマンドがインストールされます。下記2つのコマンドは実行環境が異なるだけで、実行できる内容は変わりません。

- `dsflow`
  - Datastore に対する処理を Dataflow ジョブとして実行します。Dataflow ジョブが立ち上げた GCE インスタンスと Datastore 間で通信を行います。
  - ※作業領域として GCS を使用するため、実行時に一時領域のパス等を指定する必要があります。
- `dsflowl`
  - Datastore に対する処理を「このコマンドを実行した」ローカルマシン上で実行します。ローカルマシンと Datastore 間で通信を行います。


# 注意

## 費用

Datastore は1エンティティ単位の読み書きで費用が発生します。大量のデータ操作を行う場合はあらかじめ対象のデータ量に注意してください。（[参考リンク](https://cloud.google.com/datastore/pricing?hl=ja)）



## 処理時間

Dataflow ジョブを実行すると、内部で GCE インスタンスが立ち上がり、インスタンスの初期化等が行われます。そのため、対象となるデータ量が少ない場合でもコマンド実行完了まで3分～5分程度は要します。
__件数が少ない（数万件程度まで）場合は Google Cloud Shell から dsflowl を実行することをお勧めします。__

## アトミック性

Datastore は大量のデータ一括操作をサポートしていないため、このツールのいずれのコマンドもアトミックに処理を行いません。例えばcopyコマンドを実行中に何らかのエラーが発生したら、一部の Entity や Kind のコピーだけが完了している中途半端な状況が発生する可能性があります。

# 使い方

## 共通オプション

- `-P --job_project (環境変数 DS_JOB_PROJECT でも指定可能)`
  - 実行プロジェクトID。`dsflow` コマンドの場合はこのプロジェクト内で Dataflow ジョブが立ち上がります。
- `src` / `dst` 
  - `/{PROJECT}/{NAMESPACE}/{KIND}` で表現する Datastore のパス。
  - `{PROJECT}` を省略して `//{NAMESPACE}/{KIND}` と表現した場合は `-P` で指定したプロジェクトが適用されます
  - `//{NAMESPACE}` のように KIND を省略して名前空間全体を指定することも可能です（名前空間ごとのコピーや削除を行う場合）
  - デフォルトネームスペースは `@default` と指定します
  - `src` と `dst` で異なる `{PROJECT}` を指定することも可能です。その場合は各プロジェクトの IAM 設定で、実行ユーザのアカウント (`dsflowl` の場合)、または Dataflow ジョブを実行する Service Accout  (`dsflow` の場合) に適切な権限を割り当ててください（[参考リンク](https://cloud.google.com/dataflow/security-and-permissions#google-cloud-platform-account)）

   

## dsflow コマンド用オプション

- `-T --temp_location  (環境変数 DS_TEMP_LOCATION でも指定可能)`
  - GCS のパス (`gs://{BUCKET}/{TEMP_PREFIX}`)。Dataflow のジョブ実行中の一時ファイル置き場
- `-S --staging_location  (環境変数 DS_STAGING_LOCATION でも指定可能)`
  - GCS のパス (`gs://{BUCKET}/{STAGING_PREFIX}`)。Dataflow のジョブ実行時に使用するパッケージのアップロード先

## サブコマンド群

下記サブコマンドが `dsflow`, `dsflowl` 双方で使用可能です。

- copy
- delete
- rename
- dump

## copy

```sh
dsflowl copy \
-P {PROJECT_NAME} \
{src_datastore_path} {dst_datastore_path}
```

※ `dsflow` コマンドを実行する場合は `-T`, `-S` オプションの指定が必要です。

- 例： `default` namespace に存在する `User` kind を `default` namespace の `User2` kind にコピーする場合

      dsflowl copy \
      -P {PROJECT_NAME} \
      //@default/User //@default/User2

- 例： `default` namespace に含まれるすべての kind を `staging` namespace にコピーする場合

      dsflowl copy \
      -P {PROJECT_NAME} \
      //@default //staging

- 例： `default` namespace に含まれる User と Log kind を `staging` namespace にコピーする場合

      dsflowl copy \
      -P {PROJECT_NAME} \
      //@default/User,Log //staging

- 例： `default` namespace に含まれるすべての kind を `staging` namespace にコピーする場合
※すでに存在する `staging` namespace を先にクリアしたい場合

      dsflowl copy \
      -P {PROJECT_NAME} \
      //@default //staging
      --clear_dst

## delete

```sh
dsflowl delete \
-P {PROJECT_NAME} \
{src_datastore_path}
```

`{src_datastore_path}` の指定方法は copy コマンドと同様。

## rename

```
dsflowl rename \
-P {PROJECT_NAME} \
{src_datastore_path} {dst_datastore_path}
```

`{src_datastore_path}` `{dst_datastore_path}` の指定方法は copy コマンドと同様。

注意：リネーム操作の実体は copy＋deleteです。アトミックにリネームをするわけではないので、途中でエラーが発生した場合はコピーだけされている可能性があります。

## dump コマンド

```
dsflowl rename \
-P {PROJECT_NAME} \
{src_datastore_path} {dst_path}
```

`{src_datastore_path}` の指定方法は copy コマンドと同様。`{dst_path}` は GCS パス (`gs://*`) またはローカルのファイルシステムのパス。

独自の方法でシリアライズした文字列を jsonl 形式（1行単位の json）で出力します。
- Timestamp型は isoformat の文字列に変換します
- キー型は `__key__` というプロパティ下に値をセットします

# Roadmap

- 1.0
  - README 整備（英語版も作る）
  - コマンドラインヘルプ 整備
- 1.x
  - json dump 結果のインポート
  - Property のリネーム、削除、コピー
  - datastore export コマンドで出力した PB ファイルのインポート（namespace 変更）
- 実装しない
  - Custom Template対応
    - 2018/06 時点で Custom Template は難しい（そもそも、Apache Beam の Datastore library が Custom Template のランタイムパラメータを受け取れるようになっておらず、かなり頑張ったが挙動がかなり怪しかったので当分やらない。また、Custom Template では1テンプレートにつき、1パイプラインしか実行できないので、Rename のような copy+delete の操作を表現できない）
