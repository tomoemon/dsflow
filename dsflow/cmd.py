# -*- coding: utf-8 -*-
from __future__ import print_function
from __future__ import absolute_import
import os
from os import path
import argparse


"""
python sample_dataflow.py \
 --setup_file ./setup.py \
 --staging_location=gs://my-dataflow-dev.appspot.com/staging \
 --template_location=gs://my-dataflow-dev.appspot.com/templates/sample \
 --project=my-dataflow-dev \
 --runner=DataflowRunner \
 --temp_location=gs://my-dataflow-dev.appspot.com/temp \
 """


def command_dump(args):
    runtime_setup_path = path.join(path.dirname(path.abspath(__file__)), "setup.py")
    # dump.run(setup_file=runtime_setup_path, staging_location=args.staging_location,
    #         temp_location=args.temp_location, project=args.job_project,
    #         source_project=args.project, source_namespace=args.namespace, source_kind=args.kind,
    #         output=args.output)
    args = " ".join(["--"+k+' "'+v+'"' for k, v in {
        "setup_file": runtime_setup_path,
        "staging_location": args.staging_location,
        "temp_location": args.temp_location,
        "project": args.job_project,
        "source_project": args.project,
        "source_namespace": args.namespace,
        "source_kind": args.kind,
        "output": args.output
    }.items()])
    print(args)
    os.system("python -m dsflow.dump " + args)


def command_commit(args):
    print(args)


def command_help(args):
    print(parser.parse_args([args.command, '--help']))


class EnvDefault(argparse.Action):
    def __init__(self, envvar, required=True, default=None, **kwargs):
        if not default and envvar:
            if envvar in os.environ:
                default = os.environ[envvar]
        if required and default:
            required = False
        super(EnvDefault, self).__init__(default=default, required=required,
                                         **kwargs)

    def __call__(self, parser, namespace, values, option_string=None):
        setattr(namespace, self.dest, values)


def add_dataflow_arguments(parser):
    parser.add_argument('-P', '--job-project', action=EnvDefault, envvar='DS_JOB_PROJECT')
    parser.add_argument('-T', '--temp-location', action=EnvDefault, envvar='DS_TEMP_LOCATION')
    parser.add_argument('-S', '--staging-location', action=EnvDefault, envvar='DS_STAGING_LOCATION')


def parse():
    # コマンドラインパーサーを作成
    parser = argparse.ArgumentParser(description='Fake git command')
    subparsers = parser.add_subparsers()

    # add コマンドの parser を作成
    parser_dump = subparsers.add_parser('dump', help='see `add -h`')
    parser_dump.add_argument('-p', '--project', help='all files', required=True)
    parser_dump.add_argument('-n', '--namespace', help='all files', required=True)
    parser_dump.add_argument('-k', '--kind', help='all files', required=True)
    parser_dump.add_argument('-o', '--output', help='all files', required=True)
    add_dataflow_arguments(parser_dump)
    parser_dump.set_defaults(handler=command_dump)

    # コマンドライン引数をパースして対応するハンドラ関数を実行
    args = parser.parse_args()
    if hasattr(args, 'handler'):
        args.handler(args)
    else:
        # 未知のサブコマンドの場合はヘルプを表示
        parser.print_help()


def run():
    print("dsflow started")
    parse()


if __name__ == '__main__':
    run()
