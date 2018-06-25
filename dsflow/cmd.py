# -*- coding: utf-8 -*-
from __future__ import print_function
from __future__ import absolute_import
import os
from os import path
import argparse
from dsflow.envdefault import EnvDefault
from dsflow.datastorepath import DatastorePath
from dsflow.gcspath import GCSPath


def format_command_arg(positional_list, optional_dict):
    positional = ""
    if positional_list:
        positional = '"' + '" "'.join(positional_list) + '"' + " "

    options = []
    for k, v in optional_dict.items():
        if isinstance(v, bool):
            if v == True:
                options.append("--" + k)
        else:
            options.append('--{} "{}"'.format(k, v))
    return positional + " ".join(options)


def command_dump(args):
    arg_string = format_command_arg(
        [args.src.path, args.output.path],
        {
            "keys_only": args.keys_only,
        }
    ) + format_dataflow_arguments(args)

    print("dump: " + arg_string)
    os.system("python -m dsflow.dsdump " + arg_string)


def command_copy(args):
    if not DatastorePath.is_consistent(args.src, args.dst):
        raise Exception(u'''can't copy from "{}" to "{}"'''.format(args.src.path, args.dst.path))

    arg_string = format_command_arg(
        [args.src.path, args.dst.path], {}
    ) + format_dataflow_arguments(args)

    if args.clear_dst:
        delete_args = argparse.Namespace(**vars(args))
        delete_args.src = args.dst
        command_delete(delete_args)

    print("copy: " + arg_string)
    os.system("python -m dsflow.dscopy " + arg_string)


def command_delete(args):
    arg_string = format_command_arg(
        [args.src.path], {}
    ) + format_dataflow_arguments(args)

    print("delete: " + arg_string)
    os.system("python -m dsflow.dsdelete " + arg_string)


def command_rename(args):
    if not DatastorePath.is_consistent(args.src, args.dst):
        raise Exception(u'''can't rename from "{}" to "{}"'''.format(args.src.path, args.dst.path))

    command_copy(args)
    command_delete(args)


def add_dataflow_arguments(parser):
    parser.add_argument('-P', '--job-project', action=EnvDefault, envvar='DS_JOB_PROJECT')
    parser.add_argument('-T', '--temp-location', action=EnvDefault, envvar='DS_TEMP_LOCATION')
    parser.add_argument('-S', '--staging-location', action=EnvDefault, envvar='DS_STAGING_LOCATION')
    parser.add_argument('-R', '--runner', action=EnvDefault, envvar='DS_RUNNER', default="DataflowRunner")


def format_dataflow_arguments(args):
    # "setup.py" という名前のファイルじゃないとエラーになる
    runtime_setup_path = path.join(path.dirname(path.abspath(__file__)), "setup.py")
    return format_command_arg([], {
        "setup_file": runtime_setup_path,
        "staging_location": args.staging_location,
        "temp_location": args.temp_location,
        "project": args.job_project,
        "runner": args.runner,
    })


def parse():
    parser = argparse.ArgumentParser(description='dsflow')
    subparsers = parser.add_subparsers()

    # dump command parser
    parser_dump = subparsers.add_parser('dump', help='see `add -h`')
    parser_dump.add_argument('src', type=DatastorePath.parse)
    parser_dump.add_argument('output', type=GCSPath.parse, help='all files')
    parser_dump.add_argument('--keys-only', action="store_true", default=False, help='all files')
    add_dataflow_arguments(parser_dump)
    parser_dump.set_defaults(handler=command_dump)

    # copy command parser
    parser_copy = subparsers.add_parser('copy', help='see `add -h`')
    parser_copy.add_argument('src', type=DatastorePath.parse)
    parser_copy.add_argument('dst', type=DatastorePath.parse)
    parser_copy.add_argument('--clear-dst', action="store_true", default=False, help='all files')
    add_dataflow_arguments(parser_copy)
    parser_copy.set_defaults(handler=command_copy)

    # rename command parser
    parser_rename = subparsers.add_parser('rename', help='see `add -h`')
    parser_rename.add_argument('src', type=DatastorePath.parse)
    parser_rename.add_argument('dst', type=DatastorePath.parse)
    parser_rename.add_argument('--clear-dst', action="store_true", default=False, help='all files')
    add_dataflow_arguments(parser_rename)
    parser_rename.set_defaults(handler=command_rename)

    # delete command parser
    parser_delete = subparsers.add_parser('delete', help='see `add -h`')
    parser_delete.add_argument('src', type=DatastorePath.parse)
    add_dataflow_arguments(parser_delete)
    parser_delete.set_defaults(handler=command_delete)

    args = parser.parse_args()
    if hasattr(args, 'handler'):
        try:
            args.handler(args)
        except Exception as e:
            parser.print_help()
            print(u"\nError: " + unicode(e))
    else:
        parser.print_help()


def run():
    parse()


if __name__ == '__main__':
    run()
