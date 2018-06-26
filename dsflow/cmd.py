# -*- coding: utf-8 -*-
from __future__ import print_function
from __future__ import absolute_import
import os
from os import path
import argparse
from dsflow.envdefault import EnvDefault
from dsflow.datastorepath import DatastorePath
from dsflow.gcspath import GCSPath


class ArgumentError(Exception):
    pass


def format_dataflow_arg(args, parser, excepts=None):
    positional_list = []
    optional_dict = {}

    if not excepts:
        excepts = []

    excepts.extend(["help", "_handler", "_parser"])

    for a in parser._positionals._group_actions:
        if a.dest in excepts:
            continue
        positional_list.append(getattr(args, a.dest))

    for a in parser._optionals._group_actions:
        if a.dest in excepts:
            continue
        optional_dict[a.dest] = getattr(args, a.dest)

    for k, v in parser._defaults.items():
        if k in excepts:
            continue
        optional_dict[k] = v

    return format_command_arg(positional_list, optional_dict)


def format_command_arg(positional_list, optional_dict):
    positional = ""
    if positional_list:
        positional = '"' + '" "'.join(str(p).replace('"', '\\"') for p in positional_list) + '" '

    options = []
    for k, v in optional_dict.items():
        if isinstance(v, bool):
            if v:
                options.append("--" + k)
        else:
            options.append('--{} "{}"'.format(str(k), str(v)))
    return positional + " ".join(options)


def command_dump(args, parsers):
    arg_string = format_dataflow_arg(args, parsers["dump"])
    os.system("python -m dsflow.dsdump " + arg_string)


def command_copy(args, parsers):
    if not args.src.is_consistent_with(args.dst):
        raise ArgumentError('''can't copy from "{}" to "{}"'''.format(args.src.path, args.dst.path))

    if args.clear_dst:
        delete_args = argparse.Namespace(**vars(args))
        delete_args.src = args.dst
        command_delete(delete_args, parsers)

    arg_string = format_dataflow_arg(args, parsers["copy"], ["clear_dst"])
    os.system("python -m dsflow.dscopy " + arg_string)


def command_delete(args, parsers):
    arg_string = format_dataflow_arg(args, parsers["delete"])
    os.system("python -m dsflow.dsdelete " + arg_string)


def command_rename(args, parsers):
    command_copy(args, parsers)
    command_delete(args, parsers)


def add_dataflow_arguments(parser):
    parser.add_argument('-P', '--project', action=EnvDefault, envvar='DS_PROJECT')
    parser.add_argument('-T', '--temp-location', action=EnvDefault, envvar='DS_TEMP_LOCATION')
    parser.add_argument('-S', '--staging-location', action=EnvDefault, envvar='DS_STAGING_LOCATION')
    parser.add_argument('-R', '--runner', action=EnvDefault,
                        choices=["DataflowRunner", "DirectRunner"], envvar='DS_RUNNER', default="DataflowRunner")

    # "setup.py" という名前のファイルじゃないとエラーになる
    runtime_setup_path = path.join(path.dirname(path.abspath(__file__)), "setup.py")
    parser.set_defaults(setup_file=runtime_setup_path)


def parse():
    parser = argparse.ArgumentParser(description='dsflow supports data maintainance on cloud datastore')
    subparsers = parser.add_subparsers()

    # dump command parser
    parser_dump = subparsers.add_parser('dump', help='dump namespace or kind')
    parser_dump.add_argument('src', type=DatastorePath.parse)
    parser_dump.add_argument('dst', type=GCSPath.parse)
    parser_dump.add_argument('--keys-only', action="store_true", default=False)
    parser_dump.add_argument('--format', choices=["json", "raw"], default="json")
    add_dataflow_arguments(parser_dump)
    parser_dump.set_defaults(_handler=command_dump)

    # copy command parser
    parser_copy = subparsers.add_parser('copy', help='copy namespace or kind')
    parser_copy.add_argument('src', type=DatastorePath.parse)
    parser_copy.add_argument('dst', type=DatastorePath.parse)
    parser_copy.add_argument('--clear-dst', action="store_true", default=False)
    add_dataflow_arguments(parser_copy)
    parser_copy.set_defaults(_handler=command_copy)

    # rename command parser
    parser_rename = subparsers.add_parser('rename', help='rename namespace or kind')
    parser_rename.add_argument('src', type=DatastorePath.parse)
    parser_rename.add_argument('dst', type=DatastorePath.parse)
    parser_rename.add_argument('--clear-dst', action="store_true", default=False)
    add_dataflow_arguments(parser_rename)
    parser_rename.set_defaults(_handler=command_rename)

    # delete command parser
    parser_delete = subparsers.add_parser('delete', help='delete namespace or kind')
    parser_delete.add_argument('src', type=DatastorePath.parse)
    add_dataflow_arguments(parser_delete)
    parser_delete.set_defaults(_handler=command_delete)

    args = parser.parse_args()
    if hasattr(args, '_handler'):
        try:
            args._handler(args, subparsers.choices)
        except ArgumentError as e:
            parser.print_help()
            print(u"\nError: " + unicode(e))
    else:
        parser.print_help()


def run():
    parse()


if __name__ == '__main__':
    run()
