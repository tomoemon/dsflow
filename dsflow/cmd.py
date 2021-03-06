# -*- coding: utf-8 -*-
import os
from os import path
import sys
import argparse
from .dsflow.lib.envdefault import EnvDefault
from .dsflow import dsdump
from .dsflow import dscopy
from .dsflow import dsdelete


class ArgumentError(Exception):
    pass


def format_dataflow_arg(args, parser, excepts=None):
    positional_list = []
    optional_dict = {}

    if not excepts:
        excepts = []

    excepts.extend(["help", "_formatter"])

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
    for k, v in sorted(optional_dict.items()):
        if not v:
            continue
        if isinstance(v, bool):
            options.append("--" + k)
        else:
            options.append('--{} "{}"'.format(str(k), str(v).replace('"', '\\"')))
    return positional + " ".join(options)


def command_dump(args, parsers):
    arg_string = format_dataflow_arg(args, parsers["dump"])
    return [sys.executable + " -m dsflow.dsdump " + arg_string]


def command_copy(args, parsers):
    if not args.src.is_consistent_with(args.dst):
        raise ArgumentError('''can't copy from "{}" to "{}"'''.format(str(args.src), str(args.dst)))

    delete_command = []
    if args.clear_dst:
        delete_args = argparse.Namespace(**vars(args))
        delete_args.src = args.dst
        delete_command = command_delete(delete_args, parsers)

    arg_string = format_dataflow_arg(args, parsers["copy"], ["clear_dst"])
    return delete_command + [sys.executable + " -m dsflow.dscopy " + arg_string]


def command_delete(args, parsers):
    arg_string = format_dataflow_arg(args, parsers["delete"])
    return [sys.executable + " -m dsflow.dsdelete " + arg_string]


def command_rename(args, parsers):
    return command_copy(args, parsers) + command_delete(args, parsers)


def add_dataflow_arguments(parser, is_direct_runner):
    parser.add_argument('-P', '--project', action=EnvDefault, envvar='DS_PROJECT')

    if is_direct_runner:
        parser.set_defaults(runner='DirectRunner')
    else:
        parser.add_argument('-T', '--temp_location', action=EnvDefault, envvar='DS_TEMP_LOCATION')
        parser.add_argument('-S', '--staging_location', action=EnvDefault, envvar='DS_STAGING_LOCATION')
        parser.add_argument('-R', '--region', action=EnvDefault, envvar='DS_REGION')
        parser.add_argument('--service_account_email',
                            default=None,
                            help='Identity to run virtual machines as.')
        parser.add_argument('--no_auth', dest='no_auth', type=bool, default=False)
        parser.add_argument('--job_name',
                            default=None,
                            help='Name of the Cloud Dataflow job.')
        parser.set_defaults(runner='DataflowRunner')

    # "setup.py" という名前のファイルじゃないとエラーになる
    runtime_setup_path = path.join(path.dirname(path.abspath(__file__)), "setup.py")
    parser.set_defaults(setup_file=runtime_setup_path)


def parse(args, is_direct_runner):
    parser = argparse.ArgumentParser(description='dsflow supports data maintainance on cloud datastore')
    subparsers = parser.add_subparsers()

    # dump command parser
    parser_dump = subparsers.add_parser('dump', help='dump namespace or kind')
    dsdump.DumpOptions._add_argparse_args(parser_dump)
    add_dataflow_arguments(parser_dump, is_direct_runner)
    parser_dump.set_defaults(_formatter=command_dump)

    # copy command parser
    parser_copy = subparsers.add_parser('copy', help='copy namespace or kind')
    dscopy.CopyOptions._add_argparse_args(parser_copy)
    parser_copy.add_argument('--clear_dst', action="store_true", default=False)
    add_dataflow_arguments(parser_copy, is_direct_runner)
    parser_copy.set_defaults(_formatter=command_copy)

    # rename command parser
    parser_rename = subparsers.add_parser('rename', help='rename namespace or kind')
    dscopy.CopyOptions._add_argparse_args(parser_rename)
    parser_rename.add_argument('--clear_dst', action="store_true", default=False)
    add_dataflow_arguments(parser_rename, is_direct_runner)
    parser_rename.set_defaults(_formatter=command_rename)

    # delete command parser
    parser_delete = subparsers.add_parser('delete', help='delete namespace or kind')
    dsdelete.DeleteOptions._add_argparse_args(parser_delete)
    add_dataflow_arguments(parser_delete, is_direct_runner)
    parser_delete.set_defaults(_formatter=command_delete)

    args = parser.parse_args(args)
    if hasattr(args, '_formatter'):
        try:
            return args._formatter(args, subparsers.choices)
        except ArgumentError as e:
            parser.print_help()
            print(u"\nError: " + str(e))
    else:
        parser.print_help()
    return []


def run(args):
    # remove command name itself
    args = args[1:]
    commands = parse(args, is_direct_runner=False)
    execute_commands(commands)


def run_local(args):
    # remove command name itself
    args = args[1:]
    commands = parse(args, is_direct_runner=True)
    execute_commands(commands)


def execute_commands(commands):
    # ホスト環境上にインストールされたパッケージのパスに合わせて
    # 素直に
    #     python -m dsflow.dsflow.dscopy
    # と実行すると、リモート環境上でも同様に dsflow.dsflow.dscopy という
    # 名前で実行しようとし、Module not found になってしまう
    # ※なぜならリモート環境上では実行時に指定した setup.py ファイル以下の階層が
    # 　パッケージングされてデプロイされるため、dsflow.dscopy という階層になる
    # そのため、ホスト環境上でも
    #     python -m dsflow.dscopy
    # と実行する必要があり、dsflow.dscopy を発見できるように PYTHONPATH を変更する
    os.environ["PYTHONPATH"] = path.dirname(path.abspath(__file__))
    for cmd in commands:
        ret = os.system(cmd)
        if ret != 0:
            return


if __name__ == '__main__':
    import sys
    run(sys.argv)
