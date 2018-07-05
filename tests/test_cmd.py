# coding: utf-8
from __future__ import print_function
from __future__ import absolute_import
import os
from os import path
import pytest

from dsflow import cmd


def test_empty_args(capsys):
    # capsys という名前の引数にした場合のみ、stdout, stderr を読み込むためのオブジェクトが渡される
    with pytest.raises(SystemExit):
        cmd.parse([], is_direct_runner=False)
    stderr = capsys.readouterr().err
    assert "error: too few arguments" in stderr


def test_unknown_subcommand(capsys):
    with pytest.raises(SystemExit):
        cmd.parse(["hoge"], is_direct_runner=False)
    stderr = capsys.readouterr().err
    assert "error: invalid choice: 'hoge' (choose from 'dump', 'copy', 'rename', 'delete')" in stderr


def test_dump_commmand(capsys):
    with pytest.raises(SystemExit):
        cmd.parse(["dump"], is_direct_runner=False)
    stderr = capsys.readouterr().err
    assert "dump: error: too few arguments" in stderr


def test_dump_commmand_with_no_args(capsys):
    with pytest.raises(SystemExit):
        cmd.parse(["dump"], is_direct_runner=False)
    stderr = capsys.readouterr().err
    assert "dump: error: too few arguments" in stderr
    assert "-P" in stderr
    assert "-T" in stderr
    assert "-S" in stderr
    assert "src" in stderr
    assert "dst" in stderr


def test_dump_command_with_right_args():
    setup_path = path.join(os.getcwd(), "dsflow", "setup.py")

    parsed = cmd.parse(
        "dump -P proj -T gs://temp -S gs://staging //srcnamespace/srckind gs://dst".split(" "),
        is_direct_runner=False)
    assert parsed == ['python -m dsflow.dsdump'
                      ' "//srcnamespace/srckind"'
                      ' "gs://dst"'
                      ' --format "json"'
                      ' --project "proj"'
                      ' --runner "DataflowRunner"'
                      ' --setup_file "' + setup_path + '"'
                      ' --staging_location "gs://staging"'
                      ' --temp_location "gs://temp"']


def test_dump_command_on_direct_runner():
    setup_path = path.join(os.getcwd(), "dsflow", "setup.py")

    parsed = cmd.parse(
        "dump -P proj //srcnamespace/srckind gs://dst".split(" "),
        is_direct_runner=True)
    assert parsed == ['python -m dsflow.dsdump'
                      ' "//srcnamespace/srckind"'
                      ' "gs://dst"'
                      ' --format "json"'
                      ' --project "proj"'
                      ' --runner "DirectRunner"'
                      ' --setup_file "' + setup_path + '"'
                      ]


def test_dump_command_with_default_namespace():
    setup_path = path.join(os.getcwd(), "dsflow", "setup.py")

    parsed = cmd.parse(
        "dump -P proj -T gs://temp -S gs://staging //@default/srckind gs://dst".split(" "),
        is_direct_runner=False)
    assert parsed == ['python -m dsflow.dsdump'
                      ' "///srckind"'
                      ' "gs://dst"'
                      ' --format "json"'
                      ' --project "proj"'
                      ' --runner "DataflowRunner"'
                      ' --setup_file "' + setup_path + '"'
                      ' --staging_location "gs://staging"'
                      ' --temp_location "gs://temp"']


def test_dump_command_with_empty_namespace():
    setup_path = path.join(os.getcwd(), "dsflow", "setup.py")

    parsed = cmd.parse(
        "dump -P proj -T gs://temp -S gs://staging ///srckind gs://dst".split(" "),
        is_direct_runner=False)
    assert parsed == ['python -m dsflow.dsdump'
                      ' "///srckind"'
                      ' "gs://dst"'
                      ' --format "json"'
                      ' --project "proj"'
                      ' --runner "DataflowRunner"'
                      ' --setup_file "' + setup_path + '"'
                      ' --staging_location "gs://staging"'
                      ' --temp_location "gs://temp"']


def test_dump_command_with_multi_kinds():
    setup_path = path.join(os.getcwd(), "dsflow", "setup.py")

    parsed = cmd.parse(
        "dump -P proj -T gs://temp -S gs://staging //srcnamespace/srckind1,srckind2 gs://dst".split(" "),
        is_direct_runner=False)
    assert parsed == ['python -m dsflow.dsdump'
                      ' "//srcnamespace/srckind1,srckind2"'
                      ' "gs://dst"'
                      ' --format "json"'
                      ' --project "proj"'
                      ' --runner "DataflowRunner"'
                      ' --setup_file "' + setup_path + '"'
                      ' --staging_location "gs://staging"'
                      ' --temp_location "gs://temp"']


def test_copy_command_with_right_args():
    setup_path = path.join(os.getcwd(), "dsflow", "setup.py")

    parsed = cmd.parse(
        "copy -P proj -T gs://temp -S gs://staging"
        " //srcnamespace/srckind //dstnamespace/dstkind".split(" "),
        is_direct_runner=False)
    assert parsed == ['python -m dsflow.dscopy'
                      ' "//srcnamespace/srckind"'
                      ' "//dstnamespace/dstkind"'
                      ' --project "proj"'
                      ' --runner "DataflowRunner"'
                      ' --setup_file "' + setup_path + '"'
                      ' --staging_location "gs://staging"'
                      ' --temp_location "gs://temp"']


def test_copy_command_with_multi_dst(capsys):
    with pytest.raises(SystemExit):
        result = cmd.parse(
            "copy -P proj -T gs://temp -S gs://staging"
            " //srcnamespace/srckind //dstnamespace/dstkind1,dstkind2".split(" "),
            is_direct_runner=False)
        print(result)
    stderr = capsys.readouterr().err
    assert 'argument dst: DatastoreDstPath must be formatted "/({PROJECT})/{NAMESPACE}(/{KIND})"' in stderr


def test_copy_command_with_clear_dst():
    setup_path = path.join(os.getcwd(), "dsflow", "setup.py")

    parsed = cmd.parse(
        "copy -P proj -T gs://temp -S gs://staging"
        " //srcnamespace/srckind //dstnamespace/dstkind --clear_dst".split(" "),
        is_direct_runner=False)
    assert parsed == [
        'python -m dsflow.dsdelete'
        ' "//dstnamespace/dstkind"'
        ' --project "proj"'
        ' --runner "DataflowRunner"'
        ' --setup_file "' + setup_path + '"'
        ' --staging_location "gs://staging"'
        ' --temp_location "gs://temp"',
        'python -m dsflow.dscopy'
        ' "//srcnamespace/srckind"'
        ' "//dstnamespace/dstkind"'
        ' --project "proj"'
        ' --runner "DataflowRunner"'
        ' --setup_file "' + setup_path + '"'
        ' --staging_location "gs://staging"'
        ' --temp_location "gs://temp"'
    ]


def test_delete_command_with_right_args():
    setup_path = path.join(os.getcwd(), "dsflow", "setup.py")

    parsed = cmd.parse(
        "delete -P proj -T gs://temp -S gs://staging //srcnamespace/srckind".split(" "), is_direct_runner=False)
    assert parsed == [
        'python -m dsflow.dsdelete'
        ' "//srcnamespace/srckind"'
        ' --project "proj"'
        ' --runner "DataflowRunner"'
        ' --setup_file "' + setup_path + '"'
        ' --staging_location "gs://staging"'
        ' --temp_location "gs://temp"'
    ]


def test_rename_command_with_right_args():
    setup_path = path.join(os.getcwd(), "dsflow", "setup.py")

    parsed = cmd.parse(
        "rename -P proj -T gs://temp -S gs://staging //srcnamespace/srckind //dstnamespace/dstkind".split(" "),
        is_direct_runner=False)
    assert parsed == [
        'python -m dsflow.dscopy'
        ' "//srcnamespace/srckind"'
        ' "//dstnamespace/dstkind"'
        ' --project "proj"'
        ' --runner "DataflowRunner"'
        ' --setup_file "' + setup_path + '"'
        ' --staging_location "gs://staging"'
        ' --temp_location "gs://temp"',
        'python -m dsflow.dsdelete'
        ' "//srcnamespace/srckind"'
        ' --project "proj"'
        ' --runner "DataflowRunner"'
        ' --setup_file "' + setup_path + '"'
        ' --staging_location "gs://staging"'
        ' --temp_location "gs://temp"'
    ]


def test_rename_command_with_clear_dst():
    setup_path = path.join(os.getcwd(), "dsflow", "setup.py")

    parsed = cmd.parse(
        "rename -P proj -T gs://temp -S gs://staging"
        " //srcnamespace/srckind //dstnamespace/dstkind --clear_dst".split(" "), is_direct_runner=False)
    assert parsed == [
        'python -m dsflow.dsdelete'
        ' "//dstnamespace/dstkind"'
        ' --project "proj"'
        ' --runner "DataflowRunner"'
        ' --setup_file "' + setup_path + '"'
        ' --staging_location "gs://staging"'
        ' --temp_location "gs://temp"',
        'python -m dsflow.dscopy'
        ' "//srcnamespace/srckind"'
        ' "//dstnamespace/dstkind"'
        ' --project "proj"'
        ' --runner "DataflowRunner"'
        ' --setup_file "' + setup_path + '"'
        ' --staging_location "gs://staging"'
        ' --temp_location "gs://temp"',
        'python -m dsflow.dsdelete'
        ' "//srcnamespace/srckind"'
        ' --project "proj"'
        ' --runner "DataflowRunner"'
        ' --setup_file "' + setup_path + '"'
        ' --staging_location "gs://staging"'
        ' --temp_location "gs://temp"'
    ]


def test_rename_command_with_multi_kinds():
    setup_path = path.join(os.getcwd(), "dsflow", "setup.py")

    parsed = cmd.parse(
        "rename -P proj -T gs://temp -S gs://staging"
        " //srcnamespace/srckind1,srckind2 //dstnamespace --clear_dst".split(" "), is_direct_runner=False)
    assert parsed == [
        'python -m dsflow.dsdelete'
        ' "//dstnamespace"'
        ' --project "proj"'
        ' --runner "DataflowRunner"'
        ' --setup_file "' + setup_path + '"'
        ' --staging_location "gs://staging"'
        ' --temp_location "gs://temp"',
        'python -m dsflow.dscopy'
        ' "//srcnamespace/srckind1,srckind2"'
        ' "//dstnamespace"'
        ' --project "proj"'
        ' --runner "DataflowRunner"'
        ' --setup_file "' + setup_path + '"'
        ' --staging_location "gs://staging"'
        ' --temp_location "gs://temp"',
        'python -m dsflow.dsdelete'
        ' "//srcnamespace/srckind1,srckind2"'
        ' --project "proj"'
        ' --runner "DataflowRunner"'
        ' --setup_file "' + setup_path + '"'
        ' --staging_location "gs://staging"'
        ' --temp_location "gs://temp"'
    ]
