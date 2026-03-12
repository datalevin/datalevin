from __future__ import annotations

import pytest

from datalevin import open_kv


pytestmark = pytest.mark.usefixtures("require_runtime")


def test_kv_methods_cover_named_and_list_dbis(tmp_path) -> None:
    kv_dir = tmp_path / "kv"
    with open_kv(str(kv_dir)) as kv:
        assert repr(kv) == "<KV open>"
        assert kv.dir() == str(kv_dir)

        kv.open_dbi("items")
        kv.open_list_dbi("list")
        kv.transact(
            [(":put", "a", "alpha"), (":put", "b", "beta"), (":put", "c", "gamma")],
            dbi_name="items",
            k_type=":string",
            v_type=":string",
        )
        kv.transact(
            [(":put", "a", 1), (":put", "a", 2), (":put", "b", 3)],
            dbi_name="list",
            k_type=":string",
            v_type=":long",
        )

        assert sorted(kv.list_dbis()) == ["items", "list"]
        assert kv.entries("items") == 3
        assert kv.get_value("items", "b", ":string", ":string", True) == "beta"
        assert kv.get_range("items", [":all"], ":string", ":string", 2, 1) == [
            ["b", "beta"],
            ["c", "gamma"],
        ]
        assert kv.get_range("list", [":all"], ":string", ":long") == [
            ["a", 1],
            ["a", 2],
            ["b", 3],
        ]

        kv.clear_dbi("items")
        assert kv.entries("items") == 0

        kv.drop_dbi("items")
        assert sorted(kv.list_dbis()) == ["list"]

    assert kv.closed() is True
    assert repr(kv) == "<KV closed>"


def test_kv_argument_validation(tmp_path) -> None:
    with open_kv(str(tmp_path / "kv")) as kv:
        kv.open_dbi("items")

        with pytest.raises(ValueError):
            kv.transact([(":put", "a", "alpha")], k_type=":string")
        with pytest.raises(ValueError):
            kv.get_value("items", "a", ":string")
        with pytest.raises(ValueError):
            kv.get_range("items", [":all"], v_type=":string")
