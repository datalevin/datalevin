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
        kv.open_dbi("blobs")
        kv.transact(
            [(":put", "buf", b"\x00\x01\x02\xff"), (":put", "arr", bytearray(b"\x09\x08\x07"))],
            dbi_name="blobs",
            k_type=":string",
            v_type=":bytes",
        )
        kv.open_dbi("blob-keys")
        kv.transact(
            [(":put", b"\x00\x01", b"\x07\x08"), (":put", b"\x00\x02", b"\x09\x0a")],
            dbi_name="blob-keys",
            k_type=":bytes",
            v_type=":bytes",
        )
        kv.put_list_items("list", "a", [1, 2], ":string", ":long")
        kv.put_list_items("list", "b", [3], ":string", ":long")

        assert sorted(kv.list_dbis()) == ["blob-keys", "blobs", "items", "list"]
        assert kv.entries("items") == 3
        item_stat = kv.stat("items")
        assert item_stat.get(":entries", item_stat.get("entries")) == 3
        assert kv.get_value("items", "b", ":string", ":string", True) == "beta"
        assert kv.get_rank("items", "b", ":string") == 1
        assert kv.get_by_rank("items", 1, ":string", ":string") == "beta"
        assert kv.get_by_rank("items", 1, ":string", ":string", False) == ["b", "beta"]
        assert kv.get_entry_by_rank("items", 1, ":string", ":string") == ["b", "beta"]
        assert kv.get_first("items", [":all"], ":string", ":string") == ["a", "alpha"]
        assert kv.get_first_n("items", 2, [":all"], ":string", ":string") == [
            ["a", "alpha"],
            ["b", "beta"],
        ]
        samples = kv.sample_kv("items", 2, ":string", ":string", False)
        assert len(samples) == 2
        assert all(sample in [["a", "alpha"], ["b", "beta"], ["c", "gamma"]] for sample in samples)
        assert kv.get_value("blobs", "buf", ":string", ":bytes", True) == b"\x00\x01\x02\xff"
        assert kv.get_value("blobs", "arr", ":string", ":bytes", True) == b"\x09\x08\x07"
        assert kv.get_value("blob-keys", b"\x00\x02", ":bytes", ":bytes", True) == b"\x09\x0a"
        assert kv.key_range("items", [":all"], ":string", 2, 1) == ["b", "c"]
        assert kv.key_range_count("items", [":all"], ":string") == 3
        assert kv.range_count("items", [":all"], ":string") == 3
        assert kv.get_range("items", [":all"], ":string", ":string", 2, 1) == [
            ["b", "beta"],
            ["c", "gamma"],
        ]
        assert kv.get_range("blob-keys", [":closed", b"\x00\x01", b"\x00\x02"], ":bytes", ":bytes") == [
            [b"\x00\x01", b"\x07\x08"],
            [b"\x00\x02", b"\x09\x0a"],
        ]
        assert kv.get_range("list", [":all"], ":string", ":long") == [
            ["a", 1],
            ["a", 2],
            ["b", 3],
        ]
        assert kv.list_range("list", [":all"], ":string", [":all"], ":long", limit=1, offset=1) == [
            ["a", 2],
        ]
        assert kv.list_range("list", [":closed", "a", "b"], ":string", [":closed", 2, 3], ":long") == [
            ["a", 2],
            ["b", 3],
        ]
        assert kv.list_range_count("list", [":all"], ":string") == 3
        assert kv.list_range_first("list", [":all"], ":string", [":all"], ":long") == ["a", 1]
        assert kv.list_range_first_n("list", 2, [":all"], ":string", [":all"], ":long") == [
            ["a", 1],
            ["a", 2],
        ]
        assert kv.key_range_list_count("list", [":all"], ":string") == 3
        assert kv.get_list("list", "a", ":string", ":long") == [1, 2]
        assert kv.get_list("list", "a", ":string", ":long", limit=1, offset=1) == [2]
        assert kv.list_count("list", "a", ":string") == 2
        assert kv.in_list("list", "a", 2, ":string", ":long") is True
        assert kv.in_list("list", "a", 9, ":string", ":long") is False
        kv.del_list_items("list", "a", ":string", values=[2], v_type=":long")
        assert kv.get_list("list", "a", ":string", ":long") == [1]
        kv.del_list_items("list", "a", ":string")
        assert kv.list_count("list", "a", ":string") == 0

        kv.sync()
        kv.set_env_flags({"nosync"}, True)
        assert ":nosync" in kv.get_env_flags()
        kv.set_env_flags([":nosync"], False)
        assert ":nosync" not in kv.get_env_flags()

        copy_dir = tmp_path / "kv-copy"
        kv.copy(str(copy_dir))
        assert copy_dir.exists()

        kv.clear_dbi("items")
        assert kv.entries("items") == 0

        kv.drop_dbi("items")
        assert sorted(kv.list_dbis()) == ["blob-keys", "blobs", "list"]

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
        with pytest.raises(ValueError):
            kv.put_list_items("items", "a", ["alpha"], None, ":string")
        with pytest.raises(ValueError):
            kv.list_range("items", [":all"], None, [":all"], ":string")
        with pytest.raises(ValueError):
            kv.list_range("items", [":all"], ":string", [":all"], None)
        with pytest.raises(ValueError):
            kv.list_range_count("items", [":all"], None)
        with pytest.raises(ValueError):
            kv.get_by_rank("items", 0, ignore_key=True)
        with pytest.raises(ValueError):
            kv.get_entry_by_rank("items", 0)
        with pytest.raises(ValueError):
            kv.get_entry_by_rank("items", 0, ":string")


def test_kv_list_functional_operations(tmp_path) -> None:
    with open_kv(str(tmp_path / "kv-list-fns")) as kv:
        kv.open_list_dbi("list")
        kv.put_list_items("list", "a", [1, 2], ":string", ":long")
        kv.put_list_items("list", "b", [3], ":string", ":long")

        values = []
        kv.visit_list("list", values.append, "a", ":string", ":long")
        assert values == [1, 2]

        pairs = []
        kv.visit_list_range("list", lambda key, value: pairs.append([key, value]), [":all"], ":string", [":all"], ":long")
        assert pairs == [["a", 1], ["a", 2], ["b", 3]]

        assert kv.list_range_filter(
            "list",
            lambda _key, value: value >= 2,
            [":all"],
            ":string",
            [":all"],
            ":long",
        ) == [["a", 2], ["b", 3]]
        assert kv.list_range_filter(
            "list",
            lambda _key, _value: True,
            [":all"],
            ":string",
            [":all"],
            ":long",
            limit=1,
            offset=1,
        ) == [["a", 2]]
        assert kv.list_range_filter_count(
            "list",
            lambda key, _value: key == "a",
            [":all"],
            ":string",
            [":all"],
            ":long",
        ) == 2
        assert kv.list_range_keep(
            "list",
            lambda key, value: f"{key}:{value}" if value > 1 else None,
            [":all"],
            ":string",
            [":all"],
            ":long",
        ) == ["a:2", "b:3"]
        assert kv.list_range_some(
            "list",
            lambda key, value: [key, value] if value == 3 else None,
            [":all"],
            ":string",
            [":all"],
            ":long",
        ) == ["b", 3]


def test_kv_operational_methods_cover_wal_snapshots_and_tx_log(tmp_path) -> None:
    with open_kv(str(tmp_path / "kv-ops"), opts={":wal?": True}) as kv:
        kv.open_dbi("items")
        kv.transact(
            [(":put", "a", "alpha")],
            dbi_name="items",
            k_type=":string",
            v_type=":string",
        )

        watermarks = kv.tx_log_watermarks()
        assert watermarks.get(":wal?", watermarks.get("wal")) is True
        assert isinstance(kv.open_tx_log(1, limit=10), list)

        snapshot = kv.create_snapshot()
        snapshots = kv.list_snapshots()
        gc = kv.gc_tx_log_segments()

        assert isinstance(snapshot, dict)
        assert isinstance(snapshots, list)
        assert isinstance(gc, dict)
