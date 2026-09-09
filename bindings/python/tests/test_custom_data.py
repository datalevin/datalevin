from __future__ import annotations

import json
import gc
import weakref
from dataclasses import dataclass

import pytest

from datalevin import UdfDescriptor, connect, create_udf_registry, datalog_kv, open_kv, q, tx


pytestmark = pytest.mark.usefixtures("require_runtime")

A = {"rank": 1, "label": "a", "extra": None}
B = {"rank": 1, "label": "b", "extra": None}
Z = {"rank": 2, "label": "λ", "extra": None}


def task_type(*, tuple_order=False):
    registry = create_udf_registry()

    @registry.order_udf("task/order", version=1)
    def order(value):
        return [value["rank"], "task"] if tuple_order else value["rank"]

    @registry.serializer_udf("task/encode", version=1)
    def encode(value):
        if value.get("label") == "bad":
            return "not bytes"
        return json.dumps(value, ensure_ascii=False).encode("utf-8")

    @registry.deserializer_udf("task/decode", version=1)
    def decode(payload):
        assert isinstance(payload, bytes)
        return json.loads(payload.decode("utf-8"))

    definition = {
        "index": {
            "type": [":long", ":string"] if tuple_order else ":long",
            "order-fn": UdfDescriptor.order_fn("task/order", version=1),
        },
        "payload": {
            "serialize": UdfDescriptor.serializer("task/encode", version=1),
            "deserialize": UdfDescriptor.deserializer("task/decode", version=1),
        },
    }
    return registry, definition


@pytest.mark.parametrize("tuple_order", [False, True])
def test_custom_kv_order_and_payload(tmp_path, tuple_order):
    registry, definition = task_type(tuple_order=tuple_order)
    opts = {":runtime-opts": {":udf-registry": registry}}
    path = str(tmp_path / "kv")
    with open_kv(path, opts=opts) as kv:
        assert kv.register_type("app/task", definition) == ":app/task"
        assert kv.register_type(":app/task", definition) == ":app/task"
        with pytest.raises(Exception, match="different definition"):
            kv.register_type("app/task", {**definition, "version": 2})
        kv.open_dbi("tasks", {":key-type": ":app/task"})
        kv.open_list_dbi("owners", {":value-type": ":app/task"})
        kv.transact([(":put", A, "a"), (":put", B, "b"), (":put", Z, "z")],
                    dbi_name="tasks")
        kv.put_list_items("owners", "alice", [A, B, Z], ":string", ":app/task")
        assert kv.get_value("tasks", dict(A)) == "a"
        assert kv.get_value("tasks", dict(B), ":app/task", ":data", True) == "b"
        assert kv.get_range("tasks", [":closed", A, B]) == [[A, "a"], [B, "b"]]
        assert kv.get_range("tasks", [":greater-than", B]) == [[Z, "z"]]
        assert kv.get_list("owners", "alice", ":string", ":app/task") == [A, B, Z]
        with pytest.raises(Exception, match="byte array"):
            kv.transact([(":del", A), (":put", {"rank": 3, "label": "bad"}, "bad")],
                        dbi_name="tasks")
        assert kv.get_value("tasks", A) == "a"
        with kv.transaction() as tx:
            tx.register_type("app/aborted", definition)
            tx.transact([(":del", B)], dbi_name="tasks")
        assert kv.get_value("tasks", B) == "b"
        # Aborted registration leaves the name available for a different version.
        assert kv.register_type("app/aborted", {**definition, "version": 2}) == ":app/aborted"

    with open_kv(path) as kv:
        kv.open_dbi("tasks")
        with pytest.raises(Exception, match="UDF registry"):
            kv.get_value("tasks", A)
    with open_kv(path, opts=opts) as kv:
        kv.open_dbi("tasks")
        assert kv.get_range("tasks", [":all"]) == [[A, "a"], [B, "b"], [Z, "z"]]
        registry.unregister(UdfDescriptor.deserializer("task/decode", version=1))
        with pytest.raises(Exception, match="UDF"):
            kv.get_range("tasks", [":all"])
        registry.register(UdfDescriptor.deserializer("task/decode", version=1),
                          lambda payload: json.loads(payload.decode("utf-8")))
        assert kv.get_value("tasks", A) == "a"


def test_custom_datalog_and_shared_registry(tmp_path):
    registry, definition = task_type()
    with connect(str(tmp_path / "db"),
                 opts={":runtime-opts": {":udf-registry": registry}}) as conn:
        assert conn.register_type("app/task", definition) == ":app/task"
        conn.update_schema({"task/value": {":db/valueType": ":app/task"},
                            "task/id": {":db/valueType": ":app/task",
                                        ":db/unique": ":db.unique/identity"}})
        conn.transact([{"db/id": 1, "task/value": A, "task/id": A},
                       {"db/id": 2, "task/value": B, "task/id": B}])
        entity, value = q.var("e"), q.var("v")
        query = q.query(find=[entity], inputs=[q.DB, value],
                        where=[q.pattern(entity, "task/value", value)])
        assert conn.query(query, A) == [[1]]
        assert conn.query(query, B) == [[2]]
        assert conn.pull("[*]", [":task/id", B])[":task/value"] == B
        assert [row[":v"] for row in conn.index_range("task/value", A, B)] == [A, B]
        kv = datalog_kv(conn)
        assert kv.register_type("app/task", definition) == ":app/task"
        kv.open_dbi("tasks", {":key-type": ":app/task"})
        kv.transact([(":put", A, "a")], dbi_name="tasks")
        assert kv.get_value("tasks", A) == "a"


@dataclass
class Task:
    rank: int
    label: str


def native_task_type():
    registry = create_udf_registry()
    nonce = 0

    @registry.order_udf("native/order")
    def order(task):
        assert isinstance(task, Task)
        if task.label == "bad-order":
            raise ValueError("bad native order")
        return task.rank

    @registry.serializer_udf("native/encode")
    def encode(task):
        nonlocal nonce
        nonce += 1
        if task.label == "bad-payload":
            return "not bytes"
        # Equal values intentionally serialize differently. Equality must be
        # evaluated after deserialization, never by comparing payload bytes.
        return json.dumps([task.rank, task.label, nonce]).encode()

    @registry.deserializer_udf("native/decode")
    def decode(payload):
        rank, label, _nonce = json.loads(payload)
        return Task(rank, label)

    definition = {
        "index": {"type": ":long", "order-fn": UdfDescriptor.order_fn("native/order")},
        "payload": {"serialize": UdfDescriptor.serializer("native/encode"),
                    "deserialize": UdfDescriptor.deserializer("native/decode")},
    }
    registry.bind_native_type("app/native-task", Task, definition)
    return registry, definition


def test_native_python_kv(tmp_path):
    registry, definition = native_task_type()
    opts = {":runtime-opts": {":udf-registry": registry}}
    path = str(tmp_path / "native-kv")
    a, b, z = Task(1, "a"), Task(1, "b"), Task(2, "λ")
    with open_kv(path, opts=opts) as kv:
        kv.register_type("app/native-task", definition)
        kv.open_dbi("tasks", {":key-type": ":app/native-task"})
        kv.register_type("app/wrong-type", definition)
        kv.open_dbi("wrong", {":key-type": ":app/wrong-type"})
        with pytest.raises(Exception, match="different custom type"):
            kv.transact([(":put", a, "a")], dbi_name="wrong")
        assert kv.entries("wrong") == 0
        # Runtime callback identities must never become durable :data values.
        kv.open_dbi("untyped")
        with pytest.raises(Exception, match="Unfreezable|freeze|serializ"):
            kv.transact([(":put", "key", a)], dbi_name="untyped")
        assert kv.entries("untyped") == 0
        kv.open_list_dbi("owners", {":value-type": ":app/native-task"})
        kv.transact([(":put", a, "a"), (":put", b, "b"), (":put", z, "z")], dbi_name="tasks")
        kv.put_list_items("owners", "alice", [a, b, z], ":string", ":app/native-task")
        assert kv.get_value("tasks", Task(1, "b")) == "b"
        assert kv.get_range("tasks", [":closed", a, b]) == [[a, "a"], [b, "b"]]
        assert kv.get_range("tasks", [":greater-than", b]) == [[z, "z"]]
        assert kv.get_list("owners", "alice", ":string", ":app/native-task") == [a, b, z]
        assert kv.list_range_keep("owners", lambda key, value: Task(value.rank, value.label),
                                  [":all"], ":string", [":all"], ":app/native-task") == [a, b, z]
        kv.transact([(":put", Task(1, "a"), "updated")], dbi_name="tasks")
        assert kv.entries("tasks") == 3
        a.label = "changed after write"
        assert kv.get_value("tasks", Task(1, "a")) == "updated"
        with pytest.raises(Exception, match="bad native order"):
            kv.transact([(":del", b), (":put", Task(3, "bad-order"), "bad")], dbi_name="tasks")
        assert kv.get_value("tasks", b) == "b"
        with pytest.raises(Exception, match="byte array"):
            kv.transact([(":del", b), (":put", Task(3, "bad-payload"), "bad")], dbi_name="tasks")
        assert kv.get_value("tasks", b) == "b"
        with kv.transaction() as tx:
            tx.transact([(":del", b)], dbi_name="tasks")
            assert tx.get_value("tasks", b) is None
        assert kv.get_value("tasks", b) == "b"
        kv.with_transaction(lambda tx: tx.transact([(":del", b)], dbi_name="tasks"))
        assert kv.get_value("tasks", b) is None

    # New runtime/codec identity after reopening must not affect stored values.
    rebound, _definition = native_task_type()
    with open_kv(path, opts={":runtime-opts": {":udf-registry": rebound}}) as kv:
        kv.open_dbi("tasks")
        assert kv.get_range("tasks", [":all"]) == [[Task(1, "a"), "updated"], [z, "z"]]
        # Reading a payload does not require its serializer to be installed.
        rebound.unregister(UdfDescriptor.serializer("native/encode"))
        assert kv.get_range("tasks", [":all"]) == [[Task(1, "a"), "updated"], [z, "z"]]
        rebound.unregister(UdfDescriptor.deserializer("native/decode"))
        with pytest.raises(Exception, match="binding|UDF"):
            kv.get_range("tasks", [":all"])


def test_native_python_datalog(tmp_path):
    registry, definition = native_task_type()
    a, b = Task(1, "a"), Task(1, "b")
    with connect(str(tmp_path / "native-db"),
                 opts={":runtime-opts": {":udf-registry": registry}}) as conn:
        conn.register_type("app/native-task", definition)
        conn.update_schema({"task/value": {":db/valueType": ":app/native-task"},
                            "task/id": {":db/valueType": ":app/native-task",
                                        ":db/unique": ":db.unique/identity"},
                            "task/many": {":db/valueType": ":app/native-task",
                                          ":db/cardinality": ":db.cardinality/many"}})
        conn.transact([{"db/id": 1, "task/value": a, "task/id": a, "task/many": [a, b]},
                       {"db/id": 2, "task/value": b, "task/id": b},
                       {"db/id": 3, "task/value": Task(1, "a")}])
        entity, value = q.var("e"), q.var("v")
        query = q.query(find=[entity], inputs=[q.DB, value],
                        where=[q.pattern(entity, "task/value", value)])
        assert sorted(conn.query(query, Task(1, "a"))) == [[1], [3]]
        assert conn.query(query, Task(1, "b")) == [[2]]
        assert sorted(row[0].label for row in conn.query("[:find ?v :where [?e :task/value ?v]]")) == ["a", "b"]
        assert conn.pull("[*]", [":task/id", b])[":task/value"] == b
        assert conn.entity(1)[":task/value"] == a
        report = conn.tx_data_to_simulated_report(tx.data(tx.add(2, "task/value", a)))
        assert report[":db-after"].pull("[*]", [":task/id", b])[":task/value"] == a
        assert conn.entity(2)[":task/value"] == b
        registry.query_udf("native/make")(lambda rank: Task(rank, "made"))
        rank = q.var("rank")
        made = q.query(find=[value], inputs=[q.DB, rank],
                       where=[q.bind_udf(UdfDescriptor.query_fn("native/make"), value, rank)])
        assert conn.query(made, 3) == [[Task(3, "made")]]
        assert sorted(row[":v"].label for row in conn.index_range("task/value", a, b)) == ["a", "a", "b"]
        conn.transact([{"db/id": "upsert", "task/id": Task(1, "b"), "task/value": a}])
        assert conn.entity(2)[":task/value"] == a
        conn.transact([(":db/retract", 1, ":task/many", Task(1, "b"))])
        assert conn.pull("[*]", 1)[":task/many"] == [a]
        with pytest.raises(Exception, match="bad native order"):
            conn.transact([(":db/retract", 1, ":task/value", a),
                           (":db/add", 4, ":task/value", Task(3, "bad-order"))])
        assert conn.entity(1)[":task/value"] == a
        conn.transact_async(tx.data(tx.add(4, "task/value", Task(4, "async")))).result(timeout=20)
        assert conn.entity(4)[":task/value"] == Task(4, "async")
        conn.with_transaction(lambda writing: writing.transact(tx.data(tx.add(4, "task/value", b))))
        assert conn.entity(4)[":task/value"] == b
        kv = datalog_kv(conn)
        kv.open_dbi("tasks", {":key-type": ":app/native-task"})
        kv.transact([(":put", a, "a")], dbi_name="tasks")
        assert kv.get_value("tasks", Task(1, "a")) == "a"


def test_native_bindings_are_scoped_to_the_runtime(tmp_path):
    registry, definition = native_task_type()
    assert registry.bind_native_type("app/native-task", Task, definition) is registry
    with pytest.raises(ValueError, match="different native type"):
        registry.bind_native_type("app/other", Task, definition)
    with pytest.raises(ValueError, match="local database"):
        open_kv("dtlv://invalid/native", opts={":runtime-opts": {":udf-registry": registry}})
    with open_kv(str(tmp_path / "ordinary")) as kv:
        kv.open_dbi("ordinary")
        with pytest.raises(Exception):
            kv.transact([(":put", Task(1, "a"), "a")], dbi_name="ordinary")
        assert kv.entries("ordinary") == 0


def test_native_equality_across_codecs_and_defensive_snapshots():
    first, _ = native_task_type()
    second, _ = native_task_type()
    left = first._native_types[Task].wrap(Task(1, "a"))
    right = second._native_types[Task].wrap(Task(1, "a"))
    different = second._native_types[Task].wrap(Task(1, "b"))
    assert left.codecId() != right.codecId()
    assert left.equals(right) and right.equals(left)
    assert left.hashCode() == right.hashCode()
    assert not left.equals(different)
    exposed = left.payload()
    exposed[0] = 0
    assert left.equals(right)


def test_native_snapshot_does_not_retain_jvm_registry():
    from datalevin._convert import to_python

    registry, _ = native_task_type()
    snapshot = registry._native_types[Task].wrap(Task(1, "a"))
    registry_ref = weakref.ref(registry)
    del registry
    gc.collect()
    assert registry_ref() is None
    # A live value retains its host codec without retaining the JVM registry.
    assert to_python(snapshot) == Task(1, "a")


def test_native_classes_can_share_a_deserializer(tmp_path):
    class Work(Task):
        pass

    registry, definition = native_task_type()

    @registry.deserializer_udf("native/decode")
    def decode(payload):
        rank, label, _nonce = json.loads(payload)
        return Work(rank, label) if label.startswith("work:") else Task(rank, label)

    registry.bind_native_type("app/work", Work, definition)
    with open_kv(str(tmp_path / "shared-serde"),
                 opts={":runtime-opts": {":udf-registry": registry}}) as kv:
        kv.register_type("app/native-task", definition)
        kv.register_type("app/work", definition)
        kv.open_dbi("tasks", {":key-type": ":app/native-task"})
        kv.open_dbi("work", {":key-type": ":app/work"})
        kv.transact([(":put", Task(1, "a"), "a")], dbi_name="tasks")
        kv.transact([(":put", Work(1, "work:a"), "work:a")], dbi_name="work")
        assert kv.get_range("tasks", [":all"]) == [[Task(1, "a"), "a"]]
        assert kv.get_range("work", [":all"]) == [[Work(1, "work:a"), "work:a"]]


def test_native_values_survive_query_and_collection_spilling(tmp_path):
    import jpype
    from datalevin._convert import to_java, to_python
    from datalevin._native import native_scope

    registry, definition = native_task_type()
    clojure = jpype.JClass("clojure.java.api.Clojure")
    a, b = Task(1, "a"), Task(1, "b")
    with connect(str(tmp_path / "native-spill"),
                 opts={":runtime-opts": {":udf-registry": registry}}) as conn:
        conn.register_type("app/native-task", definition)
        conn.update_schema({"task/value": {":db/valueType": ":app/native-task"}})
        conn.transact([{"db/id": 1, "task/value": a},
                       {"db/id": 2, "task/value": b},
                       {"db/id": 3, "task/value": Task(1, "a")}])
        pressure = clojure.var("datalevin.spill", "memory-pressure").deref()
        previous = pressure.deref()
        vector = clojure.var("datalevin.spill", "new-spillable-vector").invoke(
            None, clojure.read("{:spill-threshold -1}"))
        try:
            with native_scope(registry):
                vector.cons(clojure.var("clojure.core", "vec").invoke(to_java([a, b])))
            assert clojure.var("datalevin.spill", "disk-count").invoke(vector) == 1
            assert to_python(vector) == [[a, b]]
            pressure.reset(jpype.JLong(99))
            assert sorted(row[0].label for row in conn.query(
                "[:find ?v :where [?e :task/value ?v]]")) == ["a", "b"]
            assert sorted(conn.query(
                "[:find ?e :in $ ?v :where [?e :task/value ?v]]", Task(1, "a"))) == [[1], [3]]
            assert sorted(row[0].label for row in conn.query(
                "[:find ?v :where [?e :task/value ?v] [?other :task/value ?v]]")) == ["a", "b"]
        finally:
            pressure.reset(previous)
            vector.empty()
