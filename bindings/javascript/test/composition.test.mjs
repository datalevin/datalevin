import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { test } from "node:test";

import {
  EdnList,
  FulltextOptions,
  IdocMatchOptions,
  Keyword,
  LookupRef,
  PatchOp,
  PullAttr,
  PullNested,
  PullSelector,
  Query,
  TxData,
  UdfDescriptor,
  UdfRegistry,
  Uuid,
  VectorSearchOptions,
  connect,
  ednList,
  idocAttr,
  q,
  quote,
  schemaAttr,
  tx,
  udfDescriptor,
  uuid
} from "../src/index.js";
import { jvmStarted, resolveClasspath } from "../src/jvm.js";

const runtimeAvailable = (() => {
  try {
    resolveClasspath();
    return true;
  } catch {
    return false;
  }
})();

function intValue(value) {
  return typeof value === "bigint" ? Number(value) : value;
}

test("query forms are pure, typed, and composable", () => {
  const entity = q.var("e");
  const name = q.var("name");
  const age = q.var("age");
  const clauses = [
    q.datom(entity, "person/name", name),
    q.datom(entity, "person/age", age)
  ];
  const minimumAge = 30;
  if (minimumAge !== null) {
    clauses.push(q.predicate(">=", age, minimumAge));
  }

  const adults = q.query({
    find: q.relation(name),
    where: clauses,
    orderBy: [q.asc(name)],
    limit: 10
  });

  assert.equal(adults instanceof Query, true);
  assert.deepEqual(adults.asData(), [
    ":find",
    "?name",
    ":where",
    ["?e", ":person/name", "?name"],
    ["?e", ":person/age", "?age"],
    [[">=", "?age", 30]],
    ":order-by",
    ["?name", ":asc"],
    ":limit",
    10
  ]);
  assert.equal(Object.isFrozen(adults), true);
  assert.equal(Object.isFrozen(q), true);
});

test("UDF registration conveniences default to JavaScript", async () => {
  const registry = new UdfRegistry("HANDLE");
  const captured = [];
  registry.register = async (descriptor, fn) => {
    captured.push(descriptor);
    return fn;
  };
  const callback = () => null;

  await registry.queryUdf("host/query", callback);
  await registry.predicateUdf("host/predicate", callback);
  await registry.txUdf("host/tx", callback);
  await registry.analyzerUdf("host/analyzer", callback);
  await registry.queryAnalyzerUdf("host/query-analyzer", callback);

  assert.deepEqual(
    captured.map((descriptor) => descriptor.lang),
    Array(5).fill(":javascript")
  );
  assert.deepEqual(captured.map((descriptor) => descriptor.kind), [
    ":query-fn",
    ":predicate",
    ":tx-fn",
    ":analyzer",
    ":query-analyzer"
  ]);

  await registry.queryUdf("host/explicit-java", callback, { lang: "java" });
  assert.equal(captured.at(-1).lang, ":java");
});

test("plain strings stay literals unless explicitly typed", () => {
  const entity = q.var("e");
  const presence = q.pattern(entity, "user/id").toForm();
  assert.equal(presence.length, 2);
  assert.equal(presence[0], entity);
  assert.equal(presence[1] instanceof Keyword, true);
  assert.equal(presence[1].toString(), ":user/id");

  const clause = q.datom(entity, "label", "?literal").toForm();
  assert.equal(clause[1] instanceof Keyword, true);
  assert.equal(clause[2], "?literal");

  const keywordLiteral = q.datom(entity, q.var("attribute"), ":literal").toForm();
  assert.equal(keywordLiteral[1].toString(), "?attribute");
  assert.equal(keywordLiteral[2], ":literal");
});

test("EDN lists and quotes are pure and preserve the vector distinction", () => {
  const wasStarted = jvmStarted();
  const pathValue = ["profile", "age"];
  const predicate = ednList(q.sym(">="), pathValue, 30);
  const age = q.var("age");
  const inner = q.query({
    find: q.scalar(age),
    where: [q.datom(q.var("entity"), "person/age", age)]
  });
  const quoted = quote(inner);
  pathValue.push("ignored");

  assert.equal(predicate instanceof EdnList, true);
  assert.equal(predicate.toForm(), predicate);
  assert.equal(predicate.at(0), q.sym(">="));
  assert.deepEqual(predicate.at(1), ["profile", "age"]);
  assert.equal(predicate.asData().at(0), ">=");
  assert.equal(Object.isFrozen(predicate), true);
  assert.equal(Object.isFrozen(predicate.items), true);

  assert.equal(quoted instanceof EdnList, true);
  assert.equal(quoted.at(0), q.sym("quote"));
  assert.equal(quoted.at(1), inner);
  assert.equal(quoted.asData().at(0), "quote");
  assert.deepEqual(quoted.asData().at(1), inner.asData());
  assert.deepEqual(q.ednList(q.sym("nil?")).asData(), ednList(q.sym("nil?")).asData());
  assert.deepEqual(q.quote(inner).asData(), quoted.asData());
  assert.equal(jvmStarted(), wasStarted);
});

test("UUID values and id-less entities are pure and composable", () => {
  const wasStarted = jvmStarted();
  const id = uuid("550E8400-E29B-41D4-A716-446655440000");
  const equivalent = uuid("550e8400-e29b-41d4-a716-446655440000");
  const anonymous = tx.entity({
    "record/id": id,
    "record/name": "Ada"
  });
  const legacy = tx.entity(null, { "record/id": id });

  assert.equal(id instanceof Uuid, true);
  assert.equal(id, equivalent);
  assert.equal(id.toString(), "550e8400-e29b-41d4-a716-446655440000");
  assert.equal(JSON.stringify(id), '"550e8400-e29b-41d4-a716-446655440000"');
  assert.equal(Object.isFrozen(id), true);
  assert.equal(jvmStarted(), wasStarted);

  assert.equal(anonymous.toForm().get(q.kw("record/id")), id);
  assert.equal(anonymous.toForm().has(q.kw("db/id")), false);
  assert.equal(legacy.toForm().get(q.kw("record/id")), id);
  assert.throws(() => uuid("not-a-uuid"), /Invalid UUID/);
  assert.throws(() => tx.entity({ name: "Ada" }, []), /attributes/);
});

test("pull selectors type grammar tokens without changing values", () => {
  const attribute = q.pullAttr("person/nickname", {
    as: "display",
    limit: null,
    default: { text: ":none" },
    xform: "str"
  });
  const form = attribute.toForm();

  assert.equal(attribute instanceof PullAttr, true);
  assert.equal(form[0] instanceof Keyword, true);
  assert.equal(form[2], "display");
  assert.equal(form[4], null);
  assert.deepEqual(form[6], { text: ":none" });
  assert.equal(form[8].toString(), "str");

  const nested = q.pullNested("person/friend", q.selector("person/name"));
  const recursive = q.pullRecursive("person/manager");
  const bounded = q.pullRecursive(q.pullAttr("person/reports", { limit: 2 }), 3);
  const selector = q.selector(attribute, nested, recursive, bounded);

  assert.equal(nested instanceof PullNested, true);
  assert.equal(selector instanceof PullSelector, true);
  assert.deepEqual(selector.asData(), [
    [
      ":person/nickname",
      ":as",
      "display",
      ":limit",
      null,
      ":default",
      { text: ":none" },
      ":xform",
      "str"
    ],
    new Map([[":person/friend", [":person/name"]]]),
    new Map([[":person/manager", "..."]]),
    new Map([[[":person/reports", ":limit", 2], 3]])
  ]);

  const legacyExpression = q.selector([
    ["person/nickname", ":default", "none", ":as", "display"]
  ]);
  assert.deepEqual(legacyExpression.asData(), [
    [":person/nickname", ":default", "none", ":as", "display"]
  ]);
});

test("transaction forms use explicit operation and attribute keywords", () => {
  const transaction = tx.data(
    tx.entity(-1, { "person/name": "?Ada", "person/status": q.kw("active") }),
    tx.add(-1, "person/age", 42),
    tx.compareAndSwap(-1, "person/age", 42, 43),
    tx.retractAttribute(-1, "person/nickname"),
    tx.ensure("person/valid?", -1)
  );

  assert.equal(transaction instanceof TxData, true);
  const entityForm = transaction.at(0).toForm();
  assert.equal(entityForm instanceof Map, true);
  assert.equal(Array.from(entityForm.keys()).every((key) => key instanceof Keyword), true);
  assert.deepEqual(transaction.asData(), [
    new Map([
      [":person/name", "?Ada"],
      [":person/status", ":active"],
      [":db/id", -1]
    ]),
    [":db/add", -1, ":person/age", 42],
    [":db.fn/cas", -1, ":person/age", 42, 43],
    [":db.fn/retractAttribute", -1, ":person/nickname"],
    [":db/ensure", ":person/valid?", -1]
  ]);
});

test("UDF values compose with typed queries and transactions", () => {
  const descriptor = UdfDescriptor.queryFn("math/inc", { version: "v1" });
  const equivalent = new UdfDescriptor({
    id: "math/inc",
    kind: "query-fn",
    lang: "javascript",
    version: "v1"
  });
  const legacy = udfDescriptor("math/legacy");

  assert.equal(descriptor, equivalent);
  assert.deepEqual(descriptor.asData(), {
    ":udf/lang": ":javascript",
    ":udf/kind": ":query-fn",
    ":udf/id": ":math/inc",
    ":udf/version": "v1"
  });
  assert.equal(Object.isFrozen(descriptor), true);
  assert.equal(Object.isFrozen(descriptor.data), true);
  assert.equal(descriptor.toForm() instanceof Map, true);
  assert.equal(
    Array.from(descriptor.toForm().keys()).every((key) => key instanceof Keyword),
    true
  );

  const number = q.var("number");
  const result = q.var("result");
  const query = q.query({
    find: q.scalar(result),
    inputs: [q.DB, number],
    where: [q.bindUdf(descriptor, result, number)]
  });
  assert.deepEqual(query.asData(), [
    ":find",
    "?result",
    ".",
    ":in",
    "$",
    "?number",
    ":where",
    [
      [
        "udf",
        new Map([
          [":udf/lang", ":javascript"],
          [":udf/kind", ":query-fn"],
          [":udf/id", ":math/inc"],
          [":udf/version", "v1"]
        ]),
        "?number"
      ],
      "?result"
    ]
  ]);
  assert.equal(
    q.udfPredicate(UdfDescriptor.predicate("score/high?"), result)
      .toForm()[0][0],
    q.sym("udf")
  );
  assert.deepEqual(q.udf(legacy, number).toForm()[1].asData(), legacy);

  const txDescriptor = UdfDescriptor.txFn("person/bootstrap");
  const predicateDescriptor = UdfDescriptor.predicate("person/valid?");
  const transaction = tx.data(
    tx.installUdf(txDescriptor),
    tx.callUdf(txDescriptor, "Ada"),
    tx.ensure(predicateDescriptor, -1),
    tx.uninstallUdf("person/bootstrap")
  );
  const txData = transaction.asData();
  assert.deepEqual(txData[0], new Map([
    [":db/ident", ":person/bootstrap"],
    [":db/udf", new Map([
      [":udf/lang", ":javascript"],
      [":udf/kind", ":tx-fn"],
      [":udf/id", ":person/bootstrap"]
    ])]
  ]));
  assert.deepEqual(txData[1], [
    ":db.fn/call",
    new Map([
      [":udf/lang", ":javascript"],
      [":udf/kind", ":tx-fn"],
      [":udf/id", ":person/bootstrap"]
    ]),
    "Ada"
  ]);
  assert.deepEqual(txData[2], [
    ":db/ensure",
    new Map([
      [":udf/lang", ":javascript"],
      [":udf/kind", ":predicate"],
      [":udf/id", ":person/valid?"]
    ]),
    -1
  ]);
  assert.deepEqual(txData[3], [
    ":db.fn/retractAttribute",
    [":db/ident", ":person/bootstrap"],
    ":db/udf"
  ]);

  assert.throws(() => UdfDescriptor.of("unknown", "bad/kind"), /Unsupported UDF kind/);
  assert.throws(
    () => UdfDescriptor.queryFn("bad/version", { version: 1.5 }),
    /version/
  );
  assert.throws(
    () => new UdfDescriptor({ id: "bad/key", extra: true }),
    /Unsupported UDF descriptor key/
  );
});

test("typed search clauses and options preserve search grammar", () => {
  const entity = q.var("entity");
  const term = q.var("term");
  const score = q.var("score");
  const vector = q.var("vector");
  const distance = q.var("distance");
  const predicate = q.var("predicate");

  const fulltextOpts = q.fulltextOptions({
    top: 5,
    display: "refs+scores",
    domains: ["people"]
  });
  const vectorOpts = q.vectorSearchOptions({
    top: 2,
    display: "refs+dists",
    domains: ["embeddings"]
  });
  const idocOpts = q.idocMatchOptions({ domains: ["profiles"] });

  assert.equal(fulltextOpts instanceof FulltextOptions, true);
  assert.equal(vectorOpts instanceof VectorSearchOptions, true);
  assert.equal(idocOpts instanceof IdocMatchOptions, true);
  assert.deepEqual(fulltextOpts.asData(), new Map([
    [":top", 5],
    [":display", ":refs+scores"],
    [":domains", ["people"]]
  ]));
  assert.deepEqual(vectorOpts.asData(), new Map([
    [":top", 2],
    [":display", ":refs+dists"],
    [":domains", ["embeddings"]]
  ]));
  assert.deepEqual(idocOpts.asData(), new Map([
    [":domains", ["profiles"]]
  ]));

  const search = q.query({
    find: q.relation(entity, score, distance),
    inputs: [q.DB, term, vector, predicate],
    where: [
      q.fulltext(
        term,
        [entity, q.IGNORE, q.IGNORE, score],
        {
          attribute: "person/name",
          options: fulltextOpts
        }
      ),
      q.vecNeighbors(
        vector,
        q.relationBinding(entity, q.IGNORE, q.IGNORE, distance),
        { options: vectorOpts }
      ),
      q.idocMatch(
        predicate,
        [entity, q.IGNORE, q.IGNORE],
        { options: idocOpts }
      )
    ]
  });
  assert.deepEqual(search.asData(), [
    ":find",
    "?entity",
    "?score",
    "?distance",
    ":in",
    "$",
    "?term",
    "?vector",
    "?predicate",
    ":where",
    [
      [
        "fulltext",
        "$",
        ":person/name",
        "?term",
        new Map([
          [":top", 5],
          [":display", ":refs+scores"],
          [":domains", ["people"]]
        ])
      ],
      [["?entity", "_", "_", "?score"]]
    ],
    [
      [
        "vec-neighbors",
        "$",
        "?vector",
        new Map([
          [":top", 2],
          [":display", ":refs+dists"],
          [":domains", ["embeddings"]]
        ])
      ],
      [["?entity", "_", "_", "?distance"]]
    ],
    [
      [
        "idoc-match",
        "$",
        "?predicate",
        new Map([[":domains", ["profiles"]]])
      ],
      [["?entity", "_", "_"]]
    ]
  ]);
});

test("typed search clauses reject invalid static shapes", () => {
  const entity = q.var("entity");

  assert.throws(
    () => q.fulltextOptions({ display: "refs+dists" }),
    /Unsupported search display/
  );
  assert.throws(
    () => q.vectorSearchOptions({ top: -1 }),
    /non-negative/
  );
  assert.throws(
    () => q.idocMatchOptions({ domains: [] }),
    /must not be empty/
  );
  assert.throws(
    () => q.fulltext(
      "Ada",
      [entity, q.IGNORE, q.IGNORE],
      { options: q.fulltextOptions({ display: "refs+scores" }) }
    ),
    /requires 4 result items/
  );
  assert.throws(
    () => q.vecNeighbors([1, 0], [entity, q.IGNORE, q.IGNORE]),
    /attribute or vector search domains/
  );
  assert.throws(
    () => q.fulltext(
      "Ada",
      [entity, q.IGNORE, q.IGNORE],
      { options: q.vectorSearchOptions({ domains: ["people"] }) }
    ),
    /Expected FulltextOptions/
  );
  assert.throws(
    () => q.idocMatch({}, [entity, q.IGNORE, "value"]),
    /q.var/
  );

  assert.equal(
    q.fulltext(
      "Ada",
      [entity, q.IGNORE, q.IGNORE, q.var("score")],
      { options: q.var("runtime_options") }
    ).toForm().length,
    2
  );
});

test("builder forms take recursive immutable snapshots", () => {
  const firstKeyword = q.kw("user/id");
  const secondKeyword = q.kw(":user/id");
  const firstSymbol = q.sym("status");
  const secondSymbol = q.sym("status");
  const tokenMap = new Map([
    [firstKeyword, 1],
    [secondKeyword, 2]
  ]);

  assert.equal(firstKeyword, secondKeyword);
  assert.equal(firstSymbol, secondSymbol);
  assert.equal(tokenMap.size, 1);
  assert.equal(tokenMap.get(q.kw("user/id")), 2);

  const literal = { labels: ["before"] };
  const clause = q.datom(q.var("entity"), "user/data", literal);
  const attrs = { "user/data": { tags: ["before"] } };
  const item = tx.entity(-1, attrs);
  const transaction = tx.data(item);
  const sourceDate = new Date("2026-08-27T00:00:00.000Z");
  const sourceBytes = new Uint8Array([1, 2]);
  const rawValue = q.raw([sourceDate, sourceBytes]);

  literal.labels.push("after");
  attrs["user/data"].tags.push("after");
  attrs["user/data"] = { tags: ["replacement"] };
  sourceDate.setUTCFullYear(2030);
  sourceBytes[0] = 9;

  const clauseForm = clause.toForm();
  const entityForm = item.toForm();
  const txForm = transaction.toForm();
  const storedData = entityForm.get(q.kw("user/data"));

  assert.deepEqual(clauseForm[2], { labels: ["before"] });
  assert.deepEqual(storedData, { tags: ["before"] });
  assert.equal(txForm[0], entityForm);
  assert.equal(Object.isFrozen(clauseForm), true);
  assert.equal(Object.isFrozen(clauseForm[2]), true);
  assert.equal(Object.isFrozen(storedData.tags), true);
  assert.equal(Object.isFrozen(txForm), true);
  assert.equal(rawValue.toForm()[0].toISOString(), "2026-08-27T00:00:00.000Z");
  assert.deepEqual(Array.from(rawValue.toForm()[1]), [1, 2]);

  assert.throws(() => clauseForm.push("changed"), TypeError);
  assert.throws(() => clauseForm[2].labels.push("changed"), TypeError);
  assert.throws(
    () => entityForm.set(q.kw("user/data"), { tags: [] }),
    /immutable/
  );
  assert.throws(
    () => Map.prototype.set.call(entityForm, q.kw("changed"), true),
    TypeError
  );
  assert.throws(() => rawValue.toForm()[0].setTime(0), /immutable/);
  assert.throws(() => {
    rawValue.toForm()[1][0] = 9;
  }, /immutable/);
});

test("transaction context-specific forms are explicit and composable", () => {
  const eve = tx.lookupRef("user/handle", "eve");
  const patches = [
    tx.patchSet(["profile", "status"], ":literal"),
    tx.patchUnset(q.kw("obsolete")),
    tx.patchUpdate(["tags"], "conj", ":literal"),
    tx.patchUpdate(q.kw("profile"), q.kw("assoc"), "role", "admin")
  ];
  const transaction = tx.data(
    tx.entity(-1, {
      "user/handle": "alice",
      "user/friend": eve,
      "user/child": tx.entity(-2, { "user/handle": "child" }),
      "user/data": { set: ":literal" }
    }),
    tx.patchIdoc(eve, "user/profile", patches),
    tx.invoke("people/inc-age", eve)
  );

  assert.equal(eve instanceof LookupRef, true);
  assert.deepEqual(eve.asData(), [":user/handle", "eve"]);
  assert.equal(patches.every((patch) => patch instanceof PatchOp), true);
  assert.equal(patches[0].toForm()[2], ":literal");
  assert.equal(patches[2].toForm()[3], ":literal");
  assert.deepEqual(transaction.asData(), [
    new Map([
      [":user/handle", "alice"],
      [":user/friend", [":user/handle", "eve"]],
      [":user/child", new Map([
        [":user/handle", "child"],
        [":db/id", -2]
      ])],
      [":user/data", { set: ":literal" }],
      [":db/id", -1]
    ]),
    [
      ":db.fn/patchIdoc",
      [":user/handle", "eve"],
      ":user/profile",
      [
        [":set", ["profile", "status"], ":literal"],
        [":unset", ":obsolete"],
        [":update", ["tags"], ":conj", ":literal"],
        [":update", ":profile", ":assoc", "role", "admin"]
      ]
    ],
    [":people/inc-age", [":user/handle", "eve"]]
  ]);
});

test("query builders reject invalid structural combinations", () => {
  const entity = q.var("entity");
  const name = q.var("name");
  const other = q.var("other");
  const clause = q.datom(entity, "person/name", name);

  assert.throws(
    () => q.query({ find: q.scalar(entity), keys: ["id"] }),
    /scalar find/
  );
  assert.throws(
    () => q.query({ find: q.collection(entity), keys: ["id"] }),
    /collection find/
  );
  assert.throws(
    () => q.query({ find: q.relation(entity, name), keys: ["id"] }),
    /field count/
  );
  assert.throws(
    () => q.query({ find: q.relation(entity), keys: [q.kw("id")] }),
    /field names/
  );

  assert.throws(() => q.joinVars(), /must not be empty/);
  assert.throws(
    () => q.joinVars(entity, { required: [entity] }),
    /distinct/
  );
  assert.throws(() => q.joinVars("entity"), /q.var/);

  assert.throws(() => q.asc("entity"), /order term/);
  assert.throws(() => q.desc(-1), /order term/);
  assert.throws(
    () => q.query({ find: q.relation(entity), orderBy: [q.asc(other)] }),
    /occur in the find/
  );
  assert.throws(
    () => q.query({ find: q.relation(entity), orderBy: [q.asc(1)] }),
    /outside the find/
  );
  assert.throws(
    () => q.query({
      find: q.relation(entity),
      orderBy: [q.asc(entity), q.desc(entity)]
    }),
    /distinct/
  );

  const branchGroup = q.and(clause, q.predicate("some?", name));
  assert.throws(
    () => q.query({ find: q.relation(entity), where: [branchGroup] }),
    /only valid as a branch/
  );
  assert.throws(() => q.not(branchGroup), /only valid as a branch/);

  const valid = q.query({
    find: q.relation(entity, name),
    keys: ["id", "name"],
    orderBy: [q.asc(entity), q.desc(1)],
    where: [q.or(branchGroup, clause)]
  });
  assert.equal(valid instanceof Query, true);
  assert.equal(q.query({
    find: q.tuple(entity, name),
    keys: ["id", "name"]
  }) instanceof Query, true);
  assert.equal(q.query({
    find: q.relation(entity),
    inputs: [q.RULES],
    where: [q.rule("and", entity)]
  }) instanceof Query, true);
});

test("rule sets validate branch arity and expose asData", () => {
  const entity = q.var("entity");
  const child = q.var("child");
  const clause = q.datom(entity, "person/child", child);
  const requiredBranch = q.ruleBranch(
    "ancestor",
    q.joinVars(child, { required: [entity] }),
    clause
  );
  const freeBranch = q.ruleBranch("ancestor", [entity, child], clause);

  assert.throws(
    () => q.rules(requiredBranch, freeBranch),
    /matching required\/free arity/
  );
  assert.throws(
    () => q.ruleBranch("ancestor", [entity], q.and(clause)),
    /top level of a rule branch/
  );

  assert.deepEqual(q.rules(requiredBranch).asData(), [
    [
      ["ancestor", ["?entity"], "?child"],
      ["?entity", ":person/child", "?child"]
    ]
  ]);
});

test(
  "composed queries and transactions execute through the JVM bridge",
  { skip: !runtimeAvailable, timeout: 30000 },
  async () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), "dtlv-js-composition-"));
    const conn = await connect(dir, {
      schema: {
        ":person/name": schemaAttr({
          valueType: ":db.type/string",
          fulltext: true,
          fulltextAutoDomain: true
        }),
        ":person/age": schemaAttr({ valueType: ":db.type/long" }),
        ":person/label": schemaAttr({ valueType: ":db.type/string" }),
        ":person/nickname": schemaAttr({ valueType: ":db.type/string" }),
        ":person/status": schemaAttr({ valueType: ":db.type/keyword" }),
        ":person/friend": schemaAttr({ valueType: ":db.type/ref" }),
        ":person/embedding": schemaAttr({ valueType: ":db.type/vec" }),
        ":user/handle": schemaAttr({
          valueType: ":db.type/string",
          unique: ":db.unique/identity"
        }),
        ":user/name": schemaAttr({ valueType: ":db.type/string" }),
        ":user/friend": schemaAttr({ valueType: ":db.type/ref" }),
        ":user/profile": idocAttr({ format: "edn", domain: "profiles" })
      },
      opts: {
        ":vector-opts": {
          ":dimensions": 2,
          ":metric-type": ":cosine"
        }
      }
    });

    try {
      await conn.transact(tx.data(
        tx.entity(-1, {
          "person/name": "Ada",
          "person/age": 42,
          "person/label": "?literal",
          "person/nickname": "Ace",
          "person/status": q.kw("active"),
          "person/friend": -2,
          "person/embedding": [1, 0]
        }),
        tx.entity(-2, {
          "person/name": "Bob",
          "person/age": 21,
          "person/label": ":literal",
          "person/status": q.kw("draft"),
          "person/embedding": [0, 1]
        })
      ));

      const entity = q.var("entity");
      const name = q.var("name");
      const age = q.var("age");
      const minimum = q.var("minimum");
      const adults = q.query({
        find: q.collection(name),
        inputs: [q.DB, minimum],
        where: [
          q.datom(entity, "person/name", name),
          q.datom(entity, "person/age", age),
          q.predicate(">=", age, minimum)
        ]
      });
      assert.deepEqual(await conn.query(adults, 30), ["Ada"]);

      const searchTerm = q.var("search_term");
      const fulltextNames = q.query({
        find: q.collection(name),
        inputs: [q.DB, searchTerm],
        where: [
          q.fulltext(
            searchTerm,
            [entity, q.IGNORE, q.IGNORE],
            {
              attribute: "person/name",
              options: q.fulltextOptions({ top: 1 })
            }
          ),
          q.datom(entity, "person/name", name)
        ]
      });
      assert.deepEqual(await conn.query(fulltextNames, "Ada"), ["Ada"]);

      const queryVector = q.var("query_vector");
      const distance = q.var("distance");
      const vectorNames = q.query({
        find: q.collection(name),
        inputs: [q.DB, queryVector],
        where: [
          q.vecNeighbors(
            queryVector,
            [entity, q.IGNORE, q.IGNORE, distance],
            {
              attribute: "person/embedding",
              options: q.vectorSearchOptions({
                top: 1,
                display: "refs+dists"
              })
            }
          ),
          q.datom(entity, "person/name", name)
        ]
      });
      assert.deepEqual(await conn.query(vectorNames, [1, 0]), ["Ada"]);

      const innerAge = q.var("inner_age");
      const youngest = q.var("youngest");
      const youngestEntity = q.var("youngest_entity");
      const youngestAgeQuery = q.query({
        find: q.relation(q.aggregate("min", innerAge)),
        where: [q.datom(q.IGNORE, "person/age", innerAge)]
      });
      const nestedQuery = q.query({
        find: q.relation(youngestEntity, youngest),
        where: [
          q.bind(
            "q",
            q.relationBinding(youngest),
            q.quote(youngestAgeQuery),
            q.DB
          ),
          q.datom(youngestEntity, "person/age", youngest)
        ]
      });
      assert.deepEqual(
        (await conn.query(nestedQuery)).map((row) => row.map(intValue)),
        [[2, 21]]
      );

      const ordered = q.query({
        find: q.relation(name, age),
        where: [
          q.datom(entity, "person/name", name),
          q.datom(entity, "person/age", age)
        ],
        orderBy: [q.desc(age)]
      });
      assert.deepEqual(
        (await conn.query(ordered)).map(([personName, personAge]) => [personName, intValue(personAge)]),
        [["Ada", 42], ["Bob", 21]]
      );

      const unordered = q.query({
        find: q.relation(name, age),
        where: [
          q.datom(entity, "person/name", name),
          q.datom(entity, "person/age", age)
        ]
      });
      const unorderedRows = await conn.query(unordered);
      assert.equal(Array.isArray(unorderedRows), true);
      assert.deepEqual(
        unorderedRows
          .map(([personName, personAge]) => [personName, intValue(personAge)])
          .sort(([left], [right]) => left.localeCompare(right)),
        [["Ada", 42], ["Bob", 21]]
      );

      const leadingQuestion = q.query({
        find: q.collection(entity),
        where: [q.datom(entity, "person/label", "?literal")]
      });
      const leadingColon = q.query({
        find: q.collection(entity),
        where: [q.datom(entity, "person/label", ":literal")]
      });
      assert.deepEqual((await conn.query(leadingQuestion)).map(intValue), [1]);
      assert.deepEqual((await conn.query(leadingColon)).map(intValue), [2]);

      const label = q.var("label");
      const byLabel = q.query({
        find: q.collection(entity),
        inputs: [q.DB, label],
        where: [q.datom(entity, "person/label", label)]
      });
      assert.deepEqual((await conn.query(byLabel, ":literal")).map(intValue), [2]);
      assert.equal(":plan" in await conn.explain(byLabel, { inputs: [":literal"] }), true);

      const byLabels = q.query({
        find: q.collection(entity),
        inputs: [q.DB, q.collectionBinding(label)],
        where: [q.datom(entity, "person/label", label)]
      });
      assert.deepEqual((await conn.query(byLabels, [":literal"])).map(intValue), [2]);

      const active = q.query({
        find: q.collection(name),
        where: [
          q.datom(entity, "person/name", name),
          q.datom(entity, "person/status", q.kw("active"))
        ]
      });
      assert.deepEqual(await conn.query(active), ["Ada"]);

      const status = q.var("status");
      const byStatus = q.query({
        find: q.collection(name),
        inputs: [q.DB, status],
        where: [
          q.datom(entity, "person/name", name),
          q.datom(entity, "person/status", status)
        ]
      });
      assert.deepEqual(await conn.query(byStatus, q.kw("active")), ["Ada"]);

      const fallbackSelector = q.selector(
        q.pullAttr("person/nickname", { default: "none", as: "nickname" })
      );
      assert.deepEqual(await conn.pull(fallbackSelector, 2), { nickname: "none" });

      const structuredFallback = q.selector(
        q.pullAttr("person/nickname", {
          default: { text: ":none" },
          as: "fallback"
        })
      );
      assert.deepEqual(await conn.pull(structuredFallback, 2), {
        fallback: { text: ":none" }
      });

      const tupleAlias = q.selector(
        q.pullAttr("person/nickname", {
          default: "none",
          as: ["label", ":literal"]
        })
      );
      const tupleAliasResult = await conn.pull(tupleAlias, 2);
      assert.equal(tupleAliasResult instanceof Map, true);
      assert.deepEqual(Array.from(tupleAliasResult), [
        [["label", ":literal"], "none"]
      ]);

      const legacyFallback = q.selector([
        ["person/nickname", ":default", "none", ":as", "nickname"]
      ]);
      assert.deepEqual(await conn.pull(legacyFallback, 2), { nickname: "none" });

      const pulledBob = q.query({
        find: q.scalar(q.pull(entity, fallbackSelector)),
        where: [q.datom(entity, "person/name", "Bob")]
      });
      assert.deepEqual(await conn.query(pulledBob), { nickname: "none" });

      const ageText = q.selector(
        q.pullAttr("person/age", { as: "age-text", xform: "str" })
      );
      assert.deepEqual(await conn.pull(ageText, 1), { "age-text": "42" });

      const friendName = q.selector(
        q.pullNested("person/friend", q.selector("person/name"))
      );
      assert.deepEqual(await conn.pull(friendName, 1), {
        ":person/friend": { ":person/name": "Bob" }
      });

      const recursiveFriend = q.selector(
        "db/id",
        q.pullRecursive(q.pullAttr("person/friend", { as: "friend" }), 1)
      );
      const recursiveResult = await conn.pull(recursiveFriend, 1);
      assert.equal(intValue(recursiveResult[":db/id"]), 1);
      assert.equal(intValue(recursiveResult.friend[":db/id"]), 2);

      const adultRule = q.ruleBranch(
        "adult",
        [entity, name, minimum],
        q.datom(entity, "person/name", name),
        q.datom(entity, "person/age", age),
        q.predicate(">=", age, minimum)
      );
      const byRule = q.query({
        find: q.collection(name),
        inputs: [q.DB, q.RULES, minimum],
        where: [q.rule("adult", entity, name, minimum)]
      });
      assert.deepEqual(await conn.query(byRule, q.rules(adultRule), 30), ["Ada"]);

      const eve = tx.lookupRef("user/handle", "eve");
      await conn.transact(tx.data(
        tx.entity(-10, {
          "user/handle": "eve",
          "user/name": "Eve",
          "user/profile": {
            status: "active",
            profile: { age: 30 },
            tags: ["a"],
            obsolete: true
          }
        })
      ));
      await conn.transact(tx.data(
        tx.entity(-11, {
          "user/handle": "alice",
          "user/friend": eve
        }),
        tx.entity(-12, {
          "user/handle": "parent",
          "user/friend": tx.entity(-13, {
            "user/handle": "child",
            "user/name": "Child"
          })
        }),
        tx.add(eve, "user/name", "Evelyn")
      ));
      await conn.transact(tx.data(
        tx.patchIdoc(eve, "user/profile", [
          tx.patchSet(["status"], ":literal"),
          tx.patchUpdate(["profile"], "assoc", "role", "admin"),
          tx.patchUpdate(["profile", "age"], "inc"),
          tx.patchUpdate(["tags"], "conj", ":literal"),
          tx.patchUnset(["obsolete"])
        ])
      ));

      const user = q.var("user");
      const handle = q.var("handle");
      const friend = q.var("friend");
      const friendHandle = q.var("friend_handle");
      const owner = q.var("owner");
      const friendByOwner = q.query({
        find: q.scalar(friendHandle),
        inputs: [q.DB, owner],
        where: [
          q.datom(user, "user/handle", owner),
          q.datom(user, "user/friend", friend),
          q.datom(friend, "user/handle", friendHandle)
        ]
      });
      assert.equal(await conn.query(friendByOwner, "alice"), "eve");
      assert.equal(await conn.query(friendByOwner, "parent"), "child");

      const userName = q.var("user_name");
      const nameByHandle = q.query({
        find: q.scalar(userName),
        inputs: [q.DB, handle],
        where: [
          q.datom(user, "user/handle", handle),
          q.datom(user, "user/name", userName)
        ]
      });
      assert.equal(await conn.query(nameByHandle, "eve"), "Evelyn");

      const profile = q.var("profile");
      const profileByHandle = q.query({
        find: q.scalar(profile),
        inputs: [q.DB, handle],
        where: [
          q.datom(user, "user/handle", handle),
          q.datom(user, "user/profile", profile)
        ]
      });
      assert.deepEqual(await conn.query(profileByHandle, "eve"), {
        status: ":literal",
        profile: { age: 31n, role: "admin" },
        tags: ["a", ":literal"]
      });

      const idocEntity = q.var("idoc_entity");
      const idocAttribute = q.var("idoc_attribute");
      const idocValue = q.var("idoc_value");
      const idocPredicate = q.var("idoc_predicate");
      const matchingHandles = q.query({
        find: q.collection(handle),
        inputs: [q.DB, idocPredicate],
        where: [
          q.idocMatch(
            idocPredicate,
            [idocEntity, idocAttribute, idocValue],
            { options: q.idocMatchOptions({ domains: ["profiles"] }) }
          ),
          q.datom(idocEntity, "user/handle", handle)
        ]
      });
      assert.deepEqual(
        await conn.query(
          matchingHandles,
          { profile: { age: q.ednList(q.sym(">="), 31) } }
        ),
        ["eve"]
      );
    } finally {
      await conn.close();
    }
  }
);
