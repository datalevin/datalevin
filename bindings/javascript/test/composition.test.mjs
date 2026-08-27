import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { test } from "node:test";

import {
  Keyword,
  LookupRef,
  PatchOp,
  PullAttr,
  PullNested,
  PullSelector,
  Query,
  TxData,
  connect,
  idocAttr,
  q,
  schemaAttr,
  tx
} from "../src/index.js";
import { resolveClasspath } from "../src/jvm.js";

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
        ":person/name": schemaAttr({ valueType: ":db.type/string" }),
        ":person/age": schemaAttr({ valueType: ":db.type/long" }),
        ":person/label": schemaAttr({ valueType: ":db.type/string" }),
        ":person/nickname": schemaAttr({ valueType: ":db.type/string" }),
        ":person/status": schemaAttr({ valueType: ":db.type/keyword" }),
        ":person/friend": schemaAttr({ valueType: ":db.type/ref" }),
        ":user/handle": schemaAttr({
          valueType: ":db.type/string",
          unique: ":db.unique/identity"
        }),
        ":user/name": schemaAttr({ valueType: ":db.type/string" }),
        ":user/friend": schemaAttr({ valueType: ":db.type/ref" }),
        ":user/profile": idocAttr({ format: "edn" })
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
          "person/friend": -2
        }),
        tx.entity(-2, {
          "person/name": "Bob",
          "person/age": 21,
          "person/label": ":literal",
          "person/status": q.kw("draft")
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
    } finally {
      await conn.close();
    }
  }
);
