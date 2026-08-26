// Logical schema constraints only. Deliberately omit workload-specific
// secondary indexes: Datalevin indexes every datom in AVE order by default,
// and this benchmark compares that out-of-the-box behavior with Neo4j.
CREATE CONSTRAINT person_id IF NOT EXISTS FOR (node:Person) REQUIRE node.id IS UNIQUE;
CREATE CONSTRAINT forum_id IF NOT EXISTS FOR (node:Forum) REQUIRE node.id IS UNIQUE;
CREATE CONSTRAINT post_id IF NOT EXISTS FOR (node:Post) REQUIRE node.id IS UNIQUE;
CREATE CONSTRAINT comment_id IF NOT EXISTS FOR (node:Comment) REQUIRE node.id IS UNIQUE;
CREATE CONSTRAINT place_id IF NOT EXISTS FOR (node:Place) REQUIRE node.id IS UNIQUE;
CREATE CONSTRAINT organisation_id IF NOT EXISTS FOR (node:Organisation) REQUIRE node.id IS UNIQUE;
CREATE CONSTRAINT tag_id IF NOT EXISTS FOR (node:Tag) REQUIRE node.id IS UNIQUE;
CREATE CONSTRAINT tag_class_id IF NOT EXISTS FOR (node:TagClass) REQUIRE node.id IS UNIQUE;
CALL db.awaitIndexes(600);
