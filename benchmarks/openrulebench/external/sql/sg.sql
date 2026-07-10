-- Same Generation using Recursive CTE
-- Works with PostgreSQL and SQLite

-- Create base tables
CREATE TABLE IF NOT EXISTS par (
    a INTEGER,
    b INTEGER
);

CREATE TABLE IF NOT EXISTS sib (
    a INTEGER,
    b INTEGER
);

-- Create indexes
CREATE INDEX IF NOT EXISTS idx_par_a ON par(a);
CREATE INDEX IF NOT EXISTS idx_par_b ON par(b);
CREATE INDEX IF NOT EXISTS idx_sib_a ON sib(a);
CREATE INDEX IF NOT EXISTS idx_sib_b ON sib(b);

-- SG with both arguments free
WITH RECURSIVE sg AS (
    -- Base case: siblings are same-generation
    SELECT a AS x, b AS y
    FROM sib
    UNION
    -- Recursive case from OpenRuleBench: par(X,Z), sg(Z,Z1), par(Y,Z1)
    SELECT p1.a AS x, p2.a AS y
    FROM par p1
    JOIN sg s ON p1.b = s.x
    JOIN par p2 ON p2.b = s.y
)
SELECT COUNT(*) AS sg_count FROM sg;

-- SG with first argument bound
WITH RECURSIVE sg_from AS (
    SELECT a AS x, b AS y
    FROM sib
    UNION
    SELECT p1.a AS x, p2.a AS y
    FROM par p1
    JOIN sg_from s ON p1.b = s.x
    JOIN par p2 ON p2.b = s.y
)
SELECT COUNT(*) AS sg_from_count FROM sg_from WHERE x = 0;
