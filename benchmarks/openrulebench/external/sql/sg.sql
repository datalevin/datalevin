-- Same Generation using Recursive CTE
-- Works with PostgreSQL and SQLite

-- Create parent table
CREATE TABLE IF NOT EXISTS parent (
    parent_node INTEGER,
    child_node INTEGER
);

-- Create indexes
CREATE INDEX IF NOT EXISTS idx_parent_parent ON parent(parent_node);
CREATE INDEX IF NOT EXISTS idx_parent_child ON parent(child_node);

-- SG with both arguments free
-- Two nodes are same generation if they have parents that are same generation
WITH RECURSIVE sg AS (
    -- Base case: everyone is same generation as themselves
    SELECT DISTINCT child_node AS x, child_node AS y
    FROM parent
    UNION
    -- Recursive case: children of same-gen parents are same-gen
    SELECT p1.child_node AS x, p2.child_node AS y
    FROM parent p1
    JOIN parent p2 ON p1.parent_node != p2.parent_node
    JOIN sg s ON p1.parent_node = s.x AND p2.parent_node = s.y
)
SELECT COUNT(*) AS sg_count FROM sg;

-- SG with first argument bound
WITH RECURSIVE sg_from AS (
    -- Base case: node is same-gen as itself
    SELECT 0 AS y
    UNION
    -- Recursive case
    SELECT p2.child_node AS y
    FROM parent p1
    JOIN parent p2 ON p1.parent_node != p2.parent_node
    JOIN sg_from s ON p1.child_node = 0 AND p2.parent_node = s.y
    WHERE p1.child_node = 0
)
SELECT COUNT(*) AS sg_from_count FROM sg_from;
