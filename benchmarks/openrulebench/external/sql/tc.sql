-- Transitive Closure using Recursive CTE
-- Works with PostgreSQL and SQLite

-- Create edge table
CREATE TABLE IF NOT EXISTS edge (
    from_node INTEGER,
    to_node INTEGER
);

-- Create index for better join performance
CREATE INDEX IF NOT EXISTS idx_edge_from ON edge(from_node);
CREATE INDEX IF NOT EXISTS idx_edge_to ON edge(to_node);

-- TC with both arguments free (full materialization)
-- This computes the entire transitive closure
WITH RECURSIVE tc AS (
    -- Base case: direct edges
    SELECT from_node AS a, to_node AS b
    FROM edge
    UNION
    -- Recursive case: extend paths
    SELECT e.from_node AS a, t.b AS b
    FROM edge e
    JOIN tc t ON e.to_node = t.a
)
SELECT COUNT(*) AS tc_count FROM tc;

-- TC with first argument bound (reachable from node 0)
WITH RECURSIVE tc_from AS (
    SELECT to_node AS b
    FROM edge
    WHERE from_node = 0
    UNION
    SELECT e.to_node AS b
    FROM edge e
    JOIN tc_from t ON e.from_node = t.b
)
SELECT COUNT(*) AS reachable_count FROM tc_from;
