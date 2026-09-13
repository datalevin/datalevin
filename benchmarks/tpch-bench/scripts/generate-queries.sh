#!/usr/bin/env bash
#
# Regenerate the canonical TPC-H query substitutions used by this benchmark.
#
# The committed queries under queries/standard/ were produced from the TPC-H
# reference query templates using qgen's default substitution values (`-d`).
# Defaults are fixed, so the generated SQL is byte-for-byte reproducible and
# identical across systems. Run this only when intentionally refreshing the
# query set; the output is committed.
#
# The PostgreSQL variants are derived from the standard text with the
# Oracle/DB2 interval precision suffix removed. queries/sqlite/ is maintained by
# hand because SQLite needs different date and string functions.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DBGEN_DIR="${DBGEN_DIR:-$HERE/dbgen}"
STD_DIR="$HERE/queries/standard"
PG_DIR="$HERE/queries/postgres"

if [[ ! -x "$DBGEN_DIR/qgen" ]]; then
  "$HERE/scripts/fetch-dbgen.sh"
fi

mkdir -p "$STD_DIR" "$PG_DIR"
for n in $(seq 1 22); do
  raw="$(DSS_QUERY="$DBGEN_DIR/queries" \
           "$DBGEN_DIR/qgen" -d -b "$DBGEN_DIR/dists.dss" "$n")"
  # TPC-H 2.1.2.9 requires a result row limit on Q2, Q3, Q10, Q18, and Q21.
  # qgen reports it as the ROWS_FETCH directive, which is only a comment in
  # portable dialects, so restore it as a real LIMIT clause.
  rows="$(printf '%s\n' "$raw" \
            | sed -n 's/^--#SET ROWS_FETCH \([0-9][0-9]*\)$/\1/p')"
  printf '%s\n' "$raw" \
    | sed -e 's/^-- using default substitutions$//' \
          -e '/^--#SET ROWS_FETCH/d' \
    > "$STD_DIR/$n.sql"
  if [[ -n "$rows" ]]; then
    perl -0777 -i -pe "s/;(\s*)\$/\nlimit $rows;\$1/" "$STD_DIR/$n.sql"
  fi
  # PostgreSQL rejects the `day (3)` precision suffix and reads plural unit
  # names more reliably than `interval '90' day`. -E keeps the alternation
  # portable between GNU and BSD sed.
  sed -E -e 's/\(3\)$//' \
      -e "s/interval '1' (day|month|year)/interval '1 \1'/" \
      -e "s/interval '([0-9]+)' (day|month|year)/interval '\1 \2s'/" \
      "$STD_DIR/$n.sql" > "$PG_DIR/$n.sql"
  echo "wrote $STD_DIR/$n.sql and $PG_DIR/$n.sql"
done

# Q15 uses a view in the reference text; both SQLite and PostgreSQL run it as a
# single-statement CTE.
cat > "$PG_DIR/15.sql" <<'SQL'
with revenue0 (supplier_no, total_revenue) as (
	select
		l_suppkey,
		sum(l_extendedprice * (1 - l_discount))
	from
		lineitem
	where
		l_shipdate >= date '1996-01-01'
		and l_shipdate < date '1996-01-01' + interval '3 months'
	group by
		l_suppkey
)
select
	s_suppkey,
	s_name,
	s_address,
	s_phone,
	total_revenue
from
	supplier,
	revenue0
where
	s_suppkey = supplier_no
	and total_revenue = (
		select
			max(total_revenue)
		from
			revenue0
	)
order by
	s_suppkey;
SQL
echo "wrote $PG_DIR/15.sql"
