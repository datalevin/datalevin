-- TPC-H foreign-key indexes for SQLite.

CREATE INDEX nation_fk1   ON nation   (n_regionkey);
CREATE INDEX supplier_fk1 ON supplier (s_nationkey);
CREATE INDEX customer_fk1 ON customer (c_nationkey);
CREATE INDEX partsupp_fk1 ON partsupp (ps_suppkey);
CREATE INDEX partsupp_fk2 ON partsupp (ps_partkey);
CREATE INDEX orders_fk1   ON orders   (o_custkey);
CREATE INDEX lineitem_fk1 ON lineitem (l_orderkey);
CREATE INDEX lineitem_fk2 ON lineitem (l_partkey, l_suppkey);
