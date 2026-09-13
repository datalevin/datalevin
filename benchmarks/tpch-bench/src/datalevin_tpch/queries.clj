(ns datalevin-tpch.queries
  "TPC-H queries translated to Datalevin Datalog.

  Each `q-N` var holds the Datalog form for TPC-H query N. The SQL equivalents
  live under queries/{postgres,sqlite}/N.sql. Parameter values match the fixed
  qgen defaults used to generate the SQL, so results are directly comparable.

  ORDER BY is part of each query, using zero-based output column indexes so
  aggregate columns are sorted too. Sorting is included in query execution.

  Date literals are precomputed from the SQL date arithmetic, e.g. the default
  Q1 predicate `l_shipdate <= date '1998-12-01' - interval '90' day` becomes the
  bound `\"1998-09-02\"`. Dates are stored as ISO-8601 strings, which compare
  correctly lexicographically.")

;; Q1: Pricing Summary Report
(def q-1
  '[:find ?l-returnflag ?l-linestatus
    (sum ?l-quantity)
    (sum ?l-extendedprice)
    (sum ?disc-price)
    (sum ?charge)
    (avg ?l-quantity)
    (avg ?l-extendedprice)
    (avg ?l-discount)
    (count ?l)
    :order-by [0 :asc 1 :asc]
    :where
    [?l :lineitem/returnflag ?l-returnflag]
    [?l :lineitem/linestatus ?l-linestatus]
    [?l :lineitem/quantity ?l-quantity]
    [?l :lineitem/extendedprice ?l-extendedprice]
    [?l :lineitem/discount ?l-discount]
    [?l :lineitem/tax ?l-tax]
    [?l :lineitem/shipdate ?l-shipdate]
    [(<= ?l-shipdate "1998-09-02")]
    [(- 1 ?l-discount) ?disc]
    [(* ?l-extendedprice ?disc) ?disc-price]
    [(* ?disc-price (+ 1 ?l-tax)) ?charge]])

;; Q6: Forecasting Revenue Change
(def q-6
  '[:find (sum ?revenue) .
    :with ?l
    :where
    [?l :lineitem/shipdate ?l-shipdate]
    [(>= ?l-shipdate "1994-01-01")]
    [(< ?l-shipdate "1995-01-01")]
    [?l :lineitem/discount ?l-discount]
    [(>= ?l-discount 0.05)]
    [(<= ?l-discount 0.07)]
    [?l :lineitem/quantity ?l-quantity]
    [(< ?l-quantity 24)]
    [?l :lineitem/extendedprice ?l-extendedprice]
    [(* ?l-extendedprice ?l-discount) ?revenue]])

;; Q2: Minimum Cost Supplier
(def q-2
  '[:find ?s-acctbal ?s-name ?n-name ?p-partkey ?p-mfgr ?s-address ?s-phone
    ?s-comment
    :order-by [0 :desc 2 :asc 1 :asc 3 :asc]
    :limit 100
    :where
    [?p :part/partkey ?p-partkey]
    [?p :part/size 15]
    [?p :part/type ?p-type]
    [(like ?p-type "%BRASS")]
    [?p :part/mfgr ?p-mfgr]
    [?ps :partsupp/partkey ?p-partkey]
    [?ps :partsupp/supplycost ?ps-supplycost]
    [?s :supplier/suppkey ?s-suppkey]
    [?ps :partsupp/suppkey ?s-suppkey]
    [?s :supplier/name ?s-name]
    [?s :supplier/address ?s-address]
    [?s :supplier/phone ?s-phone]
    [?s :supplier/acctbal ?s-acctbal]
    [?s :supplier/comment ?s-comment]
    [?s :supplier/nationkey ?s-nationkey]
    [?n :nation/nationkey ?s-nationkey]
    [?n :nation/name ?n-name]
    [?n :nation/regionkey ?n-regionkey]
    [?r :region/regionkey ?n-regionkey]
    [?r :region/name "EUROPE"]
    [(q [:find (min ?sc) .
         :in $ ?pk
         :where
         [?ps2 :partsupp/partkey ?pk]
         [?ps2 :partsupp/supplycost ?sc]
         [?ps2 :partsupp/suppkey ?sk2]
         [?s2 :supplier/suppkey ?sk2]
         [?s2 :supplier/nationkey ?nk2]
         [?n2 :nation/nationkey ?nk2]
         [?n2 :nation/regionkey ?rk2]
         [?r2 :region/regionkey ?rk2]
         [?r2 :region/name "EUROPE"]] $ ?p-partkey) ?mincost]
    [(= ?ps-supplycost ?mincost)]])

;; Q3: Shipping Priority
(def q-3
  '[:find ?l-orderkey (sum ?revenue) ?o-orderdate ?o-shippriority
    :order-by [1 :desc 2 :asc]
    :limit 10
    :with ?l
    :where
    [?c :customer/mktsegment "BUILDING"]
    [?c :customer/custkey ?c-custkey]
    [?o :orders/custkey ?c-custkey]
    [?o :orders/orderkey ?l-orderkey]
    [?o :orders/orderdate ?o-orderdate]
    [(< ?o-orderdate "1995-03-15")]
    [?o :orders/shippriority ?o-shippriority]
    [?l :lineitem/orderkey ?l-orderkey]
    [?l :lineitem/shipdate ?l-shipdate]
    [(> ?l-shipdate "1995-03-15")]
    [?l :lineitem/extendedprice ?ep]
    [?l :lineitem/discount ?disc]
    [(- 1 ?disc) ?x]
    [(* ?ep ?x) ?revenue]])

;; Q4: Order Priority Checking
(def q-4
  '[:find ?o-orderpriority (count-distinct ?o)
    :order-by [0 :asc]
    :where
    [?o :orders/orderdate ?od]
    [(>= ?od "1993-07-01")]
    [(< ?od "1993-10-01")]
    [?o :orders/orderpriority ?o-orderpriority]
    [?o :orders/orderkey ?ok]
    [?l :lineitem/orderkey ?ok]
    [?l :lineitem/commitdate ?cd]
    [?l :lineitem/receiptdate ?rd]
    [(< ?cd ?rd)]])

;; Q5: Local Supplier Volume
(def q-5
  '[:find ?n-name (sum ?revenue)
    :order-by [1 :desc]
    :with ?l
    :where
    [?c :customer/custkey ?c-custkey]
    [?o :orders/custkey ?c-custkey]
    [?o :orders/orderkey ?o-orderkey]
    [?o :orders/orderdate ?od]
    [(>= ?od "1994-01-01")]
    [(< ?od "1995-01-01")]
    [?l :lineitem/orderkey ?o-orderkey]
    [?l :lineitem/suppkey ?l-suppkey]
    [?s :supplier/suppkey ?l-suppkey]
    [?c :customer/nationkey ?c-nationkey]
    [?s :supplier/nationkey ?c-nationkey]
    [?s :supplier/nationkey ?s-nationkey]
    [?n :nation/nationkey ?s-nationkey]
    [?n :nation/name ?n-name]
    [?n :nation/regionkey ?nrk]
    [?r :region/regionkey ?nrk]
    [?r :region/name "ASIA"]
    [?l :lineitem/extendedprice ?ep]
    [?l :lineitem/discount ?disc]
    [(- 1 ?disc) ?x]
    [(* ?ep ?x) ?revenue]])

;; Q7: Volume Shipping
(def q-7
  '[:find ?supp-nation ?cust-nation ?l-year (sum ?volume)
    :order-by [0 :asc 1 :asc 2 :asc]
    :with ?l
    :where
    [?s :supplier/suppkey ?s-suppkey]
    [?l :lineitem/suppkey ?s-suppkey]
    [?s :supplier/nationkey ?s-nationkey]
    [?n1 :nation/nationkey ?s-nationkey]
    [?n1 :nation/name ?supp-nation]
    [?l :lineitem/orderkey ?l-orderkey]
    [?l :lineitem/shipdate ?l-shipdate]
    [(>= ?l-shipdate "1995-01-01")]
    [(<= ?l-shipdate "1996-12-31")]
    [?o :orders/orderkey ?l-orderkey]
    [?o :orders/custkey ?c-custkey]
    [?c :customer/custkey ?c-custkey]
    [?c :customer/nationkey ?c-nationkey]
    [?n2 :nation/nationkey ?c-nationkey]
    [?n2 :nation/name ?cust-nation]
    [(or (and (= ?supp-nation "FRANCE") (= ?cust-nation "GERMANY"))
         (and (= ?supp-nation "GERMANY") (= ?cust-nation "FRANCE")))]
    [(subs ?l-shipdate 0 4) ?ys]
    [(parse-long ?ys) ?l-year]
    [?l :lineitem/extendedprice ?ep]
    [?l :lineitem/discount ?disc]
    [(- 1 ?disc) ?x]
    [(* ?ep ?x) ?volume]])

;; Q8: National Market Share
;;
;; A single aggregate relation computes total and Brazil-only volume with a
;; mutually exclusive or-join, avoiding a second relation that can be empty for
;; a given year.
(def q-8
  '[:find ?o-year ?mkt
    :order-by [0 :asc]
    :where
    [(q [:find ?y (sum ?volume) (sum ?bz)
         :with ?l
         :where
         [?p :part/partkey ?p-partkey]
         [?p :part/type "ECONOMY ANODIZED STEEL"]
         [?l :lineitem/partkey ?p-partkey]
         [?s :supplier/suppkey ?s-suppkey]
         [?l :lineitem/suppkey ?s-suppkey]
         [?l :lineitem/orderkey ?l-orderkey]
         [?o :orders/orderkey ?l-orderkey]
         [?o :orders/custkey ?o-custkey]
         [?o :orders/orderdate ?o-orderdate]
         [(>= ?o-orderdate "1995-01-01")]
         [(<= ?o-orderdate "1996-12-31")]
         [?c :customer/custkey ?o-custkey]
         [?c :customer/nationkey ?c-nationkey]
         [?n1 :nation/nationkey ?c-nationkey]
         [?n1 :nation/regionkey ?n1-regionkey]
         [?r :region/regionkey ?n1-regionkey]
         [?r :region/name "AMERICA"]
         [?s :supplier/nationkey ?s-nationkey]
         [?n2 :nation/nationkey ?s-nationkey]
         [?n2 :nation/name ?nation]
         [?l :lineitem/extendedprice ?ep]
         [?l :lineitem/discount ?disc]
         [(- 1 ?disc) ?x]
         [(* ?ep ?x) ?volume]
         (or-join [?nation ?weight]
                  (and [(= ?nation "BRAZIL")] [(ground 1.0) ?weight])
                  (and [(not= ?nation "BRAZIL")] [(ground 0.0) ?weight]))
         [(* ?volume ?weight) ?bz]
         [(subs ?o-orderdate 0 4) ?ys]
         [(parse-long ?ys) ?y]] $) [[?o-year ?total ?brazil]]]
    [(/ ?brazil ?total) ?mkt]])

;; Q9: Product Type Profit Measure
(def q-9
  '[:find ?nation ?o-year (sum ?amount)
    :order-by [0 :asc 1 :desc]
    :with ?l
    :where
    [?p :part/partkey ?p-partkey]
    [?p :part/name ?p-name]
    [(like ?p-name "%green%")]
    [?l :lineitem/partkey ?p-partkey]
    [?s :supplier/suppkey ?s-suppkey]
    [?l :lineitem/suppkey ?s-suppkey]
    [?s :supplier/nationkey ?s-nationkey]
    [?n :nation/nationkey ?s-nationkey]
    [?n :nation/name ?nation]
    [?l :lineitem/orderkey ?l-orderkey]
    [?o :orders/orderkey ?l-orderkey]
    [?o :orders/orderdate ?o-orderdate]
    [?ps :partsupp/partkey ?p-partkey]
    [?ps :partsupp/suppkey ?s-suppkey]
    [?ps :partsupp/supplycost ?ps-supplycost]
    [?l :lineitem/quantity ?l-quantity]
    [?l :lineitem/extendedprice ?ep]
    [?l :lineitem/discount ?disc]
    [(- 1 ?disc) ?x]
    [(* ?ep ?x) ?a]
    [(* ?ps-supplycost ?l-quantity) ?b]
    [(- ?a ?b) ?amount]
    [(subs ?o-orderdate 0 4) ?ys]
    [(parse-long ?ys) ?o-year]])

;; Q10: Returned Item Reporting
(def q-10
  '[:find ?c-custkey ?c-name (sum ?revenue) ?c-acctbal ?n-name ?c-address
    ?c-phone ?c-comment
    :order-by [2 :desc]
    :limit 20
    :with ?l
    :where
    [?c :customer/custkey ?c-custkey]
    [?c :customer/name ?c-name]
    [?c :customer/acctbal ?c-acctbal]
    [?c :customer/address ?c-address]
    [?c :customer/phone ?c-phone]
    [?c :customer/comment ?c-comment]
    [?c :customer/nationkey ?c-nationkey]
    [?n :nation/nationkey ?c-nationkey]
    [?n :nation/name ?n-name]
    [?o :orders/custkey ?c-custkey]
    [?o :orders/orderkey ?o-orderkey]
    [?o :orders/orderdate ?od]
    [(>= ?od "1993-10-01")]
    [(< ?od "1994-01-01")]
    [?l :lineitem/orderkey ?o-orderkey]
    [?l :lineitem/returnflag "R"]
    [?l :lineitem/extendedprice ?ep]
    [?l :lineitem/discount ?disc]
    [(- 1 ?disc) ?x]
    [(* ?ep ?x) ?revenue]])

;; Q11: Important Stock Identification
(def q-11
  '[:find ?ps-partkey ?value
    :order-by [1 :desc]
    :where
    [(q [:find ?pk (sum ?v)
         :with ?ps
         :where
         [?ps :partsupp/suppkey ?sk]
         [?ps :partsupp/partkey ?pk]
         [?ps :partsupp/availqty ?q]
         [?ps :partsupp/supplycost ?sc]
         [?s :supplier/suppkey ?sk]
         [?s :supplier/nationkey ?nk]
         [?n :nation/nationkey ?nk]
         [?n :nation/name "GERMANY"]
         [(* ?sc ?q) ?v]] $) [[?ps-partkey ?value]]]
    [(q [:find (sum ?v) .
         :with ?ps
         :where
         [?ps :partsupp/suppkey ?sk]
         [?ps :partsupp/availqty ?q]
         [?ps :partsupp/supplycost ?sc]
         [?s :supplier/suppkey ?sk]
         [?s :supplier/nationkey ?nk]
         [?n :nation/nationkey ?nk]
         [?n :nation/name "GERMANY"]
         [(* ?sc ?q) ?v]] $) ?total]
    [(* ?total 0.0001) ?threshold]
    [(> ?value ?threshold)]])

;; Q12: Shipping Modes and Order Priority
(def q-12
  '[:find ?l-shipmode (sum ?high) (sum ?low)
    :order-by [0 :asc]
    :with ?l
    :where
    [?l :lineitem/orderkey ?ok]
    [?l :lineitem/shipmode ?l-shipmode]
    [(or (= ?l-shipmode "MAIL") (= ?l-shipmode "SHIP"))]
    [?l :lineitem/commitdate ?cd]
    [?l :lineitem/receiptdate ?rd]
    [(< ?cd ?rd)]
    [?l :lineitem/shipdate ?sd]
    [(< ?sd ?cd)]
    [(>= ?rd "1994-01-01")]
    [(< ?rd "1995-01-01")]
    [?o :orders/orderkey ?ok]
    [?o :orders/orderpriority ?o-pri]
    [(or (= ?o-pri "1-URGENT") (= ?o-pri "2-HIGH")) ?hi]
    (or-join [?hi ?high ?low]
             (and [(= ?hi true)] [(ground 1) ?high] [(ground 0) ?low])
             (and [(= ?hi false)] [(ground 0) ?high] [(ground 1) ?low]))])

;; Q13: Customer Distribution
;;
;; SQL uses a LEFT OUTER JOIN so customers with no qualifying orders count as
;; zero. The two or-join branches are mutually exclusive: the first uses a
;; correlated count for customers with matching orders, the second uses `not`
;; to catch the rest and bind zero.
(def q-13
  '[:find ?c-count (count ?c)
    :order-by [1 :desc 0 :desc]
    :where
    (or-join [?c ?c-count]
             (and [?c :customer/custkey ?ck]
                  [(q [:find (count ?o) .
                       :in $ ?ck
                       :where
                       [?o :orders/custkey ?ck]
                       [?o :orders/comment ?oc]
                       [(not (re-find #"special.*requests" ?oc))]] $ ?ck) ?c-count])
             (and [?c :customer/custkey ?ck]
                  (not [?o :orders/custkey ?ck]
                       [?o :orders/comment ?oc]
                       [(not (re-find #"special.*requests" ?oc))])
                  [(ground 0) ?c-count]))])

;; Q14: Promotion Effect
;;
;; `if` is not resolvable inside a nested query, so the SQL CASE becomes a
;; per-row contribution: `(or (and ?promo? ?v) 0.0)`. Aggregating that over
;; every in-range lineitem keeps the numerator row present (0.0 when no promo
;; part qualifies) instead of the empty result a promo-only filter produces.
;; A scalar result preserves SQL's NULL when no lineitems qualify; the verifier
;; wraps it as one row, just as it does for the other ungrouped aggregates.
(def q-14
  '[:find ?promo .
    :where
    [(q [:find (sum ?contrib) .
         :with ?l
         :where
         [?l :lineitem/shipdate ?sd]
         [(>= ?sd "1995-09-01")]
         [(< ?sd "1995-10-01")]
         [?l :lineitem/partkey ?pk]
         [?p :part/partkey ?pk]
         [?p :part/type ?pt]
         [(like ?pt "PROMO%") ?promo?]
         [?l :lineitem/extendedprice ?ep]
         [?l :lineitem/discount ?d]
         [(- 1 ?d) ?x]
         [(* ?ep ?x) ?v]
         [(or (and ?promo? ?v) 0.0) ?contrib]] $) ?pvsum]
    [(q [:find (sum ?v) .
         :with ?l
         :where
         [?l :lineitem/shipdate ?sd]
         [(>= ?sd "1995-09-01")]
         [(< ?sd "1995-10-01")]
         [?l :lineitem/partkey ?pk]
         [?p :part/partkey ?pk]
         [?l :lineitem/extendedprice ?ep]
         [?l :lineitem/discount ?d]
         [(- 1 ?d) ?x]
         [(* ?ep ?x) ?v]] $) ?vsum]
    [(/ (* 100.0 ?pvsum) ?vsum) ?promo]])

;; Q15: Top Supplier
(def q-15
  '[:find ?s-suppkey ?s-name ?s-address ?s-phone ?total
    :order-by [0 :asc]
    :where
    [(q [:find (max ?t) .
         :where
         [(q [:find ?sk (sum ?rev)
              :with ?l
              :where
              [?l :lineitem/shipdate ?sd]
              [(>= ?sd "1996-01-01")]
              [(< ?sd "1996-04-01")]
              [?l :lineitem/suppkey ?sk]
              [?l :lineitem/extendedprice ?ep]
              [?l :lineitem/discount ?d]
              [(- 1 ?d) ?x]
              [(* ?ep ?x) ?rev]] $) [[?sk ?t]]]] $) ?max]
    [(q [:find ?sk (sum ?rev)
         :with ?l
         :where
         [?l :lineitem/shipdate ?sd]
         [(>= ?sd "1996-01-01")]
         [(< ?sd "1996-04-01")]
         [?l :lineitem/suppkey ?sk]
         [?l :lineitem/extendedprice ?ep]
         [?l :lineitem/discount ?d]
         [(- 1 ?d) ?x]
         [(* ?ep ?x) ?rev]] $) [[?s-suppkey ?total]]]
    [(= ?total ?max)]
    [?s :supplier/suppkey ?s-suppkey]
    [?s :supplier/name ?s-name]
    [?s :supplier/address ?s-address]
    [?s :supplier/phone ?s-phone]])

;; Q16: Parts/Supplier Relationship
(def q-16
  '[:find ?p-brand ?p-type ?p-size (count-distinct ?ps-suppkey)
    :order-by [3 :desc 0 :asc 1 :asc 2 :asc]
    :where
    [?p :part/partkey ?pk]
    [?p :part/brand ?p-brand]
    [?p :part/type ?p-type]
    [?p :part/size ?p-size]
    [(not= ?p-brand "Brand#45")]
    [(not-like ?p-type "MEDIUM POLISHED%")]
    [(or (= ?p-size 49) (= ?p-size 14) (= ?p-size 23) (= ?p-size 45)
         (= ?p-size 19) (= ?p-size 3) (= ?p-size 36) (= ?p-size 9))]
    [?ps :partsupp/partkey ?pk]
    [?ps :partsupp/suppkey ?ps-suppkey]
    (not [?s :supplier/suppkey ?ps-suppkey]
         [?s :supplier/comment ?sc]
         [(like ?sc "%Customer%Complaints%")])])

;; Q17: Small-Quantity-Order Revenue
(def q-17
  '[:find (sum ?ep7) .
    :with ?l
    :where
    [?p :part/partkey ?pk]
    [?p :part/brand "Brand#23"]
    [?p :part/container "MED BOX"]
    [?l :lineitem/partkey ?pk]
    [?l :lineitem/quantity ?q]
    [(q [:find (avg ?q2) .
         :with ?l2
         :in $ ?pk
         :where
         [?l2 :lineitem/partkey ?pk]
         [?l2 :lineitem/quantity ?q2]] $ ?pk) ?avgq]
    [(* 0.2 ?avgq) ?thr]
    [(< ?q ?thr)]
    [?l :lineitem/extendedprice ?ep]
    [(/ ?ep 7.0) ?ep7]])

;; Q18: Large Volume Customer
(def q-18
  '[:find ?c-name ?c-custkey ?ok ?o-orderdate ?o-totalprice
    (sum ?l-quantity)
    :with ?l
    :order-by [4 :desc 3 :asc]
    :limit 100
    :where
    [(q [:find ?ok (sum ?q)
         :with ?l
         :where
         [?l :lineitem/orderkey ?ok]
         [?l :lineitem/quantity ?q]] $) [[?ok ?qsum]]]
    [(> ?qsum 300)]
    [?o :orders/orderkey ?ok]
    [?o :orders/custkey ?c-custkey]
    [?o :orders/orderdate ?o-orderdate]
    [?o :orders/totalprice ?o-totalprice]
    [?c :customer/custkey ?c-custkey]
    [?c :customer/name ?c-name]
    [?l :lineitem/orderkey ?ok]
    [?l :lineitem/quantity ?l-quantity]])

;; Q19: Discounted Revenue
(def q-19
  '[:find (sum ?revenue) .
    :with ?l
    :where
    [?l :lineitem/partkey ?pk]
    [?p :part/partkey ?pk]
    [?l :lineitem/quantity ?q]
    [?l :lineitem/shipmode ?sm]
    [?l :lineitem/shipinstruct ?si]
    [?l :lineitem/extendedprice ?ep]
    [?l :lineitem/discount ?d]
    [(- 1 ?d) ?x]
    [(* ?ep ?x) ?revenue]
    [?p :part/brand ?brand]
    [?p :part/container ?cont]
    [?p :part/size ?psize]
    [(or (and (= ?brand "Brand#12")
              (or (= ?cont "SM CASE") (= ?cont "SM BOX")
                  (= ?cont "SM PACK") (= ?cont "SM PKG"))
              (>= ?q 1) (<= ?q 11)
              (>= ?psize 1) (<= ?psize 5)
              (or (= ?sm "AIR") (= ?sm "AIR REG"))
              (= ?si "DELIVER IN PERSON"))
         (and (= ?brand "Brand#23")
              (or (= ?cont "MED BAG") (= ?cont "MED BOX")
                  (= ?cont "MED PKG") (= ?cont "MED PACK"))
              (>= ?q 10) (<= ?q 20)
              (>= ?psize 1) (<= ?psize 10)
              (or (= ?sm "AIR") (= ?sm "AIR REG"))
              (= ?si "DELIVER IN PERSON"))
         (and (= ?brand "Brand#34")
              (or (= ?cont "LG CASE") (= ?cont "LG BOX")
                  (= ?cont "LG PACK") (= ?cont "LG PKG"))
              (>= ?q 20) (<= ?q 30)
              (>= ?psize 1) (<= ?psize 15)
              (or (= ?sm "AIR") (= ?sm "AIR REG"))
              (= ?si "DELIVER IN PERSON")))]] )

;; Q20: Potential Part Promotion
(def q-20
  '[:find ?s-name ?s-address
    :order-by [0 :asc]
    :where
    [?s :supplier/suppkey ?sk]
    [?s :supplier/name ?s-name]
    [?s :supplier/address ?s-address]
    [?s :supplier/nationkey ?snk]
    [?n :nation/nationkey ?snk]
    [?n :nation/name "CANADA"]
    [?ps :partsupp/suppkey ?sk]
    [?ps :partsupp/partkey ?pk]
    [?ps :partsupp/availqty ?avail]
    [?p :part/partkey ?pk]
    [?p :part/name ?pname]
    [(like ?pname "forest%")]
    [(q [:find (sum ?lq) .
         :with ?l
         :in $ ?pk ?sk
         :where
         [?l :lineitem/partkey ?pk]
         [?l :lineitem/suppkey ?sk]
         [?l :lineitem/shipdate ?sd]
         [(>= ?sd "1994-01-01")]
         [(< ?sd "1995-01-01")]
         [?l :lineitem/quantity ?lq]] $ ?pk ?sk) ?sumq]
    [(* 0.5 ?sumq) ?thr]
    [(> ?avail ?thr)]])

;; Q21: Suppliers Who Kept Orders Waiting
(def q-21
  '[:find ?s-name (count-distinct ?l1)
    :order-by [1 :desc 0 :asc]
    :limit 100
    :where
    [?s :supplier/suppkey ?sk]
    [?s :supplier/name ?s-name]
    [?s :supplier/nationkey ?snk]
    [?n :nation/nationkey ?snk]
    [?n :nation/name "SAUDI ARABIA"]
    [?l1 :lineitem/suppkey ?sk]
    [?l1 :lineitem/orderkey ?ok]
    [?l1 :lineitem/receiptdate ?r1]
    [?l1 :lineitem/commitdate ?c1]
    [(> ?r1 ?c1)]
    [?o :orders/orderkey ?ok]
    [?o :orders/orderstatus "F"]
    (not [?l3 :lineitem/orderkey ?ok]
         [?l3 :lineitem/suppkey ?s3]
         [(not= ?s3 ?sk)]
         [?l3 :lineitem/receiptdate ?r3]
         [?l3 :lineitem/commitdate ?c3]
         [(> ?r3 ?c3)])
    [?l2 :lineitem/orderkey ?ok]
    [?l2 :lineitem/suppkey ?s2]
    [(not= ?s2 ?sk)]])

;; Q22: Global Sales Opportunity
(def q-22
  '[:find ?cntrycode (count ?c) (sum ?c-acctbal)
    :order-by [0 :asc]
    :where
    [?c :customer/custkey ?ck]
    [?c :customer/phone ?phone]
    [(subs ?phone 0 2) ?cntrycode]
    [(or (= ?cntrycode "13") (= ?cntrycode "31") (= ?cntrycode "23")
         (= ?cntrycode "29") (= ?cntrycode "30") (= ?cntrycode "18")
         (= ?cntrycode "17"))]
    [?c :customer/acctbal ?c-acctbal]
    [(q [:find (avg ?b) .
         :with ?c2
         :where
         [?c2 :customer/acctbal ?b]
         [(> ?b 0.0)]
         [?c2 :customer/phone ?p2]
         [(subs ?p2 0 2) ?cc2]
         [(or (= ?cc2 "13") (= ?cc2 "31") (= ?cc2 "23") (= ?cc2 "29")
              (= ?cc2 "30") (= ?cc2 "18") (= ?cc2 "17"))]] $) ?avgbal]
    [(> ?c-acctbal ?avgbal)]
    (not [?o :orders/custkey ?ck])])

;; ---------------------------------------------------------------------------
;; Registry

(defn query-ids
  "Sorted query numbers that have a Datalog translation."
  []
  (->> (keys (ns-publics 'datalevin-tpch.queries))
       (keep (fn [sym]
               (when-let [m (re-matches #"q-(\d+)" (name sym))]
                 (Long/parseLong (second m)))))
       sort
       vec))

(defn datalog
  "The Datalog form for query number `n`."
  [n]
  (let [sym (symbol "datalevin-tpch.queries" (str "q-" n))]
    (or (some-> (ns-resolve 'datalevin-tpch.queries sym) var-get)
        (throw (ex-info (str "No Datalog translation for query " n)
                        {:query n :available (query-ids)})))))

(defn ordering
  "Output column index/direction pairs for query `n`, or nil if unordered."
  [n]
  (some->> (datalog n)
           (drop-while #(not= :order-by %))
           second
           (partition 2)))

(defn limit
  "The `:limit` for query `n`, or nil when the query returns every row."
  [n]
  (some->> (datalog n)
           (take-while #(not= :where %))
           (drop-while #(not= :limit %))
           second))
