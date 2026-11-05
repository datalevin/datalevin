-- TPC-C-derived comparison schema: int64, float64, and text in every backend.
-- Floating-point money is intentional in this derived workload, not audited TPC-C.

CREATE TABLE warehouse (
  w_id       bigint       NOT NULL,
  w_ytd      double precision NOT NULL,
  w_tax      double precision  NOT NULL,
  w_name     text   NOT NULL,
  w_street_1 text   NOT NULL,
  w_street_2 text   NOT NULL,
  w_city     text   NOT NULL,
  w_state    text       NOT NULL,
  w_zip      text       NOT NULL,
  PRIMARY KEY (w_id)
);

CREATE TABLE district (
  d_id        bigint       NOT NULL,
  d_w_id      bigint       NOT NULL,
  d_ytd       double precision NOT NULL,
  d_tax       double precision  NOT NULL,
  d_next_o_id bigint       NOT NULL,
  d_name      text   NOT NULL,
  d_street_1  text   NOT NULL,
  d_street_2  text   NOT NULL,
  d_city      text   NOT NULL,
  d_state     text       NOT NULL,
  d_zip       text       NOT NULL,
  PRIMARY KEY (d_w_id, d_id)
);

CREATE TABLE customer (
  c_id           bigint       NOT NULL,
  c_d_id         bigint       NOT NULL,
  c_w_id         bigint       NOT NULL,
  c_first        text   NOT NULL,
  c_middle       text       NOT NULL,
  c_last         text   NOT NULL,
  c_street_1     text   NOT NULL,
  c_street_2     text   NOT NULL,
  c_city         text   NOT NULL,
  c_state        text       NOT NULL,
  c_zip          text       NOT NULL,
  c_phone        text      NOT NULL,
  c_since        text   NOT NULL,
  c_credit       text       NOT NULL,
  c_credit_lim   double precision NOT NULL,
  c_discount     double precision  NOT NULL,
  c_balance      double precision NOT NULL,
  c_ytd_payment  double precision NOT NULL,
  c_payment_cnt  bigint       NOT NULL,
  c_delivery_cnt bigint       NOT NULL,
  c_data         text  NOT NULL,
  PRIMARY KEY (c_w_id, c_d_id, c_id)
);

CREATE TABLE history (
  h_c_id   bigint       NOT NULL,
  h_c_d_id bigint       NOT NULL,
  h_c_w_id bigint       NOT NULL,
  h_d_id   bigint       NOT NULL,
  h_w_id   bigint       NOT NULL,
  h_date   text   NOT NULL,
  h_amount double precision  NOT NULL,
  h_data   text   NOT NULL
);

CREATE TABLE new_order (
  no_o_id bigint NOT NULL,
  no_d_id bigint NOT NULL,
  no_w_id bigint NOT NULL,
  PRIMARY KEY (no_w_id, no_d_id, no_o_id)
);

CREATE TABLE orders (
  o_id         bigint       NOT NULL,
  o_d_id       bigint       NOT NULL,
  o_w_id       bigint       NOT NULL,
  o_c_id       bigint       NOT NULL,
  o_entry_d    text   NOT NULL,
  o_carrier_id bigint,
  o_ol_cnt     bigint       NOT NULL,
  o_all_local  bigint       NOT NULL,
  PRIMARY KEY (o_w_id, o_d_id, o_id)
);

CREATE TABLE order_line (
  ol_o_id        bigint       NOT NULL,
  ol_d_id        bigint       NOT NULL,
  ol_w_id        bigint       NOT NULL,
  ol_number      bigint       NOT NULL,
  ol_i_id        bigint       NOT NULL,
  ol_supply_w_id bigint       NOT NULL,
  ol_delivery_d  text,
  ol_quantity    bigint       NOT NULL,
  ol_amount      double precision NOT NULL,
  ol_dist_info   text      NOT NULL,
  PRIMARY KEY (ol_w_id, ol_d_id, ol_o_id, ol_number)
);

CREATE TABLE item (
  i_id    bigint       NOT NULL,
  i_im_id bigint       NOT NULL,
  i_name  text   NOT NULL,
  i_price double precision  NOT NULL,
  i_data  text   NOT NULL,
  PRIMARY KEY (i_id)
);

CREATE TABLE stock (
  s_i_id       bigint       NOT NULL,
  s_w_id       bigint       NOT NULL,
  s_quantity   bigint       NOT NULL,
  s_dist_01    text      NOT NULL,
  s_dist_02    text      NOT NULL,
  s_dist_03    text      NOT NULL,
  s_dist_04    text      NOT NULL,
  s_dist_05    text      NOT NULL,
  s_dist_06    text      NOT NULL,
  s_dist_07    text      NOT NULL,
  s_dist_08    text      NOT NULL,
  s_dist_09    text      NOT NULL,
  s_dist_10    text      NOT NULL,
  s_ytd        bigint       NOT NULL,
  s_order_cnt  bigint       NOT NULL,
  s_remote_cnt bigint       NOT NULL,
  s_data       text   NOT NULL,
  PRIMARY KEY (s_w_id, s_i_id)
);

CREATE INDEX customer_name_idx ON customer (c_w_id, c_d_id, c_last, c_first);
CREATE INDEX orders_customer_idx ON orders (o_w_id, o_d_id, o_c_id, o_id);
