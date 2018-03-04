CREATE TABLE r2 (c0 bigint,c1 bigint,c2 bigint,c3 bigint);
LOAD DATA LOCAL INFILE 'r2.tbl' Into table r2 Fields Terminated by '|' Lines Terminated by '\n';
