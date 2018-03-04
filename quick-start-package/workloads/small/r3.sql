CREATE TABLE r3 (c0 bigint,c1 bigint,c2 bigint,c3 bigint);
LOAD DATA LOCAL INFILE 'r3.tbl' Into table r3 Fields Terminated by '|' Lines Terminated by '\n';
