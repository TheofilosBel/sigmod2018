CREATE TABLE r5 (c0 bigint,c1 bigint,c2 bigint,c3 bigint);
LOAD DATA LOCAL INFILE 'r5.tbl' Into table r5 Fields Terminated by '|' Lines Terminated by '\n';
