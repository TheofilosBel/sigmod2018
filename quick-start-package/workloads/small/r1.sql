CREATE TABLE r1 (c0 bigint,c1 bigint,c2 bigint);
LOAD DATA LOCAL INFILE 'r1.tbl' Into table r1 Fields Terminated by '|' Lines Terminated by '\n';
