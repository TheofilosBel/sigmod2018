CREATE TABLE r10 (c0 bigint,c1 bigint,c2 bigint);
LOAD DATA LOCAL INFILE 'r10.tbl' Into table r10 Fields Terminated by '|' Lines Terminated by '\n';
