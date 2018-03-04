CREATE TABLE r12 (c0 bigint,c1 bigint,c2 bigint,c3 bigint,c4 bigint);
LOAD DATA LOCAL INFILE 'r12.tbl' Into table r12 Fields Terminated by '|' Lines Terminated by '\n';
