
CREATE TABLE rank
( identity integer NOT NULL AUTO_INCREMENT
, sequence integer NOT NULL
, title varchar(100) NOT NULL
, PRIMARY KEY (identity)
, UNIQUE KEY sequence (sequence)
) ENGINE=InnoDB;

