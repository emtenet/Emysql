# DDL for emtenet_SUITE tests
# Author: Michael Taylor
# 14/09/2013

USE hello_database;

DROP TABLE IF EXISTS `rank`;
CREATE TABLE `rank`
( `identity` integer NOT NULL AUTO_INCREMENT
, `sequence` integer NOT NULL
, `title` varchar(100) NOT NULL
, PRIMARY KEY (`identity`)
, UNIQUE KEY `sequence` (`sequence`)
, UNIQUE KEY `title` (`title`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

