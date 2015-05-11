DROP KEYSPACE IF EXISTS bench;
CREATE KEYSPACE IF NOT EXISTS bench WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 } 
AND DURABLE_WRITES=false;
USE bench;

DROP TABLE IF EXISTS "tuples";
CREATE TABLE "tuples" (
	client int,
	sensor int,
	time bigint,
	value int,
	
	PRIMARY KEY ((client, sensor), time, value)
);
