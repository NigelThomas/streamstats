create or replace schema test;
set schema 'test';
set path 'test';

CREATE OR REPLACE JAR streamstatsjar
LIBRARY 'file:/home/sqlstream/streamstats/streamstats.jar'
OPTIONS(0);


CREATE or replace FUNCTION streamstats
(  c cursor
)
returns table
( ROWTIME timestamp
, rowcount int
, bounds int
, timeouts int
)
    language java
    parameter style system defined java
    no sql
    external name 'STREAMSTATSJAR:com.sqlstream.plugin.streamstats.StreamStats.getStats';


