//cassandra debug log, slow queries:

cqlsh --connect-timeout=120 --request-timeout=3600 10.0.0.13

SELECT * FROM mts_src.ticks WHERE ticker_id=9 and ddate ='2019-08-08' AND db_tsunx >= 1565294285786 ORDER BY ts ASC, db_tsunx ASC;

REMOVE TS from ticks as ABS UNUSABLE !!!