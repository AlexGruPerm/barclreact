package barclreact

trait CassQueries {
  val sqlBCalcProps = " select * from mts_meta.bars_property "
  val sqlTickers = " select ticker_id,ticker_code from mts_meta.tickers "
  val sqlFirstTicksDdates = " select ticker_id, min(ddate) as ddate from mts_src.ticks_count_days group by ticker_id "
  val sqlFirstTickTs = "select db_tsunx from mts_src.ticks where ticker_id = :tickerId and ddate= :pDdate order by ts,db_tsunx limit 1"
}
