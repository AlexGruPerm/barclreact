package barclreact

trait CassQueries {
  val sqlBCalcProps = " select * from mts_meta.bars_property "

  val sqlTickers = " select ticker_id,ticker_code from mts_meta.tickers "

  val sqlFirstTicksDdates = " select ticker_id, min(ddate) as ddate from mts_src.ticks_count_days group by ticker_id "

  val sqlFirstTickTs =
    """ select db_tsunx
      |   from mts_src.ticks
      |   where ticker_id = :tickerId and
      |         ddate     = :pDdate
      |   order by ts,db_tsunx
      |   limit 1 """.stripMargin

  val sqlLastTickDdate = " select ddate from mts_src.ticks_count_days where ticker_id = :tickerId limit 1 "

  val sqlLastTickTs = " select db_tsunx from mts_src.ticks where ticker_id = :tickerId and ddate= :pDdate limit 1 "

  // allow filtering here because we don't restrict by field ts, we need remove ts at all.
  val sqlTicksByTsIntervalONEDate =
    """ select db_tsunx,ask,bid
      |   from mts_src.ticks
      |  where ticker_id = :tickerId and
      |        ddate     = :pDate and
      |        db_tsunx >= :dbTsunxBegin and
      |        db_tsunx <= :dbTsunxEnd
      |  order by  ts ASC, db_tsunx ASC
      |  allow filtering """.stripMargin

  val sqlLastBarMaxDdate =
    """ select max(ddate) as ddate
      |   from mts_bars.bars_bws_dates
      |  where ticker_id     = :tickerId and
      |        bar_width_sec = :barDeepSec """.stripMargin

  val sqlLastBar =
    """ select *
      |   from mts_bars.bars
      |   where ticker_id     = :tickerId and
      |         bar_width_sec = :barDeepSec and
      |         ddate         = :maxDdate
      |   limit 1 """.stripMargin

  val sqlSaveBar =
    """insert into mts_bars.bars(
            ticker_id,
            ddate,
            bar_width_sec,
            ts_begin,
            ts_end,
            o,
            h,
            l,
            c,
            h_body,
            h_shad,
            btype,
            ticks_cnt,
            disp,
            log_co
            )
        values(
        	  :p_ticker_id,
        	  :p_ddate,
        	  :p_bar_width_sec,
            :p_ts_begin,
            :p_ts_end,
            :p_o,
            :p_h,
            :p_l,
            :p_c,
            :p_h_body,
            :p_h_shad,
            :p_btype,
            :p_ticks_cnt,
            :p_disp,
            :p_log_co
            ) """.stripMargin

  val sqlSaveBarDdatesMetaWide =
    """ insert into mts_bars.bars_bws_dates(ticker_id,bar_width_sec,ddate,curr_ts,cnt,ts_end_min,ts_end_max)
      | values(:tickerId,:bws,:pDdate,:currTs,:cnt,:tsEndMin,:tsEndMax); """.stripMargin

}
