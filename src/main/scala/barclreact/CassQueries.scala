package ticksloader

trait CassQueries {

  val sqlFirstDdateTick :String = "select min(ddate) as ddate from mts_src.ticks_count_days where ticker_id = :tickerID"

  val sqlFirstTsFrom :String = "select min(db_tsunx) as ts from mts_src.ticks where ticker_id = :tickerID and ddate = :minDdate allow filtering"

  val sqlMaxDdate :String = "select max(ddate) as ddate from mts_src.ticks_count_days where ticker_id = :tickerID and ticks_count>0 allow filtering"

  val sqlMaxTs :String = "select max(db_tsunx) as ts    from mts_src.ticks where ticker_id = :tickerID and ddate = :maxDdate allow filtering"

  val sqlSaveTickDb ="""
     insert into mts_src.ticks(ticker_id,ddate,ts,db_tsunx,ask,bid)
                           values(:tickerID,:ddate,:ts,:db_tsunx,:ask,:bid) """

  val sqlSaveTicksByDay =
    """ update mts_src.ticks_count_days
           set ticks_count = ticks_count + :pTicksCount
         where ticker_id = :tickerID and
               ddate     = :ddate """

  val sqlSaveTicksCntTotal =
    """  update mts_src.ticks_count_total
            set ticks_count = ticks_count + :pTicksCount
          where ticker_id   = :tickerID """

  /**
    * All the time read whole dataset for one partition (tickerID+readDate)
    * from db_tsunx > fromTs to the end.
    */
  val sqlReadTicks :String =
    """
      select *
        from mts_src.ticks
       where ticker_id = :tickerID and
             ddate     = :readDate and
             db_tsunx > :fromTs
       allow filtering """

  val sqlReadTicksWholeDate :String =
    """
      select *
        from mts_src.ticks
       where ticker_id = :tickerID and
             ddate     = :readDate
       allow filtering """
}
