package barclreact

import java.time.{Instant, LocalDate, ZoneOffset}

import org.slf4j.LoggerFactory

/**
  * Since the driver now has access to Java 8 types, some of the CQL to Java type mappings have changed when it comes
  * to temporal types such as date and timestamp:
  *
  * getDate has been replaced by getLocalDate and returns java.time.LocalDate;
  * getTime has been replaced by getLocalTime and returns java.time.LocalTime instead of a long representing
  * nanoseconds since midnight;
  *
  * getTimestamp has been replaced by getInstant and returns java.time.Instant instead of java.util.Date.
  * The corresponding setter methods were also changed to expect these new types as inputs.
  *
*/

/**
  * Class just for read ticks from db:
  * Don't read lastBar from DB at all. lastBar is incoming parameter.
  * 1) read last tick.
  * 2) if lastBar is empty than try read from firstTick otherwise from tsend.lastBar
  * 3)
*/
class TicksReader(sess :CassSessionInstance.type, thisTickerBws :TickerBws, lastBar :Option[Bar]) {
  private val log = LoggerFactory.getLogger(getClass.getName)
  private val tickerId :Int = thisTickerBws.ticker.tickerId
  private val bws :Int = thisTickerBws.bws
  private val lastTickTs :Long =sess.getLastTickTs(tickerId)
  log.info(s"For ${thisTickerBws.getActorName} lastTick.ts = "+lastTickTs)
  private val zo = ZoneOffset.ofHours(+3)

 def getTicks :seqTicksWithReadDuration = {
   if (lastTickTs == 0L) seqTicksWithReadDuration(Nil, 0L)
   else {
     try {
      val readFromTs :Long =
        lastBar match {
          case Some (lastBar) => lastBar.ts_end
          case None => thisTickerBws.ticker.minTsSrc
        }
      readTicksRecurs(tickerId, readFromTs, readFromTs+1, bws)
   } catch {
       case e: com.datastax.oss.driver.api.core.DriverTimeoutException =>
         log.error(s"EXCEPTION: (1) DriverTimeoutException (readTicksRecurs) ${e.getMessage} ${e.getCause}")
         seqTicksWithReadDuration(Nil, 0L)
       case e: Throwable =>
         log.error(s"EXCEPTION: (2) Throwable (readTicksRecurs) ${e.getMessage} ${e.getCause}")
         seqTicksWithReadDuration(Nil, 0L)
     }
   }
 }

  private def intervalSecondsDouble(sqTicks :Seq[Tick]) :Double =
    (sqTicks.last.dbTsunx.toDouble - sqTicks.head.dbTsunx.toDouble) / 1000


  private def readTicksRecurs(tickerId :Int, readFromTs: Long, readToTs: Long, bws :Int): seqTicksWithReadDuration = {
    val (seqTicks, readMsec) = getTicksByInterval(tickerId, readFromTs, readToTs)

    if (seqTicks.sqTicks.isEmpty && lastTickTs > readToTs)
      readTicksRecurs(tickerId, readFromTs, readToTs + sess.readByMins *60 * 1000L, bws)

    else if (seqTicks.sqTicks.isEmpty && lastTickTs <= readToTs)
      seqTicksWithReadDuration(seqTicks.sqTicks, readMsec)

    else if (lastTickTs > readFromTs && lastTickTs < readToTs)
      seqTicksWithReadDuration(seqTicks.sqTicks, readMsec)

    else if (seqTicks.sqTicks.nonEmpty && intervalSecondsDouble(seqTicks.sqTicks) < bws.toDouble &&
      lastTickTs > readToTs)
      readTicksRecurs(tickerId, readFromTs, readToTs + sess.readByMins *60 * 1000L, bws)

    else
      seqTicksWithReadDuration(seqTicks.sqTicks, readMsec)
  }

  /**
    * from input ts(Begin-End) calculate sequence of ddates.
    * Next iterate through this dates and read data per single partitions ticker_id + ddate
    *
    *
  */
  private def getTicksByInterval(tickerId: Int, tsBegin: Long, tsEnd: Long) :(seqTicksObj, Long) = {
    val beginYear = Instant.ofEpochMilli(tsBegin).atOffset(zo).getYear
    val beginDay = Instant.ofEpochMilli(tsBegin).atOffset(zo).getDayOfYear

    val endYear = Instant.ofEpochMilli(tsEnd).atOffset(zo).getYear
    val endDay = Instant.ofEpochMilli(tsEnd).atOffset(zo).getDayOfYear

    val seqDaysRead :Seq[LocalDate] = if (beginYear == endYear) {
      (beginDay to endDay).map(dayNum => LocalDate.ofYearDay(beginYear,dayNum))
    } else if (beginYear == (endYear-1)) {
      (beginDay to 365).map(dayNum => LocalDate.ofYearDay(beginYear, dayNum)) ++
        (1 to endDay).map(dayNum => LocalDate.ofYearDay(endYear, dayNum))
    } else {
      Seq(LocalDate.ofYearDay(beginYear, beginDay))
    }

    val t1 = System.currentTimeMillis
    (seqTicksObj(
      seqDaysRead
        .flatMap(readDayForRead => sess.getTicksByDateTsInterval(tickerId,readDayForRead,tsBegin,tsEnd))
    )
    , System.currentTimeMillis - t1)
  }



}
