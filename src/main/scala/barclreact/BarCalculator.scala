package barclreact

import java.time.Instant

import akka.event.LoggingAdapter

/**
  * This class contains main logic of bar calculator:
  * 1) read ticks
  * 2) calculate bars
  * 3) save bars
  * 4) return sleep interval in ms.
*/
class BarCalculator(sess :CassSessionInstance.type, thisTickerBws :TickerBws, lastBar :Option[Bar],log :LoggingAdapter)
extends CommonFuncs {
  //val log = LoggerFactory.getLogger(getClass.getName)
  val tickerID: Int = thisTickerBws.ticker.tickerId
  val bws: Int = thisTickerBws.bws
  val ticksReader = new TicksReader(sess, thisTickerBws, lastBar, log)
  val readTicksRes: seqTicksWithReadDuration = ticksReader.getTicks
  log.info(readTicksRes.getTickStat)

  /**
    * return tuple(Option(LastBar, sleepIntervalMs))
    * cacultae bars from read ticks readTicksRes, save it all and
    * return Option(LastBar,SleepIntervalMs)
    */
  def calculateBars: (Option[Bar], Int) = {
    if (readTicksRes.seqTicks.isEmpty) {
      val sleepIntEmptyTicks: Int = 3000
      (lastBar, sleepIntEmptyTicks)
    } else {
      val t1 = System.currentTimeMillis
      val bars: Seq[Bar] = getCalculatedBars(tickerID, readTicksRes.seqTicks, bws * 1000L, ticksReader.lastTickTs)

      val sleepInt: Int =
        if (bars.nonEmpty) {
          log.info(s" For ${thisTickerBws.getActorName} count ${bars.size} bars. dur : ${System.currentTimeMillis - t1}")
          /*
          log.info(s" For ${thisTickerBws.getActorName} FIRSTBAR = ${bars.head}")
          log.info(s" For ${thisTickerBws.getActorName}  LASTBAR = ${bars.last}")
          */
          sess.saveBars(bars)
          getSleepForCaseNonEmptyBars(bars.last.ts_end, readTicksRes.seqTicks.last, bws, ticksReader.lastTickTs)
        } else {
           log.info(s" For ${thisTickerBws.getActorName} EMPTY bars. dur : ${System.currentTimeMillis - t1}")
           getSleepForCaseEmptyBars(readTicksRes.seqTicks.head, readTicksRes.seqTicks.last, bws, ticksReader.lastTickTs)
        }

      log.info("----------- from calculateBars   return for sleep = "+sleepInt)

      if (bars.isEmpty)
        (lastBar, sleepInt)
      else
        (bars.lastOption, sleepInt)

    }
  }

  /**
    * Calculate sleep time for case:
    * 1) nonEmpty read seqTicks
    * 2) calculate bars, but there is no one bar, it means that not enough ticks data for at least one Bar.
    * 3)
    */
  private def getSleepForCaseEmptyBars(firstReadTick: Tick, lastReadTick: Tick, bws: Int, commLastTickTs: Long): Int =
    //todo : replace this comparison into fucnc. Not readable.
    if (Instant.ofEpochMilli(commLastTickTs).atOffset(zo).getDayOfYear ==
        Instant.ofEpochMilli(lastReadTick.dbTsunx).atOffset(zo).getDayOfYear) {
      val seqTickLastFirst :Long = (lastReadTick.dbTsunx - firstReadTick.dbTsunx) / 1000L
      Seq(bws*1000L, (bws - seqTickLastFirst)*1000L).min.toInt
    }
    else
      0


  private def getSleepForCaseNonEmptyBars(lastBarTsEnd: Long, lastReadTick: Tick, bws: Int, commLastTickTs :Long): Int =
  //todo : replace this comparison into fucnc. Not readable.
    if (Instant.ofEpochMilli(lastBarTsEnd).atOffset(zo).getDayOfYear ==
        Instant.ofEpochMilli(commLastTickTs).atOffset(zo).getDayOfYear &&
       (commLastTickTs - lastBarTsEnd)/1000L  < bws
    )
      Seq(bws * 1000L, (bws - ((lastReadTick.dbTsunx - lastBarTsEnd) / 1000L)*1000L)).min.toInt
    else
      0


  private def getCalculatedBars(tickerId: Int, seqTicks: Seq[Tick], barDeepSec: Long, commonLastTickTs :Long): Seq[Bar] = {
    val seqBarSides :Seq[TsPoint] = seqTicks.head.dbTsunx.to(seqTicks.last.dbTsunx).by(barDeepSec)
      .zipWithIndex
      .map(TsPoint.create)

    val seqBarRanges :Seq[TsIntervalGrp] = seqBarSides.init.zip(seqBarSides.tail)
      .map(TsIntervalGrp.create)

    def getGroupThisElement(elm: Long) :Int =
      seqBarRanges.find(bs => bs.tsBegin <= elm && bs.tsEnd > elm)
        .map(_.groupNumber).getOrElse(0)

    seqTicks.groupBy(elm => getGroupThisElement(elm.dbTsunx))
      .filter(_._1 != 0).toSeq.sortBy(_._1)
      .map(elm => new BarWitchTicks(tickerId, (barDeepSec/1000L).toInt, elm._2))
  }

}

case class TsPoint(
                    ts    :Long,
                    index :Int
                  )
object TsPoint{
  def create(initVal :(Long,Int)) :TsPoint = {
    new TsPoint(initVal._1,initVal._2)
  }
}

case class TsIntervalGrp(
                          tsBegin     :Long,
                          tsEnd       :Long,
                          groupNumber :Int
                        )
object TsIntervalGrp {
  def create(initVal :(TsPoint,TsPoint)) :TsIntervalGrp = {
    new TsIntervalGrp(initVal._1.ts, initVal._2.ts, initVal._2.index)
  }
}