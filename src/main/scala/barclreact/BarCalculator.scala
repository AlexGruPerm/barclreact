package barclreact

import org.slf4j.LoggerFactory

/**
  * This class contains main logic of bar calculator:
  * 1) read ticks
  * 2) calculate bars
  * 3) save bars
  * 4) return sleep interval in ms.
*/
class BarCalculator(sess :CassSessionInstance.type, thisTickerBws :TickerBws, lastBar :Option[Bar]) {
  val log = LoggerFactory.getLogger(getClass.getName)
  val tickerID :Int = thisTickerBws.ticker.tickerId
  val bws :Int = thisTickerBws.bws
  val ticksReader = new TicksReader(sess, thisTickerBws, lastBar)
  val readTicksRes :seqTicksWithReadDuration = ticksReader.getTicks
  log.info(readTicksRes.getTickStat)

  /**
    * return tuple(Option(LastBar, sleepIntervalMs))
    * cacultae bars from read ticks readTicksRes, save it all and
    * return Option(LastBar,SleepIntervalMs)
   */
  def calculateBars :(Option[Bar],Int) = {
    if (readTicksRes.seqTicks.isEmpty) {
      val sleepIntEmptyTicks :Int = 3000
      (lastBar,sleepIntEmptyTicks)
    } else {
      val t1 = System.currentTimeMillis
      val bars: Seq[Bar] = getCalculatedBars(tickerID, readTicksRes.seqTicks, bws * 1000L)

      if (bars.nonEmpty) {
        log.info(s" For ${thisTickerBws.getActorName} count ${bars.size} bars. dur : ${System.currentTimeMillis - t1}")
        log.info(s" For ${thisTickerBws.getActorName} FIRSTBAR = ${bars.head}")
        log.info(s" For ${thisTickerBws.getActorName}  LASTBAR = ${bars.last}")
        sess.saveBars(bars)
      } else {
        log.info(s" For ${thisTickerBws.getActorName} EMPTY bars. dur : ${System.currentTimeMillis - t1}")
      }

      val sleepInt :Int = 3000 //todo: may be new fucntion to sparse code !!!

      if (bars.isEmpty)
        (lastBar,sleepInt)
      else
        (bars.lastOption,sleepInt)

    }
  }

  private def getCalculatedBars(tickerId: Int, seqTicks: Seq[Tick], barDeepSec: Long): Seq[Bar] = {
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