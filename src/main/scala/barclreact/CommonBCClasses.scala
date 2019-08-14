package barclreact

import java.time.LocalDate

case class seqTicksWithReadDuration(seqTicks :Seq[Tick], durationMs :Long) {
  def getTickStat :String =
      s"SIZE = ${seqTicks.size} DUR : $durationMs ms."
  /*
  def getTickStat :String =
    if (seqTicks.nonEmpty) {
      s"SIZE = ${seqTicks.size} FISRT = ${seqTicks.head} LAST = ${seqTicks.last} WIDTH = ${(seqTicks.last.dbTsunx - seqTicks.head.dbTsunx) / 1000L} sec.  DUR : $durationMs ms."
    } else {
      s"SIZE = ${seqTicks.size} DUR : $durationMs ms."
    }
  */
}

case class seqTicksObj(
                        sqTicks :Seq[Tick]
                      )

case class Tick(
                 tickerId  :Int,
                 dDate     :LocalDate,
                 dbTsunx   :Long,
                 ask       :Double,
                 bid       :Double
               ) {
  override def toString: String =
    "( "+dDate+" - "+dbTsunx+" )"
}
/*
case class seqTicksObj(
                        sqTicks :Seq[Tick]
                      )
*/

final case class CassConnectException(private val message: String = "",
                                      private val cause: Throwable = None.orNull)
  extends Exception(message, cause)

case class Ticker(
                   tickerId    :Int,
                   tickerCode  :String,
                   minDdateSrc :Option[LocalDate],
                   minTsSrc    :Long
                 )

case class FirstTick(
                   tickerId    :Int,
                   minDdateSrc :LocalDate,
                   minTsSrc    :Long
                 )

case class BarCalcProperty(tickerId  :Int,
                           bws       :Int,
                           isEnabled :Int)

case class TickerBws(
                     ticker :Ticker,
                     bws    :Int
                    ){
  def getActorName :String =
    "CalcActor"+"_"+ticker.tickerId+"_"+bws
}

case class RunRequest(command: String, thisTickerBws :TickerBws,lastBar :Option[Bar])
case class DoneResponse(thisTickerBws :TickerBws,lastBar :Option[Bar], sleepMsBeforeNextCalc :Int)

/**
  * Common Bar description.
*/
trait  Bar {
  def ticker_id       :Int
  def ddate           :Long
  def ddateFromTick   :LocalDate
  def bar_width_sec   :Int
  def ts_begin        :Long
  def ts_end          :Long
  def o               :Double
  def h               :Double
  def l               :Double
  def c               :Double
  def h_body          :Double
  def h_shad          :Double
  def btype           :String
  def ticks_cnt       :Int
  def disp            :Double
  def log_co          :Double
  override def toString =
    "bws ="+bar_width_sec+" [ "+ts_begin+":"+ts_end+"] ohlc=["+o+","+h+","+l+","+c+"] "+btype+"   body,shad=["+h_body+","+h_shad+"]"
  //def getBarOutput :String = s"DDATE = $ddateFromTick TSBEGIN = $ts_begin TSEND = $ts_end TICKSCNT = $ticks_cnt"
}

/**
  * Bars that we read from DB mts_bars.bars
*/
case class BarRead (
                    ticker_id       :Int,
                    ddate           :Long,
                    ddateFromTick   :LocalDate,
                    bar_width_sec   :Int,
                    ts_begin        :Long,
                    ts_end          :Long,
                    o               :Double,
                    h               :Double,
                    l               :Double,
                    c               :Double,
                    h_body          :Double,
                    h_shad          :Double,
                    btype           :String,
                    ticks_cnt       :Int,
                    disp            :Double,
                    log_co          :Double
                   ) extends Bar

/**
  * Bars that we calculate from Ticks.
*/
class BarWitchTicks (p_ticker_id : Int, p_bar_width_sec : Int, barTicks : Seq[Tick]) extends Bar{

  def simpleRound4Double(valueD : Double) = {
    (valueD * 10000).round / 10000.toDouble
  }

  def simpleRound5Double(valueD : Double) = {
    (valueD * 100000).round / 100000.toDouble
  }

  def simpleRound6Double(valueD : Double) = {
    (valueD * 1000000).round / 1000000.toDouble
  }

  val ticker_id       :Int = p_ticker_id
  val ddate           :Long =  barTicks.last.dbTsunx
  val ddateFromTick   :LocalDate = barTicks.last.dDate
  val bar_width_sec   :Int= p_bar_width_sec
  val ts_begin        :Long = barTicks(0).dbTsunx
  /**
    * It was barTicks.last.db_tsunx, but exists cases when
    * we have barTicks with just 1 ticks. For example we read seq of ticks with size 4
    * And calculate bars with BWS=30.
    * And in our seq interval between 1-st and 2-nd ticks is 49 sec.
    * We need calculate ts_end as ts_begin + bar_width_sec (*1000)
    * It was bad idea when (ts_begin = ts_end in BAR)
    * OLD CODE:
    * val ts_end :Long = barTicks.last.db_tsunx
    *
    */
  val ts_end          :Long = if (barTicks.size==1) ts_begin+bar_width_sec*1000L else barTicks.last.dbTsunx
  /*
    println(" >>>> INSIDE CONSTRUCTOR BAR: barTicks.size="+barTicks.size+" ts_begin="+ts_begin+" ts_end="+ts_end+" ddate(ts_begin)="+core.LocalDate.fromMillisSinceEpoch(ts_begin)
      +"  ddate(barTicks.last.db_tsunx)="+core.LocalDate.fromMillisSinceEpoch(ddate)+" possibleDDATE="+ddateFromTick
    )
    */

  val o               :Double = simpleRound5Double((barTicks(0).ask + barTicks(0).bid)/2)
  val h               :Double = simpleRound5Double(barTicks.map(x => (x.ask+x.bid)/2).max)
  val l               :Double = simpleRound5Double(barTicks.map(x => (x.ask+x.bid)/2).min)
  val c               :Double = simpleRound5Double((barTicks.last.ask + barTicks.last.bid)/2)
  val h_body          :Double = simpleRound5Double(math.abs(c-o))
  val h_shad          :Double = simpleRound5Double(math.abs(h-l))
  val btype           :String =(o compare c).signum match {
    case -1 => "g" // bOpen < bClose
    case  0 => "n" // bOpen = bClose
    case  1 => "r" // bOpen > bClose
  }
  val ticks_cnt       :Int = if (barTicks.nonEmpty) barTicks.size else 0
  //standard deviation
  val disp            :Double =  if (ticks_cnt != 0)
    simpleRound6Double(
      Math.sqrt(
        barTicks.map(x => Math.pow((x.ask+x.bid)/2,2)).sum/ticks_cnt-
          Math.pow( barTicks.map(x => (x.ask+x.bid)/2).sum/ticks_cnt ,2)
      )
    )
  else 0
  val log_co          :Double = simpleRound5Double(Math.log(c)-Math.log(o))

  override def toString =
    "[ "+ts_begin+":"+ts_end+"] ohlc=["+o+","+h+","+l+","+c+"] "+btype+"   body,shad=["+h_body+","+h_shad+"]"

}
