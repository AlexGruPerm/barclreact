
import java.net.InetSocketAddress
import java.time.LocalDate
import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.cql.{BoundStatement, Row}
import com.typesafe.config.{Config, ConfigFactory}
import scala.jdk.CollectionConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

trait CassQueries {
  val queryMaxDateFa = """ select max(ddate) as faLastDate
                        |   from mts_bars.bars_fa
                        |  where ticker_id     = :pTickerId and
                        |        bar_width_sec = :pBarWidthSec
                        |  allow filtering """.stripMargin

  val queryMinDateBar = """ select min(ddate) as mindate
                         |   from mts_bars.bars_bws_dates
                         |  where ticker_id     = :pTickerId and
                         |        bar_width_sec = :pBarWidthSec """.stripMargin

  val queryReadBarsOneDate = """ select ts_begin,ts_end,o,h,l,c
                               |   from mts_bars.bars
                               |  where ticker_id     = :pTickerId and
                               |        bar_width_sec = :pBarWidthSec and
                               |        ddate         = :pDate
                               |  order by ts_end; """.stripMargin

  val querySaveFa =
    """ insert into mts_bars.bars_fa(
                                      ticker_id,
                                      ddate,
                                      bar_width_sec,
                                      ts_end,
                                      c,
                                      log_oe,
                                      ts_end_res,
                                      dursec_res,
                                      ddate_res,
                                      c_res,
                                      res_type)
                               values(
                                       :p_ticker_id,
                                       :p_ddate,
                                       :p_bar_width_sec,
                                       :p_ts_end,
                                       :p_c,
                                       :p_log_oe,
                                       :p_ts_end_res,
                                       :p_dursec_res,
                                       :p_ddate_res,
                                       :p_c_res,
                                       :p_res_type
                                      ) """

}


trait CassSession extends CassQueries {

  val config :Config = ConfigFactory.load()

  def createSession(node :String,dc :String,port :Int = 9042) :CqlSession =
    CqlSession.builder()
      .addContactPoint(new InetSocketAddress(node, port))
      .withLocalDatacenter(dc).build()

  def prepareSql(sess :CqlSession,sqlText :String) :BoundStatement =
    try {
      sess.prepare(sqlText).bind()
    }
    catch {
      case e: com.datastax.oss.driver.api.core.servererrors.SyntaxError =>
        println(" prepareSQL - "+e.getMessage)
        throw e
    }
}

case class barsForFutAnalyze(
                              tickerId    :Int,
                              barWidthSec :Int,
                              dDate       :LocalDate,
                              ts_begin    :Long,
                              ts_end      :Long,
                              o           :Double,
                              h           :Double,
                              l           :Double,
                              c           :Double
                            ){
  val minOHLC = Seq(o,h,l,c).min
  val maxOHLC = Seq(o,h,l,c).max
}

case class barsFutAnalyzeRes(
                              srcBar   : barsForFutAnalyze,
                              resAnal  : Option[barsForFutAnalyze],
                              p        : Double,
                              resType  : String
                            )

case class barsResToSaveDB(
                            tickerId     :Int,
                            dDate        :LocalDate,
                            barWidthSec  :Int,
                            ts_end       :Long,
                            c            :Double,
                            log_oe       :Double,
                            ts_end_res   :Long,
                            dursec_res   :Int,
                            ddate_res    :LocalDate,
                            c_res        :Double,
                            res_type     :String
                          )

class CassSessionInstance extends CassSession {
  val sess: CqlSession = createSession("10.241.5.234", "datacenter1")

  private val prepMaxDateFa: BoundStatement = prepareSql(sess, queryMaxDateFa)
  private val prepMinDateBar: BoundStatement = prepareSql(sess, queryMinDateBar)
  private val prepReadBarsOneDate: BoundStatement = prepareSql(sess, queryReadBarsOneDate)
  private val prepSaveFa: BoundStatement = prepareSql(sess, querySaveFa)

  lazy val lvGetFaLastDate: (Int, Int) => Option[LocalDate] =
    (tickerID, barWidthSec) => Option(sess.execute(prepMaxDateFa
      .setInt("pTickerId", tickerID)
      .setInt("pBarWidthSec", barWidthSec))
      .one().getLocalDate("faLastDate"))

  def getFaLastDate(tickerID: Int, barWidthSec: Int): Option[LocalDate] = Option(sess.execute(prepMaxDateFa
    .setInt("pTickerId", tickerID)
    .setInt("pBarWidthSec", barWidthSec))
    .one().getLocalDate("faLastDate"))

  def getBarMinDate(tickerID: Int, barWidthSec: Int): Option[LocalDate] = Option(sess.execute(prepMinDateBar
    .setInt("pTickerId", tickerID)
    .setInt("pBarWidthSec", barWidthSec)).one().getLocalDate("mindate"))

  val rowToBarData = (row: Row, tickerID: Int, barWidthSec: Int, dDate: LocalDate) => {
    barsForFutAnalyze(
      tickerID,
      barWidthSec,
      dDate,
      row.getLong("ts_begin"),
      row.getLong("ts_end"),
      row.getDouble("o"),
      row.getDouble("h"),
      row.getDouble("l"),
      row.getDouble("c")
    )
  }

  /** Read bars from DB beginning from startReadDate if Some,
   * otherwise return empty Seq.
   */
  def readBars(tickerID: Int, barWidthSec: Int, startReadDate: Option[LocalDate], cntDays: Integer = 1): Seq[barsForFutAnalyze] =
    startReadDate match {
      case Some(startDate) =>
        (0 to cntDays - 1).flatMap {
          addDays =>
            sess.execute(prepReadBarsOneDate
              .setInt("pTickerId", tickerID)
              .setInt("pBarWidthSec", barWidthSec)
              .setLocalDate("pDate", startDate.plusDays(addDays)))
              .all()
              .iterator.asScala.toSeq.map(r => rowToBarData(r, tickerID, barWidthSec, startDate.plusDays(addDays)))
              .sortBy(sr => (sr.dDate, sr.ts_end))
        }
      case None => Nil
    }

  def makeAnalyze(seqB: Seq[barsForFutAnalyze], p: Double): Seq[barsFutAnalyzeRes] = {
    for (
      currBarWithIndex <- seqB.zipWithIndex;
      currBar = currBarWithIndex._1;
      searchSeq = seqB.drop(currBarWithIndex._2 + 1)
    ) yield {
      val pUp: Double = (Math.exp(Math.log(currBar.c) + p) * 10000).round / 10000.toDouble
      val pDw: Double = (Math.exp(Math.log(currBar.c) - p) * 10000).round / 10000.toDouble

      searchSeq.find(srcElm => (pUp >= srcElm.minOHLC && pUp <= srcElm.maxOHLC) ||
        (pDw >= srcElm.minOHLC && pDw <= srcElm.maxOHLC) ||
        (pUp <= srcElm.maxOHLC && pDw >= srcElm.minOHLC)
      ) match {
        case Some(fBar) => {
          if (pUp >= fBar.minOHLC && pUp <= fBar.maxOHLC) barsFutAnalyzeRes(currBar, Some(fBar), p, "mx")
          else if (pDw >= fBar.minOHLC && pDw <= fBar.maxOHLC) barsFutAnalyzeRes(currBar, Some(fBar), p, "mn")
          else if (pUp <= fBar.maxOHLC && pDw >= fBar.minOHLC) barsFutAnalyzeRes(currBar, Some(fBar), p, "bt")
          else barsFutAnalyzeRes(currBar, None, p, "nn")
        }
        case None => barsFutAnalyzeRes(currBar, None, p, "nn")
      }
    }
  }

  def saveBarsFutAnal(seqFA: Seq[barsResToSaveDB]): Unit = {
    for (t <- seqFA) {
      sess.execute(prepSaveFa
        .setInt("p_ticker_id", t.tickerId)
        .setLocalDate("p_ddate", t.dDate)
        .setInt("p_bar_width_sec", t.barWidthSec)
        .setLong("p_ts_end", t.ts_end)
        .setDouble("p_c", t.c)
        .setDouble("p_log_oe", t.log_oe)
        .setLong("p_ts_end_res", t.ts_end_res)
        .setInt("p_dursec_res", t.dursec_res)
        .setLocalDate("p_ddate_res", t.ddate_res)
        .setDouble("p_c_res", t.c_res)
        .setString("p_res_type", t.res_type)
      )
    }

  }

}
//=========================================================================================================
//0.0025 = 30 b.p.
//val pUp: Double = (Math.exp(Math.log(1.2100) + 0.0025) * 10000).round / 10000.toDouble



  val sess :CassSessionInstance = new CassSessionInstance

  val (tickerID, barWidthSec) = (1, 30)

  //todo: eliminate unnecessary reads.
  val faLastDate: Option[LocalDate] = sess.getFaLastDate (tickerID, barWidthSec)
  val barMinDate: Option[LocalDate] = sess.getBarMinDate (tickerID, barWidthSec)

  println (s"faLastDate = $faLastDate barMinDate = $barMinDate ")

  val seqBars: Seq[barsForFutAnalyze] = sess.readBars (
  tickerID,
  barWidthSec,
  faLastDate.orElse (barMinDate),
  10)

  println (s"seqBars.size = ${seqBars.size} ")

val seqPercents :Seq[Double] = Seq(0.0025)//,0.1,0.15)

val t1FAnal = System.currentTimeMillis
val futuresFutAnalRes :Seq[Future[Seq[barsFutAnalyzeRes]]] = seqPercents.map(p => Future{sess.makeAnalyze(seqBars, p)})
val values = Future.sequence(futuresFutAnalRes)
val futAnalRes: Seq[barsFutAnalyzeRes] = Await.result(values,Duration.Inf).flatten
val t2FAnal = System.currentTimeMillis
println("After analyze RES.size = " + futAnalRes.size + " Duration " + (t2FAnal - t1FAnal) + " msecs.")

val resFSave: Seq[barsResToSaveDB] = futAnalRes
  //.sortBy(t => t.srcBar.ts_end)
  .filter(elm => elm.resAnal.isDefined)
  .map(r => barsResToSaveDB(
    r.srcBar.tickerId,
    r.srcBar.dDate,
    r.srcBar.barWidthSec,
    r.srcBar.ts_end,
    r.srcBar.c,
    r.p,
    r.resAnal match {
      case Some(ri) => ri.ts_end
    },
    r.resAnal match {
      case Some(ri) => Math.round((ri.ts_end - r.srcBar.ts_end)/1000L).toInt
    },
    r.resAnal match {
      case Some(ri) => ri.dDate
    },
    r.resAnal match {
      case Some(ri) => ri.c
    },
    r.resType
  ))

println(s"futAnalRes.size = ${futAnalRes.size} resFSave.size = ${resFSave.size}")

val t1Save = System.currentTimeMillis
sess.saveBarsFutAnal(resFSave)
val t2Save = System.currentTimeMillis
println("Duration of saving into mts_bars.bars_fa - " + (t2Save - t1Save) + " msecs.")

