package barclreact

import java.net.InetSocketAddress
import java.time.LocalDate

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.cql.{BatchStatement, BoundStatement, DefaultBatchType, Row}
import com.typesafe.config.{Config, ConfigFactory}

import scala.collection.JavaConverters._
import org.slf4j.LoggerFactory

trait CassSession extends CassQueries {
  val log = LoggerFactory.getLogger(getClass.getName)

  val config :Config = ConfigFactory.load()
  val confConnectPath :String = "cassandra.connection."
  val readByMins :Int = config.getInt("cassandra.read-property.read-by-minutes")

  def getNodeAddressDc(path :String) :(String,String) =
    (config.getString(confConnectPath+"ip"),
      config.getString(confConnectPath+"dc"))

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
        log.error(" prepareSQL - "+e.getMessage)
        throw e
    }
}


object CassSessionInstance extends CassSession{
  private val (node :String,dc :String) = getNodeAddressDc("src")
  log.info("CassSessionInstance DB Address : "+node+" - "+dc)
  log.info("CassSessionInstance 'cassandra.read-property.read-by-minutes' = "+readByMins)
  val sess :CqlSession = createSession(node,dc)
  log.info("CassSessionInstance session is connected = " + !sess.isClosed)

  private val prepBCalcProps :BoundStatement = prepareSql(sess,sqlBCalcProps)
  private val prepTickers :BoundStatement = prepareSql(sess,sqlTickers)
  private val prepFirstTickTs :BoundStatement = prepareSql(sess,sqlFirstTickTs)
  private val prepFirstTicksDdates :BoundStatement = prepareSql(sess,sqlFirstTicksDdates)
  private val prepLastTickDdate :BoundStatement = prepareSql(sess,sqlLastTickDdate)
  private val prepLastTickTs :BoundStatement = prepareSql(sess,sqlLastTickTs)
  private val prepTicksByTsIntervalONEDate :BoundStatement = prepareSql(sess,sqlTicksByTsIntervalONEDate)
  private val prepTicksByTsIntervalONEDateFrom :BoundStatement = prepareSql(sess,sqlTicksByTsIntervalONEDateFrom)
  private val prepLastBarMaxDdate :BoundStatement = prepareSql(sess,sqlLastBarMaxDdate)
  private val prepLastBar :BoundStatement = prepareSql(sess,sqlLastBar)
  private val prepSaveBar :BoundStatement = prepareSql(sess,sqlSaveBar)
  private val prepSaveBarDdatesMetaWide :BoundStatement = prepareSql(sess,sqlSaveBarDdatesMetaWide)

  private def getAllBarsProperties : Seq[BarCalcProperty] =
    sess.execute(prepBCalcProps).all().iterator.asScala.map(
      row => BarCalcProperty(
        row.getInt("ticker_id"),
        row.getInt("bar_width_sec"),
        row.getInt("is_enabled")
      )).toList.filter(bp => bp.isEnabled==1)


  private def getFirstDbTsunx(tickerId :Int, thisDdate :LocalDate) :Long =
    sess.execute(prepFirstTickTs
      .setInt("tickerId",tickerId)
      .setLocalDate("pDdate",thisDdate))
      .one().getLong("db_tsunx")


  private def getFirstTicksMeta : Seq[Ticker] = {
     val tMetaFirstTicks :Seq[FirstTick] = sess.execute(prepFirstTicksDdates)
        .all().iterator.asScala
        .map { row =>
          val thisTickerID :Int = row.getInt("ticker_id")
          val thisDdate :LocalDate = row.getLocalDate("ddate")
          FirstTick(
            thisTickerID,
            thisDdate,
            getFirstDbTsunx(thisTickerID, thisDdate)
          )
        }.toSeq

    sess.execute(prepTickers)
      .all().iterator.asScala.map(row => (row.getInt("ticker_id"),row.getString("ticker_code")))
      .toList.sortBy(t => t)
      .map { thisTicker =>
        val (tickerID: Int, tickerCode: String) = thisTicker //for readability
        Ticker(
          tickerID,
          tickerCode,
          tMetaFirstTicks.find(_.tickerId == tickerID).map(_.minDdateSrc),
          tMetaFirstTicks.find(_.tickerId == tickerID).map(_.minTsSrc).getOrElse(0L)
        )
      }
  }


  /**
    * Return LastTick.db_tsunx by tickerId or 0L is no data exists.
  */
  def getLastTickTs(tickerId :Int) :Long = {
    val dataSet = sess.execute(prepLastTickDdate.setInt("tickerId", tickerId))
    // getAvailableWithoutFetching == 0 means that no one rows in dataset
    if (dataSet.getAvailableWithoutFetching == 0) 0L
     else
      sess.execute(prepLastTickTs
        .setInt("tickerId", tickerId)
        .setLocalDate("pDdate", dataSet.one().getLocalDate("ddate")))
        .one().getLong("db_tsunx")
  }


  /**
    * We need Leave only tickers for which exists bar property
    */
  def getTickersWithBws :Seq[TickerBws] =
    getFirstTicksMeta
      .flatMap(thisTicker =>
        getAllBarsProperties.collect{
          case bp if bp.tickerId == thisTicker.tickerId => TickerBws(thisTicker,bp.bws)
        }
      )


  private val rowToSeqTicksWDate = (rowT: Row, tickerID: Int, pDate :LocalDate) => {
    Tick(
      tickerID,
      pDate,
      rowT.getLong("db_tsunx"),
      rowT.getDouble("ask"),
      rowT.getDouble("bid")
    )
  }


  def getTicksByDateTsInterval(tickerId :Int, readDate :LocalDate, tsBegin :Long, tsEnd :Long) : Seq[Tick] = {
    val t1 = System.currentTimeMillis
    val dataset = sess.execute(prepTicksByTsIntervalONEDate
      .setInt("tickerId", tickerId)
      .setLocalDate("pDate", readDate)
      .setLong("dbTsunxBegin", tsBegin)
      .setLong("dbTsunxEnd", tsEnd))

    val resSeq :Seq[Tick] = dataset.all().iterator.asScala.toSeq.map(r => rowToSeqTicksWDate(r, tickerId, readDate))

    log.info("getTicksByDateTsInterval for "+tickerId+" ("+readDate+ ") rows = "+resSeq.size+
    " duration = "+(System.currentTimeMillis - t1)+" ms.")
    resSeq
  }

  def getTicksByDateTsIntervalFrom(tickerId :Int, readDate :LocalDate, tsBegin :Long) : Seq[Tick] = {
    val t1 = System.currentTimeMillis
    val dataset = sess.execute(prepTicksByTsIntervalONEDateFrom
      .setInt("tickerId", tickerId)
      .setLocalDate("pDate", readDate)
      .setLong("dbTsunxBegin", tsBegin))

    val resSeq :Seq[Tick] = dataset.all().iterator.asScala.toSeq.map(r => rowToSeqTicksWDate(r, tickerId, readDate))

    log.info("getTicksByDateTsIntervalFrom for "+tickerId+" ("+readDate+ ") rows = "+resSeq.size+
      " duration = "+(System.currentTimeMillis - t1)+" ms.")
    resSeq
  }


  private def rowToBar(row :Row) :Bar = {
     BarRead (
       row.getInt("ticker_id"),
       row.getLong("ts_end"),
       row.getLocalDate("ddate"),
       row.getInt("bar_width_sec"),
       row.getLong("ts_begin"),
       row.getLong("ts_end"),
       row.getDouble("o"),
       row.getDouble("h"),
       row.getDouble("l"),
       row.getDouble("c"),
       row.getDouble("h_body"),
       row.getDouble("h_shad"),
       row.getString("btype"),
       row.getInt("ticks_cnt"),
       row.getDouble("disp"),
       row.getDouble("log_co")
     )
  }

  def getLastBar(thisTickerBws :TickerBws) :Option[Bar] = {
    val tickerID :Int = thisTickerBws.ticker.tickerId
    val bws :Int = thisTickerBws.bws
    val maxDdate :Option[LocalDate] =
      Option(sess.execute(prepLastBarMaxDdate
        .setInt("tickerId", tickerID)
        .setInt("barDeepSec", bws)
      ).one().getLocalDate("ddate"))

    val resLastBar :Option[Bar] =
      maxDdate match {
        case Some(md :LocalDate) => {
          Option(
            rowToBar(
            sess.execute(
              prepLastBar
                .setInt("tickerId", tickerID)
                .setInt("barDeepSec", bws)
                .setLocalDate("maxDdate",md))
              .one())
          )
        }
        case None => None
      }
    resLastBar
  }


  def saveBars(seqBarsCalced: Seq[Bar]) :Unit = {
    require(seqBarsCalced.nonEmpty, "Seq of Bars for saving is Empty.")
    import java.time.Instant
    val currUnixTimestamp : Long = Instant.now.toEpochMilli

    val seqBarsParts = seqBarsCalced.grouped(300)
    for(thisPartOfSeq  <- seqBarsParts) {
      val builder = BatchStatement.builder(DefaultBatchType.UNLOGGED)
      thisPartOfSeq.foreach {
        b =>
          builder.addStatement(prepSaveBar
            .setInt("p_ticker_id", b.ticker_id)
            .setLocalDate("p_ddate", b.ddateFromTick)
            .setInt("p_bar_width_sec", b.bar_width_sec)
            .setLong("p_ts_begin", b.ts_begin)
            .setLong("p_ts_end", b.ts_end)
            .setDouble("p_o", b.o)
            .setDouble("p_h", b.h)
            .setDouble("p_l", b.l)
            .setDouble("p_c", b.c)
            .setDouble("p_h_body", b.h_body)
            .setDouble("p_h_shad", b.h_shad)
            .setString("p_btype", b.btype)
            .setInt("p_ticks_cnt", b.ticks_cnt)
            .setDouble("p_disp", b.disp)
            .setDouble("p_log_co", b.log_co))
      }
      //logger.info("saveBars  before execute batch.size()="+batch.size())
      try {
        val batch = builder.build()
        sess.execute(batch)
        batch.clear()
      } catch {
        case e : com.datastax.oss.driver.api.core.DriverException => log.error(s"DriverException when save bars ${e.getMessage} ${e.getCause}")
          throw e
      }
    }

    seqBarsCalced
      .map(bc => (bc.ticker_id,bc.bar_width_sec,bc.ddateFromTick))
      .distinct
      .sortBy(b => b._3.toEpochDay)(Ordering[Long])
      .map{tpl =>

        sess.execute(prepSaveBarDdatesMetaWide
          .setInt("tickerId", tpl._1)
          .setInt("bws", tpl._2)
          .setLocalDate("pDdate", tpl._3)
          .setLong("currTs", currUnixTimestamp)
          .setInt("cnt", seqBarsCalced.count(p => p.ticker_id == tpl._1 && p.bar_width_sec == tpl._2 && p.ddateFromTick == tpl._3))
          .setLong("tsEndMin", seqBarsCalced.filter(p => p.ticker_id == tpl._1 && p.bar_width_sec == tpl._2 && p.ddateFromTick == tpl._3).head.ts_end)
          .setLong("tsEndMax", seqBarsCalced.filter(p => p.ticker_id == tpl._1 && p.bar_width_sec == tpl._2 && p.ddateFromTick == tpl._3).last.ts_end)
        )
      }

  }


}

