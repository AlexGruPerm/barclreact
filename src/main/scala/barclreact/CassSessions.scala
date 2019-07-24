package barclreact

import java.net.InetSocketAddress
import java.time.LocalDate

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.cql.BoundStatement
import com.typesafe.config.{Config, ConfigFactory}
import scala.collection.JavaConverters._
import org.slf4j.LoggerFactory

trait CassSession extends CassQueries {
  val log = LoggerFactory.getLogger(getClass.getName)

  val config :Config = ConfigFactory.load()
  val confConnectPath :String = "cassandra.connection."

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
  val sess :CqlSession = createSession(node,dc)

  private val prepBCalcProps :BoundStatement = prepareSql(sess,sqlBCalcProps)
  private val prepTickers :BoundStatement = prepareSql(sess,sqlTickers)
  private val prepFirstTickTs :BoundStatement = prepareSql(sess,sqlFirstTickTs)
  private val prepFirstTicksDdates :BoundStatement = prepareSql(sess,sqlFirstTicksDdates)

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
    * We need Leave only tickers for which exists bar property
    *
    */
  def getTickersWithBws :Seq[TickerBws] =
    getFirstTicksMeta
      .flatMap(thisTicker =>
        getAllBarsProperties.collect{
          case bp if bp.tickerId == thisTicker.tickerId => TickerBws(thisTicker,bp.bws)
        }
      )


}

