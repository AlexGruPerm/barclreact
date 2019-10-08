
import java.net.InetSocketAddress
import java.time.LocalDate

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.cql.BoundStatement
import com.typesafe.config.{Config, ConfigFactory}
import org.slf4j.LoggerFactory

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

trait CassQueries {
  val queryMaxDateFa = """ select max(ddate) as faLastDate
                        |   from mts_bars.bars_fa
                        |  where ticker_id     = :p_ticker_id and
                        |        bar_width_sec = :p_bar_width_sec
                        |  allow filtering """.stripMargin

  val queryMinDateBar = """ select min(ddate) as mindate
                         |   from mts_bars.bars_bws_dates
                         |  where ticker_id     = :p_ticker_id and
                         |        bar_width_sec = :p_bar_width_sec """.stripMargin
}


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

object CassSessionInstance extends CassSession {
  val sess: CqlSession = createSession("10.241.5.234", "datacenter1")
  log.info("CassSessionInstance session is connected = " + !sess.isClosed)

  private val prepMaxDateFa: BoundStatement = prepareSql(sess, queryMaxDateFa)
  private val prepMinDateBar: BoundStatement = prepareSql(sess, queryMinDateBar)

  def getFaLastDate(tickerID :Int,barWidthSec :Int) :Option[LocalDate] = Option(sess.execute(prepMaxDateFa
    .setInt("p_ticker_id", tickerID)
    .setInt("p_bar_width_sec", barWidthSec))
    .one().getLocalDate("faLastDate"))

  def getBarMinDate(tickerID :Int,barWidthSec :Int) :Option[LocalDate] = Option(sess.execute(prepMinDateBar
    .setInt("p_ticker_id", tickerID)
    .setInt("p_bar_width_sec", barWidthSec)).one().getLocalDate("mindate"))

  def readBars() :Seq[Bar] ={

  }

  def close = sess.close()

}

//=========================================================================================================



val sess :CassSessionInstance.type  = CassSessionInstance

val (tickerID,barWidthSec) = (1,30)

val faLastDate :Option[LocalDate] = sess.getFaLastDate(tickerID,barWidthSec)
val barMinDate :Option[LocalDate] = sess.getBarMinDate(tickerID,barWidthSec)

println(s"faLastDate = $faLastDate barMinDate = $barMinDate")





sess.close



