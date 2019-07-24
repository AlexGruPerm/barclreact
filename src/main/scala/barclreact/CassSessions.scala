package barclreact

import java.net.InetSocketAddress
import java.time.LocalDate
import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.cql.{BoundStatement, Row}
import com.typesafe.config.{Config, ConfigFactory}
import org.slf4j.LoggerFactory
import scala.annotation.tailrec
import scala.collection.JavaConverters._

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

  //todo: add here try except for misprinting sqls!
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
  log.debug("CassSessionSrc ip-dc = "+node+" - "+dc)
  val sess :CqlSession = createSession(node,dc)

  val prepFirstDdateTickSrc :BoundStatement = prepareSql(sess,sqlFirstDdateTick)
  val prepFirstTsSrc :BoundStatement = prepareSql(sess,sqlFirstTsFrom)
  val prepMaxDdateSrc :BoundStatement = prepareSql(sess,sqlMaxDdate)
  val prepMaxTsSrc :BoundStatement = prepareSql(sess,sqlMaxTs)
  val prepReadTicksSrc :BoundStatement = prepareSql(sess,sqlReadTicks).setIdempotent(true)
  val prepReadTicksSrcWholeDate :BoundStatement = prepareSql(sess, sqlReadTicksWholeDate).setIdempotent(true)

  //todo: maybe add here local cache (with key - tickerId) to eliminate unnecessary DB queries.
  def getMinExistDdateSrc(tickerId :Int) :LocalDate =
    sess.execute(prepFirstDdateTickSrc
      .setInt("tickerID",tickerId))
      .one().getLocalDate("ddate")

  //todo: maybe add here local cache (with key - tickerId+thisDate) to eliminate unnecessary DB queries.
  def getFirstTsForDateSrc(tickerId :Int, thisDate :LocalDate) :Long =
    sess.execute(prepFirstTsSrc
      .setInt("tickerID", tickerId)
      .setLocalDate("minDdate",thisDate))
      .one().getLong("ts")

  def getMaxDdate(tickerID :Int) :LocalDate =
    sess.execute(prepMaxDdateSrc
      .setInt("tickerID",tickerID))
      .one().getLocalDate("ddate")

  def getMaxTs(tickerID :Int,thisDdate :LocalDate) :Long =
    sess.execute(prepMaxTsSrc
      .setInt("tickerID",tickerID)
      .setLocalDate("maxDdate",thisDdate))
      .one().getLong("ts")

  val rowToTick :(Row => Tick) = (row: Row) =>
    Tick(
      row.getInt("ticker_id"),
      row.getLocalDate("ddate"),
      row.getLong("ts"),
      row.getLong("db_tsunx"),
      row.getDouble("ask"),
      row.getDouble("bid")
    )


  @tailrec def getTicksSrc(tickerId :Int, thisDate :LocalDate, fromTs :Long) :Seq[Tick] = {

   val st :Seq[Tick] =
     if (fromTs != 0L)
     sess.execute(prepReadTicksSrc
      .setInt("tickerID", tickerId)
      .setLocalDate("readDate", thisDate)
      .setLong("fromTs", fromTs)).all().iterator.asScala.toSeq.map(rowToTick).toList
    else
     sess.execute(prepReadTicksSrcWholeDate
       .setInt("tickerID", tickerId)
       .setLocalDate("readDate", thisDate)
     ).all().iterator.asScala.toSeq.map(rowToTick).toList

    if (st.nonEmpty) st
    else
      getTicksSrc(tickerId,thisDate.plusDays(1),0L)//fromTs)
  }


}

