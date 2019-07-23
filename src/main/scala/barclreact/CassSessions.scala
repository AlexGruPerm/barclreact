package ticksloader

import java.net.InetSocketAddress
import java.time.LocalDate

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.cql.{BatchStatement, BoundStatement, DefaultBatchType, Row}
import com.typesafe.config.{Config, ConfigFactory}
import org.slf4j.LoggerFactory

import scala.annotation.tailrec
import scala.collection.JavaConverters._

trait CassSession extends CassQueries {
  val log = LoggerFactory.getLogger(getClass.getName)
  //val config :Config = ConfigFactory.load(s"application.conf")
  val config :Config = ConfigFactory.load()
  val confConnectPath :String = "loader.connection."

  def getNodeAddressDc(path :String) :(String,String) =
    (config.getString(confConnectPath+"address-"+path),
      config.getString(confConnectPath+"dc-"+path))

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

object CassSessionSrc extends CassSession{
  private val (node :String,dc :String) = getNodeAddressDc("src")
  log.debug("CassSessionSrc address-dc = "+node+" - "+dc)
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


object CassSessionDest extends CassSession{
  private val (node :String,dc :String) = getNodeAddressDc("dest")
  log.debug("CassSessionDest address-dc = "+node+" - "+dc)
  val sess :CqlSession = createSession(node,dc)

  val prepMaxDdateDest :BoundStatement = prepareSql(sess,sqlMaxDdate)
  val prepMaxTsDest :BoundStatement = prepareSql(sess,sqlMaxTs)
  val prepSaveTickDbDest :BoundStatement = prepareSql(sess,sqlSaveTickDb)
  val prepSaveTicksByDayDest :BoundStatement = prepareSql(sess,sqlSaveTicksByDay)
  val prepSaveTicksCntTotalDest :BoundStatement = prepareSql(sess,sqlSaveTicksCntTotal)

  def getMaxExistDdateDest(tickerId :Int) :LocalDate =
    sess.execute(prepMaxDdateDest
      .setInt("tickerID",tickerId))
      .one().getLocalDate("ddate")

  def getMaxTsBydateDest(tickerId :Int, thisDate :LocalDate) :Long =
    sess.execute(prepMaxTsDest
      .setInt("tickerID",tickerId)
      .setLocalDate("maxDdate",thisDate))
      .one().getLong("ts")

  def getInsertTickStatement(thisTick :Tick) =
    prepSaveTickDbDest
      .setInt("tickerID", thisTick.ticker_id)
      .setLocalDate("ddate", thisTick.ddate)
      .setLong("ts", thisTick.ts)
      .setLong("db_tsunx", thisTick.db_tsunx)
      .setDouble("ask", thisTick.ask)
      .setDouble("bid", thisTick.bid)

  def saveTicksCountByDay(tickerID :Int, distDdate :LocalDate, ticksCount :Long) =
    sess.execute(prepSaveTicksByDayDest
      .setInt("tickerID", tickerID)
      .setLocalDate("ddate", distDdate)
      .setLong("pTicksCount", ticksCount)
    )

  def  saveTicksCount(tickerID :Int, seqTicksSize :Long) =
    sess.execute(
      prepSaveTicksCntTotalDest
        .setInt("tickerID",tickerID)
        .setLong("pTicksCount",seqTicksSize)
    )


  def saveTicks(seqReadedTicks :Seq[Tick],
                currState      :IndTickerLoaderState) :Long = {
    val seqTicksSize :Long = seqReadedTicks.size
    seqReadedTicks.map(_.ddate).distinct.sortBy(_.getDayOfYear).foreach {distDdate =>
      val partSqTicks = seqReadedTicks.filter(t => t.ddate == distDdate).grouped(5000)
      partSqTicks.foreach {
        thisPart =>
          val builder = BatchStatement.builder(DefaultBatchType.UNLOGGED)
          thisPart.foreach {
            t => builder.addStatement(getInsertTickStatement(t))
          }
          val batch = builder.build()
          sess.execute(batch)
          batch.clear()
      }
    }

    seqReadedTicks.map(elm => elm.ddate).distinct.foreach {distDdate =>
      saveTicksCountByDay(currState.tickerID,distDdate,seqReadedTicks.count(tick => tick.ddate == distDdate))
    }

    saveTicksCount(currState.tickerID,seqTicksSize)
    seqTicksSize
  }

}