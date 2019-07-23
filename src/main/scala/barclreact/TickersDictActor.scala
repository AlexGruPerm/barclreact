package ticksloader

import java.time.LocalDate

import akka.actor.{Actor, Props}
import akka.event.Logging
import com.datastax.oss.driver.api.core.cql.Row

import scala.collection.JavaConverters._

class TickersDictActor(cassSrc :CassSessionSrc.type) extends Actor {

  val log = Logging(context.system, this)
  log.info("TickersDictActor init")

  val rowToTicker = (row: Row) => {
    val thisTickerId :Int = row.getInt("ticker_id")
    val thisTickerMinDdate :LocalDate = cassSrc.getMinExistDdateSrc(thisTickerId)
    Ticker(
      thisTickerId,
      row.getString("ticker_code"),
      thisTickerMinDdate,
      cassSrc.getFirstTsForDateSrc(thisTickerId,thisTickerMinDdate)
     )
  }

  def readTickersFromDb :Seq[Ticker] = {
    import com.datastax.oss.driver.api.core.cql.SimpleStatement
    val statement = SimpleStatement.newInstance("select ticker_id,ticker_code from mts_meta.tickers")
    cassSrc.sess.execute(statement).all().iterator.asScala.toSeq.map(rowToTicker).sortBy(_.tickerId).toList
    /*
    val seqt :Seq[Ticker] = cassSrc.sess.execute(statement).all().iterator.asScala.toSeq.map(rowToTicker).sortBy(_.tickerId).toList
    seqt.foreach(println)
    seqt
    */
  }

  override def receive: Receive = {
    case
    "get" => {
      log.info(" TickersDictActor - get tickers from dictionary .")
      context.parent ! readTickersFromDb//.filter(t => Seq(1).contains(t.tickerId))
    }
    case "stop" => context.stop(self)
    case _ => log.info(getClass.getName +" unknown message.")
  }

  override def postStop(): Unit = {
    log.info("postStop event in "+self.path.name)
  }
}

object TickersDictActor {
  def props(cassSrc :CassSessionSrc.type): Props = Props(new TickersDictActor(cassSrc))
}






