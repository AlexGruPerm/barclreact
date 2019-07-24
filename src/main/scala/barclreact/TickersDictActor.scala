package barclreact

import java.time.LocalDate

import akka.actor.{Actor, Props}
import akka.event.Logging
import com.datastax.oss.driver.api.core.cql.Row

import scala.collection.JavaConverters._

class TickersDictActor(cassSrc :CassSessionInstance.type) extends Actor {

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
  }

  override def receive: Receive = {
    case
    "get" => {
      log.info(" TickersDictActor - get tickers from dictionary .")
      context.parent ! readTickersFromDb
    }
    case "stop" => context.stop(self)
    case _ => log.info(getClass.getName +" unknown message.")
  }

  override def postStop(): Unit = {
    log.info("postStop event in "+self.path.name)
  }
}

object TickersDictActor {
  def props(sess :CassSessionInstance.type): Props = Props(new TickersDictActor(sess))
}






