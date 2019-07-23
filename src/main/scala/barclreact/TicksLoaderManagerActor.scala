package ticksloader

import akka.actor.{Actor, ActorRef, Props}
import akka.event.Logging
import com.typesafe.config.{Config, ConfigFactory}

/**
  *  This is a main Actor that manage child Actors (load ticks by individual ticker_id)
  *  Created ans called by message "begin load" from Main app.
  */
class TicksLoaderManagerActor extends Actor {
  val log = Logging(context.system, this)

  val config :Config = //ConfigFactory.load(s"application.conf")
    try {
    //ConfigFactory.load(s"application.conf")
    ConfigFactory.load()
  } catch {
    case te: Throwable => {
      log.error("ConfigFactory.load (1) - cause:"+te.getCause+" msg:"+te.getMessage)
      throw te
    }
    case e:Exception => {
      log.error("ConfigFactory.load (2) - cause:"+e.getCause+" msg:"+e.getMessage)
      throw e
    }
  }

    /**
      * If the gap is more then readByHours than read by this interval or all ticks.
      */
    val readByMinutes :Int = config.getInt("loader.load-property.read-by-minutes")
    log.info("Config loaded successful readByMinutes="+readByMinutes)

    //todo: replace this 2 blocks on 2 methods or remove exception blocks into method.

    val (sessSrc :CassSessionSrc.type,sessDest :CassSessionDest.type)  =
      try {
        (CassSessionSrc,CassSessionDest)
      } catch {
        case s: com.datastax.oss.driver.api.core.servererrors.SyntaxError => {
          log.error("[0] ERROR when get CassSessionXXX SyntaxError - msg="+s.getMessage+" cause="+s.getCause)
        }
        case c: CassConnectException => {
          log.error("[1] ERROR when get CassSessionXXX ["+c.getMessage+"] Cause["+c.getCause+"]")
          throw c
        }
        case de : com.datastax.oss.driver.api.core.DriverTimeoutException =>
          log.error("[2] ERROR when get CassSessionXXX ["+de.getMessage+"] ["+de.getCause+"] "+de.getExecutionInfo.getErrors)
          throw de
        case ei : java.lang.ExceptionInInitializerError =>
          log.error("[3] ERROR when get CassSessionXXX ["+ei.getMessage+"] ["+ei.getCause+"] "+ei.printStackTrace())
          throw ei
        case e: Throwable =>
          log.error("[4] ERROR when get CassSessionXXX ["+e.getMessage+"] class=["+e.getClass.getName+"]")
          throw e
      }

    def proccessTickers(sender :ActorRef, seqTickers :Seq[Ticker]) = {
      //log.info("TicksLoaderManagerActor receive ["+seqTickers.size+"] tickers from "+sender.path.name+" first is "+seqTickers(0).tickerCode)

      //Creation Actors for each ticker and run it all.
      seqTickers.foreach{ticker =>
        log.info("Creation Actor for - ["+ticker.tickerCode+"]")
        context.actorOf(IndividualTickerLoader.props(sessSrc,sessDest), "IndividualTickerLoader"+ticker.tickerId)
      }

      seqTickers.foreach{
        ticker =>
          log.info("run Actor IndividualTickerLoader"+ticker.tickerId+" for ["+ticker.tickerCode+"]")
          context.actorSelection("/user/TicksLoaderManagerActor/IndividualTickerLoader"+ticker.tickerId) !
            ("run", ticker, readByMinutes)
          Thread.sleep(300)
      }
    }

    override def receive: Receive = {
    //case "stop" => context.stop(self)
    case "begin load" => context.actorOf(TickersDictActor.props(sessSrc), "TickersDictActor") ! "get"
    case ("ticks_saved", thisTicker :Ticker) => sender ! ("run", thisTicker,readByMinutes)
    case seqTickers :Seq[Ticker] => proccessTickers(sender,seqTickers)
    case _ => log.info(getClass.getName +" unknown message.")
  }

}

object TicksLoaderManagerActor {
  def props: Props = Props(new TicksLoaderManagerActor)
}
