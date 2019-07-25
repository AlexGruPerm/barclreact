package barclreact

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.event.Logging
import com.typesafe.config.Config

import scala.concurrent.duration._
/*
import akka.actor.{Actor, ActorRef, Props}
import akka.event.Logging
import com.typesafe.config.Config
import scala.concurrent.duration._
*/

/**
  *  This is a main Actor that manage child Actors
  *  Created and called by message "calculate" from Main app.
  */
class BarCalculatorManagerActor(config :Config, sess :CassSessionInstance.type) extends Actor {
  val log = Logging(context.system, this)
  log.info("Basic constructor of Actor BarCalculatorManagerActor.")
  /**
    * Remove it, it's inside Session object
  val readByMins :Int = config.getInt("cassandra.read-property.read-by-minutes")
  log.info(s"On each iteration of calculation we will read new ticks recursively by readByMins = $readByMins mis.")
  */
  import scala.concurrent.ExecutionContext.Implicits.global


  override def receive: Receive = {
    //'calculate' - run root Actor from Main application.
    case "calculate" => {
      log.info("Get 'calculate' message. Read tickers and bws from DB. Create child actors :CalcActor")
      val seqTickerBws :Seq[TickerBws] = sess.getTickersWithBws
      processTickers(sender,seqTickerBws)
    }
    //responses from child actors.
    case DoneResponse(tickerBws, lastBar, sleepMsBeforeNextCalc, respondTo) => {
      log.info(s"mes: 'done' from ${tickerBws.getActorName} wants sleep $sleepMsBeforeNextCalc ms.")
      ActorSystem("BCSystem").scheduler
        .scheduleOnce(sleepMsBeforeNextCalc.millisecond, respondTo, RunRequest("calc", tickerBws, lastBar, context.self))
    }
    case _ => log.info(getClass.getName +" Unknown message from ["+sender.path.name+"]")
  }


    def processTickers(sender :ActorRef, seqTickers :Seq[TickerBws]) :Unit =
      seqTickers//.withFilter(t => t.ticker.tickerId == 1 && Seq(30,60,300,600,1800,3600).contains(t.bws)) //todo: manual filter here -------------------------------
        .foreach{thisTickerBws =>
        log.info("Creation Actor for "+thisTickerBws.ticker.tickerCode+" bws = "+thisTickerBws.bws)
        val thisCalcActor = context.actorOf(CalcActor.props(sess), thisTickerBws.getActorName)
        thisCalcActor ! RunRequest("run",thisTickerBws,None,context.self)
      }

}


object BarCalculatorManagerActor {
  def props(config :Config, sess :CassSessionInstance.type ): Props = Props(new BarCalculatorManagerActor(config,sess))
}
