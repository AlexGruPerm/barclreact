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
  val readByMins :Int = config.getInt("cassandra.read-property.read-by-minutes")
  log.info(s"On each iteration of calculation we will read new ticks recursively by readByMins = $readByMins mis.")

  import scala.concurrent.ExecutionContext.Implicits.global
  override def receive: Receive = {
    //from Main application
    case "calculate" => {
      log.info("Get 'calculate' message. Read tickers and bws from DB. Create child actors :CalcActor")
      val seqTickerBws :Seq[TickerBws] = sess.getTickersWithBws
      proccessTickers(sender,seqTickerBws)
    }
    //responses from child actors.
    case DoneResponse(command, tickerBws, lastBar, sleepMsBeforeNextCalc, respondTo)  if command=="done" => {
      log.info("Back message 'done' from child "+tickerBws.getActorName+s" wants sleep $sleepMsBeforeNextCalc ms.")
      ActorSystem("BCSystem").scheduler
        .scheduleOnce(sleepMsBeforeNextCalc millisecond, respondTo, RunRequest("calc",tickerBws,None,context.self))
    }

    case _ => log.info(getClass.getName +" Unknown message from ["+sender.path.name+"]")
  }


    def proccessTickers(sender :ActorRef, seqTickers :Seq[TickerBws]):Unit =
      seqTickers.withFilter(t => t.ticker.tickerId==1 && t.bws==30)
        .foreach{thisTickerBws =>
        log.info("Creation Actor for "+thisTickerBws.ticker.tickerCode+" bws = "+thisTickerBws.bws)
        val thisCalcActor = context.actorOf(CalcActor.props(sess), thisTickerBws.getActorName)
        thisCalcActor ! RunRequest("run",thisTickerBws,None,context.self)
      }

}



object BarCalculatorManagerActor {
  def props(config :Config, sess :CassSessionInstance.type ): Props = Props(new BarCalculatorManagerActor(config,sess))
}
