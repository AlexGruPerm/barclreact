package barclreact

import akka.actor.{Actor, ActorRef, Props}
import akka.event.Logging
import com.typesafe.config.Config


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

  override def receive: Receive = {
    //'calculate' - run root Actor from Main application.
    case "calculate" => {
      log.info("Get 'calculate' message. Read tickers and bws from DB. Create child actors :CalcActor")
      val seqTickerBws :Seq[TickerBws] = sess.getTickersWithBws
      processTickers(sender,seqTickerBws)
    }
    //responses from child actors - never receive it because loop messages inside child.
    case DoneResponse(tickerBws, lastBar, sleepMsBeforeNextCalc) => {
      log.info(s"mes: 'done' from ${tickerBws.getActorName} wants sleep $sleepMsBeforeNextCalc ms = ${sleepMsBeforeNextCalc/1000L} seconds. ")
      /*
      ActorSystem("BCSystem").scheduler
        .scheduleOnce(sleepMsBeforeNextCalc.millisecond, sender(), RunRequest("calc", tickerBws, lastBar))
      */
      log.info("===============================================================================")
    }
    case _ => log.info(getClass.getName +" Unknown message from ["+sender.path.name+"]")
  }


    def processTickers(sender :ActorRef, seqTickers :Seq[TickerBws]) :Unit =
      //scala.util.Random.shuffle(seqTickers)
      seqTickers
        //.filter(t => Seq(1,2,3,4).contains(t.ticker.tickerId) && Seq(30,60,300,600,1800,3600).contains(t.bws))
        .sortBy(st => st.bws)(Ordering[Int].reverse)
        .foreach{thisTickerBws =>
          log.info("Creation Actor for "+thisTickerBws.ticker.tickerCode+" bws = "+thisTickerBws.bws)
          val thisCalcActor = context.actorOf(CalcActor.props(sess),
            thisTickerBws.getActorName)
          thisCalcActor ! RunRequest("run",thisTickerBws,None)
      }

}


object BarCalculatorManagerActor {
  def props(config :Config, sess :CassSessionInstance.type ): Props = Props(new BarCalculatorManagerActor(config,sess))
}
