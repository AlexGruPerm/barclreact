package barclreact

import akka.actor.{Actor, Props}
import akka.event.Logging

//import scala.collection.JavaConverters._
//import scala.concurrent.duration._

class CalcActor(sess :CassSessionInstance.type) extends Actor {
  val log = Logging(context.system, this)

  //RunRequest("run",thisTickerBws,context.self)

  override def receive: Receive = {
    case RunRequest(command, tickerBws, lastBar, respondTo) => {
      if (command == "run")
      log.info("Message 'run' from parent actor. This Actor tickerID=" + tickerBws.ticker.tickerId + " bws=" + tickerBws.bws + " lastBar" + lastBar)

      if (command=="calc")
        log.info("Message 'calc' from parent actor. Next iteration. This Actor tickerID="+tickerBws.ticker.tickerId+" bws="+tickerBws.bws+" lastBar")

      Thread.sleep(1000)
      //val lastCalculatedSavedBar : Bar = ...
      val sleepMsBeforeNextCalc :Int = 5000
      val lastCalcBarThisIter :Option[Bar] = None
      respondTo ! DoneResponse("done",tickerBws,lastCalcBarThisIter,sleepMsBeforeNextCalc,context.self)
    }


  }

}


object CalcActor {
  def props(sess :CassSessionInstance.type): Props = Props(new CalcActor(sess))
}




