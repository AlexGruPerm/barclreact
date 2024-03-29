package barclreact

import akka.actor.{Actor, Props, Timers}
import akka.event.Logging

import scala.concurrent.duration._
//import scala.collection.JavaConverters._
//import scala.concurrent.duration._

class CalcActor(sess :CassSessionInstance.type) extends Actor with Timers {
  val log = Logging(context.system, this)

  def logCommands(command :String, tickerBws :TickerBws) ={
    if (command == "run")
      log.info("Msg: 'run' from parent. Actor - " + tickerBws.getActorName)
    if (command == "calc")
      log.info("Msg: 'calc' from parent. Next iteration. Actor - "+tickerBws.getActorName)
  }

  override def receive: Receive = {
    case RunRequest(command, tickerBws, lastBar) => {
      require(!(command == "calc" && lastBar == None),
        "Command 'calc' and last bar is None. It's no possible case of logic.")
      logCommands(command, tickerBws)

      val usingThisLastBar: Option[Bar] =
        if (command == "run") sess.getLastBar(tickerBws)
        else
          lastBar

      //log.info(s" '$command' for ${tickerBws.getActorName} LAST_BAR = ${usingThisLastBar}")

      // lastBar=None for run and Some for calc.
      val barCalculatorInstance = new BarCalculator(sess, tickerBws, usingThisLastBar, log)
      /**
        * Call calculateBars for read ticks, bars calculation and saving.
        * And it returns Option(Last calculated Bar)
        */
      val (lastCalcBarThisIter: Option[Bar], sleepMsBeforeNextCalc: Int) = barCalculatorInstance.calculateBars

      val msWillSleep: Int =
        if (tickerBws.bws <= 600) {
          tickerBws.bws * 1000
        } else {
          if (sleepMsBeforeNextCalc > 0)
            Seq(tickerBws.bws * 1000, sleepMsBeforeNextCalc - 1).min
          else
            Seq(tickerBws.bws * 1000, ((-1 * sleepMsBeforeNextCalc) - 1)).min
        }

      log.info("-----------------------------------------")
      log.info("  ")
      log.info(s"Now ${tickerBws.getActorName} will sleep $msWillSleep ms.")
      log.info("  ")
      log.info("-----------------------------------------")
      val uuid = java.util.UUID.randomUUID
      timers.startSingleTimer(uuid, RunRequest("calc", tickerBws, lastCalcBarThisIter), msWillSleep.millis)

      //val sleepMsBeforeNextCalc :Int = 3000
      //sender() ! DoneResponse(tickerBws,lastCalcBarThisIter,sleepMsBeforeNextCalc)
    }
  }

}


object CalcActor {
  def props(sess :CassSessionInstance.type): Props = Props(new CalcActor(sess))
}




