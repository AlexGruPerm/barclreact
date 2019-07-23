package ticksloader

import java.time.LocalDate

import akka.actor.{Actor, ActorSystem, Props}
import akka.event.Logging
import com.datastax.oss.driver.api.core.CqlSession

import scala.concurrent.duration._

//import scala.collection.JavaConverters._
//import scala.concurrent.duration._

class IndividualTickerLoader(cassSrc :CassSessionSrc.type, cassDest :CassSessionDest.type) extends Actor {
  val log = Logging(context.system, this)

  def checkISClose(cass :CqlSession,sessType :String):Unit ={
    if (cass.isClosed){
      log.error("Session "+sessType+" is closed.")
    }
  }


  def getCurrentState(thisTicker :Ticker) :IndTickerLoaderState = {
    val tickerID :Int = thisTicker.tickerId
    val maxExistDdateDest :LocalDate = cassDest.getMaxExistDdateDest(tickerID)
    val minSrcDate :LocalDate = thisTicker.minDdateSrc
    val firstTsSrcForDate :Long = thisTicker.minTsSrc

    val maxTsDest :Long = maxExistDdateDest
    match {
      case null => 0L
      case maxDateDest :LocalDate => cassDest.getMaxTsBydateDest(tickerID,maxDateDest)
    }

    val maxDdateSrc :LocalDate = cassSrc.getMaxDdate(tickerID)
    val maxTsSrc :Long = cassSrc.getMaxTs(tickerID,maxDdateSrc)

    IndTickerLoaderState(tickerID, thisTicker.tickerCode, minSrcDate, maxDdateSrc, maxTsSrc, maxExistDdateDest, maxTsDest)
  }


  def readTicks(currState :IndTickerLoaderState, thisTicker :Ticker) :Seq[Tick] = {

    val st: Seq[Tick] =
       (currState.minSrcDate,
        currState.maxDdateSrc,
        currState.maxDdateDest)
    match {
      //Load first date when abs. NO DATA EXISTS.
      case (minSrcDate :LocalDate,maxDdateSrc :LocalDate, null)
      => log.info("   READ TICKS INTERVAL DDATES=" + currState.maxDdateDest + " -  fromTS = " + currState.maxTsDest + " for " + currState.tickerID)
        cassSrc.getTicksSrc(currState.tickerID, minSrcDate, thisTicker.minTsSrc)
      case (minSrcDate :LocalDate, maxDdateSrc :LocalDate, maxDdateDest :LocalDate) if maxDdateSrc.getDayOfYear >= maxDdateDest.getDayOfYear
      => cassSrc.getTicksSrc(currState.tickerID, maxDdateDest, currState.maxTsDest)
      case _ => log.info("ANY CASE")
        Nil
    }
    st
  }



  override def receive: Receive = {
    case ("run", thisTicker: Ticker, readByMinutes: Int) =>
      val tickerID: Int = thisTicker.tickerId
      val tickerCode: String = thisTicker.tickerCode

      log.info("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
      log.info("ACTOR (" + self.path.name + ") IS RUNNING FOR [" + tickerCode + "]")

      val currState: IndTickerLoaderState = getCurrentState(thisTicker)
      log.info("  FOR [" + tickerCode + "] STATE = " + currState)

      /*
      if (currState.gapDays == 0 && currState.gapSeconds <= 60) {
        log.info("SMALL GAP [" + currState.gapSeconds + "] SECONDS, USING PAUSE.")
        Thread.sleep(30000)
      }
      */

      val seqReadedTicks: Seq[Tick] = if (currState.maxTsSrc != currState.maxTsDest) {
        val rTicks : Seq[Tick] = readTicks(currState, thisTicker)
        log.info("    FOR [" + tickerCode + "] READ " + rTicks.size + " TICKS")
        rTicks
      } else {
        log.info("    FOR [" + tickerCode + "] NO READ TICKS, BCS: maxTsSrc == maxTsDest ")
        Nil
      }

      val ticksSaved: Long = if (seqReadedTicks.nonEmpty) {
        val tSaved :Long = cassDest.saveTicks(seqReadedTicks, currState)
        log.info("      FOR [" + tickerCode + "] SAVE " + tSaved + " TICKS")
        tSaved
      } else {
        log.info("      FOR [" + tickerCode + "] SAVE NO TICKS TO SAVE")
        0L
      }


      /**
        * If we have a gap then use pause.
      */
      import scala.concurrent.ExecutionContext.Implicits.global
      if (currState.gapDays == 0 && currState.gapSeconds <= 60) {
        log.info("SMALL GAP [" + currState.gapSeconds + "] SECONDS, USING PAUSE.")
        val system = ActorSystem("LoadTickersSystem")
          system.scheduler.scheduleOnce(30 seconds, context.parent, ("ticks_saved", thisTicker))
      }
      else {
        context.parent ! ("ticks_saved", thisTicker)
      }


    case "stop" =>
      log.info("Stopping "+self.path.name)
      context.stop(self)
    case _ => log.info(getClass.getName + " unknown message.")
  }

}

object IndividualTickerLoader {
  def props(cassSrc :CassSessionSrc.type, cassDest :CassSessionDest.type): Props = Props(new IndividualTickerLoader(cassSrc,cassDest))
}




