package barclreact

import akka.actor.ActorSystem
import com.typesafe.config.{Config, ConfigFactory}
import org.slf4j.LoggerFactory

/**
  * We create main root Actor (BCManagerActor) that will born and support child actors.
  * Each child actor has a personal pair of keys (tickerID + bar_wisth_sec next BWS)
  * When BCManagerActor receive message "calculate" it open cassandra session, if successful than read
  * all tickers from mts_meta.tickers and enabled bar properties from mts_meta.bars_property where
  * for each tickers defined individual widths of bars.
  * Next BCManagerActor create child Actors and run it, will response and rerun it again for next steps
  * of calculation.
  *
  * We read config and open connection to database here to eliminate unnecessary next activity if something wrong
  * with DB session on config unread.
  *
  * Pay attention on article : https://www.chrisstucchio.com/blog/2013/actors_vs_futures.html
  *
  *  select max(db_tsunx) from mts_src.ticks where ticker_id=1 and ddate='2019-07-25';
  *  cassandra DB unixts 1564049820
  *  utc                 1564049823
  *  no difference
  *  https://www.unixtimestamp.com
*/
object MainBarCalculator extends App {
  val log = LoggerFactory.getLogger(getClass.getName)
  log.info("========================================== BEGIN ============================================")

  val config :Config =
    try {
      val cf = ConfigFactory.load()
      log.info("Config loaded successful.")
      cf
    } catch {
      case e: Throwable => {
        log.error("ConfigFactory.load exception - cause:"+e.getCause+" message:"+e.getMessage)
        throw e
      }
      case e:Exception => {
        log.error("ConfigFactory.load exception - cause:"+e.getCause+" message:"+e.getMessage)
        throw e
      }
    }

  val sessInstance :CassSessionInstance.type  =
    try {
      CassSessionInstance
    } catch {
      case e: com.datastax.oss.driver.api.core.servererrors.SyntaxError => {
        log.error("CassSessionInstance[0] EXCEPTION SyntaxError msg="+e.getMessage+" cause="+e.getCause)
        throw e
      }
      case e: CassConnectException => {
        log.error("CassSessionInstance[1] EXCEPTION CassConnectException msg="+e.getMessage+" cause="+e.getCause)
        throw e
      }
      case e : com.datastax.oss.driver.api.core.DriverTimeoutException =>
        log.error("CassSessionInstance[2] EXCEPTION DriverTimeoutException msg="+e.getMessage+" cause="+e.getCause)
        throw e
      case e : java.lang.ExceptionInInitializerError =>
        log.error("CassSessionInstance[3] EXCEPTION ExceptionInInitializerError msg="+e.getMessage+" cause="+e.getCause)
        throw e
      case e: Throwable =>
        log.error("CassSessionInstance[4] EXCEPTION Throwable msg="+e.getMessage+" cause="+e.getCause)
        throw e
    }
  require(!sessInstance.sess.isClosed, "Cassandra session must be opened.")

  val system = ActorSystem("BCSystem")

  val sysDispatcher = system.getDispatcher
  log.info(s"ActorSystem Dispatcher = $sysDispatcher")

  log.info(s"Actor system created. startTime = ${system.startTime}")
  val MainBcActor = system.actorOf(BarCalculatorManagerActor.props(config,sessInstance).withDispatcher("my-dispatcher"), "BCManagerActor")
  log.info(s"Main bar calculator Actor (BCManagerActor) created. path.name = ${MainBcActor.path.name}")
  log.info("Run BCManagerActor with sending 'calculate' message.")
  MainBcActor ! "calculate"

  //setFetchSize() try for read ticks.

  log.info("========================================== END ============================================")
}





