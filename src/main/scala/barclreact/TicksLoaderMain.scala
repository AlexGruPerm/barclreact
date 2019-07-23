package ticksloader

import akka.actor.ActorSystem
import org.slf4j.LoggerFactory

object TicksLoaderMain extends App {
  val log = LoggerFactory.getLogger(getClass.getName)
  val system = ActorSystem("LoadTickersSystem")
  val ticksLoader = system.actorOf(TicksLoaderManagerActor.props, "TicksLoaderManagerActor")
  log.info("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ BEGIN LOADING TICKS ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
  /*
  val cp = System.getProperty("java.class.path")
  val sep = System.getProperty("path.separator")
  cp.split(sep).foreach(elm => log.info("ClassPathElm: "+elm))
  */
  ticksLoader ! "begin load"
}
