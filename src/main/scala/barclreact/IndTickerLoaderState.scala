package ticksloader

import java.time.LocalDate

case class IndTickerLoaderState(tickerID     :Int,
                                tickerCode   :String,
                                minSrcDate   :LocalDate,
                                maxDdateSrc  :LocalDate,
                                maxTsSrc     :Long,
                                maxDdateDest :LocalDate,
                                maxTsDest    :Long){
  val gapSeconds :Long = (maxTsSrc-maxTsDest)/1000L
  val gapDays :Long = gapSeconds/60/60/24

  override def toString: String =
    " "+tickerID+" ["+tickerCode+"] "+" minDdateSrc=["+minSrcDate+"] DEST -> SRC ("+
      maxDdateDest+" -> "+maxDdateSrc+") ("+
      maxTsDest+" -> "+maxTsSrc+") DAYS:"+gapDays+" SECS:"+gapSeconds

}



