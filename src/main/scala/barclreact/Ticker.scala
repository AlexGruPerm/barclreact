package ticksloader

import java.time.LocalDate

case class Ticker (
                   tickerId    :Int,
                   tickerCode  :String,
                   minDdateSrc :LocalDate,
                   minTsSrc    :Long
                  )

