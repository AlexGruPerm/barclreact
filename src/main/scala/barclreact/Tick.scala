package ticksloader

import java.time.LocalDate

case class Tick(
                 ticker_id  :Int,
                 ddate      :LocalDate,
                 ts         :Long,
                 db_tsunx   :Long,
                 ask        :Double,
                 bid        :Double
               )

