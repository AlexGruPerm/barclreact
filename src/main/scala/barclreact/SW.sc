

val r :Int = 29971

val rd2 = r/2


/*
import java.time.{Instant, LocalDate, ZoneOffset}

val tsBegin: Long=1562619600133L
val tsEnd: Long=1562964899640L

val zo = ZoneOffset.ofHours(+3)

val beginYear = Instant.ofEpochMilli(tsBegin).atOffset(zo).getYear
val beginDay = Instant.ofEpochMilli(tsBegin).atOffset(zo).getDayOfYear

val endYear = Instant.ofEpochMilli(tsEnd).atOffset(zo).getYear
val endDay = Instant.ofEpochMilli(tsEnd).atOffset(zo).getDayOfYear

val sD :Seq[LocalDate] =
if (beginYear == endYear) {
  (beginDay to endDay).map(dayNum => LocalDate.ofYearDay(beginYear,dayNum))
} else if (beginYear == (endYear-1)) {
      (beginDay to 365).map(dayNum => LocalDate.ofYearDay(beginYear, dayNum)) ++
      (1 to endDay).map(dayNum => LocalDate.ofYearDay(endYear, dayNum))
} else {
  Seq(LocalDate.ofYearDay(beginYear, beginDay))
}

*/

  //.foreach(println)
