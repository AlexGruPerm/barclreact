package ticksloader

final case class CassConnectException(private val message: String = "",
                                 private val cause: Throwable = None.orNull)
  extends Exception(message, cause)
