package zcs.jni

object ColumnType extends Enumeration {
  type ColumnType = Value
  val Boolean, Int, Long, String = Value
}

object ColumnEncoding extends Enumeration {
  type ColumnEncoding = Value
  val None = Value
}

object ColumnCompression extends Enumeration {
  type ColumnCompression = Value
  val None, LZ4, LZ4HC, ZSTD = Value
}

object StringLocation extends Enumeration {
  type StringLocation = Value
  val Start, End, Any = Value
}
