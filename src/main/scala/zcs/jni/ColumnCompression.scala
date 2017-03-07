package zcs.jni

object ColumnCompression extends Enumeration {
  type ColumnCompression = Value
  val None, LZ4, LZ4HC, ZSTD = Value
}
