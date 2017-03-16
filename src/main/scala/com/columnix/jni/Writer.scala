package com.columnix.jni

private[columnix] class Writer extends Serializable {

  System.loadLibrary("columnix")

  type Pointer = Long

  @native def create(path: String, rowGroupSize: Long): Pointer = ???

  @native def free(writer: Pointer): Unit = ???

  @native def setMetadata(writer: Pointer, metadata: String): Unit = ???

  @native def finish(writer: Pointer, sync: Boolean): Unit = ???

  @native def addColumn(writer: Pointer, name: String, columnType: Int,
                        encoding: Int, compression: Int, compressionLevel: Int): Unit = ???

  @native def putNull(writer: Pointer, index: Int): Unit = ???

  @native def putBoolean(writer: Pointer, index: Int, value: Boolean): Unit = ???

  @native def putInt(writer: Pointer, index: Int, value: Int): Unit = ???

  @native def putLong(writer: Pointer, index: Int, value: Long): Unit = ???

  @native def putFloat(writer: Pointer, index: Int, value: Float): Unit = ???

  @native def putDouble(writer: Pointer, index: Int, value: Double): Unit = ???

  @native def putString(writer: Pointer, index: Int, value: String): Unit = ???
}
