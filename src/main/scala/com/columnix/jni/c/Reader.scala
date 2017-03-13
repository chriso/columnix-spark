package com.columnix.jni.c

private[jni] class Reader {

  System.loadLibrary("columnix")

  type Pointer = Long

  import Predicate.{Pointer => PredicatePointer}

  @native def create(path: String): Long = ???

  @native def createMatching(path: String, predicate: PredicatePointer): Pointer = ???

  @native def free(reader: Pointer): Unit = ???

  @native def getMetadata(reader: Pointer): String = ???

  @native def columnCount(reader: Pointer): Int = ???

  @native def rowCount(reader: Pointer): Long = ???

  @native def rewind(reader: Pointer): Unit = ???

  @native def next(reader: Pointer): Boolean = ???

  @native def columnName(reader: Pointer, index: Int): String = ???

  @native def columnType(reader: Pointer, index: Int): Int = ???

  @native def columnEncoding(reader: Pointer, index: Int): Int = ???

  @native def columnCompression(reader: Pointer, index: Int): Int = ???

  @native def isNull(reader: Pointer, index: Int): Boolean = ???

  @native def getBoolean(reader: Pointer, index: Int): Boolean = ???

  @native def getInt(reader: Pointer, index: Int): Int = ???

  @native def getLong(reader: Pointer, index: Int): Long = ???

  @native def getString(reader: Pointer, index: Int): String = ???

  @native def getStringBytes(reader: Pointer, index: Int): Array[Byte] = ???
}
