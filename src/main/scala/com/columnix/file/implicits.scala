package com.columnix.file

object implicits {

  sealed trait Getter[T] {
    def get(reader: FileReader, index: Int): T
  }

  sealed trait Putter[T] {
    def put(writer: FileWriter, index: Int, value: T): Unit
  }

  implicit val booleanGetter = new Getter[Boolean] {
    def get(reader: FileReader, index: Int): Boolean =
      reader.getBoolean(index)
  }

  implicit val booleanPutter = new Putter[Boolean] {
    def put(writer: FileWriter, index: Int, value: Boolean): Unit =
      writer.putBoolean(index, value)
  }

  implicit val intGetter = new Getter[Int] {
    def get(reader: FileReader, index: Int): Int =
      reader.getInt(index)
  }

  implicit val intPutter = new Putter[Int] {
    def put(writer: FileWriter, index: Int, value: Int): Unit =
      writer.putInt(index, value)
  }

  implicit val longGetter = new Getter[Long] {
    def get(reader: FileReader, index: Int): Long =
      reader.getLong(index)
  }

  implicit val longPutter = new Putter[Long] {
    def put(writer: FileWriter, index: Int, value: Long): Unit =
      writer.putLong(index, value)
  }

  implicit val stringGetter = new Getter[String] {
    def get(reader: FileReader, index: Int): String =
      reader.getString(index)
  }

  implicit val stringPutter = new Putter[String] {
    def put(writer: FileWriter, index: Int, value: String): Unit =
      writer.putString(index, value)
  }

  implicit class RichFileReader(reader: FileReader) {

    def get[T: Getter](index: Int): Option[T] =
      if (reader.isNull(index)) None
      else Some(implicitly[Getter[T]].get(reader, index))

    def collect[T: Getter](index: Int): Seq[Option[T]] = {
      reader.rewind()
      val buffer = scala.collection.mutable.ArrayBuffer.empty[Option[T]]
      while (reader.next)
        buffer += get[T](index)
      buffer
    }
  }

  implicit class RichFileWriter(writer: FileWriter) {

    def put[T: Putter](index: Int, value: T): Unit =
      implicitly[Putter[T]].put(writer, index, value)

    def put[T: Putter](index: Int, value: Option[T]): Unit =
      value match {
        case Some(v) => put[T](index, v)
        case None => writer.putNull(index)
      }

    def put[T: Putter](index: Int, values: Seq[Option[T]]): Unit =
      values foreach (put(index, _))
  }

}
