package zcs.jni

class ReaderTest extends Test {

  behavior of "Reader"

  it should "have idempotent close()" in test { file =>
    withWriter(file)(_.finish())
    withReader(file) { reader =>
      reader.close()
      reader.close()
    }
  }
}