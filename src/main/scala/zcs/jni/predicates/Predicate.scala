package zcs.jni.predicates

abstract class Predicate {

  protected var ptr = 0L

  def close() {
    if (ptr == 0)
      return
    nativeFree(ptr)
    ptr = 0
  }

  private[zcs] def steal: Long = {
    if (ptr == 0)
      throw new NullPointerException
    val tmp = ptr
    ptr = 0
    tmp
  }

  @native protected def nativeNegate(ptr: Long): Long = ???

  @native protected def nativeFree(ptr: Long): Unit = ???
}
