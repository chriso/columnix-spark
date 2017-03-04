package zcs.jni.predicates;

public abstract class Predicate implements AutoCloseable {
    static {
        System.loadLibrary("zcs");
    }

    long ptr;

    native long nativeNegate(long ptr);

    native void nativeFree(long ptr);

    public void close() {
        if (ptr == 0)
            return;
        nativeFree(ptr);
        ptr = 0;
    }

    // FIXME: this should not be public
    public long steal() {
        if (ptr == 0)
            throw new NullPointerException();
        long tmp = ptr;
        ptr = 0;
        return tmp;
    }
}
