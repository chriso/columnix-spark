package zcs.jni;

/**
 * Writer writes column data to a zcs file.
 */
public class Writer implements AutoCloseable {
    static {
        System.loadLibrary("zcs");
    }

    private long ptr;

    /**
     * Create a new writer.
     *
     * @param path         Path to a zcs file to write to. The file is created if it does not exist
     * @param rowGroupSize Number of rows per row group (default: 100000)
     * @throws Exception
     */
    public Writer(String path, long rowGroupSize) throws Exception {
        ptr = nativeNew(path, rowGroupSize);
    }

    public Writer(String path) throws Exception {
        this(path, 100000L);
    }

    /**
     * Finish writes by writing the file footer.
     *
     * @param sync Whether to fsync() after finishing writes (default: true)
     * @throws Exception
     */
    public void finish(boolean sync) throws Exception {
        nativeFinish(ptr, sync);
    }

    public void finish() throws Exception {
        finish(true);
    }

    /**
     * Close the writer.
     *
     * @throws Exception
     */
    public void close() throws Exception {
        if (ptr == 0)
            return;
        nativeFree(ptr);
        ptr = 0;
    }

    private native long nativeNew(String path, long rowGroupSize);

    private native void nativeFree(long ptr);

    private native void nativeFinish(long ptr, boolean sync);
}
