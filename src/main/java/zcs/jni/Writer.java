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

    /**
     * Add a column.
     *
     * @param type             Type of the column
     * @param encoding         Encoding of the column
     * @param compression      Compression algorithm to use when writing column data
     * @param compressionLevel Compression level
     * @throws Exception
     */
    public void addColumn(ColumnType type, ColumnEncoding encoding, ColumnCompression compression,
                          int compressionLevel) throws Exception {
        nativeAddColumn(ptr, type.ordinal(), encoding.ordinal(), compression.ordinal(),
                compressionLevel);
    }

    public void addColumn(ColumnType type) throws Exception {
        addColumn(type, ColumnEncoding.None, ColumnCompression.None, 0);
    }

    /**
     * Write a null value to the specified column.
     *
     * @param index Column index
     * @throws Exception
     */
    public void putNull(int index) throws Exception {
        nativePutNull(ptr, index);
    }

    /**
     * Write a boolean value to the specified column.
     *
     * @param index Column index
     * @param value Boolean value to write
     * @throws Exception
     */
    public void putBoolean(int index, boolean value) throws Exception {
        nativePutBoolean(ptr, index, value);
    }

    /**
     * Write an int value to the specified column.
     *
     * @param index Column index
     * @param value Int value to write
     * @throws Exception
     */
    public void putInt(int index, int value) throws Exception {
        nativePutInt(ptr, index, value);
    }

    /**
     * Write a long value to the specified column.
     *
     * @param index Column index
     * @param value Long value to write
     * @throws Exception
     */
    public void putLong(int index, long value) throws Exception {
        nativePutLong(ptr, index, value);
    }

    /**
     * Write a string value to the specified column.
     *
     * @param index Column index
     * @param value String value to write
     * @throws Exception
     */
    public void putString(int index, String value) throws Exception {
        nativePutString(ptr, index, value);
    }

    private native long nativeNew(String path, long rowGroupSize);

    private native void nativeFree(long ptr);

    private native void nativeFinish(long ptr, boolean sync);

    private native void nativeAddColumn(long ptr, int type, int encoding, int compression,
                                        int compressionLevel);

    private native void nativePutNull(long ptr, int index);

    private native void nativePutBoolean(long ptr, int index, boolean value);

    private native void nativePutInt(long ptr, int index, int value);

    private native void nativePutLong(long ptr, int index, long value);

    private native void nativePutString(long ptr, int index, String value);
}
