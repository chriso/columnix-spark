package zcs.jni;

public class Reader implements AutoCloseable {
    static {
        System.loadLibrary("zcs");
    }

    private long ptr;

    private ColumnType[] columnTypes;

    /**
     * Read data from a file.
     *
     * @param path Path of the file to read
     * @throws Exception If something goes wrong
     */
    public Reader(String path) throws Exception {
        ptr = nativeNew(path);
        columnTypes = ColumnType.values();
    }

    /**
     * Close the reader.
     */
    public void close() {
        if (ptr == 0)
            return;
        nativeFree(ptr);
        ptr = 0;
    }

    /**
     * Count the number of columns in the file.
     *
     * @return Column count
     * @throws NullPointerException If the reader is closed
     */
    public long columnCount() {
        return nativeColumnCount(ptr);
    }

    /**
     * Get a column's type.
     *
     * @param column Index of the column
     * @return Type of the column
     * @throws NullPointerException     If the reader is closed
     * @throws IllegalArgumentException If the index is out of bounds
     */
    public ColumnType columnType(int column) throws NullPointerException, IllegalArgumentException {
        int id = nativeColumnType(ptr, column);
        return columnTypes[id];
    }

    private native long nativeNew(String path);

    private native void nativeFree(long ptr);

    private native long nativeColumnCount(long ptr);

    private native int nativeColumnType(long ptr, int index);
}
