package zcs.jni;

public class Reader implements AutoCloseable {
    static {
        System.loadLibrary("zcs");
    }

    private long ptr;

    private ColumnType[] columnTypes;
    private ColumnEncoding[] columnEncodings;
    private ColumnCompression[] columnCompressions;

    /**
     * Read data from a file.
     *
     * @param path Path of the file to read
     * @throws Exception If something goes wrong
     */
    public Reader(String path) throws Exception {
        ptr = nativeNew(path);
        columnTypes = ColumnType.values();
        columnEncodings = ColumnEncoding.values();
        columnCompressions = ColumnCompression.values();
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
     * @throws NullPointerException      If the reader is closed
     * @throws IndexOutOfBoundsException If the index is out of bounds
     */
    public ColumnType columnType(int column) throws NullPointerException, IndexOutOfBoundsException {
        int id = nativeColumnType(ptr, column);
        return columnTypes[id];
    }

    /**
     * Get a column's encoding type.
     *
     * @param column Index of the column
     * @return Encoding of the column
     * @throws NullPointerException      If the reader is closed
     * @throws IndexOutOfBoundsException If the index is out of bounds
     */
    public ColumnEncoding columnEncoding(int column) throws NullPointerException, IndexOutOfBoundsException {
        int id = nativeColumnEncoding(ptr, column);
        return columnEncodings[id];
    }

    /**
     * Get a column's compression type.
     *
     * @param column Index of the column
     * @return Encoding of the column
     * @throws NullPointerException      If the reader is closed
     * @throws IndexOutOfBoundsException If the index is out of bounds
     */
    public ColumnCompression columnCompression(int column) throws NullPointerException, IndexOutOfBoundsException {
        int id = nativeColumnCompression(ptr, column);
        return columnCompressions[id];
    }

    private native long nativeNew(String path);

    private native void nativeFree(long ptr);

    private native long nativeColumnCount(long ptr);

    private native int nativeColumnType(long ptr, int index);

    private native int nativeColumnEncoding(long ptr, int index);

    private native int nativeColumnCompression(long ptr, int index);
}
