package zcs.jni;

import java.io.Serializable;

/**
 * Reader reads data from a zcs file.
 *
 * Example (read long values from column 0):
 *
 *    try (Reader reader = new Reader(path)) {
 *        while (reader.next()) {
 *            if (reader.isNull(0)) {
 *                System.out.println("(null)")
 *            } else {
 *                System.out.println(reader.getLong(0))
 *            }
 *        }
 *    }
 */
public class Reader implements AutoCloseable, Serializable {
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

        // cache id => enum lookups
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
    public int columnCount() throws NullPointerException {
        return nativeColumnCount(ptr);
    }

    /**
     * Count the number of rows in the file.
     *
     * @return Row count
     * @throws NullPointerException If the reader is closed
     */
    public long rowCount() throws NullPointerException {
        return nativeRowCount(ptr);
    }

    /**
     * Rewind the reader.
     *
     * @throws NullPointerException If the reader is closed
     */
    public void rewind() throws NullPointerException {
        nativeRewind(ptr);
    }

    /**
     * Advance the reader to the next row.
     *
     * @return True if there is another row to read, and false otherwise
     * @throws NullPointerException If the reader is closed
     * @throws Exception            If an error occurred during iteration
     */
    public boolean next() throws Exception {
        return nativeNext(ptr);
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

    /**
     * Check if a column value is null at the reader's current position.
     *
     * @param column Index of the column
     * @return True if the column value is null, and false otherwise
     * @throws NullPointerException      If the reader is closed
     * @throws IndexOutOfBoundsException If the index is out of bounds
     * @throws Exception                 If something goes wrong
     */
    public boolean isNull(int column) throws Exception {
        return nativeIsNull(ptr, column);
    }

    /**
     * Get a boolean value from the current row.
     *
     * @param column Index of the column
     * @return Column value
     * @throws NullPointerException      If the reader is closed
     * @throws IndexOutOfBoundsException If the index is out of bounds
     * @throws Exception                 If something goes wrong
     */
    public boolean getBoolean(int column) throws Exception {
        return nativeGetBoolean(ptr, column);
    }

    /**
     * Get an int value from the current row.
     *
     * @param column Index of the column
     * @return Column value
     * @throws NullPointerException      If the reader is closed
     * @throws IndexOutOfBoundsException If the index is out of bounds
     * @throws Exception                 If something goes wrong
     */
    public int getInt(int column) throws Exception {
        return nativeGetInt(ptr, column);
    }

    /**
     * Get a long value from the current row.
     *
     * @param column Index of the column
     * @return Column value
     * @throws NullPointerException      If the reader is closed
     * @throws IndexOutOfBoundsException If the index is out of bounds
     * @throws Exception                 If something goes wrong
     */
    public long getLong(int column) throws Exception {
        return nativeGetLong(ptr, column);
    }

    /**
     * Get a string value from the current row.
     *
     * @param column Index of the column
     * @return Column value
     * @throws NullPointerException      If the reader is closed
     * @throws IndexOutOfBoundsException If the index is out of bounds
     * @throws Exception                 If something goes wrong
     */
    public String getString(int column) throws Exception {
        return nativeGetString(ptr, column);
    }

    private native long nativeNew(String path);

    private native void nativeFree(long ptr);

    private native int nativeColumnCount(long ptr);

    private native long nativeRowCount(long ptr);

    private native void nativeRewind(long ptr);

    private native boolean nativeNext(long ptr);

    private native int nativeColumnType(long ptr, int index);

    private native int nativeColumnEncoding(long ptr, int index);

    private native int nativeColumnCompression(long ptr, int index);

    private native boolean nativeIsNull(long ptr, int index);

    private native boolean nativeGetBoolean(long ptr, int index);

    private native int nativeGetInt(long ptr, int index);

    private native long nativeGetLong(long ptr, int index);

    private native String nativeGetString(long ptr, int index);
}
