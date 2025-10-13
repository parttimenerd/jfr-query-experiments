package me.bechberger.jfr.duckdb.query;

import com.jakewharton.fliptables.FlipTableConverters;
import java.io.*;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.util.Calendar;
import java.util.Map;
import java.util.stream.IntStream;
import me.bechberger.jfr.duckdb.RuntimeSQLException;
import org.duckdb.DuckDBConnection;

/** Executes queries on a DuckDB database connection and prints the results. */
public class QueryExecutor {

    public enum OutputFormat {
        TABLE,
        CSV
    }

    private final DuckDBConnection conn;

    public QueryExecutor(DuckDBConnection connection) {
        conn = connection;
    }

    public void executeQuery(String query, OutputFormat format, PrintStream out) {
        executeQuery(query, format, out, -1);
    }

    public void executeQuery(String query, OutputFormat format, PrintStream out, int maxCellWidth) {
        try (var stmt = conn.createStatement()) {
            var rs = stmt.executeQuery(query);
            switch (format) {
                case TABLE -> printTable(rs, out);
                case CSV -> printCSV(rs, out, maxCellWidth);
            }
        } catch (SQLException e) {
            throw new RuntimeSQLException("Error executing query: " + query, e);
        }
    }

    /** Executes a query and returns the result as a string in the specified format. */
    public String executeQuery(String query, OutputFormat format, int maxCellWidth) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (PrintStream ps = new PrintStream(new BufferedOutputStream(baos))) {
            executeQuery(query, format, ps, maxCellWidth);
            ps.flush();
            return baos.toString();
        }
    }

    private void printCSV(ResultSet rs, PrintStream out, int maxCellWidth) throws SQLException {
        var meta = rs.getMetaData();
        String header =
                IntStream.range(1, meta.getColumnCount() + 1)
                        .mapToObj(
                                i -> {
                                    try {
                                        return meta.getColumnName(i);
                                    } catch (SQLException e) {
                                        throw new RuntimeException(e);
                                    }
                                })
                        .reduce((a, b) -> a + "," + b)
                        .orElse("");
        out.println(header);
        while (rs.next()) {
            String row =
                    IntStream.range(1, meta.getColumnCount() + 1)
                            .mapToObj(i -> getStringValue(rs, i, maxCellWidth))
                            .reduce((a, b) -> a + "," + b)
                            .orElse("");
            out.println(row);
        }
        out.flush();
    }

    private String getStringValue(ResultSet rs, int columnIndex, int maxWidth) {
        try {
            String value = rs.getString(columnIndex);
            if (value == null) {
                return "";
            }
            var escaped =
                    value.replace("\"", "\"\"")
                            .replace(",", "\\,")
                            .replace("\n", "\\n")
                            .replace("\r", "\\r");
            if (maxWidth > 0 && escaped.length() > maxWidth) {
                return "\"" + escaped.substring(0, maxWidth - 3) + "...\"";
            }
            return escaped;
        } catch (SQLException e) {
            throw new RuntimeSQLException("Cannot get value at column " + columnIndex, e);
        }
    }

    private void printTable(ResultSet rs, PrintStream out) throws SQLException {
        out.println(FlipTableConverters.fromResultSet(wrap(rs)));
    }

    /**
     * Wraps a ResultSet to be compatible with FlipTableConverters.fromResultSet. This is necessary
     * because DuckDB's ResultSet implementation does not implement all methods.
     *
     * @param rs the original ResultSet
     * @return a wrapped ResultSet
     */
    public static ResultSet wrap(ResultSet rs) {

        return new ResultSet() {
            @Override
            public <T> T unwrap(Class<T> iface) throws SQLException {
                return rs.unwrap(iface);
            }

            @Override
            public boolean isWrapperFor(Class<?> iface) throws SQLException {
                return rs.isWrapperFor(iface);
            }

            public boolean next() throws SQLException {
                return rs.next();
            }

            public void close() throws SQLException {
                rs.close();
            }

            public boolean wasNull() throws SQLException {
                return rs.wasNull();
            }

            public String getString(int columnIndex) throws SQLException {
                return rs.getString(columnIndex);
            }

            public boolean getBoolean(int columnIndex) throws SQLException {
                return rs.getBoolean(columnIndex);
            }

            public byte getByte(int columnIndex) throws SQLException {
                return rs.getByte(columnIndex);
            }

            public short getShort(int columnIndex) throws SQLException {
                return rs.getShort(columnIndex);
            }

            public int getInt(int columnIndex) throws SQLException {
                return rs.getInt(columnIndex);
            }

            public long getLong(int columnIndex) throws SQLException {
                return rs.getLong(columnIndex);
            }

            public float getFloat(int columnIndex) throws SQLException {
                return rs.getFloat(columnIndex);
            }

            public double getDouble(int columnIndex) throws SQLException {
                return rs.getDouble(columnIndex);
            }

            @Deprecated(since = "1.2")
            public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
                return rs.getBigDecimal(columnIndex, scale);
            }

            public byte[] getBytes(int columnIndex) throws SQLException {
                return rs.getBytes(columnIndex);
            }

            public Date getDate(int columnIndex) throws SQLException {
                return rs.getDate(columnIndex);
            }

            public Time getTime(int columnIndex) throws SQLException {
                return rs.getTime(columnIndex);
            }

            public Timestamp getTimestamp(int columnIndex) throws SQLException {
                return rs.getTimestamp(columnIndex);
            }

            public InputStream getAsciiStream(int columnIndex) throws SQLException {
                return rs.getAsciiStream(columnIndex);
            }

            @Deprecated(since = "1.2")
            public InputStream getUnicodeStream(int columnIndex) throws SQLException {
                return rs.getUnicodeStream(columnIndex);
            }

            public InputStream getBinaryStream(int columnIndex) throws SQLException {
                return rs.getBinaryStream(columnIndex);
            }

            public String getString(String columnLabel) throws SQLException {
                return rs.getString(columnLabel);
            }

            public boolean getBoolean(String columnLabel) throws SQLException {
                return rs.getBoolean(columnLabel);
            }

            public byte getByte(String columnLabel) throws SQLException {
                return rs.getByte(columnLabel);
            }

            public short getShort(String columnLabel) throws SQLException {
                return rs.getShort(columnLabel);
            }

            public int getInt(String columnLabel) throws SQLException {
                return rs.getInt(columnLabel);
            }

            public long getLong(String columnLabel) throws SQLException {
                return rs.getLong(columnLabel);
            }

            public float getFloat(String columnLabel) throws SQLException {
                return rs.getFloat(columnLabel);
            }

            public double getDouble(String columnLabel) throws SQLException {
                return rs.getDouble(columnLabel);
            }

            @Deprecated(since = "1.2")
            public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
                return rs.getBigDecimal(columnLabel, scale);
            }

            public byte[] getBytes(String columnLabel) throws SQLException {
                return rs.getBytes(columnLabel);
            }

            public Date getDate(String columnLabel) throws SQLException {
                return rs.getDate(columnLabel);
            }

            public Time getTime(String columnLabel) throws SQLException {
                return rs.getTime(columnLabel);
            }

            public Timestamp getTimestamp(String columnLabel) throws SQLException {
                return rs.getTimestamp(columnLabel);
            }

            public InputStream getAsciiStream(String columnLabel) throws SQLException {
                return rs.getAsciiStream(columnLabel);
            }

            @Deprecated(since = "1.2")
            public InputStream getUnicodeStream(String columnLabel) throws SQLException {
                return rs.getUnicodeStream(columnLabel);
            }

            public InputStream getBinaryStream(String columnLabel) throws SQLException {
                return rs.getBinaryStream(columnLabel);
            }

            public SQLWarning getWarnings() throws SQLException {
                return rs.getWarnings();
            }

            public void clearWarnings() throws SQLException {
                rs.clearWarnings();
            }

            public String getCursorName() throws SQLException {
                return rs.getCursorName();
            }

            public ResultSetMetaData getMetaData() throws SQLException {
                return rs.getMetaData();
            }

            public Object getObject(int columnIndex) throws SQLException {
                return rs.getObject(columnIndex);
            }

            public Object getObject(String columnLabel) throws SQLException {
                return rs.getObject(columnLabel);
            }

            public int findColumn(String columnLabel) throws SQLException {
                return rs.findColumn(columnLabel);
            }

            public Reader getCharacterStream(int columnIndex) throws SQLException {
                return rs.getCharacterStream(columnIndex);
            }

            public Reader getCharacterStream(String columnLabel) throws SQLException {
                return rs.getCharacterStream(columnLabel);
            }

            public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
                return rs.getBigDecimal(columnIndex);
            }

            public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
                return rs.getBigDecimal(columnLabel);
            }

            public boolean isBeforeFirst() {
                return true;
            }

            public boolean isAfterLast() {
                return false;
            }

            public boolean isFirst() throws SQLException {
                return rs.isFirst();
            }

            public boolean isLast() throws SQLException {
                return rs.isLast();
            }

            public void beforeFirst() throws SQLException {
                rs.beforeFirst();
            }

            public void afterLast() throws SQLException {
                rs.afterLast();
            }

            public boolean first() throws SQLException {
                return rs.first();
            }

            public boolean last() throws SQLException {
                return rs.last();
            }

            public int getRow() throws SQLException {
                return rs.getRow();
            }

            public boolean absolute(int row) throws SQLException {
                return rs.absolute(row);
            }

            public boolean relative(int rows) throws SQLException {
                return rs.relative(rows);
            }

            public boolean previous() throws SQLException {
                return rs.previous();
            }

            public void setFetchDirection(int direction) throws SQLException {
                rs.setFetchDirection(direction);
            }

            public int getFetchDirection() throws SQLException {
                return rs.getFetchDirection();
            }

            public void setFetchSize(int rows) throws SQLException {
                rs.setFetchSize(rows);
            }

            public int getFetchSize() throws SQLException {
                return rs.getFetchSize();
            }

            public int getType() throws SQLException {
                return rs.getType();
            }

            public int getConcurrency() throws SQLException {
                return rs.getConcurrency();
            }

            public boolean rowUpdated() throws SQLException {
                return rs.rowUpdated();
            }

            public boolean rowInserted() throws SQLException {
                return rs.rowInserted();
            }

            public boolean rowDeleted() throws SQLException {
                return rs.rowDeleted();
            }

            public void updateNull(int columnIndex) throws SQLException {
                rs.updateNull(columnIndex);
            }

            public void updateBoolean(int columnIndex, boolean x) throws SQLException {
                rs.updateBoolean(columnIndex, x);
            }

            public void updateByte(int columnIndex, byte x) throws SQLException {
                rs.updateByte(columnIndex, x);
            }

            public void updateShort(int columnIndex, short x) throws SQLException {
                rs.updateShort(columnIndex, x);
            }

            public void updateInt(int columnIndex, int x) throws SQLException {
                rs.updateInt(columnIndex, x);
            }

            public void updateLong(int columnIndex, long x) throws SQLException {
                rs.updateLong(columnIndex, x);
            }

            public void updateFloat(int columnIndex, float x) throws SQLException {
                rs.updateFloat(columnIndex, x);
            }

            public void updateDouble(int columnIndex, double x) throws SQLException {
                rs.updateDouble(columnIndex, x);
            }

            public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
                rs.updateBigDecimal(columnIndex, x);
            }

            public void updateString(int columnIndex, String x) throws SQLException {
                rs.updateString(columnIndex, x);
            }

            public void updateBytes(int columnIndex, byte[] x) throws SQLException {
                rs.updateBytes(columnIndex, x);
            }

            public void updateDate(int columnIndex, Date x) throws SQLException {
                rs.updateDate(columnIndex, x);
            }

            public void updateTime(int columnIndex, Time x) throws SQLException {
                rs.updateTime(columnIndex, x);
            }

            public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
                rs.updateTimestamp(columnIndex, x);
            }

            public void updateAsciiStream(int columnIndex, InputStream x, int length)
                    throws SQLException {
                rs.updateAsciiStream(columnIndex, x, length);
            }

            public void updateBinaryStream(int columnIndex, InputStream x, int length)
                    throws SQLException {
                rs.updateBinaryStream(columnIndex, x, length);
            }

            public void updateCharacterStream(int columnIndex, Reader x, int length)
                    throws SQLException {
                rs.updateCharacterStream(columnIndex, x, length);
            }

            public void updateObject(int columnIndex, Object x, int scaleOrLength)
                    throws SQLException {
                rs.updateObject(columnIndex, x, scaleOrLength);
            }

            public void updateObject(int columnIndex, Object x) throws SQLException {
                rs.updateObject(columnIndex, x);
            }

            public void updateNull(String columnLabel) throws SQLException {
                rs.updateNull(columnLabel);
            }

            public void updateBoolean(String columnLabel, boolean x) throws SQLException {
                rs.updateBoolean(columnLabel, x);
            }

            public void updateByte(String columnLabel, byte x) throws SQLException {
                rs.updateByte(columnLabel, x);
            }

            public void updateShort(String columnLabel, short x) throws SQLException {
                rs.updateShort(columnLabel, x);
            }

            public void updateInt(String columnLabel, int x) throws SQLException {
                rs.updateInt(columnLabel, x);
            }

            public void updateLong(String columnLabel, long x) throws SQLException {
                rs.updateLong(columnLabel, x);
            }

            public void updateFloat(String columnLabel, float x) throws SQLException {
                rs.updateFloat(columnLabel, x);
            }

            public void updateDouble(String columnLabel, double x) throws SQLException {
                rs.updateDouble(columnLabel, x);
            }

            public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
                rs.updateBigDecimal(columnLabel, x);
            }

            public void updateString(String columnLabel, String x) throws SQLException {
                rs.updateString(columnLabel, x);
            }

            public void updateBytes(String columnLabel, byte[] x) throws SQLException {
                rs.updateBytes(columnLabel, x);
            }

            public void updateDate(String columnLabel, Date x) throws SQLException {
                rs.updateDate(columnLabel, x);
            }

            public void updateTime(String columnLabel, Time x) throws SQLException {
                rs.updateTime(columnLabel, x);
            }

            public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
                rs.updateTimestamp(columnLabel, x);
            }

            public void updateAsciiStream(String columnLabel, InputStream x, int length)
                    throws SQLException {
                rs.updateAsciiStream(columnLabel, x, length);
            }

            public void updateBinaryStream(String columnLabel, InputStream x, int length)
                    throws SQLException {
                rs.updateBinaryStream(columnLabel, x, length);
            }

            public void updateCharacterStream(String columnLabel, Reader reader, int length)
                    throws SQLException {
                rs.updateCharacterStream(columnLabel, reader, length);
            }

            public void updateObject(String columnLabel, Object x, int scaleOrLength)
                    throws SQLException {
                rs.updateObject(columnLabel, x, scaleOrLength);
            }

            public void updateObject(String columnLabel, Object x) throws SQLException {
                rs.updateObject(columnLabel, x);
            }

            public void insertRow() throws SQLException {
                rs.insertRow();
            }

            public void updateRow() throws SQLException {
                rs.updateRow();
            }

            public void deleteRow() throws SQLException {
                rs.deleteRow();
            }

            public void refreshRow() throws SQLException {
                rs.refreshRow();
            }

            public void cancelRowUpdates() throws SQLException {
                rs.cancelRowUpdates();
            }

            public void moveToInsertRow() throws SQLException {
                rs.moveToInsertRow();
            }

            public void moveToCurrentRow() throws SQLException {
                rs.moveToCurrentRow();
            }

            public Statement getStatement() throws SQLException {
                return rs.getStatement();
            }

            public Object getObject(int columnIndex, Map<String, Class<?>> map)
                    throws SQLException {
                return rs.getObject(columnIndex, map);
            }

            public Ref getRef(int columnIndex) throws SQLException {
                return rs.getRef(columnIndex);
            }

            public Blob getBlob(int columnIndex) throws SQLException {
                return rs.getBlob(columnIndex);
            }

            public Clob getClob(int columnIndex) throws SQLException {
                return rs.getClob(columnIndex);
            }

            public Array getArray(int columnIndex) throws SQLException {
                return rs.getArray(columnIndex);
            }

            public Object getObject(String columnLabel, Map<String, Class<?>> map)
                    throws SQLException {
                return rs.getObject(columnLabel, map);
            }

            public Ref getRef(String columnLabel) throws SQLException {
                return rs.getRef(columnLabel);
            }

            public Blob getBlob(String columnLabel) throws SQLException {
                return rs.getBlob(columnLabel);
            }

            public Clob getClob(String columnLabel) throws SQLException {
                return rs.getClob(columnLabel);
            }

            public Array getArray(String columnLabel) throws SQLException {
                return rs.getArray(columnLabel);
            }

            public Date getDate(int columnIndex, Calendar cal) throws SQLException {
                return rs.getDate(columnIndex, cal);
            }

            public Date getDate(String columnLabel, Calendar cal) throws SQLException {
                return rs.getDate(columnLabel, cal);
            }

            public Time getTime(int columnIndex, Calendar cal) throws SQLException {
                return rs.getTime(columnIndex, cal);
            }

            public Time getTime(String columnLabel, Calendar cal) throws SQLException {
                return rs.getTime(columnLabel, cal);
            }

            public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
                return rs.getTimestamp(columnIndex, cal);
            }

            public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
                return rs.getTimestamp(columnLabel, cal);
            }

            public URL getURL(int columnIndex) throws SQLException {
                return rs.getURL(columnIndex);
            }

            public URL getURL(String columnLabel) throws SQLException {
                return rs.getURL(columnLabel);
            }

            public void updateRef(int columnIndex, Ref x) throws SQLException {
                rs.updateRef(columnIndex, x);
            }

            public void updateRef(String columnLabel, Ref x) throws SQLException {
                rs.updateRef(columnLabel, x);
            }

            public void updateBlob(int columnIndex, Blob x) throws SQLException {
                rs.updateBlob(columnIndex, x);
            }

            public void updateBlob(String columnLabel, Blob x) throws SQLException {
                rs.updateBlob(columnLabel, x);
            }

            public void updateClob(int columnIndex, Clob x) throws SQLException {
                rs.updateClob(columnIndex, x);
            }

            public void updateClob(String columnLabel, Clob x) throws SQLException {
                rs.updateClob(columnLabel, x);
            }

            public void updateArray(int columnIndex, Array x) throws SQLException {
                rs.updateArray(columnIndex, x);
            }

            public void updateArray(String columnLabel, Array x) throws SQLException {
                rs.updateArray(columnLabel, x);
            }

            public RowId getRowId(int columnIndex) throws SQLException {
                return rs.getRowId(columnIndex);
            }

            public RowId getRowId(String columnLabel) throws SQLException {
                return rs.getRowId(columnLabel);
            }

            public void updateRowId(int columnIndex, RowId x) throws SQLException {
                rs.updateRowId(columnIndex, x);
            }

            public void updateRowId(String columnLabel, RowId x) throws SQLException {
                rs.updateRowId(columnLabel, x);
            }

            public int getHoldability() throws SQLException {
                return rs.getHoldability();
            }

            public boolean isClosed() throws SQLException {
                return rs.isClosed();
            }

            public void updateNString(int columnIndex, String nString) throws SQLException {
                rs.updateNString(columnIndex, nString);
            }

            public void updateNString(String columnLabel, String nString) throws SQLException {
                rs.updateNString(columnLabel, nString);
            }

            public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
                rs.updateNClob(columnIndex, nClob);
            }

            public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
                rs.updateNClob(columnLabel, nClob);
            }

            public NClob getNClob(int columnIndex) throws SQLException {
                return rs.getNClob(columnIndex);
            }

            public NClob getNClob(String columnLabel) throws SQLException {
                return rs.getNClob(columnLabel);
            }

            public SQLXML getSQLXML(int columnIndex) throws SQLException {
                return rs.getSQLXML(columnIndex);
            }

            public SQLXML getSQLXML(String columnLabel) throws SQLException {
                return rs.getSQLXML(columnLabel);
            }

            public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
                rs.updateSQLXML(columnIndex, xmlObject);
            }

            public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
                rs.updateSQLXML(columnLabel, xmlObject);
            }

            public String getNString(int columnIndex) throws SQLException {
                return rs.getNString(columnIndex);
            }

            public String getNString(String columnLabel) throws SQLException {
                return rs.getNString(columnLabel);
            }

            public Reader getNCharacterStream(int columnIndex) throws SQLException {
                return rs.getNCharacterStream(columnIndex);
            }

            public Reader getNCharacterStream(String columnLabel) throws SQLException {
                return rs.getNCharacterStream(columnLabel);
            }

            public void updateNCharacterStream(int columnIndex, Reader x, long length)
                    throws SQLException {
                rs.updateNCharacterStream(columnIndex, x, length);
            }

            public void updateNCharacterStream(String columnLabel, Reader reader, long length)
                    throws SQLException {
                rs.updateNCharacterStream(columnLabel, reader, length);
            }

            public void updateAsciiStream(int columnIndex, InputStream x, long length)
                    throws SQLException {
                rs.updateAsciiStream(columnIndex, x, length);
            }

            public void updateBinaryStream(int columnIndex, InputStream x, long length)
                    throws SQLException {
                rs.updateBinaryStream(columnIndex, x, length);
            }

            public void updateCharacterStream(int columnIndex, Reader x, long length)
                    throws SQLException {
                rs.updateCharacterStream(columnIndex, x, length);
            }

            public void updateAsciiStream(String columnLabel, InputStream x, long length)
                    throws SQLException {
                rs.updateAsciiStream(columnLabel, x, length);
            }

            public void updateBinaryStream(String columnLabel, InputStream x, long length)
                    throws SQLException {
                rs.updateBinaryStream(columnLabel, x, length);
            }

            public void updateCharacterStream(String columnLabel, Reader reader, long length)
                    throws SQLException {
                rs.updateCharacterStream(columnLabel, reader, length);
            }

            public void updateBlob(int columnIndex, InputStream inputStream, long length)
                    throws SQLException {
                rs.updateBlob(columnIndex, inputStream, length);
            }

            public void updateBlob(String columnLabel, InputStream inputStream, long length)
                    throws SQLException {
                rs.updateBlob(columnLabel, inputStream, length);
            }

            public void updateClob(int columnIndex, Reader reader, long length)
                    throws SQLException {
                rs.updateClob(columnIndex, reader, length);
            }

            public void updateClob(String columnLabel, Reader reader, long length)
                    throws SQLException {
                rs.updateClob(columnLabel, reader, length);
            }

            public void updateNClob(int columnIndex, Reader reader, long length)
                    throws SQLException {
                rs.updateNClob(columnIndex, reader, length);
            }

            public void updateNClob(String columnLabel, Reader reader, long length)
                    throws SQLException {
                rs.updateNClob(columnLabel, reader, length);
            }

            public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
                rs.updateNCharacterStream(columnIndex, x);
            }

            public void updateNCharacterStream(String columnLabel, Reader reader)
                    throws SQLException {
                rs.updateNCharacterStream(columnLabel, reader);
            }

            public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
                rs.updateAsciiStream(columnIndex, x);
            }

            public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
                rs.updateBinaryStream(columnIndex, x);
            }

            public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
                rs.updateCharacterStream(columnIndex, x);
            }

            public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
                rs.updateAsciiStream(columnLabel, x);
            }

            public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
                rs.updateBinaryStream(columnLabel, x);
            }

            public void updateCharacterStream(String columnLabel, Reader reader)
                    throws SQLException {
                rs.updateCharacterStream(columnLabel, reader);
            }

            public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
                rs.updateBlob(columnIndex, inputStream);
            }

            public void updateBlob(String columnLabel, InputStream inputStream)
                    throws SQLException {
                rs.updateBlob(columnLabel, inputStream);
            }

            public void updateClob(int columnIndex, Reader reader) throws SQLException {
                rs.updateClob(columnIndex, reader);
            }

            public void updateClob(String columnLabel, Reader reader) throws SQLException {
                rs.updateClob(columnLabel, reader);
            }

            public void updateNClob(int columnIndex, Reader reader) throws SQLException {
                rs.updateNClob(columnIndex, reader);
            }

            public void updateNClob(String columnLabel, Reader reader) throws SQLException {
                rs.updateNClob(columnLabel, reader);
            }

            public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
                return rs.getObject(columnIndex, type);
            }

            public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
                return rs.getObject(columnLabel, type);
            }

            public void updateObject(
                    int columnIndex, Object x, SQLType targetSqlType, int scaleOrLength)
                    throws SQLException {
                rs.updateObject(columnIndex, x, targetSqlType, scaleOrLength);
            }

            public void updateObject(
                    String columnLabel, Object x, SQLType targetSqlType, int scaleOrLength)
                    throws SQLException {
                rs.updateObject(columnLabel, x, targetSqlType, scaleOrLength);
            }

            public void updateObject(int columnIndex, Object x, SQLType targetSqlType)
                    throws SQLException {
                rs.updateObject(columnIndex, x, targetSqlType);
            }

            public void updateObject(String columnLabel, Object x, SQLType targetSqlType)
                    throws SQLException {
                rs.updateObject(columnLabel, x, targetSqlType);
            }
        };
    }
}
