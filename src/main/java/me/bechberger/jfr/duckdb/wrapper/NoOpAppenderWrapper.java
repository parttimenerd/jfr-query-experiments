package me.bechberger.jfr.duckdb.wrapper;

import java.sql.Array;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class NoOpAppenderWrapper implements AppenderWrapper {
    private final PreparedStatement statement;
    private List<PrepParam> currentRow = new ArrayList<>();

    sealed interface PrepParam {
        void setInPreparedStatement(PreparedStatement ps, int index) throws SQLException;
    }

    record StringParam(String value) implements PrepParam {
        @Override
        public void setInPreparedStatement(PreparedStatement ps, int index) throws SQLException {
            ps.setObject(index, value);
        }
    }

    record LongParam(long value) implements PrepParam {
        @Override
        public void setInPreparedStatement(PreparedStatement ps, int index) throws SQLException {
            ps.setObject(index, value);
        }
    }

    record IntParam(int value) implements PrepParam {
        @Override
        public void setInPreparedStatement(PreparedStatement ps, int index) throws SQLException {
            ps.setObject(index, value);
        }
    }

    record ShortParam(short value) implements PrepParam {
        @Override
        public void setInPreparedStatement(PreparedStatement ps, int index) throws SQLException {
            ps.setObject(index, value);
        }
    }

    record ByteParam(byte value) implements PrepParam {
        @Override
        public void setInPreparedStatement(PreparedStatement ps, int index) throws SQLException {
            ps.setObject(index, value);
        }
    }

    record BooleanParam(boolean value) implements PrepParam {
        @Override
        public void setInPreparedStatement(PreparedStatement ps, int index) throws SQLException {
            ps.setObject(index, value);
        }
    }

    record DoubleParam(double value) implements PrepParam {
        @Override
        public void setInPreparedStatement(PreparedStatement ps, int index) throws SQLException {
            ps.setObject(index, value);
        }
    }

    record FloatParam(float value) implements PrepParam {
        @Override
        public void setInPreparedStatement(PreparedStatement ps, int index) throws SQLException {
            ps.setObject(index, value);
        }
    }

    record NullParam() implements PrepParam {
        @Override
        public void setInPreparedStatement(PreparedStatement ps, int index) throws SQLException {
            ps.setObject(index, null);
        }
    }

    record ArrayParam(int[] value) implements PrepParam {
        @Override
        public void setInPreparedStatement(PreparedStatement ps, int index) throws SQLException {
            // Create a JDBC array
            Array sqlArray = ps.getConnection().createArrayOf("INTEGER", Arrays.stream(value).boxed().toArray());
            ps.setArray(index, sqlArray);
        }
    }

    public NoOpAppenderWrapper(PreparedStatement statement) {
        this.statement = statement;
    }

    @Override
    public void begin() throws SQLException {
        currentRow = new ArrayList<>();
    }

    @Override
    public void end() throws SQLException {
        for (int i = 0; i < currentRow.size(); i++) {
            currentRow.get(i).setInPreparedStatement(statement, i + 1);
        }
        statement.addBatch();
    }

    @Override
    public void close() throws SQLException {
        if (!currentRow.isEmpty()) {
            statement.executeBatch();
        }
        statement.close();
    }

    public void append(String value) throws SQLException {
        currentRow.add(new StringParam(value));
    }

    public void append(long value) throws SQLException {
        currentRow.add(new LongParam(value));
    }

    public void append(int value) throws SQLException {
        currentRow.add(new IntParam(value));
    }

    public void append(short value) throws SQLException {
        currentRow.add(new ShortParam(value));
    }

    public void append(byte value) throws SQLException {
        currentRow.add(new ByteParam(value));
    }

    public void append(boolean value) throws SQLException {
        currentRow.add(new BooleanParam(value));
    }

    public void append(double value) throws SQLException {
        currentRow.add(new DoubleParam(value));
    }

    public void append(float value) throws SQLException {
        currentRow.add(new FloatParam(value));
    }

    @Override
    public void appendNull() throws SQLException {
        currentRow.add(new NullParam());
    }

    @Override
    public void append(int[] value) throws SQLException {
        currentRow.add(new ArrayParam(value));
    }

    @Override
    public void append(Instant value) throws SQLException {
        throw new UnsupportedOperationException();
    }
}