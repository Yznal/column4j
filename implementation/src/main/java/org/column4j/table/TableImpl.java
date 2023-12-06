package org.column4j.table;

import org.column4j.ColumnType;
import org.column4j.mutable.MutableColumn;
import org.column4j.mutable.aggregated.*;
import org.column4j.table.query.insert.InsertQuery;
import org.column4j.table.query.select.SelectionOrAggregationQuery;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author sibmaks
 * @since 0.0.1
 */
class TableImpl implements Table {
    private final ColumnsMap columnsMap;
    private final AtomicInteger cursor;

    private TableImpl(ColumnsMap columnsMap) {
        this.columnsMap = columnsMap;
        this.cursor = new AtomicInteger(0);
    }

    @Override
    public InsertQuery insert() {
        int cursorIndex = cursor.getAndIncrement();

        return new InsertQuery() {
            final AtomicBoolean committed = new AtomicBoolean(false);

            @Override
            public InsertQuery column(String columnName, byte value) {
                AggregatedByteMutableColumn mutableColumn = columnsMap.getColumn(ColumnType.INT8, columnName);
                assertColumnNotNull(mutableColumn, columnName);
                mutableColumn.write(cursorIndex, value);
                return this;
            }

            @Override
            public InsertQuery column(String columnName, short value) {
                AggregatedShortMutableColumn mutableColumn = columnsMap.getColumn(ColumnType.INT16, columnName);
                assertColumnNotNull(mutableColumn, columnName);
                mutableColumn.write(cursorIndex, value);
                return this;
            }

            @Override
            public InsertQuery column(String columnName, int value) {
                AggregatedIntMutableColumn mutableColumn = columnsMap.getColumn(ColumnType.INT32, columnName);
                assertColumnNotNull(mutableColumn, columnName);
                mutableColumn.write(cursorIndex, value);
                return this;
            }

            @Override
            public InsertQuery column(String columnName, long value) {
                AggregatedLongMutableColumn mutableColumn = columnsMap.getColumn(ColumnType.INT64, columnName);
                assertColumnNotNull(mutableColumn, columnName);
                mutableColumn.write(cursorIndex, value);
                return this;
            }

            @Override
            public InsertQuery column(String columnName, float value) {
                AggregatedFloatMutableColumn mutableColumn = columnsMap.getColumn(ColumnType.FLOAT32, columnName);
                assertColumnNotNull(mutableColumn, columnName);
                mutableColumn.write(cursorIndex, value);
                return this;
            }

            @Override
            public InsertQuery column(String columnName, double value) {
                AggregatedDoubleMutableColumn mutableColumn = columnsMap.getColumn(ColumnType.FLOAT64, columnName);
                assertColumnNotNull(mutableColumn, columnName);
                mutableColumn.write(cursorIndex, value);
                return this;
            }

            @Override
            public InsertQuery column(String columnName, String value) {
                AggregatedStringMutableColumn mutableColumn = columnsMap.getColumn(ColumnType.STRING, columnName);
                assertColumnNotNull(mutableColumn, columnName);
                mutableColumn.write(cursorIndex, value);
                return this;
            }

            @Override
            public void commit() {
                if (!committed.compareAndSet(false, true)) {
                    throw new IllegalStateException("Query already committed");
                }
                /**
                 * Here we need to update our indexes
                 */
            }

            private static void assertColumnNotNull(MutableColumn<?> mutableColumn, String columnName) {
                if (mutableColumn == null) {
                    throw new IllegalArgumentException("Column " + columnName + " does not exist or has another type");
                }
            }
        };
    }

    @Override
    public SelectionOrAggregationQuery select() {
        throw new UnsupportedOperationException("To be implemented");
    }

    static class TableBuilderImpl implements TableBuilder {
        private final Map<String, AggregatedColumn<?, ?>> columns;

        public TableBuilderImpl() {
            this.columns = new HashMap<>();
        }

        @Override
        public TableBuilderImpl column(String columnName,
                                       AggregatedByteMutableColumn column) {
            assertColumnShouldNotExists(columnName);
            var columnType = column.type();

            if (columnType == ColumnType.INT8) {
                columns.put(columnName, column);
            } else {
                throw new IllegalArgumentException("Column type is invalid, should be INT8");
            }
            return this;
        }

        @Override
        public TableBuilderImpl column(String columnName,
                                       AggregatedShortMutableColumn column) {
            assertColumnShouldNotExists(columnName);
            var columnType = column.type();

            if (columnType == ColumnType.INT16) {
                columns.put(columnName, column);
            } else {
                throw new IllegalArgumentException("Column type is invalid, should be INT16");
            }
            return this;
        }

        @Override
        public TableBuilderImpl column(String columnName,
                                       AggregatedIntMutableColumn column) {
            assertColumnShouldNotExists(columnName);
            var columnType = column.type();

            if (columnType == ColumnType.INT32) {
                columns.put(columnName, column);
            } else {
                throw new IllegalArgumentException("Column type is invalid, should be INT32");
            }
            return this;
        }

        @Override
        public TableBuilderImpl column(String columnName,
                                       AggregatedLongMutableColumn column) {
            assertColumnShouldNotExists(columnName);
            var columnType = column.type();

            if (columnType == ColumnType.INT64) {
                columns.put(columnName, column);
            } else {
                throw new IllegalArgumentException("Column type is invalid, should be INT64");
            }
            return this;
        }

        @Override
        public TableBuilderImpl column(String columnName,
                                       AggregatedFloatMutableColumn column) {
            assertColumnShouldNotExists(columnName);
            var columnType = column.type();

            if (columnType == ColumnType.FLOAT32) {
                columns.put(columnName, column);
            } else {
                throw new IllegalArgumentException("Column type is invalid, should be FLOAT32");
            }
            return this;
        }

        @Override
        public TableBuilderImpl column(String columnName,
                                       AggregatedDoubleMutableColumn column) {
            assertColumnShouldNotExists(columnName);
            var columnType = column.type();

            if (columnType == ColumnType.FLOAT64) {
                columns.put(columnName, column);
            } else {
                throw new IllegalArgumentException("Column type is invalid, should be FLOAT64");
            }
            return this;
        }

        @Override
        public TableBuilderImpl column(String columnName,
                                       AggregatedStringMutableColumn column) {
            assertColumnShouldNotExists(columnName);
            var columnType = column.type();

            if (columnType == ColumnType.STRING) {
                columns.put(columnName, column);
            } else {
                throw new IllegalArgumentException("Column type is invalid, should be STRING");
            }
            return this;
        }

        private void assertColumnShouldNotExists(String columnName) {
            if (columns.containsKey(columnName)) {
                throw new IllegalArgumentException("Column " + columnName + " already exists");
            }
        }

        @Override
        public Table build() {
            if (columns.isEmpty()) {
                throw new IllegalStateException("Columns must not be empty");
            }
            var columnsMap = new ColumnsMap(columns);
            return new TableImpl(columnsMap);
        }
    }

}
