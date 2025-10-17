package me.bechberger.jfr.duckdb.query;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * A table printer that outputs something close to <code>jfr</code>
 * without any external dependencies.
 */
public class TablePrinter {

    public record Column(String header, boolean alignLeft) {}

    private final List<Column> columns;
    private final List<List<String>> rows;

    public TablePrinter(List<Column> columns, List<List<String>> rows) {
        this.columns = columns;
        this.rows = rows;
    }

    private static final java.util.Set<Integer> RIGHT_ALIGNED_SQL_TYPES = java.util.Set.of(
            java.sql.Types.INTEGER,
            java.sql.Types.BIGINT,
            java.sql.Types.DECIMAL,
            java.sql.Types.DOUBLE,
            java.sql.Types.FLOAT,
            java.sql.Types.NUMERIC,
            java.sql.Types.REAL,
            java.sql.Types.SMALLINT,
            java.sql.Types.TINYINT
    );

    private static class MightRightAlign {
        private final boolean initialLeft;
        private int rightAlignedCount = 0;
        private int leftAlignedCount = 0;

        MightRightAlign(boolean initialLeft) {
            if (initialLeft) {
                leftAlignedCount++;
            } else {
                rightAlignedCount++;
            }
            this.initialLeft = initialLeft;
        }

        void consider(String val) {
            if (val == null || val.isEmpty()) {
                if (initialLeft) {
                    leftAlignedCount++;
                } else {
                    rightAlignedCount++;
                }
                return;
            };
            // check if number (allowing leading + or -) and suffixes like %, k, M, B (consisting of letters)
            if (val.matches("[+-]?\\d+(\\.\\d+)?%?[a-zA-Zµ]*")) {
                rightAlignedCount++;
            } else {
                leftAlignedCount++;
            }
        }

        boolean isRightAligned() {
            return rightAlignedCount > leftAlignedCount;
        }
    }

    public static TablePrinter fromResultSet(ResultSet resultSet) throws SQLException {
        java.sql.ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        int columnCount = resultSetMetaData.getColumnCount();
        List<Column> columns = new ArrayList<>();
        List<MightRightAlign> mightRightAligns = new ArrayList<>();
        for (int col = 0; col < columnCount; col++) {
            String header = resultSetMetaData.getColumnName(col + 1);
            // right align numbers, left align everything else
            boolean alignLeft = !RIGHT_ALIGNED_SQL_TYPES.contains(resultSetMetaData.getColumnType(col + 1));
            columns.add(new Column(header, alignLeft));
            mightRightAligns.add(new MightRightAlign(alignLeft));
        }

        List<List<String>> rows = new ArrayList<>();
        while (resultSet.next()) {
            List<String> row = new ArrayList<>();
            for (int col = 0; col < columnCount; col++) {
                String val = resultSet.getString(col + 1);
                // treat SQL NULL as empty string to avoid null handling downstream
                row.add(val == null ? "" : val);
                mightRightAligns.get(col).consider(val);
            }
            rows.add(row);
        }
        // finalize alignment decisions
        for (int col = 0; col < columnCount; col++) {
            Column oldCol = columns.get(col);
            boolean finalAlignLeft = !mightRightAligns.get(col).isRightAligned();
            if (oldCol.alignLeft != finalAlignLeft) {
                columns.set(col, new Column(oldCol.header, finalAlignLeft));
            }
        }
        return new TablePrinter(columns, rows);
    }

    public void print() {
        print(System.out, 120);
    }

    public String toString(int maxWidth) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintStream ps = new PrintStream(baos);
        print(ps, maxWidth);
        return baos.toString();
    }

    public void print(PrintStream printStream, int maxWidth) {
        List<Integer> columnWidths = computeColumnWidths(maxWidth);
        printStream.println(header(columnWidths));
        printStream.println(separator(columnWidths));
        for (List<String> row : rows) {
            for (int col = 0; col < columns.size(); col++) {
                Column column = columns.get(col);
                int width = columnWidths.get(col);
                String cell = row.size() > col && row.get(col) != null ? row.get(col) : "";
                if (col > 0) {
                    printStream.append(" ");
                }
                String truncated = truncate(cell, width);
                if (column.alignLeft) {
                    printStream.printf("%-" + width + "s", truncated);
                } else {
                    printStream.printf("%" + width + "s", truncated);
                }
            }
            printStream.println();
        }
    }

    private String header(List<Integer> columnWidths) {
        StringBuilder sb = new StringBuilder();
        for (int col = 0; col < columns.size(); col++) {
            Column column = columns.get(col);
            int width = columnWidths.get(col);
            String header = column.header;
            if (col > 0) {
                sb.append(" ");
            }
            String truncated = truncate(header, width);
            if (column.alignLeft) {
                sb.append(String.format("%-" + width + "s", truncated));
            } else {
                sb.append(String.format("%" + width + "s", truncated));
            }
        }
        return sb.toString();
    }

    private String separator(List<Integer> columnWidths) {
        StringBuilder sb = new StringBuilder();
        for (int col = 0; col < columns.size(); col++) {
            int width = columnWidths.get(col);
            if (col > 0) {
                sb.append(" ");
            }
            sb.append("-".repeat(Math.max(0, width)));
        }
        return sb.toString();
    }

    private List<Integer> computeColumnWidths(int maxWidth) {
        // account for spaces between columns
        int available = Math.max(0, maxWidth - (columns.size() - 1));
        int n = columns.size();
        int[] maxColWidths = new int[n];
        for (int col = 0; col < n; col++) {
            int maxColWidth = columns.get(col).header == null ? 0 : columns.get(col).header.length();
            for (List<String> row : rows) {
                String cell = row.size() > col && row.get(col) != null ? row.get(col) : "";
                maxColWidth = Math.max(maxColWidth, cell.length());
            }
            // at least 1
            maxColWidth = Math.max(1, maxColWidth);
            maxColWidths[col] = maxColWidth;
        }

        // Sum widths for right aligned columns (they keep their max)
        int sumRight = 0;
        int leftCount = 0;
        for (int i = 0; i < n; i++) {
            if (!columns.get(i).alignLeft) {
                sumRight += maxColWidths[i];
            } else {
                leftCount++;
            }
        }

        int remaining = available - sumRight;
        if (remaining < leftCount) {
            // not enough space to give at least 1 char per left column -> fall back to give 1 each
            remaining = Math.max(leftCount, 0);
        }

        int[] assigned = new int[n];
        // assign rights their max
        for (int i = 0; i < n; i++) {
            if (!columns.get(i).alignLeft) {
                assigned[i] = maxColWidths[i];
            }
        }

        // If there are no left columns, but remaining < 0, we need to shrink right columns proportionally
        if (leftCount == 0) {
            if (sumRight <= available) {
                // fits
                // nothing to do
            } else {
                // shrink right columns proportionally but don't go below 1
                int needToShrink = sumRight - available;
                // simple greedy: repeatedly reduce the widest right column
                int[] rightCols = new int[n];
                int rightCount = 0;
                for (int i = 0; i < n; i++) if (!columns.get(i).alignLeft) rightCols[rightCount++] = i;
                while (needToShrink > 0 && rightCount > 0) {
                    // find widest
                    int idx = -1; int widest = -1;
                    for (int j = 0; j < rightCount; j++) {
                        int colIndex = rightCols[j];
                        if (assigned[colIndex] > widest && assigned[colIndex] > 1) {
                            widest = assigned[colIndex]; idx = colIndex;
                        }
                    }
                    if (idx == -1) break; // cannot shrink further
                    assigned[idx]--;
                    needToShrink--;
                }
            }
        } else {
            // allocate for left columns: start with their max, but if sum exceeds remaining, reduce fairly
            int sumLeftMax = 0;
            for (int i = 0; i < n; i++) if (columns.get(i).alignLeft) sumLeftMax += maxColWidths[i];
            if (sumLeftMax <= remaining) {
                // give everyone their max
                for (int i = 0; i < n; i++) if (columns.get(i).alignLeft) assigned[i] = maxColWidths[i];
            } else {
                // allocate iteratively: give columns min(max, avg remaining), lock those below avg and repeat
                int rem = remaining;
                boolean[] locked = new boolean[n];
                int remainingCols = leftCount;
                int iterations = 0;
                while (remainingCols > 0 && iterations < n + 5) {
                    iterations++;
                    int avg = Math.max(1, rem / remainingCols);
                    boolean anyLocked = false;
                    for (int i = 0; i < n; i++) {
                        if (!columns.get(i).alignLeft || locked[i]) continue;
                        if (maxColWidths[i] <= avg) {
                            assigned[i] = maxColWidths[i];
                            rem -= assigned[i];
                            locked[i] = true;
                            remainingCols--;
                            anyLocked = true;
                        }
                    }
                    if (!anyLocked) {
                        // give avg to all remaining
                        for (int i = 0; i < n; i++) {
                            if (!columns.get(i).alignLeft || locked[i]) continue;
                            assigned[i] = avg;
                            rem -= assigned[i];
                        }
                        break;
                    }
                }
                // if rem > 0 distribute one by one to remaining unlocked columns (up to their max)
                if (rem > 0) {
                    for (int i = 0; i < n && rem > 0; i++) {
                        if (!columns.get(i).alignLeft) continue;
                        if (assigned[i] < maxColWidths[i]) {
                            assigned[i]++;
                            rem--;
                        }
                    }
                }
                // ensure every left column has at least 1
                for (int i = 0; i < n; i++) if (columns.get(i).alignLeft && assigned[i] == 0) assigned[i] = 1;
            }
        }

        // final safety: ensure sum assigned <= available, if not shrink some columns
        int sumAssigned = 0;
        for (int i = 0; i < n; i++) sumAssigned += assigned[i];
        int overflow = sumAssigned - available;
        if (overflow > 0) {
            // shrink non-left first (they often are small) then lefts, but don't go below 1
            for (int pass = 0; pass < 2 && overflow > 0; pass++) {
                for (int i = 0; i < n && overflow > 0; i++) {
                    if (pass == 0 && columns.get(i).alignLeft) continue;
                    if (assigned[i] > 1) {
                        int reduce = Math.min(assigned[i] - 1, overflow);
                        assigned[i] -= reduce;
                        overflow -= reduce;
                    }
                }
            }
        }

        List<Integer> result = new ArrayList<>();
        for (int i = 0; i < n; i++) result.add(assigned[i]);
        return result;
    }

    private static String truncate(String s, int width) {
        if (s == null) return "";
        if (s.length() <= width) return s;
        if (width <= 0) return "";
        if (width == 1) return s.substring(0, 1);
        // leave last char for ellipsis
        if (width <= 2) return s.substring(0, width);
        // use simple truncation with a single char ellipsis
        return s.substring(0, width - 1) + "…";
    }
}