package me.bechberger.jfr.extended.engine.exception;

import me.bechberger.jfr.extended.ast.ASTNode;
import me.bechberger.jfr.extended.table.CellValue;
import me.bechberger.jfr.extended.table.JfrTable;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Exception thrown when row-level evaluation fails during query execution.
 * 
 * This exception provides detailed information about the row being processed,
 * the available columns, and the specific evaluation context.
 * 
 * @author Query Engine Team
 * @since 1.0
 */
public class RowEvaluationException extends QueryEvaluationException {
    
    private final int rowIndex;
    private final List<String> availableColumns;
    private final JfrTable.Row problematicRow;
    private final RowEvaluationContext evaluationContext;
    private final String expression;
    
    /**
     * Enumeration of different row evaluation contexts.
     */
    public enum RowEvaluationContext {
        WHERE_CLAUSE("WHERE clause evaluation"),
        SELECT_EXPRESSION("SELECT expression evaluation"),
        ORDER_BY("ORDER BY expression evaluation"),
        HAVING_CLAUSE("HAVING clause evaluation"),
        GROUP_BY("GROUP BY expression evaluation"),
        CASE_WHEN("CASE WHEN condition evaluation"),
        FUNCTION_ARGUMENT("function argument evaluation"),
        FIELD_ACCESS("field access evaluation");
        
        private final String description;
        
        RowEvaluationContext(String description) {
            this.description = description;
        }
        
        public String getDescription() {
            return description;
        }
    }
    
    /**
     * Creates a new RowEvaluationException with detailed information.
     * 
     * @param rowIndex The index of the row being processed
     * @param availableColumns List of available column names
     * @param problematicRow The actual row data (can be null)
     * @param context The evaluation context
     * @param expression The expression being evaluated
     * @param errorNode The AST node where the error occurred
     * @param cause The underlying cause of the error
     */
    public RowEvaluationException(int rowIndex, List<String> availableColumns,
                                JfrTable.Row problematicRow, RowEvaluationContext context,
                                String expression, ASTNode errorNode, Throwable cause) {
        super(
            buildEvaluationContext(rowIndex, context, expression),
            buildProblematicValue(availableColumns, problematicRow),
            determineErrorType(cause),
            errorNode,
            cause
        );
        this.rowIndex = rowIndex;
        this.availableColumns = availableColumns != null ? List.copyOf(availableColumns) : List.of();
        this.problematicRow = problematicRow;
        this.evaluationContext = context;
        this.expression = expression;
    }
    
    /**
     * Creates a RowEvaluationException for field access errors.
     */
    public static RowEvaluationException forFieldAccessError(String fieldName,
                                                           int rowIndex,
                                                           List<String> availableColumns,
                                                           JfrTable.Row row,
                                                           ASTNode errorNode) {
        return new RowEvaluationException(
            rowIndex, availableColumns, row, RowEvaluationContext.FIELD_ACCESS,
            "field access: " + fieldName, errorNode,
            new NoSuchFieldException("Field '" + fieldName + "' not found")
        );
    }
    
    /**
     * Creates a RowEvaluationException for null value access.
     */
    public static RowEvaluationException forNullValueAccess(String operation,
                                                          int rowIndex,
                                                          List<String> availableColumns,
                                                          JfrTable.Row row,
                                                          RowEvaluationContext context,
                                                          ASTNode errorNode) {
        return new RowEvaluationException(
            rowIndex, availableColumns, row, context,
            operation + " on null value", errorNode,
            new NullPointerException("Null value encountered during " + operation)
        );
    }
    
    /**
     * Creates a RowEvaluationException for type conversion errors.
     */
    public static RowEvaluationException forTypeConversionError(String expression,
                                                              CellValue value,
                                                              String targetType,
                                                              int rowIndex,
                                                              RowEvaluationContext context,
                                                              ASTNode errorNode) {
        return new RowEvaluationException(
            rowIndex, List.of(), null, context,
            expression + " (converting " + value.getType() + " to " + targetType + ")",
            errorNode,
            new ClassCastException("Cannot convert " + value.getType() + " to " + targetType)
        );
    }
    
    /**
     * Creates a RowEvaluationException for WHERE clause errors.
     */
    public static RowEvaluationException forWhereClauseError(String condition,
                                                           int rowIndex,
                                                           List<String> availableColumns,
                                                           JfrTable.Row row,
                                                           ASTNode errorNode,
                                                           Throwable cause) {
        return new RowEvaluationException(
            rowIndex, availableColumns, row, RowEvaluationContext.WHERE_CLAUSE,
            "WHERE condition: " + condition, errorNode, cause
        );
    }
    
    /**
     * Builds the evaluation context string.
     */
    private static String buildEvaluationContext(int rowIndex, RowEvaluationContext context, String expression) {
        return String.format("%s for row %d: %s", context.getDescription(), rowIndex, expression);
    }
    
    /**
     * Builds the problematic value description.
     */
    private static String buildProblematicValue(List<String> availableColumns, JfrTable.Row row) {
        StringBuilder sb = new StringBuilder();
        
        if (availableColumns != null && !availableColumns.isEmpty()) {
            sb.append("Available columns: ").append(String.join(", ", availableColumns));
        }
        
        if (row != null && !row.getCells().isEmpty()) {
            if (sb.length() > 0) sb.append("; ");
            sb.append("Row data: ");
            List<String> cellDescriptions = row.getCells().stream()
                .limit(5) // Limit to first 5 cells to avoid huge error messages
                .map(cell -> cell.getType().toString())
                .collect(Collectors.toList());
            sb.append("[").append(String.join(", ", cellDescriptions));
            if (row.getCells().size() > 5) {
                sb.append(", ... (").append(row.getCells().size() - 5).append(" more)");
            }
            sb.append("]");
        }
        
        return sb.toString();
    }
    
    /**
     * Determines the appropriate error type based on the cause.
     */
    private static EvaluationErrorType determineErrorType(Throwable cause) {
        if (cause instanceof NoSuchFieldException) {
            return EvaluationErrorType.INVALID_STATE;
        } else if (cause instanceof NullPointerException) {
            return EvaluationErrorType.NULL_POINTER;
        } else if (cause instanceof ClassCastException) {
            return EvaluationErrorType.INVALID_CONVERSION;
        } else if (cause instanceof IndexOutOfBoundsException) {
            return EvaluationErrorType.OUT_OF_BOUNDS;
        } else if (cause instanceof ArithmeticException) {
            return EvaluationErrorType.DIVISION_BY_ZERO;
        }
        return EvaluationErrorType.UNSUPPORTED_OPERATION;
    }
    
    // Getters for accessing specific error information
    
    public int getRowIndex() {
        return rowIndex;
    }
    
    public List<String> getAvailableColumns() {
        return availableColumns;
    }
    
    public JfrTable.Row getProblematicRow() {
        return problematicRow;
    }
    
    public RowEvaluationContext getRowEvaluationContext() {
        return evaluationContext;
    }
    
    public String getExpression() {
        return expression;
    }
    
    /**
     * Returns true if this exception has information about available columns.
     */
    public boolean hasAvailableColumns() {
        return !availableColumns.isEmpty();
    }
    
    /**
     * Returns true if this exception has row data.
     */
    public boolean hasRowData() {
        return problematicRow != null;
    }
    
    /**
     * Returns a detailed description of the row evaluation error.
     */
    public String getRowEvaluationDescription() {
        StringBuilder sb = new StringBuilder();
        sb.append("Row ").append(rowIndex).append(": ");
        sb.append(evaluationContext.getDescription());
        
        if (expression != null) {
            sb.append(" (").append(expression).append(")");
        }
        
        if (hasAvailableColumns()) {
            sb.append(" - Available: ").append(String.join(", ", availableColumns));
        }
        
        return sb.toString();
    }
    
    /**
     * Returns the cell value at the specified column index, if row data is available.
     */
    public CellValue getCellValue(int columnIndex) {
        if (problematicRow == null || columnIndex < 0 || columnIndex >= problematicRow.getCells().size()) {
            return null;
        }
        return problematicRow.getCell(columnIndex);
    }
    
    /**
     * Returns the cell value for the specified column name, if available.
     */
    public CellValue getCellValue(String columnName) {
        if (problematicRow == null || !hasAvailableColumns()) {
            return null;
        }
        
        int columnIndex = availableColumns.indexOf(columnName);
        if (columnIndex < 0) {
            return null;
        }
        
        return getCellValue(columnIndex);
    }
}
