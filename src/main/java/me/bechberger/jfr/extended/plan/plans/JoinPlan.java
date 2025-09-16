package me.bechberger.jfr.extended.plan.plans;

import me.bechberger.jfr.extended.ast.ASTNode;
import me.bechberger.jfr.extended.ast.ASTNodes.StandardJoinType;
import me.bechberger.jfr.extended.plan.PlanExecutionException;
import me.bechberger.jfr.extended.plan.QueryExecutionContext;
import me.bechberger.jfr.extended.plan.QueryResult;
import me.bechberger.jfr.extended.plan.exception.JoinExecutionException;
import me.bechberger.jfr.extended.plan.exception.LeftJoinSideException;
import me.bechberger.jfr.extended.plan.exception.RightJoinSideException;
import me.bechberger.jfr.extended.plan.exception.JoinProcessingException;
import me.bechberger.jfr.extended.table.JfrTable;
import me.bechberger.jfr.extended.table.CellValue;

import java.util.*;

/**
 * Abstract base class for all JOIN operations.
 * 
 * This class provides the foundation for different types of JOIN operations
 * including INNER, LEFT OUTER, RIGHT OUTER, and FULL OUTER joins.
 * 
 * @author JFR Query Plan Architecture
 * @since 3.0
 */
public abstract class JoinPlan extends AbstractStreamingPlan {
    
    protected final StreamingQueryPlan leftPlan;
    protected final StreamingQueryPlan rightPlan;
    protected final StandardJoinType joinType;
    protected final String leftJoinField;
    protected final String rightJoinField;
    
    /**
     * Create a new JOIN plan.
     * 
     * @param sourceNode The AST node this plan was created from
     * @param leftPlan The left side of the join
     * @param rightPlan The right side of the join
     * @param joinType The type of join (INNER, LEFT, RIGHT, FULL)
     * @param leftJoinField The field name from the left table to join on
     * @param rightJoinField The field name from the right table to join on
     */
    protected JoinPlan(ASTNode sourceNode, StreamingQueryPlan leftPlan, StreamingQueryPlan rightPlan,
                      StandardJoinType joinType, String leftJoinField, String rightJoinField) {
        super(sourceNode);
        this.leftPlan = leftPlan;
        this.rightPlan = rightPlan;
        this.joinType = joinType;
        this.leftJoinField = leftJoinField;
        this.rightJoinField = rightJoinField;
    }
    
    @Override
    public QueryResult execute(QueryExecutionContext context) throws PlanExecutionException {
        try {
            // Execute both sides of the join
            QueryResult leftResult = leftPlan.execute(context);
            QueryResult rightResult = rightPlan.execute(context);
            
            // Check for failures and throw specific exceptions
            if (!leftResult.isSuccess() && !rightResult.isSuccess()) {
                // Both sides failed - throw a general JOIN exception with both errors
                String error = "Both sides of JOIN failed. Left: " + leftResult.getError().getMessage() + 
                              "; Right: " + rightResult.getError().getMessage();
                return QueryResult.failure(new JoinExecutionException(error, leftResult.getError()));
            } else if (!leftResult.isSuccess()) {
                // Left side failed
                return QueryResult.failure(new LeftJoinSideException(leftResult.getError().getMessage(), leftResult.getError()));
            } else if (!rightResult.isSuccess()) {
                // Right side failed  
                return QueryResult.failure(new RightJoinSideException(rightResult.getError().getMessage(), rightResult.getError()));
            }
            
            JfrTable leftTable = leftResult.getTable();
            JfrTable rightTable = rightResult.getTable();
            
            // Perform the join
            JfrTable joinedTable = performJoin(leftTable, rightTable, context);
            
            return QueryResult.success(joinedTable);
            
        } catch (Exception e) {
            throw new JoinProcessingException(e.getMessage(), e);
        }
    }
    
    /**
     * Perform the actual join operation.
     * This method must be implemented by concrete join implementations.
     * 
     * @param leftTable The left table
     * @param rightTable The right table
     * @param context The execution context
     * @return The joined table
     */
    protected abstract JfrTable performJoin(JfrTable leftTable, JfrTable rightTable, QueryExecutionContext context);
    
    /**
     * Create the schema for the joined table.
     * This combines columns from both tables, handling name conflicts.
     * 
     * @param leftTable The left table
     * @param rightTable The right table
     * @return List of columns for the joined table
     */
    protected List<JfrTable.Column> createJoinedSchema(JfrTable leftTable, JfrTable rightTable) {
        List<JfrTable.Column> columns = new ArrayList<>();
        
        // Add all columns from left table
        for (JfrTable.Column col : leftTable.getColumns()) {
            columns.add(col);
        }
        
        // Add columns from right table, handling name conflicts
        for (JfrTable.Column col : rightTable.getColumns()) {
            String originalName = col.name();
            String columnName = originalName;
            
            // Check if column name conflicts with left table
            boolean hasConflict = leftTable.getColumns().stream()
                .anyMatch(leftCol -> leftCol.name().equals(originalName));
            
            if (hasConflict) {
                // Add a suffix to distinguish right table columns
                columnName = originalName + "_right";
            }
            
            columns.add(new JfrTable.Column(columnName, col.type()));
        }
        
        return columns;
    }
    
    /**
     * Get the value of a join field from a table row.
     * 
     * @param table The table
     * @param rowIndex The row index
     * @param fieldName The field name
     * @return The field value, or null if not found
     */
    protected CellValue getJoinFieldValue(JfrTable table, int rowIndex, String fieldName) {
        try {
            int columnIndex = table.getColumnIndex(fieldName);
            return table.getCell(rowIndex, columnIndex);
        } catch (Exception e) {
            return null;
        }
    }
    
    /**
     * Compare two values for equality in JOIN context.
     * In SQL semantics, null values do not match each other.
     * 
     * @param left The left value
     * @param right The right value
     * @return true if the values are equal
     */
    protected boolean valuesEqual(CellValue left, CellValue right) {
        if (left == null && right == null) return true;
        if (left == null || right == null) return false;
        
        // In SQL semantics, null values do not match each other
        if (left instanceof CellValue.NullValue || right instanceof CellValue.NullValue) {
            return false;
        }
        
        // Use CellValue's equals method for proper comparison
        return left.equals(right);
    }    /**
     * Create a joined row by combining values from left and right tables.
     * 
     * @param leftTable The left table
     * @param leftRowIndex The left row index (-1 for null values)
     * @param rightTable The right table
     * @param rightRowIndex The right row index (-1 for null values)
     * @param joinedColumns The schema of the joined table
     * @return The joined row
     */
    protected List<CellValue> createJoinedRow(JfrTable leftTable, int leftRowIndex,
                                            JfrTable rightTable, int rightRowIndex,
                                            List<JfrTable.Column> joinedColumns) {
        List<CellValue> row = new ArrayList<>();
        
        // Add values from left table
        for (int i = 0; i < leftTable.getColumnCount(); i++) {
            if (leftRowIndex >= 0) {
                row.add(leftTable.getCell(leftRowIndex, i));
            } else {
                // Null value for outer join
                row.add(new CellValue.NullValue());
            }
        }
        
        // Add values from right table
        for (int i = 0; i < rightTable.getColumnCount(); i++) {
            if (rightRowIndex >= 0) {
                row.add(rightTable.getCell(rightRowIndex, i));
            } else {
                // Null value for outer join
                row.add(new CellValue.NullValue());
            }
        }
        
        return row;
    }
    
    @Override
    public String explain() {
        return String.format("%s JOIN (%s ON %s.%s = %s.%s)",
            joinType.name(),
            formatPlanName(),
            "left", leftJoinField,
            "right", rightJoinField);
    }
    
    @Override
    public boolean supportsStreaming() {
        // JOIN operations typically need to materialize at least one side
        return false;
    }
    
    /**
     * Format the plan name for explain output.
     */
    protected String formatPlanName() {
        return String.format("JoinPlan[%s]", joinType.name());
    }
}
