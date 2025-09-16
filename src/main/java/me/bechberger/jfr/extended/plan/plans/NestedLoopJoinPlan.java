package me.bechberger.jfr.extended.plan.plans;

import me.bechberger.jfr.extended.ast.ASTNode;
import me.bechberger.jfr.extended.ast.ASTNodes.StandardJoinType;
import me.bechberger.jfr.extended.plan.QueryExecutionContext;
import me.bechberger.jfr.extended.table.JfrTable;
import me.bechberger.jfr.extended.table.CellValue;
import me.bechberger.jfr.extended.table.StandardJfrTable;

import java.util.*;

/**
 * Nested Loop JOIN implementation for small tables or when hash join is not suitable.
 * 
 * This implementation uses a nested loop approach where for each row in the outer table,
 * it scans through all rows in the inner table to find matches. While less efficient
 * than hash joins for large tables, it's simpler and works well for small datasets.
 * 
 * @author JFR Query Plan Architecture
 * @since 3.0
 */
public class NestedLoopJoinPlan extends JoinPlan {
    
    /**
     * Create a new nested loop JOIN plan.
     * 
     * @param sourceNode The AST node this plan was created from
     * @param leftPlan The left side of the join
     * @param rightPlan The right side of the join
     * @param joinType The type of join (INNER, LEFT, RIGHT, FULL)
     * @param leftJoinField The field name from the left table to join on
     * @param rightJoinField The field name from the right table to join on
     */
    public NestedLoopJoinPlan(ASTNode sourceNode, StreamingQueryPlan leftPlan, StreamingQueryPlan rightPlan,
                             StandardJoinType joinType, String leftJoinField, String rightJoinField) {
        super(sourceNode, leftPlan, rightPlan, joinType, leftJoinField, rightJoinField);
    }
    
    @Override
    protected JfrTable performJoin(JfrTable leftTable, JfrTable rightTable, QueryExecutionContext context) {
        // Create the joined table schema
        List<JfrTable.Column> joinedColumns = createJoinedSchema(leftTable, rightTable);
        StandardJfrTable result = new StandardJfrTable(joinedColumns);
        
        // Special handling for CROSS JOIN - join every left row with every right row
        if (joinType == StandardJoinType.CROSS) {
            for (int leftRowIndex = 0; leftRowIndex < leftTable.getRowCount(); leftRowIndex++) {
                for (int rightRowIndex = 0; rightRowIndex < rightTable.getRowCount(); rightRowIndex++) {
                    // Create joined row for CROSS JOIN (no condition check needed)
                    List<CellValue> joinedRow = createJoinedRow(leftTable, leftRowIndex, rightTable, rightRowIndex, joinedColumns);
                    result.addRow(joinedRow.toArray(new CellValue[0]));
                }
            }
            return result;
        }
        
        // Track matched rows for outer joins
        Set<Integer> matchedLeftRows = new HashSet<>();
        Set<Integer> matchedRightRows = new HashSet<>();
        
        // Nested loop join with equality condition
        for (int leftRowIndex = 0; leftRowIndex < leftTable.getRowCount(); leftRowIndex++) {
            CellValue leftValue = getJoinFieldValue(leftTable, leftRowIndex, leftJoinField);
            boolean foundMatch = false;
            
            for (int rightRowIndex = 0; rightRowIndex < rightTable.getRowCount(); rightRowIndex++) {
                CellValue rightValue = getJoinFieldValue(rightTable, rightRowIndex, rightJoinField);
                
                if (valuesEqual(leftValue, rightValue)) {
                    // Found a match
                    foundMatch = true;
                    matchedLeftRows.add(leftRowIndex);
                    matchedRightRows.add(rightRowIndex);
                    
                    // Create joined row
                    List<CellValue> joinedRow = createJoinedRow(leftTable, leftRowIndex, rightTable, rightRowIndex, joinedColumns);
                    result.addRow(joinedRow.toArray(new CellValue[0]));
                }
            }
            
            // Handle unmatched left rows for LEFT and FULL outer joins
            if (!foundMatch && shouldIncludeUnmatchedLeftRows()) {
                List<CellValue> joinedRow = createJoinedRow(leftTable, leftRowIndex, rightTable, -1, joinedColumns);
                result.addRow(joinedRow.toArray(new CellValue[0]));
            }
        }
        
        // Handle unmatched right rows for RIGHT and FULL outer joins
        if (shouldIncludeUnmatchedRightRows()) {
            for (int rightRowIndex = 0; rightRowIndex < rightTable.getRowCount(); rightRowIndex++) {
                if (!matchedRightRows.contains(rightRowIndex)) {
                    List<CellValue> joinedRow = createJoinedRow(leftTable, -1, rightTable, rightRowIndex, joinedColumns);
                    result.addRow(joinedRow.toArray(new CellValue[0]));
                }
            }
        }
        
        return result;
    }
    
    /**
     * Determine if unmatched left rows should be included in the result.
     * This is true for LEFT and FULL outer joins.
     */
    private boolean shouldIncludeUnmatchedLeftRows() {
        return switch (joinType) {
            case INNER -> false;
            case LEFT -> true;
            case RIGHT -> false;
            case FULL -> true;
            case CROSS -> false; // CROSS JOIN doesn't have unmatched rows concept
        };
    }
    
    /**
     * Determine if unmatched right rows should be included in the result.
     * This is true for RIGHT and FULL outer joins.
     */
    private boolean shouldIncludeUnmatchedRightRows() {
        return switch (joinType) {
            case INNER -> false;
            case LEFT -> false;
            case RIGHT -> true;
            case FULL -> true;
            case CROSS -> false; // CROSS JOIN doesn't have unmatched rows concept
        };
    }
    
    @Override
    public String explain() {
        return String.format("NestedLoopJoinPlan[%s] (O(n*m) complexity)", joinType.name());
    }
    
    @Override
    protected String formatPlanName() {
        return String.format("NestedLoopJoinPlan[%s]", joinType.name());
    }
}
