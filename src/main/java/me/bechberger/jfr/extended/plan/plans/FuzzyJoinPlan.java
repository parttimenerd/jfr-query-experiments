package me.bechberger.jfr.extended.plan.plans;

import me.bechberger.jfr.extended.ast.ASTNode;
import me.bechberger.jfr.extended.ast.ASTNodes.FuzzyJoinType;
import me.bechberger.jfr.extended.plan.PlanExecutionException;
import me.bechberger.jfr.extended.plan.QueryExecutionContext;
import me.bechberger.jfr.extended.plan.QueryResult;
import me.bechberger.jfr.extended.plan.exception.JoinExecutionException;
import me.bechberger.jfr.extended.plan.exception.LeftJoinSideException;
import me.bechberger.jfr.extended.plan.exception.RightJoinSideException;
import me.bechberger.jfr.extended.plan.exception.JoinProcessingException;
import me.bechberger.jfr.extended.table.JfrTable;
import me.bechberger.jfr.extended.table.CellValue;
import me.bechberger.jfr.extended.table.StandardJfrTable;

import java.util.*;

/**
 * Fuzzy JOIN implementation for temporal correlations in JFR data.
 * 
 * This specialized JOIN implementation is designed for JFR event correlation
 * where exact matches are rare but temporal proximity is important. It supports
 * NEAREST, PREVIOUS, and AFTER join types with configurable tolerance.
 * 
 * @author JFR Query Plan Architecture
 * @since 3.0
 */
public class FuzzyJoinPlan extends AbstractStreamingPlan {
    
    private final StreamingQueryPlan leftPlan;
    private final StreamingQueryPlan rightPlan;
    private final FuzzyJoinType joinType;
    private final String joinField;
    private final CellValue tolerance;
    private final CellValue threshold;
    
    /**
     * Create a new fuzzy JOIN plan.
     * 
     * @param sourceNode The AST node this plan was created from
     * @param leftPlan The left side of the join
     * @param rightPlan The right side of the join
     * @param joinType The type of fuzzy join (NEAREST, PREVIOUS, AFTER)
     * @param joinField The field name to join on (typically timestamp)
     * @param tolerance The maximum time difference to consider for matching
     * @param threshold Additional threshold for fuzzy matching
     */
    public FuzzyJoinPlan(ASTNode sourceNode, StreamingQueryPlan leftPlan, StreamingQueryPlan rightPlan,
                        FuzzyJoinType joinType, String joinField, CellValue tolerance, CellValue threshold) {
        super(sourceNode);
        this.leftPlan = leftPlan;
        this.rightPlan = rightPlan;
        this.joinType = joinType;
        this.joinField = joinField;
        this.tolerance = tolerance;
        this.threshold = threshold;
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
                String error = "Both sides of FUZZY JOIN failed. Left: " + leftResult.getError().getMessage() + 
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
            
            // Perform the fuzzy join
            JfrTable joinedTable = performFuzzyJoin(leftTable, rightTable, context);
            
            return QueryResult.success(joinedTable);
            
        } catch (Exception e) {
            throw new JoinProcessingException(e.getMessage(), e);
        }
    }
    
    /**
     * Perform the fuzzy join operation.
     * 
     * @param leftTable The left table
     * @param rightTable The right table
     * @param context The execution context
     * @return The joined table
     */
    private JfrTable performFuzzyJoin(JfrTable leftTable, JfrTable rightTable, QueryExecutionContext context) {
        // Create the joined table schema
        List<JfrTable.Column> joinedColumns = createJoinedSchema(leftTable, rightTable);
        StandardJfrTable result = new StandardJfrTable(joinedColumns);
        
        // Sort right table by join field for efficient searching
        List<Integer> sortedRightIndices = createSortedIndices(rightTable, joinField);
        
        // For each row in the left table, find the best match in the right table
        for (int leftRowIndex = 0; leftRowIndex < leftTable.getRowCount(); leftRowIndex++) {
            CellValue leftValue = getJoinFieldValue(leftTable, leftRowIndex, joinField);
            
            if (leftValue != null) {
                int bestMatchIndex = findBestMatch(leftValue, rightTable, sortedRightIndices);
                
                if (bestMatchIndex >= 0) {
                    // Create joined row
                    List<CellValue> joinedRow = createJoinedRow(leftTable, leftRowIndex, rightTable, bestMatchIndex, joinedColumns);
                    result.addRow(joinedRow.toArray(new CellValue[0]));
                }
            }
        }
        
        return result;
    }
    
    /**
     * Create a sorted list of indices for the right table based on the join field.
     * 
     * @param table The table to sort
     * @param fieldName The field to sort by
     * @return List of row indices sorted by the field values
     */
    private List<Integer> createSortedIndices(JfrTable table, String fieldName) {
        List<Integer> indices = new ArrayList<>();
        for (int i = 0; i < table.getRowCount(); i++) {
            indices.add(i);
        }
        
        // Sort indices based on the join field values
        indices.sort((i1, i2) -> {
            CellValue v1 = getJoinFieldValue(table, i1, fieldName);
            CellValue v2 = getJoinFieldValue(table, i2, fieldName);
            
            if (v1 == null && v2 == null) return 0;
            if (v1 == null) return -1;
            if (v2 == null) return 1;
            
            // Compare values (assuming they are comparable)
            return compareValues(v1, v2);
        });
        
        return indices;
    }
    
    /**
     * Find the best match for a given value in the right table.
     * 
     * @param targetValue The value to match
     * @param rightTable The right table
     * @param sortedIndices Sorted indices of the right table
     * @return The index of the best match, or -1 if no suitable match found
     */
    private int findBestMatch(CellValue targetValue, JfrTable rightTable, List<Integer> sortedIndices) {
        switch (joinType) {
            case NEAREST:
                return findNearestMatch(targetValue, rightTable, sortedIndices);
            case PREVIOUS:
                return findPreviousMatch(targetValue, rightTable, sortedIndices);
            case AFTER:
                return findAfterMatch(targetValue, rightTable, sortedIndices);
            default:
                return -1;
        }
    }
    
    /**
     * Find the nearest match (closest in time).
     */
    private int findNearestMatch(CellValue targetValue, JfrTable rightTable, List<Integer> sortedIndices) {
        int bestIndex = -1;
        double bestDistance = Double.MAX_VALUE;
        
        for (int index : sortedIndices) {
            CellValue rightValue = getJoinFieldValue(rightTable, index, joinField);
            if (rightValue != null) {
                double distance = Math.abs(getNumericValue(targetValue) - getNumericValue(rightValue));
                
                if (distance < bestDistance && isWithinTolerance(distance)) {
                    bestDistance = distance;
                    bestIndex = index;
                }
            }
        }
        
        return bestIndex;
    }
    
    /**
     * Find the previous match (most recent before the target).
     */
    private int findPreviousMatch(CellValue targetValue, JfrTable rightTable, List<Integer> sortedIndices) {
        int bestIndex = -1;
        double targetNumeric = getNumericValue(targetValue);
        
        for (int index : sortedIndices) {
            CellValue rightValue = getJoinFieldValue(rightTable, index, joinField);
            if (rightValue != null) {
                double rightNumeric = getNumericValue(rightValue);
                
                if (rightNumeric <= targetNumeric && isWithinTolerance(targetNumeric - rightNumeric)) {
                    bestIndex = index; // Keep the latest (closest to target)
                }
            }
        }
        
        return bestIndex;
    }
    
    /**
     * Find the after match (earliest after the target).
     */
    private int findAfterMatch(CellValue targetValue, JfrTable rightTable, List<Integer> sortedIndices) {
        double targetNumeric = getNumericValue(targetValue);
        
        for (int index : sortedIndices) {
            CellValue rightValue = getJoinFieldValue(rightTable, index, joinField);
            if (rightValue != null) {
                double rightNumeric = getNumericValue(rightValue);
                
                if (rightNumeric >= targetNumeric && isWithinTolerance(rightNumeric - targetNumeric)) {
                    return index; // Return the first (earliest) match
                }
            }
        }
        
        return -1;
    }
    
    /**
     * Check if a distance is within the configured tolerance.
     */
    private boolean isWithinTolerance(double distance) {
        if (tolerance == null) return true;
        return distance <= getNumericValue(tolerance);
    }
    
    /**
     * Get the numeric value of a CellValue for comparison.
     */
    private double getNumericValue(CellValue value) {
        if (value instanceof CellValue.NumberValue num) {
            return num.value();
        } else if (value instanceof CellValue.TimestampValue ts) {
            return ts.value().toEpochMilli();
        } else if (value instanceof CellValue.DurationValue dur) {
            return dur.value().toNanos();
        }
        return 0.0;
    }
    
    /**
     * Compare two CellValues for sorting.
     */
    private int compareValues(CellValue v1, CellValue v2) {
        return Double.compare(getNumericValue(v1), getNumericValue(v2));
    }
    
    /**
     * Create the schema for the joined table.
     */
    private List<JfrTable.Column> createJoinedSchema(JfrTable leftTable, JfrTable rightTable) {
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
     */
    private CellValue getJoinFieldValue(JfrTable table, int rowIndex, String fieldName) {
        try {
            int columnIndex = table.getColumnIndex(fieldName);
            return table.getCell(rowIndex, columnIndex);
        } catch (Exception e) {
            return null;
        }
    }
    
    /**
     * Create a joined row by combining values from left and right tables.
     */
    private List<CellValue> createJoinedRow(JfrTable leftTable, int leftRowIndex,
                                          JfrTable rightTable, int rightRowIndex,
                                          List<JfrTable.Column> joinedColumns) {
        List<CellValue> row = new ArrayList<>();
        
        // Add values from left table
        for (int i = 0; i < leftTable.getColumnCount(); i++) {
            if (leftRowIndex >= 0) {
                row.add(leftTable.getCell(leftRowIndex, i));
            } else {
                row.add(new CellValue.NullValue());
            }
        }
        
        // Add values from right table
        for (int i = 0; i < rightTable.getColumnCount(); i++) {
            if (rightRowIndex >= 0) {
                row.add(rightTable.getCell(rightRowIndex, i));
            } else {
                row.add(new CellValue.NullValue());
            }
        }
        
        return row;
    }
    
    @Override
    public String explain() {
        return String.format("FuzzyJoinPlan[%s] (tolerance=%s, threshold=%s)", 
            joinType.name(), tolerance, threshold);
    }
    
    @Override
    public boolean supportsStreaming() {
        return false; // Fuzzy joins need to sort data
    }
}
