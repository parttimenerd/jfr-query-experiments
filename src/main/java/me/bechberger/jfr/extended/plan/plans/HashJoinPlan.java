package me.bechberger.jfr.extended.plan.plans;

import me.bechberger.jfr.extended.ast.ASTNode;
import me.bechberger.jfr.extended.ast.ASTNodes.StandardJoinType;
import me.bechberger.jfr.extended.plan.QueryExecutionContext;
import me.bechberger.jfr.extended.table.JfrTable;
import me.bechberger.jfr.extended.table.CellValue;
import me.bechberger.jfr.extended.table.StandardJfrTable;

import java.util.*;

/**
 * Hash-based JOIN implementation for efficient JOIN operations.
 * 
 * This implementation uses a hash table to build an index on the smaller table's
 * join key, then probes with the larger table. It supports all standard JOIN types:
 * INNER, LEFT OUTER, RIGHT OUTER, and FULL OUTER.
 * 
 * @author JFR Query Plan Architecture
 * @since 3.0
 */
public class HashJoinPlan extends JoinPlan {
    
    /**
     * Create a new hash JOIN plan.
     * 
     * @param sourceNode The AST node this plan was created from
     * @param leftPlan The left side of the join
     * @param rightPlan The right side of the join
     * @param joinType The type of join (INNER, LEFT, RIGHT, FULL)
     * @param leftJoinField The field name from the left table to join on
     * @param rightJoinField The field name from the right table to join on
     */
    public HashJoinPlan(ASTNode sourceNode, StreamingQueryPlan leftPlan, StreamingQueryPlan rightPlan,
                       StandardJoinType joinType, String leftJoinField, String rightJoinField) {
        super(sourceNode, leftPlan, rightPlan, joinType, leftJoinField, rightJoinField);
    }
    
    @Override
    protected JfrTable performJoin(JfrTable leftTable, JfrTable rightTable, QueryExecutionContext context) {
        // Create the joined table schema
        List<JfrTable.Column> joinedColumns = createJoinedSchema(leftTable, rightTable);
        StandardJfrTable result = new StandardJfrTable(joinedColumns);
        
        // Build hash index on the smaller table
        JfrTable buildTable, probeTable;
        String buildField, probeField;
        boolean leftIsBuild;
        
        if (leftTable.getRowCount() <= rightTable.getRowCount()) {
            buildTable = leftTable;
            probeTable = rightTable;
            buildField = leftJoinField;
            probeField = rightJoinField;
            leftIsBuild = true;
        } else {
            buildTable = rightTable;
            probeTable = leftTable;
            buildField = rightJoinField;
            probeField = leftJoinField;
            leftIsBuild = false;
        }
        
        // Build hash index
        Map<CellValue, List<Integer>> hashIndex = buildHashIndex(buildTable, buildField);
        
        // Track which rows from the build table were matched (for outer joins)
        Set<Integer> matchedBuildRows = new HashSet<>();
        
        // Probe phase
        for (int probeRowIndex = 0; probeRowIndex < probeTable.getRowCount(); probeRowIndex++) {
            CellValue probeValue = getJoinFieldValue(probeTable, probeRowIndex, probeField);
            List<Integer> matchingBuildRows = hashIndex.get(probeValue);
            
            if (matchingBuildRows != null && !matchingBuildRows.isEmpty()) {
                // Found matches
                for (int buildRowIndex : matchingBuildRows) {
                    matchedBuildRows.add(buildRowIndex);
                    
                    // Create joined row
                    List<CellValue> joinedRow;
                    if (leftIsBuild) {
                        joinedRow = createJoinedRow(buildTable, buildRowIndex, probeTable, probeRowIndex, joinedColumns);
                    } else {
                        joinedRow = createJoinedRow(probeTable, probeRowIndex, buildTable, buildRowIndex, joinedColumns);
                    }
                    
                    result.addRow(joinedRow.toArray(new CellValue[0]));
                }
            } else {
                // No matches found
                if (shouldIncludeUnmatchedProbeRow()) {
                    // Create joined row with nulls for build table
                    List<CellValue> joinedRow;
                    if (leftIsBuild) {
                        joinedRow = createJoinedRow(buildTable, -1, probeTable, probeRowIndex, joinedColumns);
                    } else {
                        joinedRow = createJoinedRow(probeTable, probeRowIndex, buildTable, -1, joinedColumns);
                    }
                    
                    result.addRow(joinedRow.toArray(new CellValue[0]));
                }
            }
        }
        
        // Handle unmatched build rows (for right outer and full outer joins)
        if (shouldIncludeUnmatchedBuildRows()) {
            for (int buildRowIndex = 0; buildRowIndex < buildTable.getRowCount(); buildRowIndex++) {
                if (!matchedBuildRows.contains(buildRowIndex)) {
                    // Create joined row with nulls for probe table
                    List<CellValue> joinedRow;
                    if (leftIsBuild) {
                        joinedRow = createJoinedRow(buildTable, buildRowIndex, probeTable, -1, joinedColumns);
                    } else {
                        joinedRow = createJoinedRow(probeTable, -1, buildTable, buildRowIndex, joinedColumns);
                    }
                    
                    result.addRow(joinedRow.toArray(new CellValue[0]));
                }
            }
        }
        
        return result;
    }
    
    /**
     * Build a hash index on the specified field of the given table.
     * In SQL semantics, null values are not indexed and cannot match.
     * 
     * @param table The table to index
     * @param fieldName The field to index on
     * @return A map from field values to lists of row indices
     */
    private Map<CellValue, List<Integer>> buildHashIndex(JfrTable table, String fieldName) {
        Map<CellValue, List<Integer>> index = new HashMap<>();
        
        for (int rowIndex = 0; rowIndex < table.getRowCount(); rowIndex++) {
            CellValue fieldValue = getJoinFieldValue(table, rowIndex, fieldName);
            
            // Skip null values - they cannot match in SQL semantics
            if (fieldValue != null && !(fieldValue instanceof CellValue.NullValue)) {
                index.computeIfAbsent(fieldValue, k -> new ArrayList<>()).add(rowIndex);
            }
        }
        
        return index;
    }
    
    /**
     * Determine if unmatched probe rows should be included in the result.
     * This is true for LEFT and FULL outer joins when the left table is the probe table,
     * or for RIGHT and FULL outer joins when the right table is the probe table.
     */
    private boolean shouldIncludeUnmatchedProbeRow() {
        return switch (joinType) {
            case INNER -> false;
            case LEFT -> true;  // Always include left table rows
            case RIGHT -> true; // Always include right table rows
            case FULL -> true;  // Always include all rows
            case CROSS -> false; // CROSS JOIN does not include unmatched rows
        };
    }
    
    /**
     * Determine if unmatched build rows should be included in the result.
     * This is true for RIGHT and FULL outer joins when the right table is the build table,
     * or for LEFT and FULL outer joins when the left table is the build table.
     */
    private boolean shouldIncludeUnmatchedBuildRows() {
        return switch (joinType) {
            case INNER -> false;
            case LEFT -> true;  // Always include left table rows
            case RIGHT -> true; // Always include right table rows
            case FULL -> true;  // Always include all rows
            case CROSS -> false; // CROSS JOIN does not include unmatched rows
        };
    }
    
    @Override
    public String explain() {
        return String.format("HashJoinPlan[%s] (build index on smaller table)", joinType.name());
    }
    
    @Override
    protected String formatPlanName() {
        return String.format("HashJoinPlan[%s]", joinType.name());
    }
}
