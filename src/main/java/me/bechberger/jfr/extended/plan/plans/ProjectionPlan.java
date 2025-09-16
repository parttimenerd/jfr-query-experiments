package me.bechberger.jfr.extended.plan.plans;

import me.bechberger.jfr.extended.ast.ASTNodes.*;
import me.bechberger.jfr.extended.plan.*;
import me.bechberger.jfr.extended.plan.evaluator.PlanExpressionEvaluator;
import me.bechberger.jfr.extended.table.JfrTable;
import me.bechberger.jfr.extended.table.StandardJfrTable;
import me.bechberger.jfr.extended.table.CellType;
import me.bechberger.jfr.extended.table.CellValue;

import java.util.List;
import java.util.ArrayList;

/**
 * Streaming plan for handling SELECT projections.
 * 
 * This plan takes the output from a source plan and projects
 * the specified columns, potentially with expressions and aliases.
 * For "SELECT *" it passes through all columns unchanged.
 * 
 * @author JFR Query Plan Architecture
 * @since 2.0
 */
public class ProjectionPlan extends AbstractStreamingPlan {
    
    private final StreamingQueryPlan sourcePlan;
    private final List<SelectItemNode> selectFields;
    private final boolean isSelectAll;
    
    /**
     * Creates a projection plan for SELECT clause processing.
     * 
     * @param selectNode the SELECT AST node
     * @param sourcePlan the plan providing input data
     * @param selectFields the list of fields to select
     */
    public ProjectionPlan(SelectNode selectNode, StreamingQueryPlan sourcePlan, List<SelectItemNode> selectFields) {
        super(selectNode);
        this.sourcePlan = sourcePlan;
        this.selectFields = List.copyOf(selectFields);
        this.isSelectAll = isSelectStar(selectFields);
    }
    
    /**
     * Check if this represents a SELECT * query.
     */
    private boolean isSelectStar(List<SelectItemNode> selectFields) {
        if (selectFields.size() != 1) {
            return false;
        }
        
        SelectItemNode selectItem = selectFields.get(0);
        ExpressionNode expr = selectItem.expression();
        
        // Check for StarNode (the actual * node from the parser)
        if (expr instanceof StarNode) {
            return true;
        }
        
        // Check for IdentifierNode with name "*"
        if (expr instanceof IdentifierNode identifier) {
            return "*".equals(identifier.name());
        }
        
        // Check for FieldAccessNode with field "*"
        if (expr instanceof FieldAccessNode field) {
            return "*".equals(field.field());
        }
        
        return false;
    }
    
    @Override
    public QueryResult execute(QueryExecutionContext context) throws PlanExecutionException {
        try {
            // Execute the source plan first
            QueryResult sourceResult = sourcePlan.execute(context);
            if (!sourceResult.isSuccess()) {
                return sourceResult; // Propagate error
            }
            
            JfrTable sourceTable = sourceResult.getTable();
            
            // For SELECT *, just return the source table unchanged
            if (isSelectAll) {
                return QueryResult.success(sourceTable);
            }
            
            // For specific columns, project them
            return executeProjection(sourceTable, context);
            
        } catch (Exception e) {
            JFRErrorContext errorContext = createErrorContext("projection", 
                "Failed to project columns: " + e.getMessage());
            return QueryResult.failure(new PlanExecutionException(
                "Projection failed", errorContext, e));
        }
    }
    
    /**
     * Execute the actual column projection.
     */
    private QueryResult executeProjection(JfrTable sourceTable, QueryExecutionContext context) 
            throws PlanExecutionException {
        try {
            // For now, implement a simple projection for basic field access
            List<JfrTable.Column> projectedColumns = new ArrayList<>();
            
            for (int i = 0; i < selectFields.size(); i++) {
                SelectItemNode field = selectFields.get(i);
                String columnName = getColumnName(field, i); // 0-based column numbering
                CellType columnType = getColumnType(field, sourceTable);
                projectedColumns.add(new JfrTable.Column(columnName, columnType));
            }
            
            JfrTable projectedTable = new StandardJfrTable(projectedColumns);
            
            // Use streaming iteration instead of indexed access to preserve streaming behavior
            System.err.println("DEBUG ProjectionPlan: Using streaming iteration over source table");
            int rowCount = 0;
            for (JfrTable.Row sourceRow : sourceTable.getRows()) {
                List<CellValue> projectedCells = new ArrayList<>();
                
                for (SelectItemNode field : selectFields) {
                    CellValue cellValue = evaluateFieldForRow(field, sourceRow, sourceTable.getColumns(), context);
                    projectedCells.add(cellValue);
                }
                
                projectedTable.addRow(new JfrTable.Row(projectedCells));
                rowCount++;
            }
            System.err.println("DEBUG ProjectionPlan: Projected " + rowCount + " rows using streaming iteration");
            
            return QueryResult.success(projectedTable);
            
        } catch (Exception e) {
            throw new PlanExecutionException("Column projection failed: " + e.getMessage(),
                createErrorContext("projection", "Projection execution failed"), e);
        }
    }
    
    /**
     * Get the output column name for a select field.
     */
    private String getColumnName(SelectItemNode field, int columnNumber) {
        if (field.alias() != null) {
            return field.alias();
        }
        
        if (field.expression() instanceof FieldAccessNode fieldAccess) {
            return fieldAccess.field();
        }
        
        if (field.expression() instanceof IdentifierNode identifier) {
            return identifier.name();
        }

        // For expressions, use column number (0-based)
        return "$" + columnNumber;
    }    /**
     * Get the output column type for a select field.
     */
    private CellType getColumnType(SelectItemNode field, JfrTable sourceTable) {
        if (field.expression() instanceof FieldAccessNode fieldAccess) {
            String fieldName = fieldAccess.field();
            for (int i = 0; i < sourceTable.getColumnCount(); i++) {
                if (sourceTable.getColumnName(i).equals(fieldName)) {
                    return sourceTable.getColumnType(i);
                }
            }
        }
        
        // Default to STRING for expressions
        return CellType.STRING;
    }
    
    /**
     * Evaluate a select field for a specific row.
     */
    private CellValue evaluateField(SelectItemNode field, JfrTable sourceTable, int rowIndex, QueryExecutionContext executionContext) {
        ExpressionNode expression = field.expression();
        
        // Handle different expression types
        if (expression instanceof FieldAccessNode fieldAccess) {
            String fieldName = fieldAccess.field();
            
            // Find the column in source table
            for (int colIndex = 0; colIndex < sourceTable.getColumnCount(); colIndex++) {
                if (sourceTable.getColumnName(colIndex).equals(fieldName)) {
                    try {
                        return sourceTable.getCell(rowIndex, colIndex);
                    } catch (Exception e) {
                        return new CellValue.NullValue();
                    }
                }
            }
            return new CellValue.NullValue();
        } else if (expression instanceof LiteralNode literal) {
            // Return the literal value directly
            return literal.value();
        } else if (expression instanceof IdentifierNode identifier) {
            String name = identifier.name();
            
            // First check if it's a column name
            for (int colIndex = 0; colIndex < sourceTable.getColumnCount(); colIndex++) {
                if (sourceTable.getColumnName(colIndex).equals(name)) {
                    try {
                        return sourceTable.getCell(rowIndex, colIndex);
                    } catch (Exception e) {
                        return new CellValue.NullValue();
                    }
                }
            }
            
            // If not a column, check if it's a variable
            Object variableValue = executionContext.getVariable(name);
            if (variableValue != null) {
                return CellValue.of(variableValue);
            }
            
            // Check lazy variables
            if (executionContext.hasLazyVariable(name)) {
                try {
                    // Debug output
                    System.err.println("DEBUG: Found lazy variable: " + name);
                    
                    // Evaluate the lazy variable using the same logic as PlanExpressionEvaluator
                    var lazyQuery = executionContext.getLazyVariable(name);
                    QueryResult result = lazyQuery.evaluate();
                    
                    System.err.println("DEBUG: Lazy query result success: " + result.isSuccess());
                    if (result.isSuccess() && result.getTable() != null) {
                        System.err.println("DEBUG: Result table has " + result.getTable().getRowCount() + " rows and " + result.getTable().getColumnCount() + " columns");
                        if (result.getTable().getRowCount() > 0 && result.getTable().getColumnCount() > 0) {
                            var cell = result.getTable().getCell(0, 0);
                            System.err.println("DEBUG: First cell value: " + cell + " (type: " + cell.getType() + ")");
                        }
                    } else {
                        System.err.println("DEBUG: Query failed or returned no table: " + (result.getError() != null ? result.getError().getMessage() : "no error"));
                    }
                    
                    // If the lazy query returns a single-cell result, extract the value
                    if (result.isSuccess() && result.getTable() != null && 
                        result.getTable().getRowCount() == 1 && result.getTable().getColumnCount() == 1) {
                        return result.getTable().getCell(0, 0);
                    }
                    
                    // If it's a multi-cell result, return the first cell of the first row
                    if (result.isSuccess() && result.getTable() != null && result.getTable().getRowCount() > 0) {
                        return result.getTable().getCell(0, 0);
                    }
                    
                    // If execution failed or returned no data
                    if (!result.isSuccess()) {
                        return new CellValue.StringValue("LAZY_ERROR: " + result.getError().getMessage());
                    }
                    
                    return new CellValue.NullValue();
                } catch (Exception e) {
                    System.err.println("DEBUG: Exception during lazy evaluation: " + e.getMessage());
                    e.printStackTrace();
                    return new CellValue.StringValue("LAZY_EVAL_ERROR: " + e.getMessage());
                }
            } else {
                System.err.println("DEBUG: No lazy variable found for: " + name);
            }
            
            return new CellValue.NullValue();
        } else {
            // For complex expressions, use the comprehensive plan expression evaluator
            try {
                PlanExpressionEvaluator planEvaluator = 
                    new PlanExpressionEvaluator(executionContext);
                return planEvaluator.evaluateExpression(expression, sourceTable, rowIndex);
            } catch (Exception e) {
                // If expression evaluation fails, return a descriptive error
                return new CellValue.StringValue("EXPR_ERROR: " + e.getMessage());
            }
        }
    }
    
    /**
     * Evaluate a select field for a specific row using streaming row access.
     * This is the streaming-optimized version that works with JfrTable.Row objects.
     */
    private CellValue evaluateFieldForRow(SelectItemNode field, JfrTable.Row row, 
                                         java.util.List<JfrTable.Column> columns, 
                                         QueryExecutionContext executionContext) {
        ExpressionNode expression = field.expression();
        
        // Handle different expression types
        if (expression instanceof FieldAccessNode fieldAccess) {
            String fieldName = fieldAccess.field();
            
            // Find the column in the column list
            for (int colIndex = 0; colIndex < columns.size(); colIndex++) {
                if (columns.get(colIndex).name().equals(fieldName)) {
                    try {
                        return row.getCells().get(colIndex);
                    } catch (Exception e) {
                        return new CellValue.NullValue();
                    }
                }
            }
            return new CellValue.NullValue();
        } else if (expression instanceof LiteralNode literal) {
            // Return the literal value directly
            return literal.value();
        } else if (expression instanceof IdentifierNode identifier) {
            String name = identifier.name();
            
            // First check if it's a column name
            for (int colIndex = 0; colIndex < columns.size(); colIndex++) {
                if (columns.get(colIndex).name().equals(name)) {
                    try {
                        return row.getCells().get(colIndex);
                    } catch (Exception e) {
                        return new CellValue.NullValue();
                    }
                }
            }
            
            // If not a column, check if it's a variable
            Object variableValue = executionContext.getVariable(name);
            if (variableValue != null) {
                return CellValue.of(variableValue);
            }
            
            // Check lazy variables
            if (executionContext.hasLazyVariable(name)) {
                try {
                    var lazyQuery = executionContext.getLazyVariable(name);
                    QueryResult result = lazyQuery.evaluate();
                    
                    // If the lazy query returns a single-cell result, extract the value
                    if (result.isSuccess() && result.getTable() != null && 
                        result.getTable().getRowCount() == 1 && result.getTable().getColumnCount() == 1) {
                        return result.getTable().getCell(0, 0);
                    }
                    
                    // If it's a multi-cell result, return the first cell of the first row
                    if (result.isSuccess() && result.getTable() != null && result.getTable().getRowCount() > 0) {
                        return result.getTable().getCell(0, 0);
                    }
                    
                    // If execution failed or returned no data
                    if (!result.isSuccess()) {
                        return new CellValue.StringValue("LAZY_ERROR: " + result.getError().getMessage());
                    }
                    
                    return new CellValue.NullValue();
                } catch (Exception e) {
                    return new CellValue.StringValue("LAZY_EVAL_ERROR: " + e.getMessage());
                }
            }
            
            return new CellValue.NullValue();
        } else {
            // For complex expressions, use the comprehensive plan expression evaluator with row context
            try {
                PlanExpressionEvaluator planEvaluator = 
                    new PlanExpressionEvaluator(executionContext);
                return planEvaluator.evaluateExpressionWithRowContext(expression, row, columns);
            } catch (Exception e) {
                // If expression evaluation fails, return a descriptive error
                return new CellValue.StringValue("EXPR_ERROR: " + e.getMessage());
            }
        }
    }
    
    @Override
    public String explain() {
        StringBuilder sb = new StringBuilder();
        sb.append("ProjectionPlan:\n");
        sb.append("  Type: ").append(isSelectAll ? "SELECT *" : "Column Projection").append("\n");
        if (!isSelectAll) {
            sb.append("  Columns: ");
            for (int i = 0; i < selectFields.size(); i++) {
                if (i > 0) sb.append(", ");
                sb.append(getColumnName(selectFields.get(i), i));
            }
            sb.append("\n");
        }
        sb.append("  Source Plan:\n");
        String sourcePlanExplain = sourcePlan.explain();
        for (String line : sourcePlanExplain.split("\n")) {
            sb.append("    ").append(line).append("\n");
        }
        
        return sb.toString();
    }
    
    @Override
    public SelectNode getSourceNode() {
        return (SelectNode) sourceNode;
    }
    
    /**
     * Get the source plan.
     */
    public StreamingQueryPlan getSourcePlan() {
        return sourcePlan;
    }
    
    /**
     * Get the select fields.
     */
    public List<SelectItemNode> getSelectFields() {
        return selectFields;
    }
    
    /**
     * Check if this is a SELECT * projection.
     */
    public boolean isSelectAll() {
        return isSelectAll;
    }
}
