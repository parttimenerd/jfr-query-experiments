package me.bechberger.jfr.extended.plan.plans;

import me.bechberger.jfr.extended.ast.ASTNode;
import me.bechberger.jfr.extended.ast.ASTNodes.ConditionNode;
import me.bechberger.jfr.extended.ast.ASTNodes.ExpressionConditionNode;
import me.bechberger.jfr.extended.ast.ASTNodes.HavingNode;
import me.bechberger.jfr.extended.ast.ASTNodes.FunctionCallNode;
import me.bechberger.jfr.extended.ast.ASTNodes.StarNode;
import me.bechberger.jfr.extended.engine.QueryEvaluatorUtils;
import me.bechberger.jfr.extended.plan.PlanExecutionException;
import me.bechberger.jfr.extended.plan.QueryExecutionContext;
import me.bechberger.jfr.extended.plan.QueryResult;
import me.bechberger.jfr.extended.plan.evaluator.ExpressionHandler;
import me.bechberger.jfr.extended.table.JfrTable;
import me.bechberger.jfr.extended.table.StandardJfrTable;
import me.bechberger.jfr.extended.plan.evaluator.PlanExpressionEvaluator;
import me.bechberger.jfr.extended.table.CellValue;

import java.util.*;

/**
 * Plan for executing HAVING clauses that filter grouped results.
 * 
 * The HAVING clause is similar to WHERE but operates on aggregated data
 * after GROUP BY has been applied. It can reference aggregate functions
 * like COUNT, SUM, AVG, etc.
 * 
 * @author JFR Query Plan Architecture
 * @since 3.0
 */
public class HavingPlan extends AbstractStreamingPlan {
    
    private final StreamingQueryPlan childPlan;
    private final ConditionNode condition;
    
    /**
     * Create a new HAVING plan.
     * 
     * @param sourceNode The AST node this plan was created from
     * @param childPlan The child plan that produces the grouped data
     * @param condition The HAVING condition to evaluate
     */
    public HavingPlan(ASTNode sourceNode, StreamingQueryPlan childPlan, ConditionNode condition) {
        super(sourceNode);
        this.childPlan = childPlan;
        this.condition = condition;
    }
    
    /**
     * Convenience constructor that extracts condition from HavingNode.
     * 
     * @param havingNode The HAVING AST node
     * @param childPlan The child plan that produces the grouped data
     */
    public HavingPlan(HavingNode havingNode, StreamingQueryPlan childPlan) {
        this(havingNode, childPlan, havingNode.condition());
    }
    
    @Override
    public QueryResult execute(QueryExecutionContext context) throws PlanExecutionException {
        context.recordPlanStart("HavingPlan", explain());
        try {
            // Execute the child plan to get the grouped data
            QueryResult childResult = childPlan.execute(context);
            
            if (!childResult.isSuccess()) {
                context.recordPlanFailure("HavingPlan", explain(), "Child plan failed: " + childResult.getError());
                return childResult;
            }
            
            JfrTable childTable = childResult.getTable();
            
            // Apply HAVING filter to each row
            JfrTable filteredTable = applyHavingFilter(childTable, context);
            
            context.recordPlanSuccess("HavingPlan", explain(), filteredTable.getRowCount());
            return QueryResult.success(filteredTable);
            
        } catch (Exception e) {
            context.recordPlanFailure("HavingPlan", explain(), e.getMessage());
            throw new PlanExecutionException("HAVING clause execution failed: " + e.getMessage(), e);
        }
    }
    
    /**
     * Apply the HAVING filter to the grouped data.
     * 
     * @param table The table with grouped data
     * @param context The execution context
     * @return The filtered table
     */
    private JfrTable applyHavingFilter(JfrTable table, QueryExecutionContext context) {
        System.err.println("DEBUG HavingPlan: Applying HAVING filter to " + table.getRowCount() + " rows");
        System.err.println("DEBUG HavingPlan: Condition: " + condition.format());
        
        // Create result table with same schema
        StandardJfrTable result = new StandardJfrTable(table.getColumns());
        
        // Evaluate condition for each row
        for (int rowIndex = 0; rowIndex < table.getRowCount(); rowIndex++) {
            JfrTable.Row row = table.getRows().get(rowIndex);
            
            // Debug: show row data
            System.err.println("DEBUG HavingPlan: Evaluating row " + rowIndex + " with values: " + row.getCells());
            
            // Evaluate the HAVING condition for this row
            boolean conditionMet = evaluateCondition(condition, row, table.getColumns(), context);
            
            System.err.println("DEBUG HavingPlan: Row " + rowIndex + " condition result: " + conditionMet);
            
            if (conditionMet) {
                result.addRow(row);
            }
        }
        
        System.err.println("DEBUG HavingPlan: Filtered result has " + result.getRowCount() + " rows");
        return result;
    }
    
    /**
     * Evaluate a condition against a row.
     * 
     * @param condition The condition to evaluate
     * @param row The row to evaluate against
     * @param columns The table columns
     * @param context The execution context
     * @return true if the condition is met
     */
    private boolean evaluateCondition(ConditionNode condition, JfrTable.Row row, 
                                    List<JfrTable.Column> columns, QueryExecutionContext context) {
        if (condition == null) {
            System.err.println("DEBUG HavingPlan: Condition is null, returning true");
            return true;
        }
        
        try {
            System.err.println("DEBUG HavingPlan: Evaluating condition of type: " + condition.getClass().getSimpleName());
            
            if (condition instanceof ExpressionConditionNode) {
                ExpressionConditionNode exprCondition = (ExpressionConditionNode) condition;
                
                System.err.println("DEBUG HavingPlan: Expression condition: " + exprCondition.expression().format());
                
                // Create a special handler for aggregate functions in HAVING clauses
                // In HAVING context, aggregate functions should reference the computed values from GROUP BY
                ExpressionHandler aggregateHandler = (expr, table, rowIndex, evaluator) -> {
                    System.err.println("DEBUG HavingPlan: Handler called for expression: " + expr.format());
                    
                    // Handle direct aggregate function calls by looking for matching computed columns
                    if (expr instanceof FunctionCallNode functionCall && 
                        QueryEvaluatorUtils.isAggregateFunction(functionCall.functionName())) {
                        
                        System.err.println("DEBUG HavingPlan: Handling aggregate function: " + functionCall.functionName());
                        
                        // Try to find a matching column name for this aggregate
                        String targetColumnName = null;
                        
                        // Check for COUNT(*) specifically
                        if ("COUNT".equalsIgnoreCase(functionCall.functionName()) && 
                            functionCall.arguments().size() == 1 && 
                            functionCall.arguments().get(0) instanceof StarNode) {
                            targetColumnName = "COUNT(*)";
                        } else {
                            // For other aggregates, construct the likely column name
                            targetColumnName = functionCall.functionName().toUpperCase() + "(...)";
                        }
                        
                        System.err.println("DEBUG HavingPlan: Looking for column: " + targetColumnName);
                        
                        // Debug: list all column names
                        System.err.println("DEBUG HavingPlan: Available columns:");
                        for (int i = 0; i < columns.size(); i++) {
                            JfrTable.Column column = columns.get(i);
                            System.err.println("DEBUG HavingPlan:   Column " + i + ": '" + column.name() + "'");
                        }
                        
                        // Look for the column in the current row
                        for (int i = 0; i < columns.size(); i++) {
                            JfrTable.Column column = columns.get(i);
                            System.err.println("DEBUG HavingPlan: Checking column " + i + ": " + column.name());
                            
                            if (column.name().equals(targetColumnName)) {
                                CellValue columnValue = row.getCells().get(i);
                                System.err.println("DEBUG HavingPlan: Found matching column with value: " + columnValue);
                                return columnValue;
                            }
                        }
                        
                        System.err.println("DEBUG HavingPlan: No matching column found, returning 0");
                        // If no matching column found, return 0 (this shouldn't happen in well-formed queries)
                        return new CellValue.NumberValue(0L);
                    }
                    
                    // Return null to let the evaluator handle all other expressions normally
                    return null;
                };
                
                // Use PlanExpressionEvaluator with aggregate handler to evaluate the expression
                PlanExpressionEvaluator evaluator = new PlanExpressionEvaluator(context, aggregateHandler);
                CellValue result = evaluator.evaluateExpressionWithRowContext(exprCondition.expression(), row, columns);
                
                System.err.println("DEBUG HavingPlan: Expression evaluation result: " + result + " (type: " + result.getClass().getSimpleName() + ")");
                
                // Convert CellValue result to boolean following FilterPlan pattern
                if (result instanceof CellValue.BooleanValue booleanValue) {
                    boolean boolResult = booleanValue.value();
                    System.err.println("DEBUG HavingPlan: Boolean result: " + boolResult);
                    return boolResult;
                } else if (result instanceof CellValue.NumberValue numberValue) {
                    boolean numberResult = numberValue.value() != 0.0;
                    System.err.println("DEBUG HavingPlan: Number result: " + numberValue.value() + " -> " + numberResult);
                    return numberResult;
                } else if (result instanceof CellValue.NullValue) {
                    System.err.println("DEBUG HavingPlan: Null result -> false");
                    return false; // Null is falsy
                } else {
                    System.err.println("DEBUG HavingPlan: Other result -> true");
                    return true; // Non-null values are truthy
                }
            }
            
            // For other condition types, treat as true for now
            // A more complete implementation would handle ComparisonConditionNode, etc.
            System.err.println("DEBUG HavingPlan: Non-expression condition, returning true");
            return true;
            
        } catch (Exception e) {
            // If evaluation fails, assume condition is false
            System.err.println("DEBUG HavingPlan: Exception during condition evaluation: " + e.getMessage());
            e.printStackTrace();
            return false;
        }
    }
    
    @Override
    public String explain() {
        return String.format("HavingPlan(condition: %s)", condition.format());
    }
    
    @Override
    public boolean supportsStreaming() {
        // HAVING typically operates on grouped data, which is already materialized
        return true;
    }
    
    /**
     * Get the child plan.
     * 
     * @return The child plan
     */
    public StreamingQueryPlan getChildPlan() {
        return childPlan;
    }
    
    /**
     * Get the HAVING condition.
     * 
     * @return The condition node
     */
    public ConditionNode getCondition() {
        return condition;
    }
}
