package me.bechberger.jfr.extended.plan.plans;

import me.bechberger.jfr.extended.ast.ASTNodes.*;
import me.bechberger.jfr.extended.plan.*;
import me.bechberger.jfr.extended.plan.exception.PlanException;
import me.bechberger.jfr.extended.plan.exception.PlanExceptionFactory;
import me.bechberger.jfr.extended.table.JfrTable;
import me.bechberger.jfr.extended.table.StandardJfrTable;
import me.bechberger.jfr.extended.table.CellValue;

import java.util.List;
import java.util.ArrayList;

/**
 * Streaming plan for filtering data with WHERE conditions.
 * 
 * This plan implements conditional filtering over streaming data,
 * applying WHERE clause conditions to filter rows from the input stream.
 * It operates on the output of ScanPlan or other streaming plans.
 * 
 * @author JFR Query Plan Architecture
 * @since 2.0
 */
public class FilterPlan extends AbstractStreamingPlan {
    
    private final StreamingQueryPlan inputPlan;
    private final ConditionNode condition;
    
    /**
     * Creates a filter plan for applying WHERE conditions.
     * 
     * @param whereNode the WHERE clause AST node
     * @param inputPlan the streaming plan that provides input data
     */
    public FilterPlan(WhereNode whereNode, StreamingQueryPlan inputPlan) {
        super(whereNode);
        this.condition = whereNode.condition();
        this.inputPlan = inputPlan;
    }
    
    @Override
    public QueryResult execute(QueryExecutionContext context) throws PlanExecutionException {
        context.recordPlanStart("FilterPlan", explain());
        try {
            // 1. Execute the input plan to get the data source
            QueryResult inputResult = inputPlan.execute(context);
            if (!inputResult.isSuccess()) {
                context.recordPlanFailure("FilterPlan", explain(), "Input plan failed: " + inputResult.getError());
                return inputResult; // Propagate errors from input plan
            }
            
            JfrTable sourceData = inputResult.getTable();
            
            // 2. If no condition, return the source data unchanged
            if (condition == null) {
                return inputResult;
            }
            
            // 3. Create a streaming filtered table that evaluates lazily
            JfrTable streamingFilteredTable = new StreamingFilteredTable(
                sourceData, condition, context);
            
            context.recordPlanSuccess("FilterPlan", explain(), sourceData.getRowCount());
            return QueryResult.success(streamingFilteredTable);
            
        } catch (Exception e) {
            context.recordPlanFailure("FilterPlan", explain(), e.getMessage());
            if (e instanceof PlanExecutionException) {
                throw e;
            }
            
            // Wrap the exception with plan context
            PlanException planException = PlanExceptionFactory.createFilterError(
                condition != null ? condition.toString() : "unknown", e, this);
            return QueryResult.failure(new PlanExecutionException(
                planException.getMessage(), planException.getErrorContext(), planException));
        }
    }
    
    /**
     * Streaming table implementation that filters rows on-demand.
     * This provides true streaming behavior without materializing all filtered rows.
     */
    private class StreamingFilteredTable extends StandardJfrTable {
        private final JfrTable sourceTable;
        private final ConditionNode filterCondition;
        private final QueryExecutionContext executionContext;
        private List<Row> cachedRows = null;
        
        public StreamingFilteredTable(JfrTable sourceTable, ConditionNode filterCondition, 
                                    QueryExecutionContext executionContext) {
            super(sourceTable.getColumns());
            this.sourceTable = sourceTable;
            this.filterCondition = filterCondition;
            this.executionContext = executionContext;
        }
        
        @Override
        public List<Row> getRows() {
            if (cachedRows == null) {
                cachedRows = new StreamingFilteredRowList();
            }
            return cachedRows;
        }
        
        @Override
        public int getRowCount() {
            // Optimize: if already materialized, return cached size
            if (cachedRows != null && cachedRows instanceof StreamingFilteredRowList) {
                StreamingFilteredRowList streamingList = (StreamingFilteredRowList) cachedRows;
                if (streamingList.materialized) {
                    System.err.println("DEBUG StreamingFilteredTable: Returning cached row count: " + streamingList.size());
                    return streamingList.size();
                }
            }
            
            // For unmaterialized data, do a lightweight count-only pass
            // This is unavoidable but at least doesn't store the filtered rows
            int count = 0;
            System.err.println("DEBUG StreamingFilteredTable: Performing count-only evaluation");
            for (Row row : sourceTable.getRows()) {
                if (evaluateConditionForRow(row, filterCondition, sourceTable.getColumns(), executionContext)) {
                    count++;
                }
            }
            System.err.println("DEBUG StreamingFilteredTable: Count-only result: " + count);
            return count;
        }
        
        /**
         * Truly streaming list that only materializes when specifically requested via size() or get().
         * The iterator() method provides streaming behavior without full materialization.
         */
        private class StreamingFilteredRowList extends ArrayList<Row> {
            private boolean materialized = false;
            
            @Override
            public int size() {
                // Only materialize for explicit size requests (not for streaming operations)
                if (!materialized) {
                    System.err.println("DEBUG StreamingFilteredRowList: size() called - materializing for count");
                    materializeIfNeeded();
                }
                return super.size();
            }
            
            @Override
            public Row get(int index) {
                // Only materialize for explicit indexed access
                if (!materialized) {
                    System.err.println("DEBUG StreamingFilteredRowList: get(" + index + ") called - materializing for indexed access");
                    materializeIfNeeded();
                }
                return super.get(index);
            }
            
            @Override
            public java.util.Iterator<Row> iterator() {
                // Always return a streaming iterator for true streaming behavior
                // This is the key optimization - iterator() does NOT trigger materialization
                return new StreamingFilterIterator();
            }
            
            @Override
            public java.util.stream.Stream<Row> stream() {
                // Create a stream that filters lazily without materialization
                return sourceTable.getRows().stream()
                    .filter(row -> evaluateConditionForRow(row, filterCondition, 
                        sourceTable.getColumns(), executionContext));
            }
            
            private void materializeIfNeeded() {
                if (!materialized) {
                    super.clear();
                    System.err.println("DEBUG StreamingFilteredRowList: Starting full materialization");
                    int added = 0;
                    for (Row row : sourceTable.getRows()) {
                        if (evaluateConditionForRow(row, filterCondition, 
                            sourceTable.getColumns(), executionContext)) {
                            super.add(row);
                            added++;
                        }
                    }
                    materialized = true;
                    System.err.println("DEBUG StreamingFilteredRowList: Materialization complete, " + added + " rows added");
                    
                    // Update context with rows processed
                    executionContext.recordRowsProcessed(sourceTable.getRowCount());
                }
            }
        }
        
        /**
         * Iterator that filters rows on-demand during iteration.
         */
        private class StreamingFilterIterator implements java.util.Iterator<Row> {
            private final java.util.Iterator<Row> sourceIterator;
            private Row nextRow = null;
            private boolean hasNextComputed = false;
            
            public StreamingFilterIterator() {
                this.sourceIterator = sourceTable.getRows().iterator();
            }
            
            @Override
            public boolean hasNext() {
                if (!hasNextComputed) {
                    computeNext();
                }
                return nextRow != null;
            }
            
            @Override
            public Row next() {
                if (!hasNext()) {
                    System.out.println("DEBUG StreamingFilterIterator: No more rows to return");
                    throw new java.util.NoSuchElementException();
                }
                Row result = nextRow;
                nextRow = null;
                hasNextComputed = false;
                System.out.println("DEBUG StreamingFilterIterator: Returning next row: " + result);
                return result;
            }
            
            private void computeNext() {
                while (sourceIterator.hasNext()) {
                    Row candidate = sourceIterator.next();
                    if (evaluateConditionForRow(candidate, filterCondition, 
                        sourceTable.getColumns(), executionContext)) {
                        nextRow = candidate;
                        hasNextComputed = true;
                        return;
                    }
                }
                nextRow = null;
                hasNextComputed = true;
            }
        }
    }
    
    /**
     * Evaluates a condition in the context of a specific row.
     * This implements the same logic as QueryEvaluator.evaluateConditionForRow
     * but adapted for the streaming plan architecture.
     */
    private boolean evaluateConditionForRow(JfrTable.Row row, ConditionNode condition, 
                                          java.util.List<JfrTable.Column> columns, 
                                          QueryExecutionContext context) {
        if (condition == null) {
            return true;
        }
        
        try {
            if (condition instanceof ExpressionConditionNode) {
                ExpressionConditionNode exprCondition = (ExpressionConditionNode) condition;
                System.err.println("DEBUG FilterPlan: Starting condition evaluation for row");
                CellValue result = evaluateExpressionInRowContext(exprCondition.expression(), row, columns, context);
                System.err.println("DEBUG FilterPlan: Condition evaluation completed, result: " + result);
                
                // Convert CellValue result to boolean
                if (result instanceof CellValue.BooleanValue booleanValue) {
                    boolean finalResult = booleanValue.value();
                    System.err.println("DEBUG FilterPlan: Final boolean result: " + finalResult);
                    return finalResult;
                } else if (result instanceof CellValue.NumberValue numberValue) {
                    boolean finalResult = numberValue.value() != 0.0;
                    System.err.println("DEBUG FilterPlan: Final boolean result from number: " + finalResult);
                    return finalResult;
                } else if (result instanceof CellValue.NumberValue floatValue) {
                    boolean finalResult = floatValue.value() != 0.0;
                    System.err.println("DEBUG FilterPlan: Final boolean result from float: " + finalResult);
                    return finalResult;
                } else if (result instanceof CellValue.NullValue) {
                    System.err.println("DEBUG FilterPlan: Final boolean result from null: false");
                    return false; // Null is falsy
                } else {
                    System.err.println("DEBUG FilterPlan: Final boolean result from other: true");
                    return true; // Non-null values are truthy
                }
            }
            
            // For other condition types, treat as true for now
            // A more complete implementation would handle ComparisonConditionNode, etc.
            return true;
            
        } catch (Exception e) {
            // If evaluation fails, exclude the row but log for debugging
            System.err.println("Warning: Filter condition evaluation failed for row: " + e.getMessage());
            e.printStackTrace();
            return false;
        }
    }
    
    /**
     * Evaluates an expression in the context of a specific row.
     * Delegates all expression evaluation to the comprehensive PlanExpressionEvaluator.
     */
    private CellValue evaluateExpressionInRowContext(ExpressionNode expression, JfrTable.Row row, 
                                                java.util.List<JfrTable.Column> columns,
                                                QueryExecutionContext context) {
        System.err.println("DEBUG FilterPlan.evaluateExpressionInRowContext: Expression type: " + expression.getClass().getSimpleName());
        
        try {
            // Use the optimized row context evaluation without table creation overhead
            me.bechberger.jfr.extended.plan.evaluator.PlanExpressionEvaluator evaluator = 
                new me.bechberger.jfr.extended.plan.evaluator.PlanExpressionEvaluator(context);
            CellValue result = evaluator.evaluateExpressionWithRowContext(expression, row, columns);
            System.err.println("DEBUG FilterPlan: PlanExpressionEvaluator result: " + result + " (type: " + result.getClass().getSimpleName() + ")");
            return result;
        } catch (Exception e) {
            // If evaluation fails, return false to be conservative in filtering
            System.err.println("DEBUG FilterPlan: Expression evaluation failed: " + e.getMessage());
            e.printStackTrace();
            return CellValue.of(false);
        }
    }
    
    @Override
    public String explain() {
        StringBuilder sb = new StringBuilder();
        sb.append("FilterPlan:\n");
        sb.append("  Condition: ").append(condition != null ? condition.toString() : "NONE").append("\n");
        sb.append("  Input Plan:\n");
        String inputExplanation = inputPlan.explain();
        for (String line : inputExplanation.split("\n")) {
            sb.append("    ").append(line).append("\n");
        }
        
        return sb.toString();
    }
    
    /**
     * Get the input plan that provides data to filter.
     */
    public StreamingQueryPlan getInputPlan() {
        return inputPlan;
    }
    
    /**
     * Get the filtering condition.
     */
    public ConditionNode getCondition() {
        return condition;
    }
}
