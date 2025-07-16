package me.bechberger.jfr.extended.engine.exception;

import me.bechberger.jfr.extended.ast.ASTNode;
import me.bechberger.jfr.extended.table.CellValue;
import me.bechberger.jfr.extended.table.JfrTable;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Exception thrown when aggregate function evaluation fails during query execution.
 * 
 * This exception provides detailed information about the aggregation that failed,
 * the data being aggregated, and the specific context of the failure.
 * 
 * @author Query Engine Team
 * @since 1.0
 */
public class AggregationEvaluationException extends QueryEvaluationException {
    
    private final String aggregateFunction;
    private final List<String> groupByFields;
    private final int groupSize;
    private final AggregationContext aggregationContext;
    private final String dataDescription;
    
    /**
     * Enumeration of different aggregation contexts.
     */
    public enum AggregationContext {
        GROUP_BY("GROUP BY clause"),
        HAVING("HAVING clause"),
        SELECT_AGGREGATE("SELECT clause aggregate"),
        WINDOW_FUNCTION("window function"),
        DISTINCT_AGGREGATE("DISTINCT aggregate"),
        NESTED_AGGREGATE("nested aggregate");
        
        private final String description;
        
        AggregationContext(String description) {
            this.description = description;
        }
        
        public String getDescription() {
            return description;
        }
    }
    
    /**
     * Creates a new AggregationEvaluationException with detailed information.
     * 
     * @param aggregateFunction The aggregate function that failed
     * @param groupByFields The fields used for grouping (can be empty)
     * @param groupSize The size of the group being aggregated
     * @param context The context in which aggregation was performed
     * @param dataDescription Description of the data being aggregated
     * @param errorNode The AST node where the error occurred
     * @param cause The underlying cause of the error
     */
    public AggregationEvaluationException(String aggregateFunction, List<String> groupByFields,
                                        int groupSize, AggregationContext context,
                                        String dataDescription, ASTNode errorNode, Throwable cause) {
        super(
            buildEvaluationContext(aggregateFunction, context, groupSize),
            buildProblematicValue(groupByFields, dataDescription),
            determineErrorType(cause),
            errorNode,
            cause
        );
        this.aggregateFunction = aggregateFunction;
        this.groupByFields = groupByFields != null ? List.copyOf(groupByFields) : List.of();
        this.groupSize = groupSize;
        this.aggregationContext = context;
        this.dataDescription = dataDescription;
    }
    
    /**
     * Creates an AggregationEvaluationException for empty group errors.
     */
    public static AggregationEvaluationException forEmptyGroup(String aggregateFunction,
                                                              List<String> groupByFields,
                                                              ASTNode errorNode) {
        return new AggregationEvaluationException(
            aggregateFunction, groupByFields, 0, AggregationContext.GROUP_BY,
            "empty group - no rows to aggregate", errorNode,
            new IllegalStateException("Cannot aggregate empty group")
        );
    }
    
    /**
     * Creates an AggregationEvaluationException for data type incompatibility.
     */
    public static AggregationEvaluationException forDataTypeIncompatibility(String aggregateFunction,
                                                                           List<CellValue> values,
                                                                           AggregationContext context,
                                                                           ASTNode errorNode) {
        String dataTypes = values.stream()
            .map(v -> v.getType().toString())
            .distinct()
            .collect(Collectors.joining(", "));
        
        return new AggregationEvaluationException(
            aggregateFunction, List.of(), values.size(), context,
            "incompatible data types: " + dataTypes, errorNode,
            new ClassCastException("Data type incompatibility in aggregation")
        );
    }
    
    /**
     * Creates an AggregationEvaluationException for invalid aggregation state.
     */
    public static AggregationEvaluationException forInvalidState(String aggregateFunction,
                                                               String stateDescription,
                                                               AggregationContext context,
                                                               ASTNode errorNode) {
        return new AggregationEvaluationException(
            aggregateFunction, List.of(), 0, context,
            "invalid aggregation state: " + stateDescription, errorNode,
            new IllegalStateException("Invalid aggregation state")
        );
    }
    
    /**
     * Creates an AggregationEvaluationException for group table processing errors.
     */
    public static AggregationEvaluationException forGroupTableError(String aggregateFunction,
                                                                   JfrTable groupTable,
                                                                   List<String> groupByFields,
                                                                   ASTNode errorNode,
                                                                   Throwable cause) {
        String description = String.format("group table with %d rows, %d columns", 
            groupTable.getRows().size(), groupTable.getColumns().size());
        
        return new AggregationEvaluationException(
            aggregateFunction, groupByFields, groupTable.getRows().size(),
            AggregationContext.GROUP_BY, description, errorNode, cause
        );
    }
    
    /**
     * Builds the evaluation context string.
     */
    private static String buildEvaluationContext(String aggregateFunction, AggregationContext context, int groupSize) {
        return String.format("aggregate function '%s' in %s (group size: %d)", 
            aggregateFunction, context.getDescription(), groupSize);
    }
    
    /**
     * Builds the problematic value description.
     */
    private static String buildProblematicValue(List<String> groupByFields, String dataDescription) {
        StringBuilder sb = new StringBuilder();
        
        if (groupByFields != null && !groupByFields.isEmpty()) {
            sb.append("GROUP BY: ").append(String.join(", ", groupByFields)).append("; ");
        }
        
        if (dataDescription != null) {
            sb.append("Data: ").append(dataDescription);
        }
        
        return sb.toString();
    }
    
    /**
     * Determines the appropriate error type based on the cause.
     */
    private static EvaluationErrorType determineErrorType(Throwable cause) {
        if (cause instanceof IllegalStateException) {
            return EvaluationErrorType.INVALID_STATE;
        } else if (cause instanceof ClassCastException) {
            return EvaluationErrorType.INVALID_CONVERSION;
        } else if (cause instanceof ArithmeticException) {
            return EvaluationErrorType.ARITHMETIC_OVERFLOW;
        } else if (cause instanceof NullPointerException) {
            return EvaluationErrorType.NULL_POINTER;
        }
        return EvaluationErrorType.INVALID_AGGREGATION;
    }
    
    // Getters for accessing specific error information
    
    public String getAggregateFunction() {
        return aggregateFunction;
    }
    
    public List<String> getGroupByFields() {
        return groupByFields;
    }
    
    public int getGroupSize() {
        return groupSize;
    }
    
    public AggregationContext getAggregationContext() {
        return aggregationContext;
    }
    
    public String getDataDescription() {
        return dataDescription;
    }
    
    /**
     * Returns true if this aggregation involved grouping.
     */
    public boolean hasGroupBy() {
        return !groupByFields.isEmpty();
    }
    
    /**
     * Returns true if this was an empty group error.
     */
    public boolean isEmptyGroupError() {
        return groupSize == 0 && aggregationContext == AggregationContext.GROUP_BY;
    }
    
    /**
     * Returns a detailed description of the aggregation that failed.
     */
    public String getAggregationDescription() {
        StringBuilder sb = new StringBuilder();
        sb.append(aggregateFunction);
        
        if (hasGroupBy()) {
            sb.append(" grouped by ").append(String.join(", ", groupByFields));
        }
        
        sb.append(" (").append(groupSize).append(" rows)");
        
        if (dataDescription != null) {
            sb.append(" - ").append(dataDescription);
        }
        
        return sb.toString();
    }
}
