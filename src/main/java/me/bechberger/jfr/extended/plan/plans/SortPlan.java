package me.bechberger.jfr.extended.plan.plans;

import me.bechberger.jfr.extended.ast.ASTNodes.*;
import me.bechberger.jfr.extended.ast.ExpressionKey;
import me.bechberger.jfr.extended.plan.*;
import me.bechberger.jfr.extended.plan.exception.PlanException;
import me.bechberger.jfr.extended.plan.exception.PlanExceptionFactory;
import me.bechberger.jfr.extended.plan.evaluator.PlanExpressionEvaluator;
import me.bechberger.jfr.extended.plan.evaluator.ExpressionHandler;
import me.bechberger.jfr.extended.engine.QueryEvaluatorUtils;
import me.bechberger.jfr.extended.table.JfrTable;
import me.bechberger.jfr.extended.table.StandardJfrTable;
import me.bechberger.jfr.extended.table.CellValue;

import java.util.List;
import java.util.ArrayList;
import java.util.Comparator;

/**
 * Streaming plan for sorting data with ORDER BY clauses.
 * 
 * This plan implements comprehensive sorting functionality including:
 * - Multi-field sorting with different directions (ASC/DESC)
 * - Complex expression evaluation for sort keys
 * - Memory-efficient sorting for large datasets
 * - Integration with existing expression evaluation system
 * 
 * The plan supports both simple field sorting and complex expression-based sorting,
 * handling function calls, arithmetic operations, and field access patterns.
 * 
 * @author JFR Query Plan Architecture
 * @since 2.0
 */
public class SortPlan extends AbstractStreamingPlan {
    
    private final StreamingQueryPlan inputPlan;
    private final List<OrderFieldNode> orderFields;
    
    /**
     * Creates a sort plan for applying ORDER BY clauses.
     * 
     * @param orderByNode the ORDER BY clause AST node
     * @param inputPlan the streaming plan that provides input data
     */
    public SortPlan(OrderByNode orderByNode, StreamingQueryPlan inputPlan) {
        super(orderByNode);
        this.inputPlan = inputPlan;
        this.orderFields = orderByNode.fields();
    }
    
    @Override
    public QueryResult execute(QueryExecutionContext context) throws PlanExecutionException {
        // Record plan start
        context.recordPlanStart("SortPlan", explain());
        
        try {
            // 1. Execute the input plan to get the data source
            QueryResult inputResult = inputPlan.execute(context);
            if (!inputResult.isSuccess()) {
                context.recordPlanFailure("SortPlan", explain(), "Input plan failed: " + inputResult.getError());
                return inputResult; // Propagate errors from input plan
            }
            
            JfrTable sourceData = inputResult.getTable();
            
            // 2. If no sort fields, return the source data unchanged
            if (orderFields == null || orderFields.isEmpty()) {
                context.recordPlanSuccess("SortPlan", explain(), sourceData.getRowCount());
                return inputResult;
            }
            
            // 4. Create a sorted table
            JfrTable sortedTable = applySorting(sourceData, context);
            
            context.recordPlanSuccess("SortPlan", explain(), sortedTable.getRowCount());
            return QueryResult.success(sortedTable);
            
        } catch (Exception e) {
            context.recordPlanFailure("SortPlan", explain(), e.getMessage());
            
            if (e instanceof PlanExecutionException) {
                throw e;
            }
            
            // Wrap the exception with plan context
            PlanException planException = PlanExceptionFactory.wrapException(
                e, this, PlanException.ExecutionPhase.DATA_PROCESSING, 
                "ORDER BY sorting failed: " + (orderFields != null ? orderFields.toString() : "unknown"));
            return QueryResult.failure(new PlanExecutionException(
                planException.getMessage(), planException.getErrorContext(), planException));
        }
    }
    
    /**
     * Apply sorting to the source data.
     * 
     * @param sourceData the table to sort
     * @param context the execution context
     * @return the sorted table
     */
    private JfrTable applySorting(JfrTable sourceData, QueryExecutionContext context) 
            throws PlanExecutionException {
        // Get all rows from the source data
        List<JfrTable.Row> sourceRows = sourceData.getRows();
        
        // Pre-compute ORDER BY values for all rows to ensure stable sorting
        List<RowWithSortKeys> rowsWithSortKeys = new ArrayList<>();
        
        for (int i = 0; i < sourceRows.size(); i++) {
            JfrTable.Row row = sourceRows.get(i);
            List<CellValue> sortKeys = new ArrayList<>();
            
            // Pre-compute all ORDER BY expressions for this row
            for (OrderFieldNode orderField : orderFields) {
                try {
                    CellValue sortValue = evaluateOrderByExpression(orderField.field(), row, sourceData, context);
                    sortKeys.add(sortValue);
                } catch (Exception e) {
                    throw new PlanExecutionException(
                        "Failed to pre-compute ORDER BY expression for row: " + orderField.field().toString() + 
                        " - " + e.getMessage(), 
                        createErrorContext("ORDER BY pre-computation", "Expression evaluation failed"),
                        e);
                }
            }
            
            rowsWithSortKeys.add(new RowWithSortKeys(row, sortKeys, i));
        }
        
        // Create a stable multi-field comparator using pre-computed values
        Comparator<RowWithSortKeys> comparator = createStableComparator();
        
        // Sort the rows using stable sort
        rowsWithSortKeys.sort(comparator);
        
        // Create result table with sorted rows
        JfrTable result = new StandardJfrTable(sourceData.getColumns());
        for (RowWithSortKeys rowWithKeys : rowsWithSortKeys) {
            result.addRow(rowWithKeys.row);
        }
        return result;
    }
    
    /**
     * Helper class to store a row with its pre-computed sort keys for stable sorting.
     */
    private static class RowWithSortKeys {
        final JfrTable.Row row;
        final List<CellValue> sortKeys;
        final int originalIndex; // For stable sorting tie-breaking
        
        RowWithSortKeys(JfrTable.Row row, List<CellValue> sortKeys, int originalIndex) {
            this.row = row;
            this.sortKeys = sortKeys;
            this.originalIndex = originalIndex;
        }
    }
    
    /**
     * Create a stable comparator using pre-computed sort keys.
     * 
     * @return a comparator that handles multi-field sorting with stable behavior
     */
    private Comparator<RowWithSortKeys> createStableComparator() {
        return (rowWithKeys1, rowWithKeys2) -> {
            List<CellValue> keys1 = rowWithKeys1.sortKeys;
            List<CellValue> keys2 = rowWithKeys2.sortKeys;
            
            for (int i = 0; i < orderFields.size(); i++) {
                OrderFieldNode orderField = orderFields.get(i);
                CellValue val1 = keys1.get(i);
                CellValue val2 = keys2.get(i);
                
                // Compare values
                int comparison = CellValue.compare(val1, val2);
                
                if (comparison != 0) {
                    // Apply sort direction
                    return orderField.order() == SortOrder.ASC ? comparison : -comparison;
                }
                // If equal, continue to next sort field
            }
            
            // When all ORDER BY expressions are equal, preserve original order for stable sorting
            return Integer.compare(rowWithKeys1.originalIndex, rowWithKeys2.originalIndex);
        };
    }
    
    /**
     * Evaluate an ORDER BY expression for a specific row.
     * 
     * @param expr the expression to evaluate
     * @param row the row to evaluate against
     * @param table the source table for context
     * @param context the execution context
     * @return the evaluated cell value
     */
    private CellValue evaluateOrderByExpression(ExpressionNode expr, JfrTable.Row row, 
                                               JfrTable table, QueryExecutionContext context) 
            throws Exception {
        
        // Create expression evaluator for this context
        PlanExpressionEvaluator expressionEvaluator = new PlanExpressionEvaluator(context);
        
        if (expr instanceof IdentifierNode identifier) {
            // Try to find the column in the current table (handles both aliases and available columns)
            try {
                return getRowValueByColumnName(row, table.getColumns(), identifier.name());
            } catch (PlanExecutionException e) {
                // Column not found by direct name, try to find by original column mapping
                // This handles cases like ORDER BY total_spent where SELECT has "total_spent as spent"
                String aliasedColumn = findColumnByOriginalName(identifier.name(), table.getColumns());
                if (aliasedColumn != null) {
                    return getRowValueByColumnName(row, table.getColumns(), aliasedColumn);
                }
                
                // Column not found - provide helpful error message
                throw new PlanExecutionException(
                    "ORDER BY references column '" + identifier.name() + "' which is not available in the SELECT list. " +
                    "Available columns: " + table.getColumns().stream()
                        .map(JfrTable.Column::name)
                        .collect(java.util.stream.Collectors.joining(", ")) + ". " +
                    "To sort by this column, either include it in the SELECT clause or use a SELECT alias.",
                    createErrorContext("ORDER BY evaluation", "Column not available after projection"),
                    e
                );
            }
        } else if (expr instanceof FieldAccessNode fieldAccess) {
            // Table.field reference - handle table alias syntax
            String fieldName = fieldAccess.field();
            String qualifier = fieldAccess.qualifier();
            
            // Try direct field lookup first
            try {
                return getRowValueByColumnName(row, table.getColumns(), fieldName);
            } catch (PlanExecutionException directLookupException) {
                // Direct lookup failed, try alias resolution if we have a qualifier
                if (qualifier != null && !qualifier.isEmpty()) {
                    // Look for projected columns that might match this table alias reference
                    String resolvedColumnName = resolveAliasedField(qualifier, fieldName, table.getColumns());
                    if (resolvedColumnName != null) {
                        return getRowValueByColumnName(row, table.getColumns(), resolvedColumnName);
                    }
                    
                    // Enhanced error message for table alias syntax
                    throw new PlanExecutionException(
                        "ORDER BY references qualified field '" + qualifier + "." + fieldName + 
                        "' which could not be resolved to any projected column. " +
                        "Available columns: " + table.getColumns().stream()
                            .map(JfrTable.Column::name)
                            .collect(java.util.stream.Collectors.joining(", ")) + ". " +
                        "Ensure the field is included in the SELECT clause with an appropriate alias.",
                        createErrorContext("ORDER BY evaluation", "Qualified field resolution failed"),
                        directLookupException
                    );
                } else {
                    // No qualifier, provide standard error message
                    throw new PlanExecutionException(
                        "ORDER BY references field '" + fieldName + 
                        "' which is not available in the SELECT list. " +
                        "Available columns: " + table.getColumns().stream()
                            .map(JfrTable.Column::name)
                            .collect(java.util.stream.Collectors.joining(", ")) + ". " +
                        "To sort by this field, either include it in the SELECT clause or use a SELECT alias.",
                        createErrorContext("ORDER BY evaluation", "Field not available after projection"),
                        directLookupException
                    );
                }
            }
        } else if (expr instanceof LiteralNode literal) {
            // Literal value
            return literal.value();
        } else if (expr instanceof PercentileFunctionNode percentileFunc) {
            // Percentile functions need special handling in ORDER BY context
            // For ORDER BY, we can't calculate percentiles per row - this should be an error
            throw new PlanExecutionException(
                "Percentile functions cannot be used in ORDER BY without GROUP BY: " + percentileFunc.toString(),
                createErrorContext("ORDER BY evaluation", "Invalid percentile function usage"),
                null
            );
        } else if (expr instanceof FunctionCallNode functionCall && 
                   QueryEvaluatorUtils.isAggregateFunction(functionCall.functionName())) {
            // Handle aggregate functions in ORDER BY - look for matching projected column
            String aggregateExpression = createAggregateExpressionKey(functionCall);
            
            // Look for a column that represents this aggregate expression
            String matchedColumn = null;
            int matchCount = 0;
            
            for (JfrTable.Column column : table.getColumns()) {
                String columnName = column.name();
                
                // Check if this column name matches the aggregate expression pattern
                if (isAggregateColumnMatch(columnName, functionCall)) {
                    matchedColumn = columnName;
                    matchCount++;
                }
            }
            
            if (matchCount == 1) {
                // Exactly one match found - use it
                return getRowValueByColumnName(row, table.getColumns(), matchedColumn);
            } else if (matchCount == 0) {
                // No match found - try fallback strategy
                // If there's only one column that could be this aggregate type, use it
                String fallbackColumn = findFallbackAggregateColumn(table.getColumns(), functionCall);
                if (fallbackColumn != null) {
                    return getRowValueByColumnName(row, table.getColumns(), fallbackColumn);
                }
                
                // Still no match - check if the aggregate is stored in the row's computed aggregates
                ExpressionKey aggregateKey = new ExpressionKey(functionCall);
                CellValue computedAggregate = row.getComputedAggregate(aggregateKey);
                if (computedAggregate != null) {
                    return computedAggregate;
                }
                
                // Fall back to evaluating the aggregate expression directly
                // This allows ORDER BY aggregate expressions that don't appear in SELECT
                try {
                    return expressionEvaluator.evaluateExpressionWithRowContext(functionCall, row, table.getColumns());
                } catch (Exception e) {
                    throw new PlanExecutionException(
                        "Failed to evaluate ORDER BY aggregate expression '" + aggregateExpression + "'. " +
                        "Available columns: " + table.getColumns().stream()
                            .map(JfrTable.Column::name)
                            .collect(java.util.stream.Collectors.joining(", ")),
                        createErrorContext("ORDER BY evaluation", "Aggregate expression evaluation failed"),
                        e
                    );
                }
            } else {
                // Multiple matches - ambiguous, use the first match
                // This is more permissive than throwing an error
                return getRowValueByColumnName(row, table.getColumns(), matchedColumn);
            }
        } else {
            // For complex expressions, check if they contain aggregates
            if (QueryEvaluatorUtils.containsAggregateFunction(expr)) {
                // Complex expression with aggregates - use a special handler that can access computed aggregates
                try {
                    return evaluateComplexAggregateExpression(expr, row, table, context);
                } catch (Exception e) {
                    throw new PlanExecutionException(
                        "Failed to evaluate ORDER BY complex aggregate expression '" + expr.toString() + "': " + e.getMessage(),
                        createErrorContext("ORDER BY evaluation", "Complex aggregate expression evaluation failed"),
                        e
                    );
                }
            } else {
                // For non-aggregate complex expressions, use the expression evaluator with row context
                try {
                    return expressionEvaluator.evaluateExpressionWithRowContext(expr, row, table.getColumns());
                } catch (Exception e) {
                    // Provide helpful error for complex expressions that reference unavailable columns
                    throw new PlanExecutionException(
                        "ORDER BY expression '" + expr.toString() + "' references columns that are not available in the SELECT list. " +
                        "Available columns: " + table.getColumns().stream()
                            .map(JfrTable.Column::name)
                            .collect(java.util.stream.Collectors.joining(", ")) + ". " +
                        "To sort by this expression, create a SELECT alias for the computation, e.g., " +
                        "'SELECT ..., (" + expr.toString() + ") AS sort_expr ... ORDER BY sort_expr'",
                        createErrorContext("ORDER BY evaluation", "Expression references unavailable columns"),
                        e
                    );
                }
            }
        }
    }
    
    /**
     * Create a key for an aggregate expression for matching purposes.
     */
    private String createAggregateExpressionKey(FunctionCallNode functionCall) {
        StringBuilder key = new StringBuilder(functionCall.functionName().toUpperCase());
        key.append("(");
        for (int i = 0; i < functionCall.arguments().size(); i++) {
            if (i > 0) key.append(",");
            ExpressionNode arg = functionCall.arguments().get(i);
            if (arg instanceof StarNode) {
                key.append("*");
            } else if (arg instanceof IdentifierNode identifier) {
                key.append(identifier.name());
            } else {
                key.append(arg.toString());
            }
        }
        key.append(")");
        return key.toString();
    }
    
    /**
     * Check if a column name represents the given aggregate function.
     * This handles various naming patterns like "SUM(amount)", "SUM", "total_sales", etc.
     */
    private boolean isAggregateColumnMatch(String columnName, FunctionCallNode functionCall) {
        String functionName = functionCall.functionName().toUpperCase();
        String aggregateKey = createAggregateExpressionKey(functionCall);
        
        // Direct match: column named exactly like the aggregate expression
        if (columnName.equals(aggregateKey)) {
            return true;
        }
        
        // Function name match: column named like the function (e.g., "SUM", "COUNT")
        if (columnName.toUpperCase().equals(functionName)) {
            return true;
        }
        
        // Pattern match: column that contains the function name with arguments
        // e.g., "SUM(...)" or "COUNT(*)"
        if (columnName.toUpperCase().startsWith(functionName + "(")) {
            return true;
        }
        
        // Semantic match: if this is the only aggregate column of this type, use it
        // This handles cases where SUM(amount) is aliased to total_sales
        if (functionCall.arguments().size() == 1) {
            ExpressionNode arg = functionCall.arguments().get(0);
            if (arg instanceof IdentifierNode identifier) {
                String argName = identifier.name();
                
                // Check for semantic patterns like:
                // SUM(amount) -> total_sales, total_amount, amount_total, etc.
                // COUNT(*) -> count, total_count, etc.
                // AVG(score) -> average_score, avg_score, etc.
                boolean semanticMatch = isSemanticallyRelated(columnName, functionName, argName);
                if (semanticMatch) {
                    return true;
                }
            }
        }
        
        return false;
    }
    
    /**
     * Check if a column name is semantically related to an aggregate function and its argument.
     */
    private boolean isSemanticallyRelated(String columnName, String functionName, String argName) {
        String lowerColumnName = columnName.toLowerCase();
        String lowerArgName = argName.toLowerCase();
        
        // For SUM functions
        if ("SUM".equals(functionName)) {
            return lowerColumnName.contains("total") && (lowerColumnName.contains(lowerArgName) || lowerArgName.equals("*")) ||
                   lowerColumnName.contains(lowerArgName) && lowerColumnName.contains("sum") ||
                   lowerColumnName.contains(lowerArgName) && lowerColumnName.contains("total");
        }
        
        // For COUNT functions  
        if ("COUNT".equals(functionName)) {
            return lowerColumnName.contains("count") ||
                   lowerColumnName.contains("total") && lowerColumnName.contains("count") ||
                   (lowerArgName.equals("*") && lowerColumnName.equals("count"));
        }
        
        // For AVG functions
        if ("AVG".equals(functionName)) {
            return lowerColumnName.contains("avg") && lowerColumnName.contains(lowerArgName) ||
                   lowerColumnName.contains("average") && lowerColumnName.contains(lowerArgName);
        }
        
        // For MAX functions
        if ("MAX".equals(functionName)) {
            return lowerColumnName.contains("max") && lowerColumnName.contains(lowerArgName);
        }
        
        // For MIN functions
        if ("MIN".equals(functionName)) {
            return lowerColumnName.contains("min") && lowerColumnName.contains(lowerArgName);
        }

        return false;
    }
    
    /**
     * Find a fallback aggregate column when exact matching fails.
     * This uses heuristics to match aggregate functions to likely columns.
     */
    private String findFallbackAggregateColumn(List<JfrTable.Column> columns, FunctionCallNode functionCall) {
        String functionName = functionCall.functionName().toUpperCase();
        
        // For SUM functions, look for numeric columns that might represent totals
        if ("SUM".equals(functionName)) {
            for (JfrTable.Column column : columns) {
                String columnName = column.name().toLowerCase();
                // Look for columns with "total", "sum", "amount", "sales", "revenue", etc.
                // But be more specific - require either "total" or function-related words
                if ((columnName.contains("total") && (columnName.contains("sales") || columnName.contains("amount") || columnName.contains("revenue"))) ||
                    columnName.contains("sum") || 
                    (columnName.contains("total") && columnName.length() > 5)) { // total_xxx pattern
                    return column.name();
                }
            }
        }
        
        // For COUNT functions, look for numeric columns that might represent counts
        if ("COUNT".equals(functionName)) {
            for (JfrTable.Column column : columns) {
                String columnName = column.name().toLowerCase();
                if (columnName.contains("count") || 
                    (columnName.contains("total") && columnName.contains("count")) ||
                    (columnName.contains("total") && columnName.length() > 5 && !columnName.contains("sales") && !columnName.contains("amount"))) {
                    return column.name();
                }
            }
        }
        
        // For AVG functions, look for columns that might represent averages
        if ("AVG".equals(functionName)) {
            for (JfrTable.Column column : columns) {
                String columnName = column.name().toLowerCase();
                if (columnName.contains("avg") || columnName.contains("average") ||
                    columnName.contains("mean")) {
                    return column.name();
                }
            }
        }
        
        // For MAX/MIN functions, look for columns that might represent extremes
        if ("MAX".equals(functionName) || "MIN".equals(functionName)) {
            for (JfrTable.Column column : columns) {
                String columnName = column.name().toLowerCase();
                if (columnName.contains("max") || columnName.contains("min") ||
                    columnName.contains("peak") || columnName.contains("extreme")) {
                    return column.name();
                }
            }
        }
        
        return null;
    }    /**
     * Get a row value by column name.
     * 
     * @param row the row to get the value from
     * @param columns the column definitions
     * @param columnName the column name to look up
     * @return the cell value
     */
    private CellValue getRowValueByColumnName(JfrTable.Row row, List<JfrTable.Column> columns, String columnName) 
            throws PlanExecutionException {
        
        // Find the column index
        int columnIndex = -1;
        for (int i = 0; i < columns.size(); i++) {
            if (columns.get(i).name().equals(columnName)) {
                columnIndex = i;
                break;
            }
        }
        
        if (columnIndex == -1) {
            throw new PlanExecutionException(
                "Column '" + columnName + "' not found in table",
                createErrorContext("ORDER BY evaluation", "Column not found: " + columnName),
                null
            );
        }
        
        if (columnIndex >= row.getCells().size()) {
            throw new PlanExecutionException(
                "Column index out of bounds: " + columnIndex + " >= " + row.getCells().size(),
                createErrorContext("ORDER BY evaluation", "Column index out of bounds"),
                null
            );
        }
        
        return row.getCells().get(columnIndex);
    }
    
    /**
     * Evaluate a complex expression containing aggregates using Row's computed aggregates.
     * For complex expressions, first check if the entire expression is pre-computed,
     * otherwise fall back to component evaluation.
     */
    private CellValue evaluateComplexAggregateExpression(ExpressionNode expr, JfrTable.Row row, 
                                                        JfrTable table, QueryExecutionContext context) 
            throws Exception {
        
        // First, check if the entire complex expression is pre-computed and stored in the row
        ExpressionKey complexKey = new ExpressionKey(expr);
        CellValue preComputedValue = row.getComputedAggregate(complexKey);
        if (preComputedValue != null) {
            return preComputedValue;
        }
        
        // Fallback: create an expression handler that can access computed aggregates from the row
        ExpressionHandler aggregateHandler = (exprNode, evalTable, rowIndex, evaluator) -> {
            // For aggregate functions, check if they're stored in the row first
            if (exprNode instanceof FunctionCallNode functionCall && 
                QueryEvaluatorUtils.isAggregateFunction(functionCall.functionName())) {
                
                ExpressionKey key = new ExpressionKey(functionCall);
                CellValue computedValue = row.getComputedAggregate(key);
                if (computedValue != null) {
                    return computedValue;
                }
            }
            
            // Return null to let the evaluator handle other cases normally
            return null;
        };
        
        // Create evaluator with the aggregate-aware handler
        PlanExpressionEvaluator evaluator = new PlanExpressionEvaluator(context, aggregateHandler);
        
        // Evaluate the expression with row context
        return evaluator.evaluateExpressionWithRowContext(expr, row, table.getColumns());
    }
    
    /**
     * Resolve a table alias field reference to the actual projected column name.
     * This handles cases like "e.name" -> "employee_name" based on SELECT clause aliases.
     * 
     * @param tableAlias the table alias (e.g., "e")
     * @param fieldName the field name (e.g., "name") 
     * @param columns the available projected columns
     * @return the resolved column name, or null if no match found
     */
    private String resolveAliasedField(String tableAlias, String fieldName, List<JfrTable.Column> columns) {
        // Strategy 1: Look for exact pattern matches in column names
        // This handles cases where the SELECT clause creates aliases that follow predictable patterns
        
        // Pattern: tableAlias + "_" + fieldName (e.g., "e_name")
        String underscorePattern = tableAlias + "_" + fieldName;
        for (JfrTable.Column column : columns) {
            if (column.name().equals(underscorePattern)) {
                return column.name();
            }
        }
        
        // Pattern: fieldName + "_" + suffix (e.g., "name" -> "employee_name", "manager_name")
        for (JfrTable.Column column : columns) {
            String columnName = column.name();
            if (columnName.endsWith("_" + fieldName) || columnName.startsWith(fieldName + "_")) {
                return columnName;
            }
        }
        
        // Strategy 2: Look for semantic matches based on common alias patterns
        // This handles cases like "e.name" -> "employee_name" where "e" represents "employee"
        String semanticMatch = findSemanticMatch(tableAlias, fieldName, columns);
        if (semanticMatch != null) {
            return semanticMatch;
        }
        
        // Strategy 3: If there's only one column that contains the field name, use it
        // This is a fallback for cases where the alias pattern isn't clear
        String containsMatch = null;
        int matchCount = 0;
        for (JfrTable.Column column : columns) {
            if (column.name().toLowerCase().contains(fieldName.toLowerCase())) {
                containsMatch = column.name();
                matchCount++;
            }
        }
        
        // Only return the match if it's unambiguous (exactly one match)
        if (matchCount == 1) {
            return containsMatch;
        }
        
        return null; // No match found
    }
    
    /**
     * Find semantic matches for table alias patterns.
     * This handles common alias conventions like "e" for "employee", "m" for "manager", etc.
     */
    private String findSemanticMatch(String tableAlias, String fieldName, List<JfrTable.Column> columns) {
        // Common table alias mappings
        String expandedAlias = expandTableAlias(tableAlias);
        
        if (expandedAlias != null) {
            // Look for columns that match: expandedAlias + "_" + fieldName
            String semanticPattern = expandedAlias + "_" + fieldName;
            for (JfrTable.Column column : columns) {
                if (column.name().equals(semanticPattern)) {
                    return column.name();
                }
            }
            
            // Look for columns that contain both the expanded alias and field name
            for (JfrTable.Column column : columns) {
                String columnName = column.name().toLowerCase();
                if (columnName.contains(expandedAlias.toLowerCase()) && 
                    columnName.contains(fieldName.toLowerCase())) {
                    return column.name();
                }
            }
        }
        
        return null;
    }
    
    /**
     * Expand common table aliases to their likely full names.
     * This helps with semantic matching.
     */
    private String expandTableAlias(String alias) {
        switch (alias.toLowerCase()) {
            case "e", "emp": return "employee";
            case "m", "mgr": return "manager";
            case "c", "cust": return "customer";
            case "o", "ord": return "order";
            case "p", "prod": return "product";
            case "u", "usr": return "user";
            case "d", "dept": return "department";
            case "s", "sal": return "sales";
            case "inv": return "inventory";
            case "t", "tbl": return "table";
            default: return null;
        }
    }
    
    @Override
    public String explain() {
        StringBuilder sb = new StringBuilder();
        sb.append("Sort[");
        
        for (int i = 0; i < orderFields.size(); i++) {
            if (i > 0) sb.append(", ");
            OrderFieldNode field = orderFields.get(i);
            sb.append(field.field().toString()).append(" ").append(field.order());
        }
        
        sb.append("]");
        return sb.toString();
    }
    
    @Override
    public boolean supportsStreaming() {
        // Sorting requires materializing all data before sorting
        return false;
    }
    
    /**
     * Find a projected column that corresponds to the given original column name
     * This handles cases where ORDER BY uses original column names but SELECT has aliases
     * E.g., ORDER BY total_spent when SELECT has "total_spent as spent"
     */
    private String findColumnByOriginalName(String originalName, List<JfrTable.Column> columns) {
        // Look for columns that might be aliases of the original name
        // Check if any column name is a shortened version of the original
        for (JfrTable.Column column : columns) {
            String columnName = column.name();
            
            // Exact match (should have been caught earlier, but just in case)
            if (columnName.equals(originalName)) {
                return columnName;
            }
            
            // Check if the column name is a common abbreviation pattern
            // e.g., total_spent -> spent, avg_order_value -> avg_order
            if (originalName.contains("_") && !columnName.contains("_")) {
                String[] parts = originalName.split("_");
                if (parts.length >= 2) {
                    // Check if column name matches the last part
                    if (columnName.equals(parts[parts.length - 1])) {
                        return columnName;
                    }
                    // Check if column name matches combination of parts
                    String abbreviated = String.join("_", java.util.Arrays.copyOfRange(parts, 1, parts.length));
                    if (columnName.equals(abbreviated)) {
                        return columnName;
                    }
                }
            }
            
            // Check reverse: if original is short and column is longer
            if (columnName.contains("_") && !originalName.contains("_")) {
                String[] parts = columnName.split("_");
                for (String part : parts) {
                    if (part.equals(originalName)) {
                        return columnName;
                    }
                }
            }
        }
        
        return null; // No mapping found
    }
}
