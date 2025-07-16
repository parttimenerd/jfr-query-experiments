package me.bechberger.jfr.extended.engine;

import jdk.jfr.EventType;
import jdk.jfr.ValueDescriptor;
import me.bechberger.jfr.extended.evaluator.FunctionRegistry;
import me.bechberger.jfr.extended.evaluator.AggregateFunctions;
import me.bechberger.jfr.extended.evaluator.FunctionUtils;
import me.bechberger.jfr.extended.ast.ASTNode;
import me.bechberger.jfr.extended.ast.ASTNodes.*;
import me.bechberger.jfr.extended.ast.ASTVisitor;
import me.bechberger.jfr.extended.ast.Location;
import me.bechberger.jfr.extended.table.JfrTable;
import me.bechberger.jfr.extended.table.StandardJfrTable;
import me.bechberger.jfr.extended.table.MultiResultTable;
import me.bechberger.jfr.extended.table.CellValue;
import me.bechberger.jfr.extended.table.CellType;
import me.bechberger.jfr.extended.table.SingleCellTable;
import me.bechberger.jfr.extended.table.StarMarkerTable;
import me.bechberger.jfr.extended.Parser;
import me.bechberger.jfr.extended.ParserException;
import me.bechberger.jfr.extended.engine.QuerySemanticValidator.QuerySemanticException;
import me.bechberger.jfr.extended.engine.exception.*;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.Comparator;
import java.util.stream.Collectors;

// RawJfrQueryExecutor interface is now in its own file

/**
 * Interface for executing extended queries  
 */
@FunctionalInterface
interface ExtendedQueryExecutor {
    JfrTable execute(QueryNode queryNode) throws Exception;
}

/**
 * Main AST evaluator for executing JFR queries.
 * 
 * This class implements the visitor pattern to traverse and evaluate
 * the AST, producing JfrTable results. It integrates all query engine
 * functionality including variable management, view definitions, and result caching.
 */
@SuppressWarnings("unused")
public class QueryEvaluator implements ASTVisitor<JfrTable> {
    
    private final FunctionRegistry functionRegistry;
    private final ExpressionEvaluator expressionEvaluator;
    private final List<EventType> eventTypes;
    private final RawJfrQueryExecutor rawJfrExecutor;
    private final JoinProcessor joinProcessor;
    private ExecutionContext context;
    private AggregateFunctions.EvaluationContext evaluationContext;
    
    /**
     * Constructor for QueryEvaluator with raw JFR query executor.
     * Gets FunctionRegistry via getInstance(), creates parser when needed.
     */
    public QueryEvaluator(RawJfrQueryExecutor rawJfrExecutor) {
        this.functionRegistry = FunctionRegistry.getInstance();
        this.expressionEvaluator = new ExpressionEvaluator(functionRegistry);
        this.rawJfrExecutor = rawJfrExecutor;
        this.joinProcessor = new JoinProcessor(JoinProcessor.createOptimizedConfig());
        this.eventTypes = new ArrayList<>(); // Event types will be retrieved as needed
        this.context = new ExecutionContext();
        
        // Create evaluation context with proper executors and context stacking support
        this.evaluationContext = new AggregateFunctions.EvaluationContext(
            // Raw JFR query executor
            (rawQueryNode) -> {
                try {
                    JfrTable result = this.jfrQuery(rawQueryNode);
                    return convertJfrTableToRows(result);
                } catch (Exception e) {
                    throw new QueryEvaluationException(
                        "raw JFR query execution: " + rawQueryNode.toString(), 
                        rawQueryNode.toString(), 
                        QueryEvaluationException.EvaluationErrorType.UNSUPPORTED_OPERATION, 
                        rawQueryNode, 
                        e
                    );
                }
            },
            // Extended query executor  
            (queryNode, ctx) -> {
                // Push new scope for nested query execution
                ctx.pushScope();
                try {
                    JfrTable result = this.extendedQuery(queryNode);
                    return convertJfrTableToRows(result);
                } finally {
                    // Always pop scope when done
                    ctx.popScope();
                }
            }
        );
    }
    
    /**
     * Initialize event types from the raw JFR executor
     */
    private void initializeEventTypes() {
        if (rawJfrExecutor != null && eventTypes.isEmpty()) {
            try {
                if (rawJfrExecutor instanceof RawJfrQueryExecutorImpl impl) {
                    eventTypes.addAll(impl.getEventTypes());
                }
                // For testing, we'll handle event type name extraction differently
            } catch (Exception e) {
                // Ignore errors during initialization
            }
        }
    }
    
    /**
     * Initialize lazy variables for all available event types.
     * Creates a variable "X = SELECT * FROM X" for each event type X.
     */
    private void initializeEventTypeVariables() {
        if (context.areEventTypeVariablesInitialized()) {
            return;
        }
        
        initializeEventTypes();
        
        // For real EventType objects
        for (EventType eventType : eventTypes) {
            String eventTypeName = eventType.getName();
            createLazyVariableForEventType(eventTypeName);
        }
        
        // For testing: also check if the executor has a getEventTypes method that returns strings
        if (rawJfrExecutor != null) {
            try {
                java.lang.reflect.Method method = rawJfrExecutor.getClass().getMethod("getEventTypes");
                @SuppressWarnings("unchecked")
                List<String> typeNames = (List<String>) method.invoke(rawJfrExecutor);
                
                for (String typeName : typeNames) {
                    createLazyVariableForEventType(typeName);
                }
            } catch (Exception e) {
                // If no getEventTypes method exists, continue without additional event types
            }
        }
        
        context.setEventTypeVariablesInitialized(true);
    }
    
    /**
     * Create a lazy variable for a given event type name
     */
    private void createLazyVariableForEventType(String eventTypeName) {
        Location loc = new Location(0, 0); // Default location for auto-generated queries
        
        // Create SELECT * clause
        List<SelectItemNode> selectItems = new ArrayList<>();
        selectItems.add(new SelectItemNode(new IdentifierNode("*", loc), null, loc));
        SelectNode selectNode = new SelectNode(selectItems, false, loc);
        
        // Create FROM clause
        List<SourceNodeBase> sources = new ArrayList<>();
        sources.add(new SourceNode(eventTypeName, null, loc));
        FromNode fromNode = new FromNode(sources, loc);
        
        // Create the query
        QueryNode query = new QueryNode(
            false, // not extended
            null, // no columns
            null, // no formatters
            selectNode,
            fromNode,
            null, // no WHERE
            null, // no GROUP BY
            null, // no HAVING
            null, // no ORDER BY
            null, // no LIMIT
            loc
        );
        
        // Create lazy variable
        LazyQuery lazyQuery = new LazyQuery(query, this);
        context.setLazyVariable(eventTypeName, lazyQuery);
    }
    
    /**
     * Execute a query with proper context management and variable scoping
     */
    public JfrTable executeWithContext(QueryNode queryNode, Map<String, Object> variables) {
        // Push new scope for this query execution
        evaluationContext.pushScope();
        
        try {
            // Set variables in current scope
            if (variables != null) {
                for (Map.Entry<String, Object> entry : variables.entrySet()) {
                    evaluationContext.setVariable(entry.getKey(), entry.getValue());
                }
            }
            
            // Execute the query
            return queryNode.accept(this);
            
        } finally {
            // Always pop scope when done
            evaluationContext.popScope();
        }
    }
    
    /**
     * Get the evaluation context for function calls
     */
    public AggregateFunctions.EvaluationContext getEvaluationContext() {
        return evaluationContext;
    }
    
    /**
     * Convert JfrTable to List<List<CellValue>> format expected by functions
     */
    private static List<List<CellValue>> convertJfrTableToRows(JfrTable table) {
        List<List<CellValue>> rows = new ArrayList<>();
        
        for (JfrTable.Row row : table.getRows()) {
            rows.add(new ArrayList<>(row.getCells()));
        }
        
        return rows;
    }
    
    /**
     * Execute a raw JFR query using the provided executor
     */
    public JfrTable jfrQuery(RawJfrQueryNode queryNode) throws Exception {
        if (rawJfrExecutor == null) {
            throw QueryExecutorException.forMissingExecutor("Raw JFR", queryNode);
        }
        return rawJfrExecutor.execute(queryNode);
    }
    
    /**
     * Execute an extended query using this evaluator
     */
    public JfrTable extendedQuery(QueryNode queryNode) {
        return queryNode.accept(this);
    }
    
    /**
     * Parse and execute a query string (possibly multi-statement)
     * Supports:
     * - Multi-statement queries with proper semantic validation
     * - Variable assignments (x := @SELECT * FROM table)
     * - View definitions (VIEW myview AS @SELECT ...)
     * - State management between statements
     */
    public JfrTable query(String queryString) throws Exception {
        try {
            // Parse and validate the query using the enhanced parser method
            // This handles both parsing and semantic validation in one step
            ProgramNode program = Parser.parseAndValidate(queryString);
            
            // Execute the program and return the last result
            return program.accept(this);
        } catch (ParserException e) {
            // ParserException already has good error messages - just re-throw it
            throw e;
        } catch (QuerySemanticValidator.QuerySemanticException e) {
            throw e;
        } catch (Exception e) {
            // Don't wrap exceptions that are already Query related exceptions
            if (e instanceof QueryExecutionException) {
                throw (QueryExecutionException) e;
            }
            
            // For other exceptions, provide a clean error message
            String errorMessage = e.getMessage() != null ? e.getMessage() : "Query execution failed";
            throw new QueryEvaluationException(
                errorMessage,
                null,
                QueryEvaluationException.EvaluationErrorType.UNSUPPORTED_OPERATION, 
                null, 
                e
            );
        }
    }
    
    @Override
    public JfrTable visitProgram(ProgramNode node) {
        if (node.statements().isEmpty()) {
            throw new QuerySyntaxException(
                "Program must contain at least one statement", 
                "", 
                QuerySyntaxException.SyntaxErrorType.SEMANTIC_VIOLATION, 
                node
            );
        }
        
        List<JfrTable> topLevelResults = new ArrayList<>();
        JfrTable lastResult = null;
        
        for (StatementNode statement : node.statements()) {
            lastResult = statement.accept(this);
            context.setCurrentResult(lastResult);
            
            // Only collect results from top-level queries (not assignments or view definitions)
            if (statement instanceof QueryNode) {
                topLevelResults.add(lastResult);
            }
        }
        
        // If we have multiple top-level query results, return a MultiResultTable
        // Otherwise, return the last result directly
        if (topLevelResults.size() > 1) {
            return new MultiResultTable(topLevelResults, lastResult);
        } else {
            return lastResult;
        }
    }
    
    @Override
    public JfrTable visitStatement(StatementNode node) {
        return node.accept(this);
    }
    
    @Override
    public JfrTable visitAssignment(AssignmentNode node) {
        // Create a lazy query instead of executing immediately
        LazyQuery lazyQuery = new LazyQuery(node.query(), this);
        context.setLazyVariable(node.variable(), lazyQuery);
        
        // Also store in evaluation context for backward compatibility
        evaluationContext.setVariable(node.variable(), lazyQuery);
        context.setLocalVariable(node.variable(), lazyQuery);
        
        // Return empty result for assignment statements
        return new StandardJfrTable(List.of());
    }
    
    @Override
    public JfrTable visitViewDefinition(ViewDefinitionNode node) {
        defineView(node.viewName(), node.query());
        // Return empty result for view definitions
        return new StandardJfrTable(List.of());
    }
    
    @Override
    public JfrTable visitQuery(QueryNode node) {
        // Basic query execution - main implementation
        // Integrates with the existing JFR query system
        
        if (node.isExtended()) {
            return executeExtendedQuery(node);
        } else {
            return executeBasicQuery(node);
        }
    }
    
    private JfrTable executeExtendedQuery(QueryNode node) {
        // Handle extended query features with proper scope management
        evaluationContext.pushScope();
        try {
            // Execute the basic query logic directly to avoid infinite recursion
            return executeBasicQuery(node);
        } finally {
            evaluationContext.popScope();
        }
    }
    
    private JfrTable executeBasicQuery(QueryNode node) throws QuerySemanticException {
        // Semantic validation before execution
        QuerySemanticValidator validator = new QuerySemanticValidator();
        validator.validate(node);
        
        // Check cache first using query hash
        String cacheKey = generateQueryCacheKey(node);
        Set<String> dependencies = extractQueryDependencies(node);
        
        JfrTable cachedResult = context.getCachedResult(cacheKey);
        if (cachedResult != null) {
            if (context.isDebugMode()) {
                System.out.println("Cache hit for query: " + cacheKey);
            }
            return cachedResult;
        }
        
        // Comprehensive query execution implementation
        try {
            // Step 1: Process FROM clause with integrated WHERE filtering to reduce data early
            JfrTable sourceData = processFromClauseWithFiltering(node.from(), node.where());
            
            // Step 2: Apply GROUP BY aggregations
            if (node.groupBy() != null) {
                sourceData = applyGroupByClause(sourceData, node.groupBy(), node.select());
                // GROUP BY already handles SELECT clause, so skip separate SELECT processing
            } else {
                // Step 4: Execute SELECT expressions on the filtered data (only if no GROUP BY)
                sourceData = executeSelectClause(sourceData, node.select());
            }
            
            // Step 3: Apply HAVING conditions on aggregated groups
            if (node.having() != null) {
                sourceData = applyHavingClause(sourceData, node.having());
            }
            
            // Step 5: Apply ORDER BY sorting
            if (node.orderBy() != null) {
                sourceData = applyOrderByClause(sourceData, node.orderBy());
            }
            
            // Step 6: Apply LIMIT restrictions
            if (node.limit() != null) {
                sourceData = applyLimitClause(sourceData, node.limit());
            }
            
            // Cache the result with dependencies for future use
            context.cacheResult(cacheKey, sourceData, dependencies);
            if (context.isDebugMode()) {
                System.out.println("Cached query result: " + cacheKey + " with dependencies: " + dependencies);
            }
            
            return sourceData;
            
        } catch (Exception e) {
            // Don't wrap exceptions that are already Query related exceptions
            if (e instanceof QueryExecutionException) {
                throw (QueryExecutionException) e;
            }
            
            // Log the error for debugging
            System.err.println("Query execution error: " + e.getMessage());
            e.printStackTrace();
            
            // Instead of returning a placeholder result that creates false data,
            // rethrow the exception so the query fails properly
            throw new QueryEvaluationException(
                "query execution", 
                null, 
                QueryEvaluationException.EvaluationErrorType.UNSUPPORTED_OPERATION, 
                node, 
                e
            );
        }
    }
    
    /**
     * Process FROM clause with integrated WHERE filtering for optimization
     * This reduces the size of intermediate results by applying filters as rows are joined
     */
    private JfrTable processFromClauseWithFiltering(FromNode fromNode, WhereNode whereNode) {
        if (fromNode == null) {
            return SingleCellTable.of("default", "");
        }
        
        List<SourceNodeBase> sources = fromNode.sources();
        if (sources.isEmpty()) {
            return SingleCellTable.of("empty", "");
        }
        
        // Process the first source as the base table and apply WHERE filtering immediately
        JfrTable result = sources.get(0).accept(this);
        result = applyWhereFilteringIfApplicable(result, whereNode);
        
        // Process any additional sources as joins with WHERE clause integration
        for (int i = 1; i < sources.size(); i++) {
            SourceNodeBase joinSource = sources.get(i);
            
            if (joinSource instanceof StandardJoinSourceNode standardJoin) {
                // Handle standard joins with WHERE clause optimization
                result = performStandardJoinWithFiltering(result, standardJoin, whereNode);
            } else if (joinSource instanceof FuzzyJoinSourceNode fuzzyJoin) {
                // Handle fuzzy joins with WHERE clause optimization
                result = performFuzzyJoinWithFiltering(result, fuzzyJoin, whereNode);
            } else {
                // Regular source - treat as cross join and apply filtering
                JfrTable rightTable = joinSource.accept(this);
                rightTable = applyWhereFilteringIfApplicable(rightTable, whereNode);
                result = performCrossJoin(result, rightTable);
                result = applyWhereFilteringIfApplicable(result, whereNode);
            }
        }
        
        // Apply any remaining WHERE conditions that couldn't be pushed down
        if (whereNode != null) {
            result = applyWhereClause(result, whereNode);
        }
        
        return result;
    }
    
    /**
     * Apply WHERE filtering if the condition can be evaluated on this table
     */
    private JfrTable applyWhereFilteringIfApplicable(JfrTable table, WhereNode whereNode) {
        if (whereNode == null || whereNode.condition() == null) {
            return table;
        }
        
        // For now, apply all WHERE conditions. A more sophisticated implementation
        // would analyze which conditions can be applied at this stage
        return applyWhereClause(table, whereNode);
    }
    
    /**
     * Perform standard join with WHERE clause optimization
     */
    private JfrTable performStandardJoinWithFiltering(JfrTable leftTable, StandardJoinSourceNode standardJoin, WhereNode whereNode) {
        // Get the right table
        SourceNode rightSource = new SourceNode(standardJoin.source(), standardJoin.alias(), standardJoin.location());
        JfrTable rightTable = visitSource(rightSource);

        // Apply WHERE filtering to right table before join to reduce join size
        rightTable = applyWhereFilteringIfApplicable(rightTable, whereNode);

        // Parse qualified field names to extract table aliases and field names
        String leftField = extractFieldNameFromQualified(standardJoin.leftJoinField());
        String rightField = extractFieldNameFromQualified(standardJoin.rightJoinField());
        
        // Get table alias for validation (optional - mainly for debugging)
        String leftAlias = extractTableAliasFromQualified(standardJoin.leftJoinField());
        String rightAlias = extractTableAliasFromQualified(standardJoin.rightJoinField());
        
        // Validate that the right alias matches the join source alias
        if (rightAlias != null && standardJoin.alias() != null && !rightAlias.equals(standardJoin.alias())) {
            throw new QueryEvaluationException(
                "join field validation",
                "Right join field alias '" + rightAlias + "' does not match source alias '" + standardJoin.alias() + "'",
                QueryEvaluationException.EvaluationErrorType.INVALID_STATE,
                standardJoin
            );
        }

        // Perform the join with optimized tables
        JfrTable result = switch (standardJoin.joinType()) {
            case INNER -> performInnerJoin(leftTable, rightTable, leftField, rightField);
            case LEFT -> performLeftJoin(leftTable, rightTable, leftField, rightField);
            case RIGHT -> performRightJoin(leftTable, rightTable, leftField, rightField);
            case FULL -> performFullJoin(leftTable, rightTable, leftField, rightField);
        };

        return result;
    }

    /**
     * Extract the unqualified field name from a potentially qualified field reference.
     * Examples: "e.threadId" -> "threadId", "threadId" -> "threadId"
     */
    private String extractFieldNameFromQualified(String qualifiedFieldName) {
        if (qualifiedFieldName == null) {
            return null;
        }
        int lastDotIndex = qualifiedFieldName.lastIndexOf('.');
        return lastDotIndex >= 0 ? qualifiedFieldName.substring(lastDotIndex + 1) : qualifiedFieldName;
    }
    
    /**
     * Extract the table alias from a qualified field reference.
     * Examples: "e.threadId" -> "e", "threadId" -> null
     */
    private String extractTableAliasFromQualified(String qualifiedFieldName) {
        if (qualifiedFieldName == null) {
            return null;
        }
        int lastDotIndex = qualifiedFieldName.lastIndexOf('.');
        return lastDotIndex >= 0 ? qualifiedFieldName.substring(0, lastDotIndex) : null;
    }
    
    /**
     * Perform fuzzy join with WHERE clause optimization
     */
    private JfrTable performFuzzyJoinWithFiltering(JfrTable leftTable, FuzzyJoinSourceNode fuzzyJoin, WhereNode whereNode) {
        // Apply WHERE filtering before performing expensive fuzzy join
        JfrTable filteredLeft = applyWhereFilteringIfApplicable(leftTable, whereNode);
        
        // Perform fuzzy join on filtered data
        return performFuzzyJoin(filteredLeft, fuzzyJoin);
    }
    
    /**
     * Process FROM clause to get the source data
     * Enhanced to handle both standard and fuzzy joins
     * @deprecated Use processFromClauseWithFiltering for better performance
     */
    private JfrTable processFromClause(FromNode fromNode) {
        if (fromNode == null) {
            return SingleCellTable.of("default", "");
        }
        
        List<SourceNodeBase> sources = fromNode.sources();
        if (sources.isEmpty()) {
            return SingleCellTable.of("empty", "");
        }
        
        // Process the first source as the base table
        JfrTable result = sources.get(0).accept(this);
        
        // Process any additional sources as joins
        for (int i = 1; i < sources.size(); i++) {
            SourceNodeBase joinSource = sources.get(i);
            
            if (joinSource instanceof StandardJoinSourceNode standardJoin) {
                // Handle standard joins
                result = performStandardJoin(result, standardJoin);
            } else if (joinSource instanceof FuzzyJoinSourceNode fuzzyJoin) {
                // Handle fuzzy joins
                result = performFuzzyJoin(result, fuzzyJoin);
            } else {
                // Regular source - treat as cross join
                JfrTable rightTable = joinSource.accept(this);
                result = performCrossJoin(result, rightTable);
            }
        }
        
        return result;
    }
    
    /**
     * Apply WHERE clause filters to the source data
     */
    private JfrTable applyWhereClause(JfrTable sourceData, WhereNode whereNode) {
        if (whereNode.condition() == null) {
            return sourceData;
        }
        
        // Create filtered result table with same columns
        JfrTable result = new StandardJfrTable(sourceData.getColumns());
        
        // Evaluate WHERE condition for each row
        for (JfrTable.Row row : sourceData.getRows()) {
            if (evaluateConditionForRow(row, whereNode.condition(), sourceData.getColumns())) {
                result.addRow(row);
            }
        }
        
        return result;
    }
    
    /**
     * Apply GROUP BY clause to aggregate data with proper aggregate function evaluation
     */
    private JfrTable applyGroupByClause(JfrTable sourceData, GroupByNode groupByNode, SelectNode selectNode) {
        // Group rows by specified fields
        Map<String, List<JfrTable.Row>> groups = new HashMap<>();
        
        for (JfrTable.Row row : sourceData.getRows()) {
            String groupKey = buildGroupKey(row, sourceData.getColumns(), groupByNode.fields());
            groups.computeIfAbsent(groupKey, k -> new ArrayList<>()).add(row);
        }
        
        // Determine result columns based on SELECT clause
        List<JfrTable.Column> resultColumns = new ArrayList<>();
        if (selectNode != null && !selectNode.items().isEmpty()) {
            for (SelectItemNode selectItem : selectNode.items()) {
                String columnName = selectItem.alias() != null ? 
                    selectItem.alias() : 
                    QueryEvaluatorUtils.extractColumnName(selectItem.expression());
                CellType columnType = QueryEvaluatorUtils.determineColumnType(selectItem.expression(), sourceData, this);
                resultColumns.add(new JfrTable.Column(columnName, columnType));
            }
        } else {
            // Fallback: use source columns if no SELECT clause
            resultColumns = new ArrayList<>(sourceData.getColumns());
        }
        
        JfrTable result = new StandardJfrTable(resultColumns);
        
        // For each group, compute aggregated values
        for (Map.Entry<String, List<JfrTable.Row>> group : groups.entrySet()) {
            List<JfrTable.Row> groupRows = group.getValue();
            if (groupRows.isEmpty()) continue;
            
            // Compute aggregated row for this group using SELECT clause
            JfrTable.Row aggregatedRow = computeAggregatedRow(groupRows, sourceData.getColumns(), selectNode);
            result.addRow(aggregatedRow);
        }
        
        return result;
    }
    
    /**
     * Compute aggregated row from a group of rows using proper SELECT clause aggregate functions
     */
    private JfrTable.Row computeAggregatedRow(List<JfrTable.Row> groupRows, List<JfrTable.Column> columns, SelectNode selectNode) {
        if (selectNode == null || selectNode.items().isEmpty()) {
            // Fallback: use simplified aggregation for backward compatibility
            return computeAggregatedRowSimple(groupRows, columns);
        }
        
        List<CellValue> aggregatedCells = new ArrayList<>();
        
        // Create a temporary table for this group to compute aggregates
        JfrTable groupTable = new StandardJfrTable(columns);
        groupRows.forEach(groupTable::addRow);
        
        // Evaluate each SELECT expression for this group
        for (SelectItemNode selectItem : selectNode.items()) {
            ExpressionNode expr = selectItem.expression();
            
            if (QueryEvaluatorUtils.containsAggregateFunction(expr)) {
                // This is an aggregate expression - compute it over the entire group
                CellValue aggregateValue = computeAggregateExpressionForGroup(expr, groupTable);
                aggregatedCells.add(aggregateValue);
            } else {
                // This is a non-aggregate expression - use the first row's value
                // (this is correct for GROUP BY since non-aggregate columns should be in GROUP BY)
                if (!groupRows.isEmpty()) {
                    CellValue value = CellValue.of(evaluateExpressionInRowContext(expr, groupRows.get(0), columns));
                    aggregatedCells.add(value);
                } else {
                    aggregatedCells.add(new CellValue.NullValue());
                }
            }
        }
        
        return new JfrTable.Row(aggregatedCells);
    }
    
    /**
     * Compute aggregated row from a group of rows - simplified fallback implementation
     */
    private JfrTable.Row computeAggregatedRowSimple(List<JfrTable.Row> groupRows, List<JfrTable.Column> columns) {
        List<CellValue> aggregatedCells = new ArrayList<>();
        
        for (int colIndex = 0; colIndex < columns.size(); colIndex++) {
            JfrTable.Column column = columns.get(colIndex);
            
            // For numeric columns, compute SUM; for others, take first value
            if (isNumericColumn(column.type())) {
                CellValue sum = computeSum(groupRows, colIndex);
                aggregatedCells.add(sum);
            } else {
                // For non-numeric columns, use the first value in the group
                CellValue firstValue = groupRows.get(0).getCells().get(colIndex);
                aggregatedCells.add(firstValue);
            }
        }
        
        return new JfrTable.Row(aggregatedCells);
    }
    
    /**
     * Compute an aggregate expression for a group of rows
     */
    private CellValue computeAggregateExpressionForGroup(ExpressionNode expr, JfrTable groupTable) {
        if (expr instanceof FunctionCallNode functionCall && isAggregateFunction(functionCall.functionName())) {
            return computeAggregateValue(functionCall, groupTable);
        } else if (expr instanceof BinaryExpressionNode binaryExpr) {
            // Handle expressions like "COUNT(*) + 1" or "SUM(duration) / COUNT(*)"
            CellValue leftValue = computeAggregateExpressionForGroup(binaryExpr.left(), groupTable);
            CellValue rightValue = computeAggregateExpressionForGroup(binaryExpr.right(), groupTable);
            return leftValue.mapBinary(rightValue, binaryExpr.operator());
        } else if (expr instanceof UnaryExpressionNode unaryExpr) {
            CellValue operandValue = computeAggregateExpressionForGroup(unaryExpr.operand(), groupTable);
            return performUnaryOperation(unaryExpr.operator(), operandValue);
        } else if (expr instanceof LiteralNode literal) {
            return QueryEvaluatorUtils.convertLiteralToValue(literal);
        } else {
            // For non-aggregate expressions in aggregate context, use first row
            if (!groupTable.getRows().isEmpty()) {
                return CellValue.of(evaluateExpressionInRowContext(expr, groupTable.getRows().get(0), groupTable.getColumns()));
            }
            return new CellValue.NullValue();
        }
    }
    
    /**
     * Compute sum of values in a specific column across group rows
     */
    private CellValue computeSum(List<JfrTable.Row> groupRows, int columnIndex) {
        return QueryEvaluatorUtils.computeSum(groupRows, columnIndex);
    }

    /**
     * Helper method: check if CellType is numeric
     */
    private boolean isNumericColumn(CellType type) {
        return QueryEvaluatorUtils.isNumericColumn(type);
    }
    
    /**
     * Execute SELECT clause on the processed data
     */
    private JfrTable executeSelectClause(JfrTable sourceData, SelectNode selectNode) {
        if (selectNode == null || selectNode.items().isEmpty()) {
            return sourceData; // Return all columns if no SELECT specified
        }
        // Check for SELECT * first
        if (selectNode.items().size() == 1) {
            SelectItemNode singleItem = selectNode.items().get(0);
            if (singleItem.expression() instanceof IdentifierNode id && "*".equals(id.name())) {
                // SELECT * - return all columns from source data
                return sourceData;
            }
        }
        // Build result columns based on SELECT items
        List<JfrTable.Column> resultColumns = new ArrayList<>();
        List<ExpressionNode> selectExpressions = new ArrayList<>();
        for (SelectItemNode selectItem : selectNode.items()) {
            String columnName = selectItem.alias() != null ? 
                selectItem.alias() : 
                QueryEvaluatorUtils.extractColumnName(selectItem.expression());
            // Determine column type by evaluating the expression on sample data
            CellType columnType = QueryEvaluatorUtils.determineColumnType(selectItem.expression(), sourceData, this);
            resultColumns.add(new JfrTable.Column(columnName, columnType));
            selectExpressions.add(selectItem.expression());
        }
        // Check if any SELECT expression contains aggregate functions
        boolean hasAggregateFunction = selectExpressions.stream()
            .anyMatch(QueryEvaluatorUtils::containsAggregateFunction);
        JfrTable result = new StandardJfrTable(resultColumns);
        
        if (hasAggregateFunction) {
            // Pre-compute aggregate values that will be used in row-level expressions
            Map<String, CellValue> precomputedAggregates = QueryEvaluatorUtils.precomputeAggregatesInSelect(selectExpressions, sourceData, this);
            
            // Check if this is a pure aggregate query (all expressions are aggregates or literals)
            boolean isPureAggregate = selectExpressions.stream()
                .allMatch(expr -> QueryEvaluatorUtils.containsAggregateFunction(expr) || 
                                 expr instanceof LiteralNode);
            
            if (isPureAggregate) {
                // Pure aggregate query - return single row with aggregate results
                List<CellValue> resultCells = new ArrayList<>();
                for (ExpressionNode expr : selectExpressions) {
                    CellValue cellValue = evaluateAggregateExpression(expr, precomputedAggregates);
                    resultCells.add(cellValue);
                }
                result.addRow(new JfrTable.Row(resultCells));
            } else {
                // Mixed aggregate/non-aggregate expressions, evaluate for each row
                // but use pre-computed aggregate values where needed
                for (JfrTable.Row sourceRow : sourceData.getRows()) {
                    List<CellValue> resultCells = new ArrayList<>();
                    for (ExpressionNode expr : selectExpressions) {
                        CellValue cellValue = CellValue.of(evaluateExpressionInRowContext(expr, sourceRow, sourceData.getColumns()));
                        resultCells.add(cellValue);
                    }
                    result.addRow(new JfrTable.Row(resultCells));
                }
            }
        } else {
            // Handle non-aggregate query - evaluate expressions for each row
            for (JfrTable.Row sourceRow : sourceData.getRows()) {
                List<CellValue> resultCells = new ArrayList<>();
                for (ExpressionNode expr : selectExpressions) {
                    CellValue cellValue = QueryEvaluatorUtils.evaluateExpressionForRow(sourceRow, expr, sourceData.getColumns(), this);
                    resultCells.add(cellValue);
                }
                result.addRow(new JfrTable.Row(resultCells));
            }
        }
        return result;
    }
    
    /**
     * Apply ORDER BY clause to sort the data
     */
    /**
     * Apply ORDER BY clause to sort the result set.
     * 
     * Supports:
     * - Multiple sort fields with different directions
     * - Complex expressions (functions, arithmetic, field access)
     * - Proper error handling for missing columns or invalid expressions
     * - ASC/DESC sort orders
     */
    private JfrTable applyOrderByClause(JfrTable sourceData, OrderByNode orderByNode) {
        if (orderByNode.fields().isEmpty()) {
            return sourceData; // No sorting needed
        }
        
        List<JfrTable.Row> sortedRows = new ArrayList<>(sourceData.getRows());
        
        // Multi-field sorting with proper precedence
        sortedRows.sort((row1, row2) -> {
            for (OrderFieldNode orderField : orderByNode.fields()) {
                try {
                    // Evaluate the expression for both rows
                    CellValue val1 = evaluateOrderByExpression(orderField.field(), row1, sourceData);
                    CellValue val2 = evaluateOrderByExpression(orderField.field(), row2, sourceData);
                    
                    // Compare values
                    int comparison = CellValue.compare(val1, val2);
                    
                    if (comparison != 0) {
                        // Apply sort direction
                        return orderField.order() == SortOrder.ASC ? comparison : -comparison;
                    }
                    // If equal, continue to next sort field
                } catch (Exception e) {
                    throw OrderByEvaluationException.expressionFailure(
                        orderField.field().toString(), 
                        e.getMessage(), 
                        null
                    );
                }
            }
            return 0; // All fields are equal
        });
        
        // Create result table with sorted rows
        JfrTable result = new StandardJfrTable(sourceData.getColumns());
        for (JfrTable.Row row : sortedRows) {
            result.addRow(row);
        }
        
        return result;
    }
    
    /**
     * Evaluate an ORDER BY expression for a specific row.
     * Handles field references, function calls, and complex expressions.
     */
    private CellValue evaluateOrderByExpression(ExpressionNode expr, JfrTable.Row row, JfrTable table) throws Exception {
        if (expr instanceof IdentifierNode identifier) {
            // Simple field reference
            return getRowValueByColumnName(row, table.getColumns(), identifier.name());
        } else if (expr instanceof FieldAccessNode fieldAccess) {
            // Table.field reference
            return getRowValueByColumnName(row, table.getColumns(), fieldAccess.field());
        } else if (expr instanceof FunctionCallNode functionCall) {
            // Function call - evaluate with current row context
            return evaluateFunctionCallInRowContext(functionCall, row, table);
        } else if (expr instanceof BinaryExpressionNode binaryExpr) {
            // Arithmetic or comparison expression
            CellValue left = evaluateOrderByExpression(binaryExpr.left(), row, table);
            CellValue right = evaluateOrderByExpression(binaryExpr.right(), row, table);
            return performArithmeticOperationWithMap(left, right, binaryExpr.operator());
        } else if (expr instanceof UnaryExpressionNode unaryExpr) {
            // Unary expression (e.g., -duration)
            CellValue operand = evaluateOrderByExpression(unaryExpr.operand(), row, table);
            return performUnaryOperation(unaryExpr.operator(), operand);
        } else if (expr instanceof LiteralNode literal) {
            // Literal value
            return literal.value();
        } else if (expr instanceof PercentileFunctionNode percentileFunc) {
            // Percentile functions need special handling in ORDER BY context
            // For ORDER BY, we can't calculate percentiles per row - this should be an error
            throw QueryExecutorException.forConfigurationError(
                "percentile function",
                "Percentile functions cannot be used in ORDER BY without GROUP BY",
                percentileFunc,
                null
            );
        } else {
            throw ExpressionEvaluationException.forFieldAccessError(
                "ORDER BY expression", new String[]{"identifier", "function", "literal", "arithmetic"},
                expr
            );
        }
    }
    
    /**
     * Evaluate a function call in the context of a single row.
     */
    private CellValue evaluateFunctionCallInRowContext(FunctionCallNode functionCall, JfrTable.Row row, JfrTable table) throws Exception {
        String functionName = functionCall.functionName().toLowerCase();
        
        // Check for aggregate functions - they might be valid if this is a GROUP BY result
        if (isAggregateFunction(functionName)) {
            // Try to find a column that matches this aggregate function
            // This handles cases like ORDER BY COUNT(*) where COUNT(*) is already computed in GROUP BY
            String aggregateKey = QueryEvaluatorUtils.createAggregateKey(functionCall);
            
            // Look for a column that matches this aggregate in several ways:
            // 1. Exact function name match (COUNT, SUM, etc.)
            // 2. Aggregate key match 
            // 3. Common aliases (count for COUNT(*), etc.)
            for (JfrTable.Column column : table.getColumns()) {
                String columnName = column.name().toLowerCase();
                String functionNameLower = functionName.toLowerCase();
                
                if (columnName.equals(functionNameLower) || 
                    columnName.equals(aggregateKey.toLowerCase()) ||
                    (functionNameLower.equals("count") && columnName.equals("count")) ||
                    columnName.startsWith(functionNameLower + "(")) {
                    // Found matching column - use its value
                    return getRowValueByColumnName(row, table.getColumns(), column.name());
                }
            }
            
            // Special handling for COUNT(*) - look for any column with "count" in the name
            if (functionCall.functionName().equalsIgnoreCase("COUNT") && 
                functionCall.arguments().size() == 1 &&
                functionCall.arguments().get(0) instanceof IdentifierNode &&
                ((IdentifierNode) functionCall.arguments().get(0)).name().equals("*")) {
                
                for (JfrTable.Column column : table.getColumns()) {
                    if (column.name().toLowerCase().contains("count")) {
                        return getRowValueByColumnName(row, table.getColumns(), column.name());
                    }
                }
            }
            
            // If no matching column found, this is likely ORDER BY with aggregate but no GROUP BY
            throw AggregationEvaluationException.forInvalidState(
                functionCall.functionName(), 
                "aggregate function used in ORDER BY without GROUP BY",
                AggregationEvaluationException.AggregationContext.SELECT_AGGREGATE,
                functionCall
            );
        }
        
        // Evaluate function arguments in row context with error handling
        List<CellValue> argValues = new ArrayList<>();
        for (int i = 0; i < functionCall.arguments().size(); i++) {
            try {
                ExpressionNode arg = functionCall.arguments().get(i);
                CellValue argValue = evaluateOrderByExpression(arg, row, table);
                argValues.add(argValue);
            } catch (Exception e) {
                throw FunctionArgumentException.forInvalidValue(
                    functionCall.functionName(), i, 
                    functionCall.arguments().get(i).toString(),
                    "evaluable expression in ORDER BY context",
                    functionCall.arguments().get(i)
                );
            }
        }
        
        // Evaluate the function using the function registry
        try {
            return functionRegistry.evaluateFunction(functionCall.functionName(), argValues, evaluationContext);
        } catch (IllegalArgumentException e) {
            // Handle argument count or type validation errors
            if (e.getMessage().contains("arguments") || e.getMessage().contains("parameter")) {
                throw FunctionArgumentException.forWrongArgumentCount(
                    functionCall.functionName(), -1, argValues.size(), functionCall
                );
            }
            // Re-throw as FunctionEvaluationException with ORDER BY context
            throw FunctionEvaluationException.forArgumentTypeMismatch(
                functionCall.functionName(), argValues, null, functionCall
            );
        } catch (Exception e) {
            throw FunctionEvaluationException.forRuntimeError(
                functionCall.functionName(), argValues,
                FunctionEvaluationException.FunctionContext.ORDER_BY_CONTEXT,
                functionCall, e
            );
        }
    }
    
    /**
     * Perform unary operation on a cell value.
     */
    private CellValue performUnaryOperation(UnaryOperator operator, CellValue operand) {
        return switch (operator) {
            case MINUS -> {
                if (operand instanceof CellValue.NumberValue numVal) {
                    yield new CellValue.NumberValue(-numVal.value());
                } else if (operand instanceof CellValue.DurationValue durVal) {
                    yield new CellValue.DurationValue(durVal.value().negated());
                } else {
                    throw ExpressionEvaluationException.forTypeConversionError(
                        "unary minus operation", operand, "numeric type", null
                    );
                }
            }
            case NOT -> {
                if (operand instanceof CellValue.BooleanValue boolVal) {
                    yield new CellValue.BooleanValue(!boolVal.value());
                } else {
                    throw ExpressionEvaluationException.forTypeConversionError(
                        "unary NOT operation", operand, "boolean type", null
                    );
                }
            }
        };
    }
    
    /**
     * Apply LIMIT clause to restrict the number of rows
     */
    private JfrTable applyLimitClause(JfrTable sourceData, LimitNode limitNode) {
        int limit = limitNode.limit();
        
        JfrTable result = new StandardJfrTable(sourceData.getColumns());
        
        int count = 0;
        for (JfrTable.Row row : sourceData.getRows()) {
            if (count >= limit) break;
            result.addRow(row);
            count++;
        }
        
        return result;
    }
    
    /**
     * Apply HAVING clause to filter aggregated groups using proper expression evaluation
     */
    private JfrTable applyHavingClause(JfrTable input, HavingNode havingNode) {
        if (havingNode == null || havingNode.condition() == null) {
            return input;
        }
        
        // Create a new table with the same columns
        JfrTable result = new StandardJfrTable(input.getColumns());
        
        // Evaluate the HAVING condition for each row using proper expression evaluation
        for (JfrTable.Row row : input.getRows()) {
            try {
                if (evaluateConditionForRow(row, havingNode.condition(), input.getColumns())) {
                    result.addRow(row);
                }
            } catch (Exception e) {
                // If evaluation fails, exclude the row but log the error
                // In a production system, this might be logged
                System.err.println("Error evaluating HAVING condition for row: " + e.getMessage());
            }
        }
        
        return result;
    }
    
    @Override
    public JfrTable visitSelect(SelectNode node) {
        // Select node processing is handled within visitQuery
        return context.getCurrentResult();
    }
    
    @Override
    public JfrTable visitFrom(FromNode node) {
        // From node processing is handled within visitQuery
        return context.getCurrentResult();
    }
    
    @Override
    public JfrTable visitSource(SourceNode node) {
        try {
            // Initialize event type variables if not done yet
            initializeEventTypeVariables();
            
            // First check if this source is a lazy variable
            LazyQuery lazyQuery = context.getLazyVariable(node.source());
            if (lazyQuery != null) {
                JfrTable result = lazyQuery.evaluate();
                
                // Store the result in context if alias is provided
                if (node.alias() != null && !node.alias().isEmpty()) {
                    context.setIntermediateResult(node.alias(), result);
                }
                
                return result;
            }
            
            // Check if this source is a view
            QueryNode viewQuery = context.getView(node.source());
            if (viewQuery != null) {
                JfrTable result = viewQuery.accept(this);
                
                // Store the result in context if alias is provided
                if (node.alias() != null && !node.alias().isEmpty()) {
                    context.setIntermediateResult(node.alias(), result);
                }
                
                return result;
            }
            
            // Otherwise, create a raw JFR query to select all events from the specified source
            String rawQuery = "SELECT * FROM " + node.source();
            RawJfrQueryNode rawQueryNode = new RawJfrQueryNode(rawQuery, node.location());
            
            // Execute the raw query to get the table for this source
            JfrTable result = jfrQuery(rawQueryNode);
            
            // Store the result in context if alias is provided
            if (node.alias() != null && !node.alias().isEmpty()) {
                context.setIntermediateResult(node.alias(), result);
            }
            
            return result;
        } catch (Exception e) {
            throw SourceTableException.dataSourceFailure(node.source(), e.getMessage(), null);
        }
    }
    
    @Override
    public JfrTable visitSubquerySource(SubquerySourceNode node) {
        // Execute the subquery with proper scope management
        evaluationContext.pushScope();
        try {
            return node.query().accept(this);
        } finally {
            evaluationContext.popScope();
        }
    }
    
    @Override
    public JfrTable visitWhere(WhereNode node) {
        // Where clause processing is handled within visitQuery
        return context.getCurrentResult();
    }
    
    @Override
    public JfrTable visitGroupBy(GroupByNode node) {
        // Group by processing is handled within visitQuery
        return context.getCurrentResult();
    }
    
    @Override
    public JfrTable visitHaving(HavingNode node) {
        // Having processing is handled within visitQuery
        return context.getCurrentResult();
    }
    
    @Override
    public JfrTable visitOrderBy(OrderByNode node) {
        // Order by processing is handled within visitQuery
        return context.getCurrentResult();
    }
    
    @Override
    public JfrTable visitOrderField(OrderFieldNode node) {
        // Order field processing is handled within visitQuery
        return context.getCurrentResult();
    }
    
    @Override
    public JfrTable visitLimit(LimitNode node) {
        // Limit processing is handled within visitQuery
        return context.getCurrentResult();
    }
    
    @Override
    public JfrTable visitFormatter(FormatterNode node) {
        // Formatter processing doesn't change the data, only presentation
        return context.getCurrentResult();
    }
    
    @Override
    public JfrTable visitProperty(PropertyNode node) {
        // Property processing doesn't change the data, only presentation
        return context.getCurrentResult();
    }
    
    @Override
    public JfrTable visitExpression(ExpressionNode node) {
        // Expression evaluation - delegate to appropriate handler
        return context.getCurrentResult();
    }
    
    @Override
    public JfrTable visitBinaryExpression(BinaryExpressionNode node) {
        // Binary expression evaluation
        return evaluateBinaryExpression(node);
    }
    
    private JfrTable evaluateBinaryExpression(BinaryExpressionNode node) {
        // Evaluate left and right operands
        JfrTable leftTable = node.left().accept(this);
        JfrTable rightTable = node.right().accept(this);
        
        // For arithmetic operations, we need to extract values and perform operations
        BinaryOperator operator = node.operator();
        
        switch (operator) {
            case ADD, SUBTRACT, MULTIPLY, DIVIDE, MODULO -> {
                return performArithmeticOperation(leftTable, rightTable, operator, node);
            }
            case IN -> {
                return performInOperation(leftTable, rightTable, node);
            }
            case EQUALS, NOT_EQUALS, LESS_THAN, GREATER_THAN, LESS_EQUAL, GREATER_EQUAL -> {
                return performComparisonOperation(leftTable, rightTable, operator, node);
            }
            case LIKE, NOT_LIKE -> {
                return performLikeOperation(leftTable, rightTable, operator, node);
            }
            case WITHIN -> {
                return performWithinOperation(leftTable, rightTable, node);
            }
            case OF -> {
                // OF operator is used as part of WITHIN expressions: timeWindow OF referenceTime
                // We need to create a special wrapper that contains both values
                CellValue timeWindow = extractSingleCellValue(leftTable);
                CellValue referenceTime = extractSingleCellValue(rightTable);
                
                // Validate that timeWindow is numeric (represents duration)
                if (!(timeWindow instanceof CellValue.NumberValue)) {
                    throw ExpressionEvaluationException.forTypeConversionError(
                        "OF operator time window (left operand)", 
                        timeWindow, 
                        "number representing time duration (e.g., 5000 for 5000 nanoseconds)", 
                        node,
                        "binary operation with operator OF (duration validation)"
                    );
                }
                if (!(referenceTime instanceof CellValue.NumberValue || referenceTime instanceof CellValue.TimestampValue)) {
                    throw ExpressionEvaluationException.forTypeConversionError(
                        "OF operator reference time (right operand)", 
                        referenceTime, 
                        "timestamp or number for temporal reference (e.g., 1000000000000)", 
                        node,
                        "binary operation with operator OF (reference time validation)"
                    );
                }
                
                // Create a composite structure to pass time window info to WITHIN operation
                // We'll use a special array with [timeWindow, referenceTime]
                java.util.List<CellValue> withinData = java.util.List.of(
                    timeWindow,
                    referenceTime
                );
                
                return SingleCellTable.of(new CellValue.ArrayValue(withinData));
            }
            default -> {
                // For unsupported operations, return current result
                return context.getCurrentResult();
            }
        }
    }
    
    /**
     * Performs arithmetic operations on two tables
     * Supports all numeric types: NUMBER, FLOAT, DURATION, TIMESTAMP, MEMORY_SIZE, RATE
     * Uses functional map approach to preserve types
     */
    private JfrTable performArithmeticOperation(JfrTable leftTable, JfrTable rightTable, BinaryOperator operator) {
        // Extract values from tables
        CellValue leftValue = extractSingleCellValue(leftTable);
        CellValue rightValue = extractSingleCellValue(rightTable);
        
        // Perform arithmetic operation using map approach
        CellValue result = performArithmeticOperationWithMap(leftValue, rightValue, operator);
        
        // Use the optimized SingleCellTable for the result
        return new SingleCellTable("result", result);
    }
    
    /**
     * Enhanced version with node context for better error reporting
     */
    private JfrTable performArithmeticOperation(JfrTable leftTable, JfrTable rightTable, BinaryOperator operator, BinaryExpressionNode node) {
        // Extract values from tables
        CellValue leftValue = extractSingleCellValue(leftTable);
        CellValue rightValue = extractSingleCellValue(rightTable);
        
        try {
            // Perform arithmetic operation using map approach
            CellValue result = performArithmeticOperationWithMap(leftValue, rightValue, operator);
            
            // Use the optimized SingleCellTable for the result
            return new SingleCellTable("result", result);
        } catch (Exception e) {
            // Enhanced error reporting with node context
            throw ExpressionEvaluationException.forTypeConversionError(
                "arithmetic operation with operator " + operator,
                leftValue,
                rightValue.getType().toString(),
                node,
                "binary operation with operator " + operator + " (arithmetic operation)"
            );
        }
    }
    
    /**
     * Handle COLLECT function calls that need access to full table context
     * This method handles COLLECT when it's called outside of GROUP BY context
     */
    private JfrTable evaluateCollectFunctionCall(FunctionCallNode node) {
        if (node.arguments().isEmpty()) {
            throw AggregationEvaluationException.forInvalidState(
                "COLLECT", "no arguments provided", 
                AggregationEvaluationException.AggregationContext.SELECT_AGGREGATE, 
                node
            );
        }
        
        // For COLLECT in SELECT context, we need to delegate to the aggregate computation
        // to ensure it gets the right source table context
        try {
            // Get the current source table from context
            JfrTable sourceTable = context.getCurrentResult();
            
            // Validate that we have the source table and can evaluate the expression
            ExpressionNode expr = node.arguments().get(0);
            if (expr instanceof IdentifierNode identifier) {
                String columnName = identifier.name();
                if (sourceTable != null) {
                    boolean hasColumn = sourceTable.getColumns().stream()
                        .anyMatch(col -> col.name().equals(columnName));
                    
                    if (!hasColumn) {
                        // Column not found in current table - this might be a context issue
                        String[] availableColumns = sourceTable.getColumns().stream()
                            .map(c -> c.name()).toArray(String[]::new);
                        throw new ColumnNotFoundException(columnName, availableColumns, expr);
                    }
                }
            }
            
            // Use the standard aggregate computation path
            if (sourceTable != null) {
                CellValue result = computeAggregateValue(node, sourceTable);
                return SingleCellTable.of("result", result);
            } else {
                // No source table available
                CellValue emptyArray = new CellValue.ArrayValue(List.of());
                return SingleCellTable.of("result", emptyArray);
            }
            
        } catch (Exception e) {
            // If any error occurs during evaluation, wrap and rethrow
            if (e instanceof ColumnNotFoundException || e instanceof AggregationEvaluationException) {
                throw e;
            }
            
            throw AggregationEvaluationException.forInvalidState(
                "COLLECT", "error evaluating expression: " + e.getMessage(), 
                AggregationEvaluationException.AggregationContext.SELECT_AGGREGATE, 
                node
            );
        }
    }
    
    /**
     * Extracts a single CellValue from a table (assuming single-cell table)
     * with proper error handling for invalid table structures
     */
    private CellValue extractSingleCellValue(JfrTable table) {
        if (table == null) {
            throw new IllegalArgumentException("Cannot extract value from null table");
        }
        
        if (table.getRows().isEmpty()) {
            // Return null value for empty tables
            return new CellValue.NullValue();
        }
        
        JfrTable.Row firstRow = table.getRows().get(0);
        if (firstRow.getCells().isEmpty()) {
            // Return null value for rows with no cells
            return new CellValue.NullValue();
        }
        
        if (table.getRows().size() > 1) {
            // Log warning for multiple rows - this might indicate a problem
            System.err.println("Warning: extractSingleCellValue called on table with " + 
                             table.getRows().size() + " rows, using first row only");
        }
        
        return firstRow.getCells().get(0);
    }
    
    /**
     * Performs arithmetic operations on CellValues using the new map-based approach
     * with comprehensive type compatibility checking
     */
    private CellValue performArithmeticOperationWithMap(CellValue left, CellValue right, BinaryOperator operator) {
        // Use the new mapBinary method with BinaryOperator
        return left.mapBinary(right, operator);
    }
    
    /**
     * Check if CellValue is numeric
     */
    private boolean isNumericValue(CellValue cellValue) {
        return QueryEvaluatorUtils.isNumericValue(cellValue);
    }
    
    /**
     * Perform IN operation
     */
    private JfrTable performInOperation(JfrTable leftTable, JfrTable rightTable) {
        CellValue leftValue = extractSingleCellValue(leftTable);
        CellValue rightValue = extractSingleCellValue(rightTable);
        
        boolean result;
        if (rightValue instanceof CellValue.ArrayValue arrayValue) {
            result = arrayValue.contains(leftValue);
        } else {
            result = leftValue.equals(rightValue);
        }
        
        return SingleCellTable.of(new CellValue.BooleanValue(result));
    }
    
    /**
     * Enhanced version with node context for better error reporting
     */
    private JfrTable performInOperation(JfrTable leftTable, JfrTable rightTable, BinaryExpressionNode node) {
        CellValue leftValue = extractSingleCellValue(leftTable);
        CellValue rightValue = extractSingleCellValue(rightTable);
        
        try {
            boolean result;
            if (rightValue instanceof CellValue.ArrayValue arrayValue) {
                result = arrayValue.contains(leftValue);
            } else {
                result = leftValue.equals(rightValue);
            }
            
            return SingleCellTable.of(new CellValue.BooleanValue(result));
        } catch (Exception e) {
            // Enhanced error reporting with node context
            throw ExpressionEvaluationException.forTypeConversionError(
                "IN operation",
                leftValue,
                "value compatible with collection or single value",
                node,
                "binary operation with operator IN"
            );
        }
    }
    
    /**
     * Perform comparison operations
     */
    private JfrTable performComparisonOperation(JfrTable leftTable, JfrTable rightTable, BinaryOperator operator) {
        CellValue leftValue = extractSingleCellValue(leftTable);
        CellValue rightValue = extractSingleCellValue(rightTable);
        
        int comparison = CellValue.compare(leftValue, rightValue);
        boolean result = switch (operator) {
            case EQUALS -> comparison == 0;
            case NOT_EQUALS -> comparison != 0;
            case LESS_THAN -> comparison < 0;
            case GREATER_THAN -> comparison > 0;
            case LESS_EQUAL -> comparison <= 0;
            case GREATER_EQUAL -> comparison >= 0;
            default -> throw ExpressionEvaluationException.forTypeConversionError(
                "comparison operation with operator " + operator, 
                new CellValue.StringValue(operator.toString()), 
                "supported comparison operator (EQUALS, NOT_EQUALS, LESS_THAN, GREATER_THAN, LESS_EQUAL, GREATER_EQUAL)", 
                null
            );
        };
        
        return SingleCellTable.of(new CellValue.BooleanValue(result));
    }
    
    /**
     * Enhanced version with node context for better error reporting
     */
    private JfrTable performComparisonOperation(JfrTable leftTable, JfrTable rightTable, BinaryOperator operator, BinaryExpressionNode node) {
        CellValue leftValue = extractSingleCellValue(leftTable);
        CellValue rightValue = extractSingleCellValue(rightTable);
        
        try {
            int comparison = CellValue.compare(leftValue, rightValue);
            boolean result = switch (operator) {
                case EQUALS -> comparison == 0;
                case NOT_EQUALS -> comparison != 0;
                case LESS_THAN -> comparison < 0;
                case GREATER_THAN -> comparison > 0;
                case LESS_EQUAL -> comparison <= 0;
                case GREATER_EQUAL -> comparison >= 0;
                default -> throw ExpressionEvaluationException.forTypeConversionError(
                    "comparison operation with operator " + operator, 
                    new CellValue.StringValue(operator.toString()), 
                    "supported comparison operator (EQUALS, NOT_EQUALS, LESS_THAN, GREATER_THAN, LESS_EQUAL, GREATER_EQUAL)", 
                    node,
                    "binary operation with operator " + operator + " (comparison operation)"
                );
            };
            
            return SingleCellTable.of(new CellValue.BooleanValue(result));
        } catch (Exception e) {
            // Enhanced error reporting with node context
            if (e instanceof ExpressionEvaluationException) {
                throw e;
            }
            throw ExpressionEvaluationException.forTypeConversionError(
                "comparison operation with operator " + operator,
                leftValue,
                rightValue.getType().toString(),
                node,
                "binary operation with operator " + operator + " (comparison operation)"
            );
        }
    }
    
    /**
     * Performs LIKE and NOT LIKE operations on two tables
     * Supports SQL-style pattern matching with % (any characters) and _ (single character)
     */
    private JfrTable performLikeOperation(JfrTable leftTable, JfrTable rightTable, BinaryOperator operator) {
        CellValue leftValue = extractSingleCellValue(leftTable);
        CellValue rightValue = extractSingleCellValue(rightTable);
        
        if (!(leftValue instanceof CellValue.StringValue)) {
            throw ExpressionEvaluationException.forTypeConversionError(
                "LIKE operation left operand", leftValue, "string", null
            );
        }
        if (!(rightValue instanceof CellValue.StringValue)) {
            throw ExpressionEvaluationException.forTypeConversionError(
                "LIKE operation right operand (pattern)", rightValue, "string", null
            );
        }
        
        String text = ((CellValue.StringValue) leftValue).value();
        String pattern = ((CellValue.StringValue) rightValue).value();
        
        // Convert SQL LIKE pattern to Java regex
        // % matches any sequence of characters
        // _ matches exactly one character
        String regexPattern = pattern
            .replace("\\", "\\\\")  // Escape backslashes first
            .replace(".", "\\.")    // Escape dots
            .replace("*", "\\*")    // Escape asterisks
            .replace("+", "\\+")    // Escape plus signs
            .replace("?", "\\?")    // Escape question marks
            .replace("^", "\\^")    // Escape carets
            .replace("$", "\\$")    // Escape dollar signs
            .replace("{", "\\{")    // Escape braces
            .replace("}", "\\}")    
            .replace("(", "\\(")    // Escape parentheses
            .replace(")", "\\)")
            .replace("[", "\\[")    // Escape brackets
            .replace("]", "\\]")
            .replace("|", "\\|")    // Escape pipes
            .replace("%", ".*")     // % becomes .*
            .replace("_", ".");     // _ becomes .
        
        boolean matches = text.matches(regexPattern);
        boolean result = switch (operator) {
            case LIKE -> matches;
            case NOT_LIKE -> !matches;
            default -> throw ExpressionEvaluationException.forTypeConversionError(
                "LIKE operator", 
                new CellValue.StringValue(operator.toString()), 
                "LIKE or NOT_LIKE", 
                null
            );
        };
        
        return SingleCellTable.of(new CellValue.BooleanValue(result));
    }
    
    /**
     * Enhanced version with node context for better error reporting
     */
    private JfrTable performLikeOperation(JfrTable leftTable, JfrTable rightTable, BinaryOperator operator, BinaryExpressionNode node) {
        CellValue leftValue = extractSingleCellValue(leftTable);
        CellValue rightValue = extractSingleCellValue(rightTable);
        
        if (!(leftValue instanceof CellValue.StringValue)) {
            throw ExpressionEvaluationException.forTypeConversionError(
                "LIKE operation left operand", leftValue, "string", node,
                "binary operation with operator " + operator + " (left operand validation)"
            );
        }
        if (!(rightValue instanceof CellValue.StringValue)) {
            throw ExpressionEvaluationException.forTypeConversionError(
                "LIKE operation right operand (pattern)", rightValue, "string", node,
                "binary operation with operator " + operator + " (pattern validation)"
            );
        }
        
        try {
            String text = ((CellValue.StringValue) leftValue).value();
            String pattern = ((CellValue.StringValue) rightValue).value();
            
            // Convert SQL LIKE pattern to Java regex
            // % matches any sequence of characters
            // _ matches exactly one character
            String regexPattern = pattern
                .replace("\\", "\\\\")  // Escape backslashes first
                .replace(".", "\\.")    // Escape dots
                .replace("*", "\\*")    // Escape asterisks
                .replace("+", "\\+")    // Escape plus signs
                .replace("?", "\\?")    // Escape question marks
                .replace("^", "\\^")    // Escape carets
                .replace("$", "\\$")    // Escape dollar signs
                .replace("{", "\\{")    // Escape braces
                .replace("}", "\\}")    
                .replace("(", "\\(")    // Escape parentheses
                .replace(")", "\\)")
                .replace("[", "\\[")    // Escape brackets
                .replace("]", "\\]")
                .replace("|", "\\|")    // Escape pipes
                .replace("%", ".*")     // % becomes .*
                .replace("_", ".");     // _ becomes .
            
            boolean matches = text.matches(regexPattern);
            boolean result = switch (operator) {
                case LIKE -> matches;
                case NOT_LIKE -> !matches;
                default -> throw ExpressionEvaluationException.forTypeConversionError(
                    "LIKE operator", 
                    new CellValue.StringValue(operator.toString()), 
                    "LIKE or NOT_LIKE", 
                    node,
                    "binary operation with operator " + operator + " (operator validation)"
                );
            };
            
            return SingleCellTable.of(new CellValue.BooleanValue(result));
        } catch (Exception e) {
            // Enhanced error reporting with node context
            if (e instanceof ExpressionEvaluationException) {
                throw e;
            }
            throw ExpressionEvaluationException.forTypeConversionError(
                "LIKE operation with operator " + operator,
                leftValue,
                "string pattern matching",
                node,
                "binary operation with operator " + operator + " (pattern matching)"
            );
        }
    }
    
    /**
     * Performs a WITHIN operation to check temporal proximity.
     * Syntax: value WITHIN timeWindow OF referenceTime
     * For example: timestamp WITHIN 5m OF startTime
     * 
     * The rightTable should contain the result of the "timeWindow OF referenceTime" expression,
     * which stores both the time window and reference time values as an array.
     * 
     * @param leftTable the value to check (timestamp)
     * @param rightTable the result of "timeWindow OF referenceTime" expression (array with [timeWindow, referenceTime])
     * @return a single-cell table with a boolean result
     */
    private JfrTable performWithinOperation(JfrTable leftTable, JfrTable rightTable, BinaryExpressionNode node) {
        CellValue valueToCheck = extractSingleCellValue(leftTable);
        CellValue rightValue = extractSingleCellValue(rightTable);
        
        // Validate value to check
        if (!(valueToCheck instanceof CellValue.NumberValue || valueToCheck instanceof CellValue.TimestampValue)) {
            throw ExpressionEvaluationException.forTypeConversionError(
                "WITHIN condition value to check (left operand)", 
                valueToCheck, 
                "timestamp or number for temporal comparison (e.g., 1000000003000)", 
                node,
                "binary operation with operator WITHIN (left operand validation)"
            );
        }
        
        // Extract time window and reference time from the OF operation result
        CellValue timeWindowValue;
        CellValue referenceTime;
        
        if (rightValue instanceof CellValue.ArrayValue) {
            // This is the result from "timeWindow OF referenceTime"
            CellValue.ArrayValue arrayValue = (CellValue.ArrayValue) rightValue;
            
            if (arrayValue.size() != 2) {
                throw ExpressionEvaluationException.forTypeConversionError(
                    "WITHIN operation OF expression result", 
                    rightValue, 
                    "array with exactly 2 elements [timeWindow, referenceTime]", 
                    node,
                    "binary operation with operator WITHIN (OF expression structure validation)"
                );
            }
            
            timeWindowValue = arrayValue.get(0);
            referenceTime = arrayValue.get(1);
        } else {
            // Fallback: treat right value as reference time with default window
            timeWindowValue = new CellValue.NumberValue(1_000_000_000.0); // 1 second default
            referenceTime = rightValue;
        }
        
        // Validate time window and reference time
        if (!(timeWindowValue instanceof CellValue.NumberValue)) {
            throw ExpressionEvaluationException.forTypeConversionError(
                "WITHIN condition time window (from OF operation)", 
                timeWindowValue, 
                "number representing time duration (e.g., 5000 for 5000 nanoseconds)", 
                node,
                "binary operation with operator WITHIN (time window validation)"
            );
        }
        if (!(referenceTime instanceof CellValue.NumberValue || referenceTime instanceof CellValue.TimestampValue)) {
            throw ExpressionEvaluationException.forTypeConversionError(
                "WITHIN condition reference time (from OF operation)", 
                referenceTime, 
                "timestamp or number for temporal comparison (e.g., 1000000000000)", 
                node,
                "binary operation with operator WITHIN (reference time validation)"
            );
        }
        
        // Extract numeric values (timestamps as nanoseconds)
        long valueTime = extractTimeValue(valueToCheck, node);
        long refTime = extractTimeValue(referenceTime, node);
        long timeWindowNanos = convertToNanoseconds(((CellValue.NumberValue) timeWindowValue).value());
        
        // Check if the value is within the time window of the reference time
        long timeDifference = Math.abs(valueTime - refTime);
        boolean isWithin = timeDifference <= timeWindowNanos;
        
        return SingleCellTable.of(new CellValue.BooleanValue(isWithin));
    }
    
    /**
     * Extracts a time value in nanoseconds from a CellValue.
     */
    private long extractTimeValue(CellValue value, BinaryExpressionNode node) {
        return switch (value) {
            case CellValue.NumberValue num -> (long) num.value(); // Cast double to long
            case CellValue.TimestampValue timestamp -> timestamp.value().toEpochMilli() * 1_000_000L; // Convert to nanoseconds
            default -> throw ExpressionEvaluationException.forTypeConversionError(
                "time extraction", 
                value, 
                "timestamp or number (representing nanoseconds)", 
                node,
                "binary operation with operator WITHIN (time value extraction)"
            );
        };
    }
    
    /**
     * Overloaded version for backward compatibility where node is not available
     */
    private long extractTimeValue(CellValue value) {
        return switch (value) {
            case CellValue.NumberValue num -> (long) num.value(); // Cast double to long
            case CellValue.TimestampValue timestamp -> timestamp.value().toEpochMilli() * 1_000_000L; // Convert to nanoseconds
            default -> throw ExpressionEvaluationException.forTypeConversionError(
                "time extraction", 
                value, 
                "timestamp or number (representing nanoseconds)", 
                null,
                "time value extraction (no node context available)"
            );
        };
    }
    
    @Override
    public JfrTable visitUnaryExpression(UnaryExpressionNode node) {
        // Unary expression evaluation
        return context.getCurrentResult();
    }
    
    @Override
    public JfrTable visitFieldAccess(FieldAccessNode node) {
        // Field access evaluation
        return context.getCurrentResult();
    }
    
    @Override
    public JfrTable visitFunctionCall(FunctionCallNode node) {
        // Function call evaluation using the function registry
        return evaluateFunctionCall(node);
    }
    
    private JfrTable evaluateFunctionCall(FunctionCallNode node) {
        String functionName = node.functionName();
        boolean distinct = node.distinct();
        
        if (!functionRegistry.isFunction(functionName)) {
            Set<String> availableFunctions = functionRegistry.getFunctionNames();
            throw new UnknownFunctionException(functionName, availableFunctions, node);
        }
        
        // Handle DISTINCT for aggregate functions specially
        if (distinct && functionRegistry.isAggregateFunction(functionName)) {
            return evaluateDistinctAggregateFunction(node);
        }
        
        // Special handling for COLLECT function - needs access to full table context
        if ("COLLECT".equalsIgnoreCase(functionName)) {
            return evaluateCollectFunctionCall(node);
        }
        
        // Special handling for COUNT(*) - detect when COUNT has a StarMarkerTable argument
        if ("COUNT".equalsIgnoreCase(functionName) && node.arguments().size() == 1) {
            ExpressionNode firstArg = node.arguments().get(0);
            JfrTable argTable = firstArg.accept(this);
            
            // Check if this is COUNT(*) by seeing if the argument evaluates to StarMarkerTable
            if (StarMarkerTable.isStarMarker(argTable)) {
                // For COUNT(*), we need to get the row count from the current evaluation context
                JfrTable currentResult = context.getCurrentResult();
                if (currentResult != null) {
                    long rowCount = currentResult.getRowCount();
                    CellValue result = new CellValue.NumberValue(rowCount);
                    return SingleCellTable.of("result", result);
                } else {
                    // Fallback: return 0 if no current result
                    CellValue result = new CellValue.NumberValue(0L);
                    return SingleCellTable.of("result", result);
                }
            }
        }
        
        // Evaluate all arguments with proper error handling
        List<CellValue> arguments = new ArrayList<>();
        for (int i = 0; i < node.arguments().size(); i++) {
            ExpressionNode arg = node.arguments().get(i);
            try {
                JfrTable argTable = arg.accept(this);
                CellValue argValue = extractSingleCellValue(argTable);
                
                // Validate argument is not null for critical functions
                if (argValue instanceof CellValue.NullValue) {
                    // Some functions may not handle nulls well - this could be enhanced
                    // with function metadata about null handling
                    if (functionName.equalsIgnoreCase("HEAD") || functionName.equalsIgnoreCase("TAIL")) {
                        throw FunctionArgumentException.forNullArgument(functionName, i, arg);
                    }
                }
                
                arguments.add(argValue);
            } catch (Exception e) {
                if (e instanceof FunctionArgumentException) {
                    throw e; // Re-throw function argument exceptions
                }
                
                // Wrap other exceptions as function argument errors
                throw FunctionArgumentException.forInvalidValue(
                    functionName, i, arg.toString(), 
                    "evaluable expression", arg
                );
            }
        }
        
        try {
            // Use the function registry to evaluate the function with proper context
            CellValue result = functionRegistry.evaluateFunction(functionName, arguments, evaluationContext);
            
            // Return result as a single-cell table
            return SingleCellTable.of("result", result);
            
        } catch (IllegalArgumentException e) {
            // Check if this is an argument count error
            if (e.getMessage().contains("arguments") || e.getMessage().contains("parameter")) {
                // Try to extract expected vs actual count from message
                throw FunctionArgumentException.forWrongArgumentCount(
                    functionName, -1, arguments.size(), node
                );
            }
            
            // Other argument validation errors - use FunctionEvaluationException factory method
            throw FunctionEvaluationException.forArgumentTypeMismatch(
                functionName, arguments, null, node
            );
        } catch (Exception e) {
            // Handle runtime errors during function execution
            throw FunctionEvaluationException.forRuntimeError(
                functionName, arguments, 
                FunctionEvaluationException.FunctionContext.ROW_CONTEXT, 
                node, e
            );
        }
    }
    
    @Override
    public JfrTable visitLiteral(LiteralNode node) {
        // LiteralNode already contains a CellValue, so just use it directly
        return SingleCellTable.of("value", node.value());
    }
    
    @Override
    public JfrTable visitIdentifier(IdentifierNode node) {
        // Identifier evaluation - look up variable or field
        String name = node.name();
        
        // Check evaluation context variables first (for function scope)
        Object contextValue = evaluationContext.getVariable(name);
        if (contextValue != null) {
            if (contextValue instanceof LazyQuery) {
                return ((LazyQuery) contextValue).evaluate();
            } else if (contextValue instanceof JfrTable) {
                return (JfrTable) contextValue;
            } else if (contextValue instanceof CellValue) {
                return SingleCellTable.of(name, (CellValue) contextValue);
            } else {
                return SingleCellTable.of(name, contextValue);
            }
        }
        
        // Check local context variables
        if (context.hasLocalVariable(name)) {
            Object value = context.getLocalVariable(name);
            if (value instanceof LazyQuery) {
                return ((LazyQuery) value).evaluate();
            } else if (value instanceof JfrTable) {
                return (JfrTable) value;
            } else if (value instanceof CellValue) {
                return SingleCellTable.of(name, (CellValue) value);
            } else if (value != null) {
                return SingleCellTable.of(name, value);
            }
        }
        
        // Check engine variables
        Object value = getVariable(name);
        if (value instanceof JfrTable) {
            return (JfrTable) value;
        } else if (value instanceof CellValue) {
            return SingleCellTable.of(name, (CellValue) value);
        } else if (value != null) {
            return SingleCellTable.of(name, value);
        }
        
        // Throw proper exception if not found
        throw new VariableNotFoundException(name, new String[0], node);
    }
    
    @Override
    public JfrTable visitNestedQuery(NestedQueryNode node) {
        // Execute nested JFR query with proper scope management
        evaluationContext.pushScope();
        try {
            if (rawJfrExecutor != null) {
                // Create a RawJfrQueryNode from the nested query string
                RawJfrQueryNode rawQuery = new RawJfrQueryNode(node.jfrQuery(), node.location());
                return rawJfrExecutor.execute(rawQuery);
            } else {
                // Fallback to placeholder result if no executor available
                List<JfrTable.Column> columns = List.of(new JfrTable.Column("nested_result", CellType.STRING));
                JfrTable result = new StandardJfrTable(columns);
                result.addRow(new JfrTable.Row(List.of(new CellValue.StringValue("Nested query: " + node.jfrQuery()))));
                return result;
            }
        } catch (Exception e) {
            // Don't wrap exceptions that are already Query related exceptions
            if (e instanceof QueryExecutionException) {
                throw (QueryExecutionException) e;
            }
            
            throw new QueryEvaluationException(
                "nested JFR query execution: " + node.jfrQuery(),
                node.jfrQuery(),
                QueryEvaluationException.EvaluationErrorType.UNSUPPORTED_OPERATION,
                node,
                e
            );
        } finally {
            evaluationContext.popScope();
        }
    }
    
    // Missing visitor methods required by ASTVisitor interface
    
    @Override
    public JfrTable visitRawJfrQuery(RawJfrQueryNode node) {
        try {
            return jfrQuery(node);
        } catch (Exception e) {
            throw new QueryEvaluationException(
                "nested JFR query execution", 
                null, 
                QueryEvaluationException.EvaluationErrorType.UNSUPPORTED_OPERATION, 
                node, 
                e
            );
        }
    }
    
    @Override
    public JfrTable visitSelectItem(SelectItemNode node) {
        throw new QueryEvaluationException(
            "select item evaluation", 
            "SelectItem should be handled by visitSelect method", 
            QueryEvaluationException.EvaluationErrorType.INVALID_STATE, 
            node
        );
    }
    
    @Override
    public JfrTable visitArrayLiteral(ArrayLiteralNode node) {
        List<CellValue> values = new ArrayList<>();
        for (ExpressionNode element : node.elements()) {
            JfrTable elementTable = element.accept(this);
            CellValue elementValue = extractSingleCellValue(elementTable);
            values.add(elementValue);
        }
        return SingleCellTable.of("array", new CellValue.ArrayValue(values));
    }
    
    @Override
    public JfrTable visitStar(StarNode node) {
        // Star (*) represents "all rows" or "count all" for aggregate functions like COUNT(*)
        // Return the specialized StarMarkerTable that aggregate functions can recognize
        return StarMarkerTable.getInstance();
    }
    
    @Override
    public JfrTable visitCondition(ConditionNode node) {
        throw new QueryEvaluationException(
            "condition evaluation",
            "Condition nodes should be handled in specific contexts (WHERE, HAVING, etc.)",
            QueryEvaluationException.EvaluationErrorType.INVALID_STATE,
            node
        );
    }
    
    @Override
    public JfrTable visitVariableDeclaration(VariableDeclarationNode node) {
        throw new QueryEvaluationException(
            "variable declaration",
            "Variable declarations should be handled by visitStatement method",
            QueryEvaluationException.EvaluationErrorType.INVALID_STATE,
            node
        );
    }
    
    @Override
    public JfrTable visitExpressionCondition(ExpressionConditionNode node) {
        return node.expression().accept(this);
    }
    
    @Override
    public JfrTable visitShowEvents(ShowEventsNode node) {
        initializeEventTypes();
        
        StringBuilder result = new StringBuilder();
        result.append("Event Types (number of events):\n");
        
        // Sort event types by name for consistent output
        List<EventType> sortedEventTypes = new ArrayList<>(eventTypes);
        sortedEventTypes.sort(Comparator.comparing(EventType::getName));
        
        for (EventType eventType : sortedEventTypes) {
            String name = eventType.getName();
            String simpleName = name.substring(name.lastIndexOf('.') + 1);
            result.append(simpleName).append(" (0) ");
        }
        
        return SingleCellTable.of("Events", result.toString().trim());
    }
    
    @Override
    public JfrTable visitShowFields(ShowFieldsNode node) {
        initializeEventTypes();
        
        String eventTypeName = node.eventType();
        StringBuilder result = new StringBuilder();
        
        // Find the matching event type
        EventType matchedEventType = null;
        for (EventType eventType : eventTypes) {
            String name = eventType.getName();
            String simpleName = name.substring(name.lastIndexOf('.') + 1);
            if (name.equals(eventTypeName) || simpleName.equals(eventTypeName)) {
                matchedEventType = eventType;
                break;
            }
        }
        
        if (matchedEventType == null) {
            result.append("Unknown event type: ").append(eventTypeName);
            return SingleCellTable.of("Error", result.toString());
        }
        
        // Format the output similar to the original JFR tool
        result.append(matchedEventType.getName()).append(":\n");
        
        List<ValueDescriptor> fields = matchedEventType.getFields();
        for (ValueDescriptor field : fields) {
            String typeName = field.getTypeName();
            // Simplify type names for common Java types
            if (typeName.startsWith("java.lang.")) {
                typeName = typeName.substring("java.lang.".length());
            }
            result.append(typeName).append(" ").append(field.getName()).append("\n");
        }
        
        return SingleCellTable.of("Fields", result.toString().trim());
    }
    
    @Override
    public JfrTable visitHelp(HelpNode node) {
        String helpContent = HelpProvider.getGeneralHelp();
        return SingleCellTable.of("Help", helpContent);
    }
    
    @Override
    public JfrTable visitHelpFunction(HelpFunctionNode node) {
        String functionName = node.functionName();
        String helpContent = HelpProvider.getFunctionHelp(functionName);
        return SingleCellTable.of("Function Help", helpContent);
    }
    
    @Override
    public JfrTable visitHelpGrammar(HelpGrammarNode node) {
        String grammarContent = HelpProvider.getGrammarHelp();
        return SingleCellTable.of("Grammar", grammarContent);
    }
    
    @Override
    public JfrTable visitFuzzyJoinSource(FuzzyJoinSourceNode node) {
        throw FeatureNotImplementedException.forInProgressFeature(
            "Fuzzy Joins", 
            "Use standard joins or multiple FROM sources as a workaround", 
            node
        );
    }
    
    @Override
    public JfrTable visitStandardJoinSource(StandardJoinSourceNode node) {
        throw new QueryEvaluationException(
            "standard join evaluation",
            "Standard joins should be handled by FROM clause processing",
            QueryEvaluationException.EvaluationErrorType.INVALID_STATE,
            node
        );
    }
    
    @Override
    public JfrTable visitPercentileFunction(PercentileFunctionNode node) {
        throw new QueryEvaluationException(
            "percentile function evaluation",
            "Percentile functions should be handled by function call processing",
            QueryEvaluationException.EvaluationErrorType.INVALID_STATE,
            node
        );
    }
    
    @Override
    public JfrTable visitPercentileSelection(PercentileSelectionNode node) {
        throw FeatureNotImplementedException.forPlannedFeature("Percentile Selection", node);
    }
    
    @Override
    public JfrTable visitWithinCondition(WithinConditionNode node) {
        // Evaluate the value to check (e.g., timestamp)
        JfrTable valueTable = node.value().accept(this);
        CellValue valueToCheck = extractSingleCellValue(valueTable);
        
        // Evaluate the time window (e.g., 5m)
        JfrTable timeWindowTable = node.timeWindow().accept(this);
        CellValue timeWindowValue = extractSingleCellValue(timeWindowTable);
        
        // Evaluate the reference time (e.g., startTime)
        JfrTable referenceTable = node.referenceTime().accept(this);
        CellValue referenceTime = extractSingleCellValue(referenceTable);
        
        // Validate value types
        if (!(valueToCheck instanceof CellValue.NumberValue || valueToCheck instanceof CellValue.TimestampValue)) {
            throw ExpressionEvaluationException.forTypeConversionError(
                "WITHIN condition value", valueToCheck, "timestamp or number", null
            );
        }
        if (!(referenceTime instanceof CellValue.NumberValue || referenceTime instanceof CellValue.TimestampValue)) {
            throw ExpressionEvaluationException.forTypeConversionError(
                "WITHIN condition reference time", referenceTime, "timestamp or number", null
            );
        }
        if (!(timeWindowValue instanceof CellValue.NumberValue)) {
            throw ExpressionEvaluationException.forTypeConversionError(
                "WITHIN condition time window", timeWindowValue, "number (duration)", null
            );
        }
        
        // Extract time values
        long valueTime = extractTimeValue(valueToCheck);
        long refTime = extractTimeValue(referenceTime);
        long timeWindowNanos = convertToNanoseconds(((CellValue.NumberValue) timeWindowValue).value());
        
        // Check if the value is within the time window of the reference time
        long timeDifference = Math.abs(valueTime - refTime);
        boolean isWithin = timeDifference <= timeWindowNanos;
        
        return SingleCellTable.of(new CellValue.BooleanValue(isWithin));
    }
    
    /**
     * Converts a time duration value to nanoseconds.
     * For now, assumes the input is already in nanoseconds.
     * In a full implementation, this would handle different time units.
     */
    private long convertToNanoseconds(double timeValue) {
        // For simplicity, assume the value is already in nanoseconds
        // In a full implementation, this would handle units like "5m", "30s", etc.
        return (long) timeValue;
    }
    
    @Override
    public JfrTable visitCaseExpression(CaseExpressionNode node) {
        // Evaluate the CASE expression
        CellValue expressionValue = null;
        if (node.expression() != null) {
            // Simple CASE: CASE expression WHEN value THEN result ...
            JfrTable expressionTable = node.expression().accept(this);
            expressionValue = extractSingleCellValue(expressionTable);
        }
        
        // Evaluate WHEN clauses in order
        for (WhenClauseNode whenClause : node.whenClauses()) {
            JfrTable conditionTable = whenClause.condition().accept(this);
            CellValue conditionValue = extractSingleCellValue(conditionTable);
            
            boolean matches;
            if (expressionValue != null) {
                // Simple CASE: compare expression value with WHEN value
                matches = expressionValue.equals(conditionValue);
            } else {
                // Searched CASE: evaluate WHEN condition as boolean
                if (conditionValue instanceof CellValue.BooleanValue boolVal) {
                    matches = boolVal.value();
                } else {
                    // Convert other types to boolean (non-null and non-zero are truthy)
                    matches = conditionValue != null && 
                             !(conditionValue instanceof CellValue.NullValue) &&
                             !conditionValue.equals(new CellValue.NumberValue(0));
                }
            }
            
            if (matches) {
                // Return the THEN result
                return whenClause.result().accept(this);
            }
        }
        
        // No WHEN clause matched, return ELSE result or NULL
        if (node.elseExpression() != null) {
            return node.elseExpression().accept(this);
        } else {
            return SingleCellTable.of("case_result", new CellValue.NullValue());
        }
    }
    
    // Missing helper methods
    
    /**
     * Get a value from a row by column name with proper exception handling
     */
    private CellValue getRowValueByColumnName(JfrTable.Row row, List<JfrTable.Column> columns, String columnName) {
        try {
            return QueryEvaluatorUtils.getRowValueByColumnName(row, columns, columnName);
        } catch (Exception e) {
            // Extract available column names for better error reporting
            String[] availableColumns = columns.stream()
                .map(JfrTable.Column::name)
                .toArray(String[]::new);
            
            throw new ColumnNotFoundException(columnName, availableColumns, null);
        }
    }
    
    /**
     * Evaluate an aggregate expression with precomputed aggregates
     */
    private CellValue evaluateAggregateExpression(ExpressionNode expr, Map<String, CellValue> precomputedAggregates) {
        if (expr instanceof FunctionCallNode functionCall) {
            String aggregateKey = QueryEvaluatorUtils.createAggregateKey(functionCall);
            if (precomputedAggregates.containsKey(aggregateKey)) {
                return precomputedAggregates.get(aggregateKey);
            }
        }
        
        // If not a precomputed aggregate, evaluate normally
        JfrTable result = expr.accept(this);
        return extractSingleCellValue(result);
    }
    
    /**
     * Efficiently evaluate DISTINCT aggregate functions by collecting unique values
     * Optimized with HashMap for fast duplicate detection
     */
    private JfrTable evaluateDistinctAggregateFunction(FunctionCallNode node) {
        String functionName = node.functionName();
        List<ExpressionNode> arguments = node.arguments();
        
        if (arguments.isEmpty()) {
            throw FunctionArgumentException.forWrongArgumentCount(
                functionName, 1, 0, node
            );
        }
        
        // Get the current evaluation context which should contain the source data
        JfrTable sourceTable = context.getCurrentResult();
        if (sourceTable == null) {
            throw AggregationEvaluationException.forInvalidState(
                functionName, "no data context available for DISTINCT evaluation",
                AggregationEvaluationException.AggregationContext.DISTINCT_AGGREGATE,
                node
            );
        }
        
        ExpressionNode fieldExpression = arguments.get(0);
        
        // Use HashMap for O(1) duplicate detection and LinkedHashSet for order preservation
        Map<CellValue, Boolean> seenValues = new HashMap<>();
        Set<CellValue> distinctValues = new LinkedHashSet<>();
        
        // Save the current context
        JfrTable originalResult = context.getCurrentResult();
        
        for (JfrTable.Row row : sourceTable.getRows()) {
            // Create a single-row table to set as context for field evaluation
            JfrTable singleRowTable = new StandardJfrTable(sourceTable.getColumns());
            singleRowTable.addRow(row);
            context.setCurrentResult(singleRowTable);
            
            // Evaluate the field expression for this row
            JfrTable fieldResult = fieldExpression.accept(this);
            CellValue fieldValue = extractSingleCellValue(fieldResult);
            
            // Only add non-null values to maintain SQL semantics
            if (!(fieldValue instanceof CellValue.NullValue)) {
                // Use HashMap for fast duplicate detection - O(1) vs Set.contains() which can be O(n)
                if (!seenValues.containsKey(fieldValue)) {
                    seenValues.put(fieldValue, true);
                    distinctValues.add(fieldValue);
                }
            }
        }
        
        // Restore the original context
        context.setCurrentResult(originalResult);
        
        // Now apply the aggregate function to the distinct values
        CellValue result = applyAggregateFunctionToDistinctValues(functionName, distinctValues, arguments, node);
        
        return SingleCellTable.of("result", result);
    }
    
    /**
     * Apply aggregate function to a collection of distinct values using function registry delegation
     */
    private CellValue applyAggregateFunctionToDistinctValues(String functionName, Set<CellValue> distinctValues, List<ExpressionNode> originalArguments, ASTNode errorNode) {
        // Prepare arguments for function registry delegation
        List<CellValue> arguments = new ArrayList<>();
        
        // Add the collection of distinct values as the first argument
        arguments.add(new CellValue.ArrayValue(new ArrayList<>(distinctValues)));
        
        // Add any additional arguments from the original function call (skip the first which was the expression we aggregated)
        for (int i = 1; i < originalArguments.size(); i++) {
            JfrTable argResult = originalArguments.get(i).accept(this);
            arguments.add(extractSingleCellValue(argResult));
        }
        
        // Delegate to function registry for consistent behavior
        try {
            return functionRegistry.evaluateFunction(functionName, arguments, evaluationContext);
        } catch (Exception e) {
            throw FunctionEvaluationException.forRuntimeError(
                functionName, arguments, 
                FunctionEvaluationException.FunctionContext.AGGREGATE_CONTEXT,
                errorNode, e
            );
        }
    }

    /**
     * Generate a cache key for the given query node.
     * This is used for query result caching.
     */
    private String generateQueryCacheKey(QueryNode node) {
        // Simple implementation - convert the query to string and hash it
        return String.valueOf(node.toString().hashCode());
    }

    /**
     * Extract dependencies from the given query node.
     * This returns the set of table/view names that the query depends on.
     */
    private Set<String> extractQueryDependencies(QueryNode node) {
        Set<String> dependencies = new HashSet<>();
        if (node.from() != null) {
            // Simple implementation - just get the table names from FROM clause sources
            for (SourceNodeBase source : node.from().sources()) {
                if (source instanceof SourceNode sourceNode) {
                    dependencies.add(sourceNode.source());
                }
            }
        }
        return dependencies;
    }

    /**
     * Create a placeholder result table for error cases.
     */
    private JfrTable createPlaceholderResult(QueryNode node, String errorMessage) {
        // Create a simple table with the error message using SingleCellTable
        return new SingleCellTable(new CellValue.StringValue(errorMessage));
    }

    /**
     * Perform cross join between two tables.
     * Creates a Cartesian product of all rows from both tables.
     */
    private JfrTable performCrossJoin(JfrTable leftTable, JfrTable rightTable) {
        // Create combined columns
        List<JfrTable.Column> leftColumns = leftTable.getColumns();
        List<JfrTable.Column> rightColumns = rightTable.getColumns();
        
        // Combine all columns (no duplicate handling for now - simple cross join)
        List<JfrTable.Column> allColumns = new ArrayList<>();
        allColumns.addAll(leftColumns);
        allColumns.addAll(rightColumns);
        
        // Create result table with combined columns
        StandardJfrTable result = new StandardJfrTable(allColumns);
        
        // Perform cross join - for each row in left table, combine with each row in right table
        List<JfrTable.Row> leftRows = leftTable.getRows();
        List<JfrTable.Row> rightRows = rightTable.getRows();
        
        for (JfrTable.Row leftRow : leftRows) {
            for (JfrTable.Row rightRow : rightRows) {
                // Combine cells from both rows
                List<CellValue> combinedCells = new ArrayList<>();
                combinedCells.addAll(leftRow.getCells());
                combinedCells.addAll(rightRow.getCells());
                
                result.addRow(new JfrTable.Row(combinedCells));
            }
        }
        
        return result;
    }

    /**
     * Evaluates an expression in the context of a specific row
     */
    public Object evaluateExpressionInRowContext(ExpressionNode expression, JfrTable.Row row, List<JfrTable.Column> columns) {
        if (expression == null) {
            return null;
        }
        
        // Handle different expression types
        if (expression instanceof LiteralNode) {
            // Return the CellValue directly to preserve type information 
            return ((LiteralNode) expression).value();
        } else if (expression instanceof IdentifierNode) {
            String columnName = ((IdentifierNode) expression).name();
            // Find the column by name
            for (JfrTable.Column column : columns) {
                if (column.name().equals(columnName)) {
                    CellValue cellValue = row.getCell(columns.indexOf(column));
                    // Return the CellValue directly to preserve type information
                    return cellValue;
                }
            }
            
            // Create available column names array for ColumnNotFoundException
            String[] availableColumns = columns.stream()
                .map(JfrTable.Column::name)
                .toArray(String[]::new);
            
            throw new ColumnNotFoundException(columnName, availableColumns, expression);
        } else if (expression instanceof FieldAccessNode) {
            FieldAccessNode fieldAccess = (FieldAccessNode) expression;
            String fieldName = fieldAccess.field();
            String qualifier = fieldAccess.qualifier();
            
            // For qualified field access (e.g., e.threadId), we need to resolve the qualifier
            // to the actual column name in a JOIN context
            if (qualifier != null && !qualifier.isEmpty()) {
                // First try the direct qualified name (table.field)
                String qualifiedName = qualifier + "." + fieldName;
                for (JfrTable.Column column : columns) {
                    if (column.name().equals(qualifiedName)) {
                        CellValue cellValue = row.getCell(columns.indexOf(column));
                        return cellValue;
                    }
                }
                
                // In JOIN contexts, look for prefixed column names
                // Try left_fieldName if qualifier might be left table
                String leftPrefixedName = "left_" + fieldName;
                for (JfrTable.Column column : columns) {
                    if (column.name().equals(leftPrefixedName)) {
                        CellValue cellValue = row.getCell(columns.indexOf(column));
                        return cellValue;
                    }
                }
                
                // Try right_fieldName if qualifier might be right table
                String rightPrefixedName = "right_" + fieldName;
                for (JfrTable.Column column : columns) {
                    if (column.name().equals(rightPrefixedName)) {
                        CellValue cellValue = row.getCell(columns.indexOf(column));
                        return cellValue;
                    }
                }
            }
            
            // Fall back to unqualified field name lookup
            for (JfrTable.Column column : columns) {
                if (column.name().equals(fieldName)) {
                    CellValue cellValue = row.getCell(columns.indexOf(column));
                    return cellValue;
                }
            }
            
            // Create available column names array for ColumnNotFoundException
            String[] availableColumns = columns.stream()
                .map(JfrTable.Column::name)
                .toArray(String[]::new);
            
            throw new ColumnNotFoundException(fieldName, availableColumns, expression);
        } else if (expression instanceof FunctionCallNode) {
            FunctionCallNode funcCall = (FunctionCallNode) expression;
            return evaluateFunctionCall(funcCall, row, columns);
        } else if (expression instanceof CaseExpressionNode) {
            return visitCaseExpression((CaseExpressionNode) expression, row, columns);
        } else if (expression instanceof BinaryExpressionNode) {
            BinaryExpressionNode binOp = (BinaryExpressionNode) expression;
            Object left = evaluateExpressionInRowContext(binOp.left(), row, columns);
            Object right = evaluateExpressionInRowContext(binOp.right(), row, columns);
            BinaryOperator operator = binOp.operator();
            
            // Route to appropriate evaluator based on operator type
            if (isComparisonOperator(operator)) {
                // Convert Objects to CellValues for comparison
                CellValue leftCell = CellValue.of(left);
                CellValue rightCell = CellValue.of(right);
                return evaluateComparison(operator, leftCell, rightCell);
            } else {
                return evaluateBinaryOperation(operator, left, right);
            }
        }
        
        throw ExpressionEvaluationException.forTypeConversionError(
            "expression of type " + expression.getClass().getSimpleName(),
            new CellValue.StringValue(expression.getClass().getName()),
            "evaluable expression", 
            null
        );
    }

    /**
     * Evaluates a condition in the context of a specific row
     */
    private boolean evaluateConditionForRow(JfrTable.Row row, ConditionNode condition, List<JfrTable.Column> columns) {
        if (condition == null) {
            return true;
        }
        
        if (condition instanceof ExpressionConditionNode) {
            ExpressionConditionNode exprCondition = (ExpressionConditionNode) condition;
            Object result = evaluateExpressionInRowContext(exprCondition.expression(), row, columns);
            
            // Convert result to boolean
            if (result instanceof Boolean) {
                return (Boolean) result;
            } else if (result instanceof Number) {
                return ((Number) result).doubleValue() != 0.0;
            } else if (result != null) {
                return true; // Non-null values are truthy
            } else {
                return false; // Null is falsy
            }
        }
        
        // For other condition types, treat as true for now
        return true;
    }

    /**
     * Evaluates a function call in the context of a specific row
     */
    private Object evaluateFunctionCall(FunctionCallNode funcCall, JfrTable.Row row, List<JfrTable.Column> columns) {
        String funcName = funcCall.functionName();
        List<ExpressionNode> args = funcCall.arguments();
        
        // Check if the function exists in the registry
        if (!functionRegistry.isFunction(funcName)) {
            Set<String> availableFunctions = functionRegistry.getFunctionNames();
            throw new UnknownFunctionException(funcName, availableFunctions, funcCall);
        }
        
        // Check if this is an aggregate function being called in row context
        if (functionRegistry.isAggregateFunction(funcName)) {
            // For aggregate functions in row context, we need to handle them specially
            // This can happen in contexts like COLLECT expressions or subqueries
            
            // Convert arguments to CellValues for evaluation
            List<CellValue> argValues = new ArrayList<>();
            for (ExpressionNode arg : args) {
                Object argResult = evaluateExpressionInRowContext(arg, row, columns);
                argValues.add(CellValue.of(argResult));
            }
            
            try {
                // Delegate to function registry with proper evaluation context
                CellValue result = functionRegistry.evaluateFunction(funcName, argValues, evaluationContext);
                return result.getValue();
            } catch (Exception e) {
                throw FunctionEvaluationException.forRuntimeError(
                    funcName, argValues, 
                    FunctionEvaluationException.FunctionContext.ROW_CONTEXT,
                    funcCall, e
                );
            }
        }
        
        // For non-aggregate functions, evaluate arguments and delegate to function registry
        List<CellValue> argValues = new ArrayList<>();
        for (ExpressionNode arg : args) {
            Object argResult = evaluateExpressionInRowContext(arg, row, columns);
            argValues.add(CellValue.of(argResult));
        }
        
        try {
            CellValue result = functionRegistry.evaluateFunction(funcName, argValues, evaluationContext);
            return result.getValue();
        } catch (Exception e) {
            throw FunctionEvaluationException.forRuntimeError(
                funcName, argValues, FunctionEvaluationException.FunctionContext.ROW_CONTEXT,
                funcCall, e
            );
        }
    }
    
    /**
     * Handles aggregate functions when called in row context.
     * Delegates to the function registry's aggregate function implementations.
     */
    private Object evaluateAggregateFunctionInRowContext(FunctionCallNode funcCall, JfrTable.Row row, List<JfrTable.Column> columns) {
        // Aggregate functions should not be evaluated in row context.
        // They should be handled at the query level through the function registry.
        throw AggregationEvaluationException.forInvalidState(
            funcCall.functionName(), 
            "aggregate function cannot be evaluated in row context - use GROUP BY or SELECT aggregate",
            AggregationEvaluationException.AggregationContext.SELECT_AGGREGATE,
            funcCall
        );
    }

    /**
     * Evaluates a CASE expression in the context of a specific row
     */
    private Object visitCaseExpression(CaseExpressionNode caseExpr, JfrTable.Row row, List<JfrTable.Column> columns) {
        // Check each WHEN clause
        for (WhenClauseNode whenClause : caseExpr.whenClauses()) {
            // Evaluate the condition as an expression and convert to boolean
            Object conditionResult = evaluateExpressionInRowContext(whenClause.condition(), row, columns);
            boolean matches = false;
            if (conditionResult instanceof Boolean) {
                matches = (Boolean) conditionResult;
            } else if (conditionResult instanceof Number) {
                matches = ((Number) conditionResult).doubleValue() != 0.0;
            } else if (conditionResult != null) {
                matches = true;
            }
            
            if (matches) {
                return evaluateExpressionInRowContext(whenClause.result(), row, columns);
            }
        }
        
        // If no WHEN clause matched, use ELSE clause or null
        if (caseExpr.elseExpression() != null) {
            return evaluateExpressionInRowContext(caseExpr.elseExpression(), row, columns);
        }
        
        return null;
    }

    /**
     * Evaluates a binary operation
     */
    private Object evaluateBinaryOperation(BinaryOperator operator, Object left, Object right) {
        if (left == null || right == null) {
            return null;
        }
        
        switch (operator) {
            case ADD:
                // Handle string concatenation first - check if either operand is a string type
                boolean isStringOp = (left instanceof CellValue && ((CellValue) left).getType() == CellType.STRING) ||
                                   (right instanceof CellValue && ((CellValue) right).getType() == CellType.STRING) ||
                                   (left instanceof String) || (right instanceof String);
                                   
                if (isStringOp) {
                    String leftStr = (left instanceof CellValue) ? left.toString() : left.toString();
                    String rightStr = (right instanceof CellValue) ? right.toString() : right.toString();
                    return leftStr + rightStr;
                }
                // For numeric operations, use CellValue type-preserving arithmetic
                return performTypedArithmetic(left, right, operator);
            case SUBTRACT:
            case MULTIPLY:
            case DIVIDE:
            case MODULO:
                // Use CellValue type-preserving arithmetic for all numeric operations
                return performTypedArithmetic(left, right, operator);
            case AND:
                return FunctionUtils.convertToBoolean(left) && FunctionUtils.convertToBoolean(right);
            case OR:
                return FunctionUtils.convertToBoolean(left) || FunctionUtils.convertToBoolean(right);
            default:
                throw ExpressionEvaluationException.forTypeConversionError(
                    "binary operation with operator " + operator,
                    new CellValue.StringValue(operator.toString()),
                    "supported binary operator", 
                    null
                );
        }
    }
    
    /**
     * Performs typed arithmetic operations using CellValue's type-preserving system
     */
    private CellValue performTypedArithmetic(Object left, Object right, BinaryOperator operator) {
        // Convert raw values to CellValues to leverage type-preserving arithmetic
        CellValue leftValue = CellValue.of(left);
        CellValue rightValue = CellValue.of(right);
        
        try {
            // Use CellValue's sophisticated type-preserving binary operations
            return leftValue.mapBinary(rightValue, operator);
        } catch (Exception e) {
            throw ExpressionEvaluationException.forArithmeticError(
                operator + " operation", 
                operator, 
                leftValue, 
                rightValue, 
                null, 
                e
            );
        }
    }

    /**
     * Evaluates a comparison operation
     */
    private boolean evaluateComparison(BinaryOperator operator, CellValue left, CellValue right) {
        if (left instanceof CellValue.NullValue && right instanceof CellValue.NullValue) {
            return operator == BinaryOperator.EQUALS;
        }
        if (left instanceof CellValue.NullValue || right instanceof CellValue.NullValue) {
            return operator == BinaryOperator.NOT_EQUALS;
        }
        
        // Try numeric comparison first
        if (left instanceof CellValue.NumberValue && right instanceof CellValue.NumberValue) {
            double leftVal = ((CellValue.NumberValue) left).value();
            double rightVal = ((CellValue.NumberValue) right).value();
            
            switch (operator) {
                case EQUALS:
                    return leftVal == rightVal;
                case NOT_EQUALS:
                    return leftVal != rightVal;
                case LESS_THAN:
                    return leftVal < rightVal;
                case LESS_EQUAL:
                    return leftVal <= rightVal;
                case GREATER_THAN:
                    return leftVal > rightVal;
                case GREATER_EQUAL:
                    return leftVal >= rightVal;
                default:
                    throw ExpressionEvaluationException.forTypeConversionError(
                        "comparison operation with operator " + operator,
                        new CellValue.StringValue(operator.toString()),
                        "supported comparison operator", 
                        null
                    );
            }
        }
        
        // Use CellValue comparison for other types
        int comparison = CellValue.compare(left, right);
        
        switch (operator) {
            case EQUALS:
                return comparison == 0;
            case NOT_EQUALS:
                return comparison != 0;
            case LESS_THAN:
                return comparison < 0;
            case LESS_EQUAL:
                return comparison <= 0;
            case GREATER_THAN:
                return comparison > 0;
            case GREATER_EQUAL:
                return comparison >= 0;
            default:
                throw ExpressionEvaluationException.forTypeConversionError(
                    "comparison operation with operator " + operator,
                    new CellValue.StringValue(operator.toString()),
                    "supported comparison operator", 
                    null
                );
        }
    }

    /**
     * Checks if an operator is a comparison operator
     */
    private boolean isComparisonOperator(BinaryOperator operator) {
        switch (operator) {
            case EQUALS:
            case NOT_EQUALS:
            case LESS_THAN:
            case GREATER_THAN:
            case LESS_EQUAL:
            case GREATER_EQUAL:
                return true;
            default:
                return false;
        }
    }

    // Missing methods implementation
    
    // Join methods - powered by optimized JoinProcessor
    private JfrTable performInnerJoin(JfrTable left, JfrTable right, String leftField, String rightField) {
        return joinProcessor.performInnerJoin(left, right, leftField, rightField);
    }
    
    private JfrTable performLeftJoin(JfrTable left, JfrTable right, String leftField, String rightField) {
        return joinProcessor.performLeftJoin(left, right, leftField, rightField);
    }
    
    private JfrTable performRightJoin(JfrTable left, JfrTable right, String leftField, String rightField) {
        return joinProcessor.performRightJoin(left, right, leftField, rightField);
    }
    
    private JfrTable performFullJoin(JfrTable left, JfrTable right, String leftField, String rightField) {
        return joinProcessor.performFullJoin(left, right, leftField, rightField);
    }
    
    private JfrTable performFuzzyJoin(JfrTable left, FuzzyJoinSourceNode fuzzyJoin) {
        // Get the right table
        SourceNode rightSource = new SourceNode(fuzzyJoin.source(), fuzzyJoin.alias(), fuzzyJoin.location());
        JfrTable rightTable = visitSource(rightSource);
        
        // Use JoinProcessor for optimized fuzzy join execution
        return joinProcessor.performFuzzyJoin(left, rightTable, fuzzyJoin);
    }
    
    private JfrTable performStandardJoin(JfrTable result, StandardJoinSourceNode standardJoin) {
        // Get the right table
        SourceNode rightSource = new SourceNode(standardJoin.source(), standardJoin.alias(), standardJoin.location());
        JfrTable rightTable = visitSource(rightSource);
        
        // Use JoinProcessor for optimized standard join execution
        return joinProcessor.performStandardJoin(result, rightTable, standardJoin);
    }
    
    /**
     * Get join performance statistics and cache metrics
     */
    public Map<String, Object> getJoinPerformanceStats() {
        return joinProcessor.getCacheStats();
    }
    
    /**
     * Clear join index cache to free memory
     */
    public void clearJoinCache() {
        joinProcessor.clearCache();
    }
    
    // View definition method
    private void defineView(String viewName, QueryNode query) {
        context.setView(viewName, query);
    }
    
    // Group key building method
    private String buildGroupKey(JfrTable.Row row, List<JfrTable.Column> columns, List<ExpressionNode> fields) {
        return QueryEvaluatorUtils.buildGroupKey(row, columns, fields);
    }
    
    // Aggregate function checking
    private boolean isAggregateFunction(String functionName) {
        return QueryEvaluatorUtils.isAggregateFunction(functionName.toLowerCase());
    }
    
    // Aggregate value computation
    public CellValue computeAggregateValue(FunctionCallNode functionCall, JfrTable groupTable) {
        String functionName = functionCall.functionName();
        List<ExpressionNode> arguments = functionCall.arguments();
        
        // Special handling for COLLECT function - evaluate expression for each row
        if ("COLLECT".equalsIgnoreCase(functionName)) {
            if (arguments.isEmpty()) {
                throw AggregationEvaluationException.forInvalidState(
                    "COLLECT", "no arguments provided", 
                    AggregationEvaluationException.AggregationContext.GROUP_BY, 
                    functionCall
                );
            }
            
            ExpressionNode expr = arguments.get(0);
            List<CellValue> collectedValues = new ArrayList<>();
            
            // Evaluate the expression for each row in the group
            for (JfrTable.Row row : groupTable.getRows()) {
                Object result = evaluateExpressionInRowContext(expr, row, groupTable.getColumns());
                // If result is already a CellValue, use it directly to preserve type information
                if (result instanceof CellValue cellValue) {
                    collectedValues.add(cellValue);
                } else {
                    collectedValues.add(CellValue.of(result));
                }
            }
            
            return new CellValue.ArrayValue(collectedValues);
        }
        
        // Convert arguments to CellValues for other aggregate functions
        List<CellValue> argValues = new ArrayList<>();
        for (ExpressionNode arg : arguments) {
            if (arg instanceof LiteralNode literal) {
                argValues.add(literal.value());
            } else if (arg instanceof IdentifierNode identifier) {
                // Get column values from group table
                String columnName = identifier.name();
                List<CellValue> columnValues = new ArrayList<>();
                for (JfrTable.Row row : groupTable.getRows()) {
                    for (int i = 0; i < groupTable.getColumns().size(); i++) {
                        if (groupTable.getColumns().get(i).name().equals(columnName)) {
                            columnValues.add(row.getCell(i));
                            break;
                        }
                    }
                }
                argValues.add(new CellValue.ArrayValue(columnValues));
            } else if (arg instanceof FieldAccessNode fieldAccess) {
                // Handle qualified field references like "i.innerValue"
                String columnName = fieldAccess.field();
                List<CellValue> columnValues = new ArrayList<>();
                for (JfrTable.Row row : groupTable.getRows()) {
                    for (int i = 0; i < groupTable.getColumns().size(); i++) {
                        if (groupTable.getColumns().get(i).name().equals(columnName)) {
                            columnValues.add(row.getCell(i));
                            break;
                        }
                    }
                }
                argValues.add(new CellValue.ArrayValue(columnValues));
            } else if (arg instanceof StarNode) {
                // Handle COUNT(*) - pass the row count directly as a NumberValue
                argValues.add(new CellValue.NumberValue(groupTable.getRowCount()));
            } else {
                // For other expression types, evaluate in the context of each row
                List<CellValue> expressionValues = new ArrayList<>();
                for (JfrTable.Row row : groupTable.getRows()) {
                    Object result = evaluateExpressionInRowContext(arg, row, groupTable.getColumns());
                    expressionValues.add(CellValue.of(result));
                }
                argValues.add(new CellValue.ArrayValue(expressionValues));
            }
        }
        
        // Create a group-specific evaluation context that can provide getAllValues
        AggregateFunctions.EvaluationContext groupContext = new AggregateFunctions.EvaluationContext(
            // Reuse the same executors as the parent context
            (rawQueryNode) -> evaluationContext.jfrQuery(rawQueryNode),
            (queryNode, ctx) -> evaluationContext.extendedQuery(queryNode)
        ) {
            @Override
            public List<CellValue> getAllValues(String fieldName) {
                List<CellValue> values = new ArrayList<>();
                for (JfrTable.Row row : groupTable.getRows()) {
                    for (int i = 0; i < groupTable.getColumns().size(); i++) {
                        if (groupTable.getColumns().get(i).name().equals(fieldName)) {
                            values.add(row.getCell(i));
                            break;
                        }
                    }
                }
                return values;
            }
            
            @Override
            public CellValue getFieldValue(String fieldName) {
                // Return the first value for single-value access
                List<CellValue> values = getAllValues(fieldName);
                return values.isEmpty() ? new CellValue.NullValue() : values.get(0);
            }
        };
        
        try {
            return functionRegistry.evaluateFunction(functionName, argValues, groupContext);
        } catch (Exception e) {
            // Extract group by fields if available
            List<String> groupByFields = groupTable.getColumns().stream()
                .map(JfrTable.Column::name)
                .collect(Collectors.toList());
            
            throw AggregationEvaluationException.forGroupTableError(
                functionName, groupTable, groupByFields, functionCall, e
            );
        }
    }
    
    /**
     * Variable access method with comprehensive scope resolution.
     * 
     * Resolves variables in the following order:
     * 1. Local variables (execution context scope)
     * 2. Global variables (engine scope)  
     * 3. Lazy variables (deferred evaluation)
     * 
     * @param name the variable name to resolve
     * @return the variable value, or null if not found
     */
    private Object getVariable(String name) {
        // Check local variables first (execution context scope)
        Object localValue = context.getLocalVariable(name);
        if (localValue != null) {
            return localValue;
        }
        
        // Check global variables (engine scope)
        Object globalValue = context.getVariable(name);
        if (globalValue != null) {
            return globalValue;
        }
        
        // Check lazy variables (deferred evaluation)
        LazyQuery lazyQuery = context.getLazyVariable(name);
        if (lazyQuery != null) {
            try {
                // Evaluate lazy variable and cache result for performance
                Object result = lazyQuery.evaluate();
                // Cache the evaluated result as a global variable for future access
                context.setVariable(name, result);
                return result;
            } catch (Exception e) {
                // Log evaluation failure but don't crash - return null instead
                if (context.isDebugMode()) {
                    System.err.println("Failed to evaluate lazy variable '" + name + "': " + e.getMessage());
                }
                return null;
            }
        }
        
        return null;
    }
}