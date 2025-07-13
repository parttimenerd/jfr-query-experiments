package me.bechberger.jfr.extended.engine;

import jdk.jfr.EventType;
import me.bechberger.jfr.extended.evaluator.FunctionRegistry;
import me.bechberger.jfr.extended.evaluator.AggregateFunctions;
import me.bechberger.jfr.extended.ast.ASTNodes.*;
import me.bechberger.jfr.extended.ast.ASTVisitor;
import me.bechberger.jfr.extended.ast.Location;
import me.bechberger.jfr.extended.table.JfrTable;
import me.bechberger.jfr.extended.table.CellValue;
import me.bechberger.jfr.extended.table.CellType;
import me.bechberger.jfr.extended.table.SingleCellTable;
import me.bechberger.jfr.extended.Parser;
import me.bechberger.jfr.extended.Lexer;
import me.bechberger.jfr.extended.Token;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;

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
    private final List<EventType> eventTypes;
    private final RawJfrQueryExecutor rawJfrExecutor;
    private ExecutionContext context;
    private AggregateFunctions.EvaluationContext evaluationContext;
    
    /**
     * Constructor for QueryEvaluator with raw JFR query executor.
     * Gets FunctionRegistry via getInstance(), creates parser when needed.
     */
    public QueryEvaluator(RawJfrQueryExecutor rawJfrExecutor) {
        this.functionRegistry = FunctionRegistry.getInstance();
        this.rawJfrExecutor = rawJfrExecutor;
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
                    throw new RuntimeException("Failed to execute raw JFR query", e);
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
            throw new UnsupportedOperationException("Raw JFR query execution not supported - no executor provided");
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
     */
    public JfrTable query(String queryString) throws Exception {
        try {
            // Tokenize the query string
            Lexer lexer = new Lexer(queryString);
            List<Token> tokens = lexer.tokenize();
            
            // Parse the tokens into an AST
            Parser parser = new Parser(tokens, queryString);
            ProgramNode program = parser.parse();
            
            // Execute the program and return the last result
            return program.accept(this);
        } catch (Exception e) {
            throw new Exception("Failed to parse and execute query: " + queryString, e);
        }
    }
    
    @Override
    public JfrTable visitProgram(ProgramNode node) {
        JfrTable lastResult = null;
        
        for (StatementNode statement : node.statements()) {
            lastResult = statement.accept(this);
            context.setCurrentResult(lastResult);
        }
        
        return lastResult;
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
        return new JfrTable(List.of());
    }
    
    @Override
    public JfrTable visitViewDefinition(ViewDefinitionNode node) {
        defineView(node.viewName(), node.query());
        // Return empty result for view definitions
        return new JfrTable(List.of());
    }
    
    @Override
    public JfrTable visitQuery(QueryNode node) {
        // Basic query execution - this is a simplified implementation
        // A full implementation would integrate with the existing JFR query system
        
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
    
    private JfrTable executeBasicQuery(QueryNode node) {
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
                sourceData = applyGroupByClause(sourceData, node.groupBy());
            }
            
            // Step 3: Apply HAVING conditions on aggregated groups
            if (node.having() != null) {
                sourceData = applyHavingClause(sourceData, node.having());
            }
            
            // Step 4: Execute SELECT expressions on the filtered/grouped data
            JfrTable result = executeSelectClause(sourceData, node.select());
            
            // Step 5: Apply ORDER BY sorting
            if (node.orderBy() != null) {
                result = applyOrderByClause(result, node.orderBy());
            }
            
            // Step 6: Apply LIMIT restrictions
            if (node.limit() != null) {
                result = applyLimitClause(result, node.limit());
            }
            
            // Cache the result with dependencies for future use
            context.cacheResult(cacheKey, result, dependencies);
            if (context.isDebugMode()) {
                System.out.println("Cached query result: " + cacheKey + " with dependencies: " + dependencies);
            }
            
            return result;
            
        } catch (Exception e) {
            // Fallback to placeholder implementation for unsupported features
            return createPlaceholderResult(node, "Error executing query: " + e.getMessage());
        }
    }
    
    /**
     * Process FROM clause with integrated WHERE filtering for optimization
     * This reduces the size of intermediate results by applying filters as rows are joined
     */
    private JfrTable processFromClauseWithFiltering(FromNode fromNode, WhereNode whereNode) {
        if (fromNode == null) {
            return new JfrTable(List.of(new JfrTable.Column("default", CellType.STRING)));
        }
        
        List<SourceNodeBase> sources = fromNode.sources();
        if (sources.isEmpty()) {
            return new JfrTable(List.of(new JfrTable.Column("empty", CellType.STRING)));
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
        
        // Perform the join with optimized tables
        JfrTable result = switch (standardJoin.joinType()) {
            case INNER -> performInnerJoinOptimized(leftTable, rightTable, standardJoin.leftJoinField(), standardJoin.rightJoinField(), whereNode);
            case LEFT -> performLeftJoinOptimized(leftTable, rightTable, standardJoin.leftJoinField(), standardJoin.rightJoinField(), whereNode);
            case RIGHT -> performRightJoinOptimized(leftTable, rightTable, standardJoin.leftJoinField(), standardJoin.rightJoinField(), whereNode);
            case FULL -> performFullJoinOptimized(leftTable, rightTable, standardJoin.leftJoinField(), standardJoin.rightJoinField(), whereNode);
        };
        
        return result;
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
            return new JfrTable(List.of(new JfrTable.Column("default", CellType.STRING)));
        }
        
        List<SourceNodeBase> sources = fromNode.sources();
        if (sources.isEmpty()) {
            return new JfrTable(List.of(new JfrTable.Column("empty", CellType.STRING)));
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
        JfrTable result = new JfrTable(sourceData.getColumns());
        
        // Evaluate WHERE condition for each row
        for (JfrTable.Row row : sourceData.getRows()) {
            if (evaluateConditionForRow(row, whereNode.condition())) {
                result.addRow(row);
            }
        }
        
        return result;
    }
    
    /**
     * Apply GROUP BY clause to aggregate data with proper aggregate function evaluation
     */
    private JfrTable applyGroupByClause(JfrTable sourceData, GroupByNode groupByNode) {
        // Group rows by specified fields
        Map<String, List<JfrTable.Row>> groups = new HashMap<>();
        
        for (JfrTable.Row row : sourceData.getRows()) {
            String groupKey = buildGroupKey(row, sourceData.getColumns(), groupByNode.fields());
            groups.computeIfAbsent(groupKey, k -> new ArrayList<>()).add(row);
        }
        
        // Create result columns (same as source for now - in real implementation this would be determined by SELECT clause)
        List<JfrTable.Column> resultColumns = new ArrayList<>(sourceData.getColumns());
        JfrTable result = new JfrTable(resultColumns);
        
        // For each group, compute aggregated values
        for (Map.Entry<String, List<JfrTable.Row>> group : groups.entrySet()) {
            List<JfrTable.Row> groupRows = group.getValue();
            if (groupRows.isEmpty()) continue;
            
            // Compute aggregated row for this group
            JfrTable.Row aggregatedRow = computeAggregatedRow(groupRows, sourceData.getColumns());
            result.addRow(aggregatedRow);
        }
        
        return result;
    }
    
    /**
     * Compute aggregated row from a group of rows
     * This is a simplified implementation - in reality would use SELECT clause aggregate functions
     */
    private JfrTable.Row computeAggregatedRow(List<JfrTable.Row> groupRows, List<JfrTable.Column> columns) {
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
     * Check if a column type is numeric for aggregation
     */
    private boolean isNumericColumn(CellType type) {
        return switch (type) {
            case NUMBER, FLOAT, DURATION, MEMORY_SIZE, RATE -> true;
            default -> false;
        };
    }
    
    /**
     * Compute sum of values in a specific column across group rows
     */
    private CellValue computeSum(List<JfrTable.Row> groupRows, int columnIndex) {
        double sum = 0.0;
        CellType resultType = CellType.NUMBER;
        
        for (JfrTable.Row row : groupRows) {
            if (columnIndex < row.getCells().size()) {
                CellValue cell = row.getCells().get(columnIndex);
                if (isNumericValue(cell)) {
                    sum += extractNumericValue(cell);
                    // Preserve the most specific type
                    if (cell.getType() == CellType.FLOAT) {
                        resultType = CellType.FLOAT;
                    }
                }
            }
        }
        
        return resultType == CellType.FLOAT ? 
            new CellValue.FloatValue(sum) : 
            new CellValue.NumberValue((long) sum);
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
                extractColumnName(selectItem.expression());
            
            // Determine column type by evaluating the expression on sample data
            CellType columnType = determineColumnType(selectItem.expression(), sourceData);
            resultColumns.add(new JfrTable.Column(columnName, columnType));
            selectExpressions.add(selectItem.expression());
        }
        
        JfrTable result = new JfrTable(resultColumns);
        
        // Evaluate SELECT expressions for each row
        for (JfrTable.Row sourceRow : sourceData.getRows()) {
            List<CellValue> resultCells = new ArrayList<>();
            
            for (ExpressionNode expr : selectExpressions) {
                CellValue cellValue = evaluateExpressionForRow(sourceRow, expr);
                resultCells.add(cellValue);
            }
            
            result.addRow(new JfrTable.Row(resultCells));
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
                    throw new RuntimeException("Error evaluating ORDER BY expression: " + e.getMessage(), e);
                }
            }
            return 0; // All fields are equal
        });
        
        // Create result table with sorted rows
        JfrTable result = new JfrTable(sourceData.getColumns());
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
            throw new IllegalArgumentException("Percentile functions like " + percentileFunc.functionName() + 
                " cannot be used in ORDER BY clause without GROUP BY. Percentiles require aggregation over multiple rows.");
        } else {
            throw new IllegalArgumentException("Unsupported expression type in ORDER BY: " + expr.getClass().getSimpleName());
        }
    }
    
    /**
     * Evaluate a function call in the context of a single row.
     */
    private CellValue evaluateFunctionCallInRowContext(FunctionCallNode functionCall, JfrTable.Row row, JfrTable table) throws Exception {
        // Evaluate function arguments in row context
        List<CellValue> argValues = new ArrayList<>();
        for (ExpressionNode arg : functionCall.arguments()) {
            argValues.add(evaluateOrderByExpression(arg, row, table));
        }
        
        // Check for aggregate functions in ORDER BY without GROUP BY
        String functionName = functionCall.functionName().toLowerCase();
        if (isAggregateFunction(functionName)) {
            throw new IllegalArgumentException("Aggregate function " + functionCall.functionName() + 
                " cannot be used in ORDER BY clause without GROUP BY. Use aggregate functions only with GROUP BY clause.");
        }
        
        // Evaluate the function using the function registry
        try {
            return functionRegistry.evaluateFunction(functionCall.functionName(), argValues, evaluationContext);
        } catch (Exception e) {
            throw new IllegalArgumentException("Error executing function " + functionCall.functionName() + 
                " in ORDER BY clause: " + e.getMessage(), e);
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
                    throw new IllegalArgumentException("Cannot apply unary minus to " + operand.getClass().getSimpleName());
                }
            }
            case NOT -> {
                if (operand instanceof CellValue.BooleanValue boolVal) {
                    yield new CellValue.BooleanValue(!boolVal.value());
                } else {
                    throw new IllegalArgumentException("Cannot apply NOT operator to " + operand.getClass().getSimpleName());
                }
            }
        };
    }
    
    /**
     * Apply LIMIT clause to restrict the number of rows
     */
    private JfrTable applyLimitClause(JfrTable sourceData, LimitNode limitNode) {
        int limit = limitNode.limit();
        
        JfrTable result = new JfrTable(sourceData.getColumns());
        
        int count = 0;
        for (JfrTable.Row row : sourceData.getRows()) {
            if (count >= limit) break;
            result.addRow(row);
            count++;
        }
        
        return result;
    }
    
    private String extractColumnName(ExpressionNode expr) {
        if (expr instanceof FieldAccessNode fieldAccess) {
            return fieldAccess.field();
        } else if (expr instanceof IdentifierNode identifier) {
            return identifier.name();
        } else if (expr instanceof FunctionCallNode functionCall) {
            return functionCall.functionName() + "()";
        }
        return "unknown";
    }
    
    /**
     * Apply HAVING clause to filter aggregated groups
     */
    private JfrTable applyHavingClause(JfrTable input, HavingNode havingNode) {
        // The HAVING clause should filter groups after aggregation
        // This is a simplified implementation that demonstrates the concept
        
        if (havingNode == null || havingNode.condition() == null) {
            return input;
        }
        
        // Create a new table with the same columns
        JfrTable result = new JfrTable(input.getColumns());
        
        // Evaluate the HAVING condition for each row
        for (JfrTable.Row row : input.getRows()) {
            if (evaluateHavingCondition(row, havingNode.condition())) {
                result.addRow(row);
            }
        }
        
        return result;
    }
    
    /**
     * Evaluate a HAVING condition against a row
     */
    private boolean evaluateHavingCondition(JfrTable.Row row, ConditionNode condition) {
        // This is a simplified evaluation
        // A full implementation would:
        // 1. Parse the condition expression
        // 2. Evaluate it against the row data
        // 3. Return true/false based on the result
        
        // Handle ExpressionConditionNode which wraps expression nodes
        if (condition instanceof ExpressionConditionNode exprCondition) {
            ExpressionNode expr = exprCondition.expression();
            
            if (expr instanceof BinaryExpressionNode binaryExpr) {
                return evaluateBinaryHavingCondition(row, binaryExpr);
            } else if (expr instanceof FunctionCallNode functionCall) {
                return evaluateFunctionHavingCondition(row, functionCall);
            }
        }
        
        // Default to including the row if we can't evaluate the condition
        return true;
    }
    
    /**
     * Evaluate a binary expression in HAVING clause
     */
    private boolean evaluateBinaryHavingCondition(JfrTable.Row row, BinaryExpressionNode binaryExpr) {
        BinaryOperator operator = binaryExpr.operator();
        
        try {
            // Evaluate left and right operands using the AST visitor
            JfrTable leftResult = binaryExpr.left().accept(this);
            JfrTable rightResult = binaryExpr.right().accept(this);
            
            CellValue leftValue = extractSingleCellValue(leftResult);
            CellValue rightValue = extractSingleCellValue(rightResult);
            
            // Handle comparison operations
            if (operator == BinaryOperator.GREATER_THAN || 
                operator == BinaryOperator.LESS_THAN || 
                operator == BinaryOperator.EQUALS ||
                operator == BinaryOperator.NOT_EQUALS ||
                operator == BinaryOperator.GREATER_EQUAL ||
                operator == BinaryOperator.LESS_EQUAL) {
                
                int comparison = CellValue.compare(leftValue, rightValue);
                return switch (operator) {
                    case GREATER_THAN -> comparison > 0;
                    case LESS_THAN -> comparison < 0;
                    case EQUALS -> comparison == 0;
                    case NOT_EQUALS -> comparison != 0;
                    case GREATER_EQUAL -> comparison >= 0;
                    case LESS_EQUAL -> comparison <= 0;
                    default -> true;
                };
            }
            
            // Handle logical operations
            if (operator == BinaryOperator.AND || operator == BinaryOperator.OR) {
                boolean leftBool = isTruthyValue(leftValue);
                boolean rightBool = isTruthyValue(rightValue);
                
                return switch (operator) {
                    case AND -> leftBool && rightBool;
                    case OR -> leftBool || rightBool;
                    default -> true;
                };
            }
            
            // Handle IN operation
            if (operator == BinaryOperator.IN) {
                if (rightValue instanceof CellValue.ArrayValue arrayValue) {
                    return arrayValue.contains(leftValue);
                } else {
                    return CellValue.compare(leftValue, rightValue) == 0;
                }
            }
            
        } catch (Exception e) {
            // If evaluation fails, log and return true to include the row
            System.err.println("Error evaluating HAVING condition: " + e.getMessage());
        }
        
        return true; // Default to including row
    }
    
    /**
     * Evaluate a function call in HAVING clause
     * HAVING clause can contain aggregate functions
     */
    private boolean evaluateFunctionHavingCondition(JfrTable.Row row, FunctionCallNode functionCall) {
        String functionName = functionCall.functionName();
        
        try {
            // For aggregate functions in HAVING, we need to evaluate them properly
            if (isAggregateFunction(functionName)) {
                // Evaluate the aggregate function using the function registry
                List<CellValue> arguments = new ArrayList<>();
                for (ExpressionNode arg : functionCall.arguments()) {
                    JfrTable argTable = arg.accept(this);
                    CellValue argValue = extractSingleCellValue(argTable);
                    arguments.add(argValue);
                }
                
                CellValue result = functionRegistry.evaluateFunction(functionName, arguments, evaluationContext);
                return isTruthyValue(result);
            } else {
                // For non-aggregate functions, evaluate normally
                List<CellValue> arguments = new ArrayList<>();
                for (ExpressionNode arg : functionCall.arguments()) {
                    JfrTable argTable = arg.accept(this);
                    CellValue argValue = extractSingleCellValue(argTable);
                    arguments.add(argValue);
                }
                
                CellValue result = functionRegistry.evaluateFunction(functionName, arguments, evaluationContext);
                return isTruthyValue(result);
            }
        } catch (Exception e) {
            // If function evaluation fails, log and return true to include the row
            System.err.println("Error evaluating function in HAVING clause: " + e.getMessage());
            return true;
        }
    }
    
    /**
     * Evaluate an expression to get its value from a row
     */
    private Object evaluateExpressionValue(JfrTable.Row row, ExpressionNode expr) {
        if (expr instanceof LiteralNode literal) {
            return literal.value();
        } else if (expr instanceof FieldAccessNode fieldAccess) {
            // Try to find the field value in the row
            String fieldName = fieldAccess.field();
            // This is simplified - a full implementation would map field names to column indices
            return 0; // Placeholder
        } else if (expr instanceof FunctionCallNode functionCall) {
            // Evaluate function call
            return evaluateFunctionValue(row, functionCall);
        }
        
        return null;
    }
    
    /**
     * Evaluate a function call to get its value
     */
    private Object evaluateFunctionValue(JfrTable.Row row, FunctionCallNode functionCall) {
        // Simplified function evaluation
        // This would use the function registry in a full implementation
        
        String functionName = functionCall.functionName();
        
        // Handle common aggregate functions
        if ("COUNT".equalsIgnoreCase(functionName)) {
            return 1; // Simplified count
        } else if ("SUM".equalsIgnoreCase(functionName)) {
            return 10; // Simplified sum
        } else if ("AVG".equalsIgnoreCase(functionName)) {
            return 5.0; // Simplified average
        }
        
        return 0;
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
            throw new RuntimeException("Failed to retrieve table for source: " + node.source(), e);
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
        // Expression evaluation - simplified implementation
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
                return performArithmeticOperation(leftTable, rightTable, operator);
            }
            case IN -> {
                return performInOperation(leftTable, rightTable);
            }
            case EQUALS, NOT_EQUALS, LESS_THAN, GREATER_THAN, LESS_EQUAL, GREATER_EQUAL -> {
                return performComparisonOperation(leftTable, rightTable, operator);
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
     * Extracts a single CellValue from a table (assuming single-cell table)
     */
    private CellValue extractSingleCellValue(JfrTable table) {
        if (table.getRows().isEmpty()) {
            return new CellValue.NullValue();
        }
        
        JfrTable.Row firstRow = table.getRows().get(0);
        if (firstRow.getCells().isEmpty()) {
            return new CellValue.NullValue();
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
     * Checks if a value is numeric (supports all numeric types)
     */
    private boolean isNumericValue(Object value) {
        if (value instanceof Number) {
            return true;
        }
        
        if (value instanceof CellValue cellValue) {
            return switch (cellValue.getType()) {
                case NUMBER, FLOAT, DURATION, TIMESTAMP, MEMORY_SIZE, RATE -> true;
                default -> false;
            };
        }
        
        return false;
    }
    
    /**
     * Checks if a value represents an integer type
     */
    private boolean isIntegerValue(Object value) {
        if (value instanceof Number number) {
            return number instanceof Byte || number instanceof Short || 
                   number instanceof Integer || number instanceof Long;
        }
        
        if (value instanceof CellValue cellValue) {
            return cellValue instanceof CellValue.NumberValue || 
                   cellValue instanceof CellValue.MemorySizeValue ||
                   cellValue instanceof CellValue.DurationValue ||
                   cellValue instanceof CellValue.TimestampValue ||
                   cellValue instanceof CellValue.RateValue;
        }
        
        return false;
    }
    
    /**
     * Extracts numeric value from CellValue types
     */
    private double extractNumericValue(CellValue cellValue) {
        return switch (cellValue.getType()) {
            case NUMBER -> ((CellValue.NumberValue) cellValue).value();
            case FLOAT -> ((CellValue.FloatValue) cellValue).value();
            case DURATION -> ((CellValue.DurationValue) cellValue).value().toNanos() / 1_000_000.0; // Convert to milliseconds
            case TIMESTAMP -> ((CellValue.TimestampValue) cellValue).value().toEpochMilli();
            case MEMORY_SIZE -> ((CellValue.MemorySizeValue) cellValue).value();
            case RATE -> ((CellValue.RateValue) cellValue).count();
            default -> throw new IllegalArgumentException(
                "Cannot extract numeric value from " + cellValue.getType()
            );
        };
    }
    
    /**
     * Check if CellValue is numeric
     */
    private boolean isNumericValue(CellValue cellValue) {
        return switch (cellValue.getType()) {
            case NUMBER, FLOAT, DURATION, TIMESTAMP, MEMORY_SIZE, RATE -> true;
            default -> false;
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
        
        if (!functionRegistry.isFunction(functionName)) {
            throw new RuntimeException("Unknown function: " + functionName);
        }
        
        // Evaluate all arguments
        List<CellValue> arguments = new ArrayList<>();
        for (ExpressionNode arg : node.arguments()) {
            JfrTable argTable = arg.accept(this);
            CellValue argValue = extractSingleCellValue(argTable);
            arguments.add(argValue);
        }
        
        try {
            // Use the function registry to evaluate the function with proper context
            CellValue result = functionRegistry.evaluateFunction(functionName, arguments, evaluationContext);
            
            // Return result as a single-cell table
            return SingleCellTable.of("result", result);
            
        } catch (Exception e) {
            throw new RuntimeException("Error evaluating function " + functionName + ": " + e.getMessage(), e);
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
        
        // Return error table if not found
        List<JfrTable.Column> columns = List.of(new JfrTable.Column("error", CellType.STRING));
        JfrTable errorTable = new JfrTable(columns);
        errorTable.addRow(new JfrTable.Row(List.of(new CellValue.StringValue("Variable not found: " + name))));
        return errorTable;
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
                JfrTable result = new JfrTable(columns);
                result.addRow(new JfrTable.Row(List.of(new CellValue.StringValue("Nested query: " + node.jfrQuery()))));
                return result;
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to execute nested JFR query: " + node.jfrQuery(), e);
        } finally {
            evaluationContext.popScope();
        }
    }
    
    @Override
    public JfrTable visitCondition(ConditionNode node) {
        // Condition evaluation is handled in WHERE clause processing
        return context.getCurrentResult();
    }
    
    @Override
    public JfrTable visitWithinCondition(WithinConditionNode node) {
        // Within condition evaluation - used for temporal relationships
        // For now, return current result
        return context.getCurrentResult();
    }
    
    @Override
    public JfrTable visitGCCorrelation(GCCorrelationNode node) {
        // GC correlation processing - advanced feature
        // For now, return current result
        return context.getCurrentResult();
    }
    
    @Override
    public JfrTable visitVariableDeclaration(VariableDeclarationNode node) {
        // Variable declaration processing
        return context.getCurrentResult();
    }
    
    @Override
    public JfrTable visitExpressionCondition(ExpressionConditionNode node) {
        // Expression condition evaluation
        return context.getCurrentResult();
    }
    
    @Override
    public JfrTable visitShowEvents(ShowEventsNode node) {
        // Initialize event types if needed
        initializeEventTypes();
        
        // Show events implementation
        List<JfrTable.Column> columns = List.of(
            new JfrTable.Column("Event Type", CellType.STRING),
            new JfrTable.Column("Count", CellType.NUMBER)
        );
        JfrTable result = new JfrTable(columns);
        
        for (EventType eventType : eventTypes) {
            result.addRow(new JfrTable.Row(List.of(
                new CellValue.StringValue(eventType.getName()),
                new CellValue.NumberValue(0L) // Would be actual count in real implementation
            )));
        }
        
        return result;
    }

    @Override
    public JfrTable visitHelp(HelpNode node) {
        return SingleCellTable.of("Help", new CellValue.StringValue(HelpProvider.getGeneralHelp()));
    }

    @Override
    public JfrTable visitHelpFunction(HelpFunctionNode node) {
        return SingleCellTable.of(
            "Function Help",
            new CellValue.StringValue(HelpProvider.getFunctionHelp(node.functionName()))
        );
    }

    @Override
    public JfrTable visitHelpGrammar(HelpGrammarNode node) {
        return SingleCellTable.of(
            "Grammar Help",
            new CellValue.StringValue(HelpProvider.getGrammarHelp())
        );
    }

    @Override
    public JfrTable visitShowFields(ShowFieldsNode node) {
        // Initialize event types if needed
        initializeEventTypes();
        
        // Show fields implementation
        String eventTypeName = node.eventType();
        
        // Find the event type
        EventType eventType = eventTypes.stream()
            .filter(et -> et.getName().equals(eventTypeName) || 
                         et.getName().endsWith("." + eventTypeName))
            .findFirst()
            .orElse(null);
        
        if (eventType == null) {
            List<JfrTable.Column> errorColumns = List.of(new JfrTable.Column("Error", CellType.STRING));
            JfrTable errorResult = new JfrTable(errorColumns);
            errorResult.addRow(new JfrTable.Row(List.of(
                new CellValue.StringValue("Event type not found: " + eventTypeName)
            )));
            return errorResult;
        }
        
        List<JfrTable.Column> columns = List.of(
            new JfrTable.Column("Field Name", CellType.STRING),
            new JfrTable.Column("Type", CellType.STRING)
        );
        JfrTable result = new JfrTable(columns);
        
        for (var field : eventType.getFields()) {
            result.addRow(new JfrTable.Row(List.of(
                new CellValue.StringValue(field.getName()),
                new CellValue.StringValue(field.getTypeName())
            )));
        }
        
        return result;
    }
    
    @Override
    public JfrTable visitFuzzyJoinSource(FuzzyJoinSourceNode node) {
        // Get the current left table from context (should be set by processFromClause)
        JfrTable leftTable = context.getCurrentResult();
        if (leftTable == null) {
            // If no left table, create a simple metadata table for testing
            List<JfrTable.Column> columns = List.of(
                new JfrTable.Column("source", CellType.STRING),
                new JfrTable.Column("alias", CellType.STRING),
                new JfrTable.Column("join_type", CellType.STRING),
                new JfrTable.Column("join_field", CellType.STRING),
                new JfrTable.Column("tolerance", CellType.STRING),
                new JfrTable.Column("threshold", CellType.STRING)
            );
            
            JfrTable result = new JfrTable(columns);
            
            // Evaluate tolerance and threshold for display
            String toleranceStr = node.tolerance() != null ? 
                extractSingleCellValue(node.tolerance().accept(this)).toString() : "default";
            String thresholdStr = node.threshold() != null ? 
                extractSingleCellValue(node.threshold().accept(this)).toString() : "1.0";
            
            List<CellValue> row = List.of(
                new CellValue.StringValue(node.source()),
                new CellValue.StringValue(node.alias() != null ? node.alias() : ""),
                new CellValue.StringValue(node.joinType().toString()),
                new CellValue.StringValue(node.joinField()),
                new CellValue.StringValue(toleranceStr),
                new CellValue.StringValue(thresholdStr)
            );
            
            result.addRow(new JfrTable.Row(row));
            return result;
        }
        
        // Perform actual fuzzy join
        return performFuzzyJoin(leftTable, node);
    }
    
    @Override
    public JfrTable visitPercentileFunction(PercentileFunctionNode node) {
        // Evaluate percentile function
        String functionName = "PERCENTILE";
        
        // Build arguments: percentile value and data expression
        List<CellValue> arguments = new ArrayList<>();
        
        // Add percentile value as first argument
        arguments.add(new CellValue.FloatValue(node.percentile()));
        
        // Evaluate the value expression
        JfrTable dataTable = node.valueExpression().accept(this);
        CellValue dataValue = extractSingleCellValue(dataTable);
        arguments.add(dataValue);
        
        // If there's a time slice filter, evaluate it as well
        if (node.timeSliceFilter() != null) {
            JfrTable filterTable = node.timeSliceFilter().accept(this);
            CellValue filterValue = extractSingleCellValue(filterTable);
            arguments.add(filterValue);
        }
        
        try {
            // Use the function registry to evaluate the percentile function
            CellValue result = functionRegistry.evaluateFunction(functionName, arguments, evaluationContext);
            
            // Return result as a single-cell table
            return SingleCellTable.of("percentile", result);
            
        } catch (Exception e) {
            throw new RuntimeException("Error evaluating percentile function: " + e.getMessage(), e);
        }
    }
    
    @Override
    public JfrTable visitPercentileSelection(PercentileSelectionNode node) {
        // Evaluate percentile selection function (P99SELECT, P95SELECT, etc.)
        String functionName = node.functionName();
        
        if (!functionRegistry.isFunction(functionName)) {
            throw new RuntimeException("Unknown percentile selection function: " + functionName);
        }
        
        // Build arguments: idField, valueExpression, tableName, percentile
        List<CellValue> arguments = new ArrayList<>();
        arguments.add(new CellValue.StringValue(node.idField()));
        
        // Evaluate the value expression
        JfrTable valueTable = node.valueExpression().accept(this);
        CellValue valueField = extractSingleCellValue(valueTable);
        arguments.add(valueField);
        
        arguments.add(new CellValue.StringValue(node.tableName()));
        arguments.add(new CellValue.FloatValue(node.percentile()));
        
        try {
            // Use the function registry to evaluate the percentile selection function
            CellValue result = functionRegistry.evaluateFunction(functionName, arguments, evaluationContext);
            
            // Return result as a single-cell table
            return SingleCellTable.of("percentile_selection", result);
            
        } catch (Exception e) {
            throw new RuntimeException("Error evaluating percentile selection function " + functionName + ": " + e.getMessage(), e);
        }
    }
    
    // ===== QUERY ENGINE FUNCTIONALITY INTEGRATED INTO QUERYEVALUATOR =====
    
    /**
     * Set a variable value
     */
    public void setVariable(String name, Object value) {
        context.setVariable(name, value);
    }
    
    /**
     * Get a variable value - handles both regular variables and lazy variables
     */
    public Object getVariable(String name) {
        // First check lazy variables (they take precedence)
        LazyQuery lazyQuery = context.getLazyVariable(name);
        if (lazyQuery != null) {
            return lazyQuery.evaluate();
        }
        
        // Fall back to regular variables
        return context.getVariable(name);
    }
    
    /**
     * Get a lazy variable if it exists
     */
    public LazyQuery getLazyVariable(String name) {
        return context.getLazyVariable(name);
    }
    
    /**
     * Define a view
     */
    public void defineView(String name, QueryNode query) {
        context.setView(name, query);
    }
    
    /**
     * Get a view definition
     */
    public QueryNode getView(String name) {
        return context.getView(name);
    }
    
    /**
     * Clear the result cache
     */
    public void clearCache() {
        context.clearCache();
    }
    
    /**
     * Get cached result if available
     */
    public JfrTable getCachedResult(String cacheKey) {
        return context.getCachedResult(cacheKey);
    }
    
    /**
     * Cache a result
     */
    public void cacheResult(String cacheKey, JfrTable result) {
        context.cacheResult(cacheKey, result);
    }
    
    /**
     * Get all variable names (including lazy variables)
     */
    public Map<String, Object> getVariables() {
        Map<String, Object> allVariables = new HashMap<>(context.getVariables());
        
        // Add lazy variables (but don't evaluate them yet)
        for (Map.Entry<String, LazyQuery> entry : context.getLazyVariables().entrySet()) {
            allVariables.put(entry.getKey(), entry.getValue());
        }
        
        return allVariables;
    }
    
    /**
     * Get all lazy variable names
     */
    public Set<String> getLazyVariableNames() {
        initializeEventTypeVariables(); // Ensure event types are initialized
        return new HashSet<>(context.getLazyVariables().keySet());
    }
    
    /**
     * Manually trigger initialization of event type variables.
     * This can be called to ensure all event types are available as variables.
     */
    public void initializeLazyVariables() {
        initializeEventTypeVariables();
    }
    
    /**
     * Get all views
     */
    public Map<String, QueryNode> getViews() {
        return context.getViews();
    }
    
    
    /**
     * Get cache statistics from the execution context
     */
    public ExecutionContext.CacheStats getCacheStats() {
        return context.getCacheStats();
    }
    
    /**
     * Configure cache settings
     */
    public void configureCaching(long maxSize, long ttlMs) {
        context.setCacheConfig(maxSize, ttlMs);
    }
    
    /**
     * Invalidate cache entries that depend on a specific resource
     */
    public void invalidateCacheDependencies(String dependency) {
        context.invalidateCacheDependencies(dependency);
    }
    
    /**
     * Execution result wrapper that includes success/failure information
     */
    public static class ExecutionResult {
        private final JfrTable result;
        private final boolean success;
        private final String errorMessage;
        private final Exception exception;
        
        public ExecutionResult(JfrTable result) {
            this.result = result;
            this.success = true;
            this.errorMessage = null;
            this.exception = null;
        }
        
        public ExecutionResult(String errorMessage, Exception exception) {
            this.result = null;
            this.success = false;
            this.errorMessage = errorMessage;
            this.exception = exception;
        }
        
        public boolean isSuccess() { return success; }
        public JfrTable getResult() { return result; }
        public String getErrorMessage() { return errorMessage; }
        public Exception getException() { return exception; }
    }
    
    /**
     * Execute a query and return a result wrapper
     */
    public ExecutionResult executeQuery(QueryNode query) {
        try {
            JfrTable result = query.accept(this);
            return new ExecutionResult(result);
        } catch (Exception e) {
            return new ExecutionResult(e.getMessage(), e);
        }
    }
    
    /**
     * Execute a complete program containing multiple statements
     */
    public ExecutionResult execute(ProgramNode program) {
        try {
            ExecutionContext context = new ExecutionContext();
            setContext(context);
            JfrTable result = visitProgram(program);
            return new ExecutionResult(result);
        } catch (Exception e) {
            return new ExecutionResult("Execution failed: " + e.getMessage(), e);
        }
    }
    
    /**
     * Set the execution context
     */
    public void setContext(ExecutionContext context) {
        this.context = context;
    }
    
    /**
     * Get the execution context
     */
    public ExecutionContext getContext() {
        return context;
    }
    
    // ===== HELPER METHODS FOR QUERY EXECUTION =====
    
    /**
     * Create placeholder result for unsupported features
     */
    private JfrTable createPlaceholderResult(QueryNode node, String message) {
        List<JfrTable.Column> columns = List.of(new JfrTable.Column("message", CellType.STRING));
        JfrTable result = new JfrTable(columns);
        result.addRow(new JfrTable.Row(List.of(new CellValue.StringValue(message))));
        return result;
    }
    
    /**
     * Evaluate a condition for a specific row in WHERE clause
     * WHERE clauses should not contain aggregate functions
     */
    private boolean evaluateConditionForRow(JfrTable.Row row, ConditionNode condition) {
        try {
            // Set current row context for evaluation
            JfrTable singleRowTable = new JfrTable(List.of(new JfrTable.Column("temp", CellType.STRING)));
            singleRowTable.addRow(row);
            context.setCurrentResult(singleRowTable);
            
            // Use the WHERE-specific condition evaluation (no aggregates allowed)
            return evaluateWhereCondition(row, condition);
        } catch (Exception e) {
            // Default to including the row if evaluation fails
            return true;
        }
    }
    
    /**
     * Evaluate a WHERE condition against a row
     * WHERE conditions cannot contain aggregate functions
     */
    private boolean evaluateWhereCondition(JfrTable.Row row, ConditionNode condition) {
        // Handle ExpressionConditionNode which wraps expression nodes
        if (condition instanceof ExpressionConditionNode exprCondition) {
            ExpressionNode expr = exprCondition.expression();
            
            if (expr instanceof BinaryExpressionNode binaryExpr) {
                return evaluateBinaryWhereCondition(row, binaryExpr);
            } else if (expr instanceof FunctionCallNode functionCall) {
                return evaluateFunctionWhereCondition(row, functionCall);
            } else if (expr instanceof FieldAccessNode fieldAccess) {
                // Simple field access - evaluate as boolean
                CellValue value = getRowValueByColumnName(row, List.of(), fieldAccess.field());
                return isTruthyValue(value);
            } else if (expr instanceof IdentifierNode identifier) {
                // Simple identifier - evaluate as boolean
                CellValue value = getRowValueByColumnName(row, List.of(), identifier.name());
                return isTruthyValue(value);
            }
        }
        
        // Default to including the row if we can't evaluate the condition
        return true;
    }
    
    /**
     * Check if a CellValue is considered "truthy" for boolean evaluation
     */
    private boolean isTruthyValue(CellValue value) {
        if (value instanceof CellValue.BooleanValue boolVal) {
            return boolVal.value();
        } else if (value instanceof CellValue.NumberValue numVal) {
            return numVal.value() != 0;
        } else if (value instanceof CellValue.FloatValue floatVal) {
            return floatVal.value() != 0.0;
        } else if (value instanceof CellValue.StringValue strVal) {
            return !strVal.value().isEmpty() && !"false".equalsIgnoreCase(strVal.value());
        } else if (value instanceof CellValue.NullValue) {
            return false;
        }
        return true; // Default to true for other types
    }
    
    /**
     * Evaluate a binary expression in WHERE clause
     */
    private boolean evaluateBinaryWhereCondition(JfrTable.Row row, BinaryExpressionNode binaryExpr) {
        BinaryOperator operator = binaryExpr.operator();
        
        try {
            // Evaluate left and right operands using the AST visitor
            JfrTable leftResult = binaryExpr.left().accept(this);
            JfrTable rightResult = binaryExpr.right().accept(this);
            
            CellValue leftValue = extractSingleCellValue(leftResult);
            CellValue rightValue = extractSingleCellValue(rightResult);
            
            // Handle comparison operations
            if (operator == BinaryOperator.GREATER_THAN || 
                operator == BinaryOperator.LESS_THAN || 
                operator == BinaryOperator.EQUALS ||
                operator == BinaryOperator.NOT_EQUALS ||
                operator == BinaryOperator.GREATER_EQUAL ||
                operator == BinaryOperator.LESS_EQUAL) {
                
                int comparison = CellValue.compare(leftValue, rightValue);
                return switch (operator) {
                    case GREATER_THAN -> comparison > 0;
                    case LESS_THAN -> comparison < 0;
                    case EQUALS -> comparison == 0;
                    case NOT_EQUALS -> comparison != 0;
                    case GREATER_EQUAL -> comparison >= 0;
                    case LESS_EQUAL -> comparison <= 0;
                    default -> true;
                };
            }
            
            // Handle logical operations
            if (operator == BinaryOperator.AND || operator == BinaryOperator.OR) {
                boolean leftBool = isTruthyValue(leftValue);
                boolean rightBool = isTruthyValue(rightValue);
                
                return switch (operator) {
                    case AND -> leftBool && rightBool;
                    case OR -> leftBool || rightBool;
                    default -> true;
                };
            }
            
            // Handle IN operation
            if (operator == BinaryOperator.IN) {
                if (rightValue instanceof CellValue.ArrayValue arrayValue) {
                    return arrayValue.contains(leftValue);
                } else {
                    return CellValue.compare(leftValue, rightValue) == 0;
                }
            }
            
        } catch (Exception e) {
            // If evaluation fails, log and return true to include the row
            System.err.println("Error evaluating WHERE condition: " + e.getMessage());
        }
        
        return true; // Default to including row
    }
    
    /**
     * Evaluate a function call in WHERE clause
     * Aggregate functions are not allowed in WHERE clauses
     */
    private boolean evaluateFunctionWhereCondition(JfrTable.Row row, FunctionCallNode functionCall) {
        String functionName = functionCall.functionName();
        
        // Check if this is an aggregate function - these are not allowed in WHERE clauses
        if (isAggregateFunction(functionName)) {
            throw new RuntimeException("Aggregate function '" + functionName + "' cannot be used in WHERE clause. Use HAVING clause instead.");
        }
        
        try {
            // Evaluate non-aggregate function using the function registry
            List<CellValue> arguments = new ArrayList<>();
            for (ExpressionNode arg : functionCall.arguments()) {
                JfrTable argTable = arg.accept(this);
                CellValue argValue = extractSingleCellValue(argTable);
                arguments.add(argValue);
            }
            
            CellValue result = functionRegistry.evaluateFunction(functionName, arguments, evaluationContext);
            return isTruthyValue(result);
            
        } catch (Exception e) {
            // If function evaluation fails, log and return true to include the row
            System.err.println("Error evaluating function in WHERE clause: " + e.getMessage());
            return true;
        }
    }
    
    /**
     * Check if a function is an aggregate function
     */
    private boolean isAggregateFunction(String functionName) {
        String upperName = functionName.toUpperCase();
        return switch (upperName) {
            case "COUNT", "SUM", "AVG", "AVERAGE", "MIN", "MAX", 
                 "STDDEV", "VARIANCE", "PERCENTILE", "MEDIAN" -> true;
            default -> upperName.startsWith("P9") || upperName.startsWith("PERCENTILE");
        };
    }
    
    /**
     * Build group key from row values for specified fields
     */
    private String buildGroupKey(JfrTable.Row row, List<JfrTable.Column> columns, List<ExpressionNode> fields) {
        StringBuilder keyBuilder = new StringBuilder();
        for (ExpressionNode field : fields) {
            if (keyBuilder.length() > 0) {
                keyBuilder.append("|");
            }
            // Extract field name and get value from row
            String fieldName = extractColumnName(field);
            CellValue value = getRowValueByColumnName(row, columns, fieldName);
            keyBuilder.append(fieldName).append("=").append(value.toString());
        }
        return keyBuilder.toString();
    }
    
    /**
     * Get row value as string for grouping
     */
    private String getRowValueAsString(JfrTable.Row row, String fieldName) {
        // Simplified implementation - would map field names to column indices
        if (!row.getCells().isEmpty()) {
            return row.getCells().get(0).toString();
        }
        return "";
    }
    
    /**
     * Determine column type by evaluating expression
     */
    private CellType determineColumnType(ExpressionNode expression, JfrTable sourceData) {
        try {
            // Try to evaluate the expression on sample data to determine type
            if (!sourceData.getRows().isEmpty()) {
                CellValue sampleValue = evaluateExpressionForRow(sourceData.getRows().get(0), expression);
                return sampleValue.getType();
            }
        } catch (Exception e) {
            // Fall back to STRING type if evaluation fails
        }
        return CellType.STRING;
    }
    
    /**
     * Evaluate expression for a specific row
     */
    private CellValue evaluateExpressionForRow(JfrTable.Row row, ExpressionNode expression) {
        try {
            // Set row context and evaluate expression
            JfrTable rowTable = new JfrTable(List.of(new JfrTable.Column("temp", CellType.STRING)));
            rowTable.addRow(row);
            context.setCurrentResult(rowTable);
            
            JfrTable result = expression.accept(this);
            return extractSingleCellValue(result);
        } catch (Exception e) {
            return new CellValue.StringValue("Error: " + e.getMessage());
        }
    }
    
    /**
     * Get row value by column name
     */
    private CellValue getRowValueByColumnName(JfrTable.Row row, List<JfrTable.Column> columns, String columnName) {
        // First try to find by exact column name match
        for (int i = 0; i < columns.size() && i < row.getCells().size(); i++) {
            if (columns.get(i).name().equals(columnName)) {
                return row.getCells().get(i);
            }
        }
        
        // If columns list is empty or no match found, try to get value from current result context
        if (context.getCurrentResult() != null) {
            JfrTable currentResult = context.getCurrentResult();
            List<JfrTable.Column> currentColumns = currentResult.getColumns();
            
            for (int i = 0; i < currentColumns.size() && i < row.getCells().size(); i++) {
                if (currentColumns.get(i).name().equals(columnName)) {
                    return row.getCells().get(i);
                }
            }
        }
        
        // If still no match, check if it's a special identifier like "*"
        if ("*".equals(columnName) && !row.getCells().isEmpty()) {
            return row.getCells().get(0); // Return first cell for "*"
        }
        
        return new CellValue.NullValue();
    }
    
    /**
     * Performs IN operation: checks if left value is contained in right array
     */
    private JfrTable performInOperation(JfrTable leftTable, JfrTable rightTable) {
        CellValue leftValue = extractSingleCellValue(leftTable);
        CellValue rightValue = extractSingleCellValue(rightTable);
        
        boolean result;
        if (rightValue instanceof CellValue.ArrayValue arrayValue) {
            // Use the optimized contains method from ArrayValue
            result = arrayValue.contains(leftValue);
        } else {
            // Fallback: convert right value to string and check if left is equal
            result = leftValue.equals(rightValue);
        }
        
        return SingleCellTable.of(new CellValue.BooleanValue(result));
    }
    
    /**
     * Performs comparison operations between two values
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
            default -> throw new IllegalArgumentException("Unsupported comparison operator: " + operator);
        };
        
        return SingleCellTable.of(new CellValue.BooleanValue(result));
    }

    @Override
    public JfrTable visitRawJfrQuery(RawJfrQueryNode node) {
        // Execute raw JFR query using the provided executor
        try {
            if (rawJfrExecutor == null) {
                throw new UnsupportedOperationException("Raw JFR query execution not supported - no executor provided");
            }
            
            return rawJfrExecutor.execute(node);
            
        } catch (Exception e) {
            throw new RuntimeException("Failed to execute raw JFR query: " + node.rawQuery(), e);
        }
    }

    @Override
    public JfrTable visitStandardJoinSource(StandardJoinSourceNode node) {
        // Get the left table from context (should be set by processFromClause)
        JfrTable leftTable = context.getCurrentResult();
        if (leftTable == null) {
            throw new IllegalStateException("No left table available for join");
        }
        
        // Get the right table by processing the source
        SourceNode rightSource = new SourceNode(node.source(), node.alias(), node.location());
        JfrTable rightTable = visitSource(rightSource);
        
        // Perform the appropriate join based on join type
        return switch (node.joinType()) {
            case INNER -> performInnerJoin(leftTable, rightTable, node.leftJoinField(), node.rightJoinField());
            case LEFT -> performLeftJoin(leftTable, rightTable, node.leftJoinField(), node.rightJoinField());
            case RIGHT -> performRightJoin(leftTable, rightTable, node.leftJoinField(), node.rightJoinField());
            case FULL -> performFullJoin(leftTable, rightTable, node.leftJoinField(), node.rightJoinField());
        };
    }
    
    /**
     * Perform inner join between two tables
     */
    private JfrTable performInnerJoin(JfrTable leftTable, JfrTable rightTable, String leftField, String rightField) {
        // Create combined column list
        List<JfrTable.Column> resultColumns = new ArrayList<>(leftTable.getColumns());
        for (JfrTable.Column rightCol : rightTable.getColumns()) {
            // Avoid duplicate column names by prefixing with table alias
            String columnName = rightCol.name().equals(rightField) ? 
                "right_" + rightCol.name() : rightCol.name();
            resultColumns.add(new JfrTable.Column(columnName, rightCol.type()));
        }
        
        JfrTable result = new JfrTable(resultColumns);
        
        // Find matching rows
        for (JfrTable.Row leftRow : leftTable.getRows()) {
            CellValue leftJoinValue = getRowValueByColumnName(leftRow, leftTable.getColumns(), leftField);
            
            for (JfrTable.Row rightRow : rightTable.getRows()) {
                CellValue rightJoinValue = getRowValueByColumnName(rightRow, rightTable.getColumns(), rightField);
                
                // Check if join condition is satisfied
                if (CellValue.compare(leftJoinValue, rightJoinValue) == 0) {
                    // Combine rows
                    List<CellValue> combinedCells = new ArrayList<>(leftRow.getCells());
                    combinedCells.addAll(rightRow.getCells());
                    result.addRow(new JfrTable.Row(combinedCells));
                }
            }
        }
        
        return result;
    }
    
    /**
     * Perform left outer join between two tables
     */
    private JfrTable performLeftJoin(JfrTable leftTable, JfrTable rightTable, String leftField, String rightField) {
        // Create combined column list
        List<JfrTable.Column> resultColumns = new ArrayList<>(leftTable.getColumns());
        for (JfrTable.Column rightCol : rightTable.getColumns()) {
            String columnName = rightCol.name().equals(rightField) ? 
                "right_" + rightCol.name() : rightCol.name();
            resultColumns.add(new JfrTable.Column(columnName, rightCol.type()));
        }
        
        JfrTable result = new JfrTable(resultColumns);
        
        // Process each left row
        for (JfrTable.Row leftRow : leftTable.getRows()) {
            CellValue leftJoinValue = getRowValueByColumnName(leftRow, leftTable.getColumns(), leftField);
            boolean foundMatch = false;
            
            // Look for matching right rows
            for (JfrTable.Row rightRow : rightTable.getRows()) {
                CellValue rightJoinValue = getRowValueByColumnName(rightRow, rightTable.getColumns(), rightField);
                
                if (CellValue.compare(leftJoinValue, rightJoinValue) == 0) {
                    // Found match - combine rows
                    List<CellValue> combinedCells = new ArrayList<>(leftRow.getCells());
                    combinedCells.addAll(rightRow.getCells());
                    result.addRow(new JfrTable.Row(combinedCells));
                    foundMatch = true;
                }
            }
            
            // If no match found, add left row with nulls for right columns
            if (!foundMatch) {
                List<CellValue> combinedCells = new ArrayList<>(leftRow.getCells());
                for (int i = 0; i < rightTable.getColumns().size(); i++) {
                    combinedCells.add(new CellValue.NullValue());
                }
                result.addRow(new JfrTable.Row(combinedCells));
            }
        }
        
        return result;
    }
    
    /**
     * Perform right outer join between two tables
     */
    private JfrTable performRightJoin(JfrTable leftTable, JfrTable rightTable, String leftField, String rightField) {
        // Right join is equivalent to left join with tables swapped
        return performLeftJoin(rightTable, leftTable, rightField, leftField);
    }
    
    /**
     * Perform full outer join between two tables
     */
    private JfrTable performFullJoin(JfrTable leftTable, JfrTable rightTable, String leftField, String rightField) {
        // Create combined column list
        List<JfrTable.Column> resultColumns = new ArrayList<>(leftTable.getColumns());
        for (JfrTable.Column rightCol : rightTable.getColumns()) {
            String columnName = rightCol.name().equals(rightField) ? 
                "right_" + rightCol.name() : rightCol.name();
            resultColumns.add(new JfrTable.Column(columnName, rightCol.type()));
        }
        
        JfrTable result = new JfrTable(resultColumns);
        
        // Track which right rows have been matched
        Set<Integer> matchedRightRows = new HashSet<>();
        
        // Process left rows (similar to left join)
        for (JfrTable.Row leftRow : leftTable.getRows()) {
            CellValue leftJoinValue = getRowValueByColumnName(leftRow, leftTable.getColumns(), leftField);
            boolean foundMatch = false;
            
            for (int rightIndex = 0; rightIndex < rightTable.getRows().size(); rightIndex++) {
                JfrTable.Row rightRow = rightTable.getRows().get(rightIndex);
                CellValue rightJoinValue = getRowValueByColumnName(rightRow, rightTable.getColumns(), rightField);
                
                if (CellValue.compare(leftJoinValue, rightJoinValue) == 0) {
                    // Found match
                    List<CellValue> combinedCells = new ArrayList<>(leftRow.getCells());
                    combinedCells.addAll(rightRow.getCells());
                    result.addRow(new JfrTable.Row(combinedCells));
                    matchedRightRows.add(rightIndex);
                    foundMatch = true;
                }
            }
            
            // If no match found, add left row with nulls
            if (!foundMatch) {
                List<CellValue> combinedCells = new ArrayList<>(leftRow.getCells());
                for (int i = 0; i < rightTable.getColumns().size(); i++) {
                    combinedCells.add(new CellValue.NullValue());
                }
                result.addRow(new JfrTable.Row(combinedCells));
            }
        }
        
        // Add unmatched right rows with nulls for left columns
        for (int rightIndex = 0; rightIndex < rightTable.getRows().size(); rightIndex++) {
            if (!matchedRightRows.contains(rightIndex)) {
                JfrTable.Row rightRow = rightTable.getRows().get(rightIndex);
                List<CellValue> combinedCells = new ArrayList<>();
                
                // Add nulls for left columns
                for (int i = 0; i < leftTable.getColumns().size(); i++) {
                    combinedCells.add(new CellValue.NullValue());
                }
                // Add right row data
                combinedCells.addAll(rightRow.getCells());
                result.addRow(new JfrTable.Row(combinedCells));
            }
        }
        
        return result;
    }
    
    // ===== OPTIMIZED JOIN METHODS WITH WHERE CLAUSE INTEGRATION =====
    
    /**
     * Perform optimized inner join with WHERE clause filtering during join operation
     * This reduces the size of intermediate results by applying filters as rows are joined
     */
    private JfrTable performInnerJoinOptimized(JfrTable leftTable, JfrTable rightTable, String leftField, String rightField, WhereNode whereNode) {
        // Create combined column list
        List<JfrTable.Column> resultColumns = new ArrayList<>(leftTable.getColumns());
        for (JfrTable.Column rightCol : rightTable.getColumns()) {
            String columnName = rightCol.name().equals(rightField) ? 
                "right_" + rightCol.name() : rightCol.name();
            resultColumns.add(new JfrTable.Column(columnName, rightCol.type()));
        }
        
        JfrTable result = new JfrTable(resultColumns);
        
        // Find matching rows and apply WHERE filtering during join
        for (JfrTable.Row leftRow : leftTable.getRows()) {
            CellValue leftJoinValue = getRowValueByColumnName(leftRow, leftTable.getColumns(), leftField);
            
            for (JfrTable.Row rightRow : rightTable.getRows()) {
                CellValue rightJoinValue = getRowValueByColumnName(rightRow, rightTable.getColumns(), rightField);
                
                // Check join condition
                if (CellValue.compare(leftJoinValue, rightJoinValue) == 0) {
                    // Create joined row
                    List<CellValue> joinedCells = new ArrayList<>(leftRow.getCells());
                    joinedCells.addAll(rightRow.getCells());
                    JfrTable.Row joinedRow = new JfrTable.Row(joinedCells);
                    
                    // Apply WHERE filtering immediately to reduce result size
                    if (whereNode == null || evaluateConditionForRow(joinedRow, whereNode.condition())) {
                        result.addRow(joinedRow);
                    }
                }
            }
        }
        
        return result;
    }
    
    /**
     * Perform optimized left outer join with WHERE clause filtering
     */
    private JfrTable performLeftJoinOptimized(JfrTable leftTable, JfrTable rightTable, String leftField, String rightField, WhereNode whereNode) {
        // Create combined column list
        List<JfrTable.Column> resultColumns = new ArrayList<>(leftTable.getColumns());
        for (JfrTable.Column rightCol : rightTable.getColumns()) {
            String columnName = rightCol.name().equals(rightField) ? 
                "right_" + rightCol.name() : rightCol.name();
            resultColumns.add(new JfrTable.Column(columnName, rightCol.type()));
        }
        
        JfrTable result = new JfrTable(resultColumns);
        
        // Process each left row
        for (JfrTable.Row leftRow : leftTable.getRows()) {
            CellValue leftJoinValue = getRowValueByColumnName(leftRow, leftTable.getColumns(), leftField);
            boolean foundMatch = false;
            
            for (JfrTable.Row rightRow : rightTable.getRows()) {
                CellValue rightJoinValue = getRowValueByColumnName(rightRow, rightTable.getColumns(), rightField);
                
                if (CellValue.compare(leftJoinValue, rightJoinValue) == 0) {
                    foundMatch = true;
                    
                    // Create joined row
                    List<CellValue> joinedCells = new ArrayList<>(leftRow.getCells());
                    joinedCells.addAll(rightRow.getCells());
                    JfrTable.Row joinedRow = new JfrTable.Row(joinedCells);
                    
                    // Apply WHERE filtering
                    if (whereNode == null || evaluateConditionForRow(joinedRow, whereNode.condition())) {
                        result.addRow(joinedRow);
                    }
                }
            }
            
            // If no match found, add left row with nulls for right columns
            if (!foundMatch) {
                List<CellValue> joinedCells = new ArrayList<>(leftRow.getCells());
                // Add null values for right table columns
                for (int i = 0; i < rightTable.getColumns().size(); i++) {
                    joinedCells.add(new CellValue.NullValue());
                }
                JfrTable.Row joinedRow = new JfrTable.Row(joinedCells);
                
                // Apply WHERE filtering
                if (whereNode == null || evaluateConditionForRow(joinedRow, whereNode.condition())) {
                    result.addRow(joinedRow);
                }
            }
        }
        
        return result;
    }
    
    /**
     * Perform optimized right outer join with WHERE clause filtering
     */
    private JfrTable performRightJoinOptimized(JfrTable leftTable, JfrTable rightTable, String leftField, String rightField, WhereNode whereNode) {
        // For simplicity, implement as left join with tables swapped
        return performLeftJoinOptimized(rightTable, leftTable, rightField, leftField, whereNode);
    }
    
    /**
     * Perform optimized full outer join with WHERE clause filtering
     */
    private JfrTable performFullJoinOptimized(JfrTable leftTable, JfrTable rightTable, String leftField, String rightField, WhereNode whereNode) {
        // Combine left and right joins, removing duplicates
        JfrTable leftResult = performLeftJoinOptimized(leftTable, rightTable, leftField, rightField, whereNode);
        JfrTable rightResult = performRightJoinOptimized(leftTable, rightTable, leftField, rightField, whereNode);
        
        // Merge results (simplified - in practice would need to detect and remove duplicates)
        JfrTable result = new JfrTable(leftResult.getColumns());
        
        // Add all left join results
        for (JfrTable.Row row : leftResult.getRows()) {
            result.addRow(row);
        }
        
        // Add right join results that aren't already included
        // (This is a simplified implementation - would need proper duplicate detection)
        for (JfrTable.Row row : rightResult.getRows()) {
            result.addRow(row);
        }
        
        return result;
    }

    @Override
    public JfrTable visitArrayLiteral(ArrayLiteralNode node) {
        // Return a table containing the array literal values
        List<JfrTable.Column> columns = List.of(new JfrTable.Column("array_value", CellType.STRING));
        JfrTable result = new JfrTable(columns);
        
        for (ExpressionNode element : node.elements()) {
            JfrTable elementResult = element.accept(this);
            // Add each element as a row
            if (!elementResult.getRows().isEmpty()) {
                result.addRow(elementResult.getRows().get(0));
            }
        }
        
        return result;
    }
    
    @Override 
    public JfrTable visitSelectItem(SelectItemNode node) {
        // Evaluate the expression and create a single-column table
        JfrTable expressionResult = node.expression().accept(this);
        
        // If alias is provided, rename the column
        if (node.alias() != null) {
            List<JfrTable.Column> renamedColumns = new ArrayList<>();
            for (JfrTable.Column col : expressionResult.getColumns()) {
                renamedColumns.add(new JfrTable.Column(node.alias(), col.type()));
                break; // Only rename first column for select item
            }
            
            JfrTable result = new JfrTable(renamedColumns);
            for (JfrTable.Row row : expressionResult.getRows()) {
                result.addRow(row);
            }
            return result;
        }
        
        return expressionResult;
    }
    
    /**
     * Perform cross join between two tables
     */
    private JfrTable performCrossJoin(JfrTable leftTable, JfrTable rightTable) {
        // Create combined column list
        List<JfrTable.Column> resultColumns = new ArrayList<>(leftTable.getColumns());
        for (JfrTable.Column rightCol : rightTable.getColumns()) {
            resultColumns.add(new JfrTable.Column("right_" + rightCol.name(), rightCol.type()));
        }
        
        JfrTable result = new JfrTable(resultColumns);
        
        // Cross product of all rows
        for (JfrTable.Row leftRow : leftTable.getRows()) {
            for (JfrTable.Row rightRow : rightTable.getRows()) {
                List<CellValue> joinedCells = new ArrayList<>(leftRow.getCells());
                joinedCells.addAll(rightRow.getCells());
                result.addRow(new JfrTable.Row(joinedCells));
            }
        }
        
        return result;
    }
    
    /**
     * Perform fuzzy join using similarity matching
     */
    private JfrTable performFuzzyJoin(JfrTable leftTable, FuzzyJoinSourceNode fuzzyJoin) {
        // Get the right table data
        SourceNode rightSource = new SourceNode(fuzzyJoin.source(), fuzzyJoin.alias(), fuzzyJoin.location());
        JfrTable rightTable = visitSource(rightSource);
        
        // Create combined column list  
        List<JfrTable.Column> resultColumns = new ArrayList<>(leftTable.getColumns());
        for (JfrTable.Column rightCol : rightTable.getColumns()) {
            String columnName = rightCol.name().equals(fuzzyJoin.joinField()) ? 
                "right_" + rightCol.name() : rightCol.name();
            resultColumns.add(new JfrTable.Column(columnName, rightCol.type()));
        }
        
        JfrTable result = new JfrTable(resultColumns);
        
        // Evaluate threshold expression to get similarity threshold
        double thresholdValue = 0.8; // Default threshold
        if (fuzzyJoin.threshold() != null) {
            JfrTable thresholdResult = fuzzyJoin.threshold().accept(this);
            if (!thresholdResult.getRows().isEmpty() && !thresholdResult.getRows().get(0).getCells().isEmpty()) {
                CellValue thresholdCell = thresholdResult.getRows().get(0).getCells().get(0);
                if (thresholdCell instanceof CellValue.FloatValue floatVal) {
                    thresholdValue = floatVal.value();
                } else if (thresholdCell instanceof CellValue.NumberValue numberVal) {
                    thresholdValue = numberVal.value();
                }
            }
        }
        
        for (JfrTable.Row leftRow : leftTable.getRows()) {
            CellValue leftJoinValue = getRowValueByColumnName(leftRow, leftTable.getColumns(), fuzzyJoin.joinField());
            
            for (JfrTable.Row rightRow : rightTable.getRows()) {
                CellValue rightJoinValue = getRowValueByColumnName(rightRow, rightTable.getColumns(), fuzzyJoin.joinField());
                
                // Calculate similarity between join values
                double similarity = calculateSimilarity(leftJoinValue, rightJoinValue);
                
                if (similarity >= thresholdValue) {
                    // Create joined row
                    List<CellValue> joinedCells = new ArrayList<>(leftRow.getCells());
                    joinedCells.addAll(rightRow.getCells());
                    result.addRow(new JfrTable.Row(joinedCells));
                }
            }
        }
        
        return result;
    }
    
    /**
     * Perform standard join (delegates to appropriate optimized join method)
     */
    private JfrTable performStandardJoin(JfrTable leftTable, StandardJoinSourceNode standardJoin) {
        // Get the right table
        SourceNode rightSource = new SourceNode(standardJoin.source(), standardJoin.alias(), standardJoin.location());
        JfrTable rightTable = visitSource(rightSource);
        
        // Delegate to optimized join methods with null WHERE clause
        return switch (standardJoin.joinType()) {
            case INNER -> performInnerJoinOptimized(leftTable, rightTable, standardJoin.leftJoinField(), standardJoin.rightJoinField(), null);
            case LEFT -> performLeftJoinOptimized(leftTable, rightTable, standardJoin.leftJoinField(), standardJoin.rightJoinField(), null);
            case RIGHT -> performRightJoinOptimized(leftTable, rightTable, standardJoin.leftJoinField(), standardJoin.rightJoinField(), null);
            case FULL -> performFullJoinOptimized(leftTable, rightTable, standardJoin.leftJoinField(), standardJoin.rightJoinField(), null);
        };
    }
    
    /**
     * Calculate similarity between two cell values for fuzzy matching
     */
    private double calculateSimilarity(CellValue left, CellValue right) {
        if (left == null || right == null) {
            return 0.0;
        }
        
        String leftStr = left.toString();
        String rightStr = right.toString();
        
        if (leftStr.equals(rightStr)) {
            return 1.0;
        }
        
        // Simple string similarity using Levenshtein distance
        int maxLen = Math.max(leftStr.length(), rightStr.length());
        if (maxLen ==  0) {
            return 1.0;
        }
        
        int distance = levenshteinDistance(leftStr, rightStr);
        return 1.0 - (double) distance / maxLen;
    }
    
    /**
     * Calculate Levenshtein distance between two strings
     */
    private int levenshteinDistance(String s1, String s2) {
        int[][] dp = new int[s1.length() + 1][s2.length() + 1];
        
        for (int i = 0; i <= s1.length(); i++) {
            dp[i][0] = i;
        }
        for (int j = 0; j <= s2.length(); j++) {
            dp[0][j] = j;
        }
        
        for (int i = 1; i <= s1.length(); i++) {
            for (int j = 1; j <= s2.length(); j++) {
                if (s1.charAt(i - 1) == s2.charAt(j - 1)) {
                    dp[i][j] = dp[i - 1][j - 1];
                } else {
                    dp[i][j] = 1 + Math.min(Math.min(dp[i - 1][j], dp[i][j - 1]), dp[i - 1][j - 1]);
                }
            }
        }
        
        return dp[s1.length()][s2.length()];
    }
    
    /**
     * Generate a cache key for a query node based on its structure
     */
    private String generateQueryCacheKey(QueryNode node) {
        StringBuilder keyBuilder = new StringBuilder();
        
        // Include query structure in the key
        keyBuilder.append("Q:");
        keyBuilder.append(node.isExtended() ? "EXT:" : "BASIC:");
        
        // Add SELECT clause
        if (node.select() != null) {
            keyBuilder.append("SELECT:");
            for (SelectItemNode item : node.select().items()) {
                keyBuilder.append(extractExpressionKey(item.expression()));
                if (item.alias() != null) {
                    keyBuilder.append(" AS ").append(item.alias());
                }
                keyBuilder.append(",");
            }
        }
        
        // Add FROM clause
        if (node.from() != null) {
            keyBuilder.append("FROM:");
            for (SourceNodeBase source : node.from().sources()) {
                keyBuilder.append(extractSourceKey(source));
                keyBuilder.append(",");
            }
        }
        
        // Add WHERE clause
        if (node.where() != null) {
            keyBuilder.append("WHERE:");
            keyBuilder.append(extractConditionKey(node.where().condition()));
        }
        
        // Add GROUP BY clause
        if (node.groupBy() != null) {
            keyBuilder.append("GROUP_BY:");
            for (ExpressionNode expr : node.groupBy().fields()) {
                keyBuilder.append(extractExpressionKey(expr));
                keyBuilder.append(",");
            }
        }
        
        // Add HAVING clause
        if (node.having() != null) {
            keyBuilder.append("HAVING:");
            keyBuilder.append(extractConditionKey(node.having().condition()));
        }
        
        // Add ORDER BY clause
        if (node.orderBy() != null) {
            keyBuilder.append("ORDER_BY:");
            for (OrderFieldNode field : node.orderBy().fields()) {
                keyBuilder.append(extractExpressionKey(field.field()));
                keyBuilder.append(":").append(field.order());
                keyBuilder.append(",");
            }
        }
        
        // Add LIMIT clause
        if (node.limit() != null) {
            keyBuilder.append("LIMIT:").append(node.limit().limit());
        }
        
        return keyBuilder.toString();
    }
    
    /**
     * Extract a key representation of an expression for caching
     */
    private String extractExpressionKey(ExpressionNode expr) {
        if (expr instanceof IdentifierNode id) {
            return "ID:" + id.name();
        } else if (expr instanceof FieldAccessNode field) {
            return "FIELD:" + (field.qualifier() != null ? field.qualifier() + "." : "") + field.field();
        } else if (expr instanceof LiteralNode literal) {
            return "LIT:" + literal.value().toString();
        } else if (expr instanceof FunctionCallNode func) {
            StringBuilder funcKey = new StringBuilder("FUNC:" + func.functionName() + "(");
            for (ExpressionNode arg : func.arguments()) {
                funcKey.append(extractExpressionKey(arg)).append(",");
            }
            funcKey.append(")");
            return funcKey.toString();
        } else if (expr instanceof BinaryExpressionNode binary) {
            return "BIN:" + extractExpressionKey(binary.left()) + ":" + binary.operator() + ":" + extractExpressionKey(binary.right());
        }
        return expr.getClass().getSimpleName();
    }
    
    /**
     * Extract a key representation of a source for caching
     */
    private String extractSourceKey(SourceNodeBase source) {
        if (source instanceof SourceNode src) {
            return "SRC:" + src.source() + (src.alias() != null ? " AS " + src.alias() : "");
        } else if (source instanceof SubquerySourceNode sub) {
            // For subqueries, create a simplified cache key 
            return "SUBQ:" + sub.query().hashCode() + (sub.alias() != null ? " AS " + sub.alias() : "");
        } else if (source instanceof StandardJoinSourceNode join) {
            return "JOIN:" + join.joinType() + ":" + join.source() + " ON " + join.leftJoinField() + "=" + join.rightJoinField();
        } else if (source instanceof FuzzyJoinSourceNode fuzzy) {
            return "FUZZY:" + fuzzy.joinType() + ":" + fuzzy.source() + " ON " + fuzzy.joinField();
        }
        return source.getClass().getSimpleName();
    }
    
    /**
     * Extract a key representation of a condition for caching
     */
    private String extractConditionKey(ConditionNode condition) {
        if (condition instanceof ExpressionConditionNode exprCond) {
            return "EXPR:" + extractExpressionKey(exprCond.expression());
        } else if (condition instanceof WithinConditionNode within) {
            return "WITHIN:" + within.toString(); // Simplified
        }
        return condition.getClass().getSimpleName();
    }
    
    /**
     * Extract query dependencies for cache invalidation
     */
    private Set<String> extractQueryDependencies(QueryNode node) {
        Set<String> dependencies = new HashSet<>();
        
        // Extract table dependencies from FROM clause
        if (node.from() != null) {
            for (SourceNodeBase source : node.from().sources()) {
                extractSourceDependencies(source, dependencies);
            }
        }
        
        // Extract variable dependencies from expressions
        extractExpressionDependencies(node, dependencies);
        
        return dependencies;
    }
    
    /**
     * Extract dependencies from a source node
     */
    private void extractSourceDependencies(SourceNodeBase source, Set<String> dependencies) {
        if (source instanceof SourceNode sourceNode) {
            dependencies.add("table:" + sourceNode.source());
        } else if (source instanceof SubquerySourceNode subquerySource) {
            // Handle subquery dependencies - check if the statement is a QueryNode
            if (subquerySource.query() instanceof QueryNode queryNode) {
                dependencies.addAll(extractQueryDependencies(queryNode));
            } else {
                // For non-query statements, add a generic dependency
                dependencies.add("subquery:" + subquerySource.query().hashCode());
            }
        } else if (source instanceof StandardJoinSourceNode joinSource) {
            dependencies.add("table:" + joinSource.source());
        } else if (source instanceof FuzzyJoinSourceNode fuzzyJoinSource) {
            dependencies.add("table:" + fuzzyJoinSource.source());
        }
    }
    
    /**
     * Extract dependencies from query expressions (variables, views, etc.)
     */
    private void extractExpressionDependencies(QueryNode node, Set<String> dependencies) {
        // Extract from SELECT clause
        if (node.select() != null) {
            for (SelectItemNode item : node.select().items()) {
                extractExpressionNodeDependencies(item.expression(), dependencies);
            }
        }
        
        // Extract from WHERE clause
        if (node.where() != null && node.where().condition() instanceof ExpressionConditionNode) {
            ExpressionConditionNode exprCond = (ExpressionConditionNode) node.where().condition();
            extractExpressionNodeDependencies(exprCond.expression(), dependencies);
        }
        
        // Extract from HAVING clause
        if (node.having() != null && node.having().condition() instanceof ExpressionConditionNode) {
            ExpressionConditionNode exprCond = (ExpressionConditionNode) node.having().condition();
            extractExpressionNodeDependencies(exprCond.expression(), dependencies);
        }
        
        // Extract from GROUP BY clause
        if (node.groupBy() != null) {
            for (ExpressionNode expr : node.groupBy().fields()) {
                extractExpressionNodeDependencies(expr, dependencies);
            }
        }
        
        // Extract from ORDER BY clause
        if (node.orderBy() != null) {
            for (OrderFieldNode field : node.orderBy().fields()) {
                extractExpressionNodeDependencies(field.field(), dependencies);
            }
        }
    }
    
    /**
     * Extract dependencies from an expression node
     */
    private void extractExpressionNodeDependencies(ExpressionNode expr, Set<String> dependencies) {
        if (expr instanceof IdentifierNode id) {
            // Check if this is a variable or view reference
            if (context.getVariable(id.name()) != null) {
                dependencies.add("variable:" + id.name());
            }
            if (context.getView(id.name()) != null) {
                dependencies.add("view:" + id.name());
            }
        } else if (expr instanceof FunctionCallNode func) {
            // Extract dependencies from function arguments
            for (ExpressionNode arg : func.arguments()) {
                extractExpressionNodeDependencies(arg, dependencies);
            }
        } else if (expr instanceof BinaryExpressionNode binary) {
            extractExpressionNodeDependencies(binary.left(), dependencies);
            extractExpressionNodeDependencies(binary.right(), dependencies);
        }
    }
}