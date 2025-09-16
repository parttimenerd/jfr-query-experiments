package me.bechberger.jfr.extended.plan.plans;

import me.bechberger.jfr.extended.ast.ASTNodes.*;
import me.bechberger.jfr.extended.ast.ExpressionKey;
import me.bechberger.jfr.extended.plan.QueryResult;
import me.bechberger.jfr.extended.plan.PlanExecutionException;
import me.bechberger.jfr.extended.plan.QueryExecutionContext;
import me.bechberger.jfr.extended.plan.evaluator.PlanExpressionEvaluator;
import me.bechberger.jfr.extended.plan.evaluator.ExpressionHandler;
import me.bechberger.jfr.extended.plan.evaluator.ExpressionEvaluationException;
import me.bechberger.jfr.extended.plan.exception.AggregationRuntimeException;
import me.bechberger.jfr.extended.engine.QueryEvaluatorUtils;
import me.bechberger.jfr.extended.table.JfrTable;
import me.bechberger.jfr.extended.table.StandardJfrTable;
import me.bechberger.jfr.extended.table.CellValue;
import me.bechberger.jfr.extended.table.CellType;
import me.bechberger.jfr.extended.evaluator.FunctionRegistry;
import me.bechberger.jfr.extended.evaluator.AggregateFunctions;

import java.util.stream.Collectors;
import me.bechberger.jfr.extended.evaluator.FunctionUtils;

import java.util.*;

/**
 * Streaming plan for aggregation operations (COUNT, SUM, AVG, etc.).
 * 
 * This plan handles queries with aggregate functions in the SELECT clause,
 * either with or without GROUP BY. It processes all rows from the source
 * and computes aggregate values.
 * 
 * Examples:
 * - SELECT COUNT(*) FROM table
 * - SELECT AVG(duration), MAX(duration) FROM events
 * - SELECT type, COUNT(*) FROM events GROUP BY type
 * 
 * @author JFR Query Plan Architecture
 * @since 2.0
 */
public class AggregationPlan extends AbstractStreamingPlan {
    
    private final StreamingQueryPlan sourcePlan;
    private final List<SelectItemNode> selectItems;
    private final GroupByNode groupByNode;
    private final List<ExpressionNode> additionalAggregates; // Aggregates from ORDER BY and HAVING
    private final FunctionRegistry functionRegistry;
    
    public AggregationPlan(SelectNode selectNode, StreamingQueryPlan sourcePlan, List<SelectItemNode> selectItems, 
                          GroupByNode groupByNode, List<ExpressionNode> additionalAggregates) {
        super(selectNode); // Pass the select node as the source AST node
        this.sourcePlan = sourcePlan;
        this.selectItems = selectItems;
        this.groupByNode = groupByNode;
        this.additionalAggregates = additionalAggregates != null ? additionalAggregates : new ArrayList<>();
        this.functionRegistry = FunctionRegistry.getInstance();
    }
    
    @Override
    public QueryResult execute(QueryExecutionContext context) throws PlanExecutionException {
        context.recordPlanStart("AggregationPlan", explain());
        try {
            // Get all rows from the source
            QueryResult sourceResult = sourcePlan.execute(context);
            if (!sourceResult.isSuccess()) {
                context.recordPlanFailure("AggregationPlan", explain(), "Source plan failed: " + sourceResult.getError());
                return sourceResult;
            }

            JfrTable sourceTable = sourceResult.getTable();
            
            // Handle GROUP BY aggregation
            if (groupByNode != null) {
                QueryResult result = executeGroupByAggregation(sourceTable, context);
                if (result.isSuccess()) {
                    context.recordPlanSuccess("AggregationPlan", explain(), result.getTable().getRowCount());
                } else {
                    context.recordPlanFailure("AggregationPlan", explain(), "GROUP BY aggregation failed");
                }
                return result;
            } else {
                // Handle simple aggregation without GROUP BY
                QueryResult result = executeSimpleAggregation(sourceTable, context);
                if (result.isSuccess()) {
                    context.recordPlanSuccess("AggregationPlan", explain(), result.getTable().getRowCount());
                } else {
                    context.recordPlanFailure("AggregationPlan", explain(), "Simple aggregation failed");
                }
                return result;
            }
            
        } catch (Exception e) {
            context.recordPlanFailure("AggregationPlan", explain(), e.getMessage());
            return QueryResult.failure(new PlanExecutionException(
                "Aggregation execution failed: " + e.getMessage(),
                null, e
            ));
        }
    }
    
    /**
     * Execute simple aggregation without GROUP BY (e.g., SELECT COUNT(*) FROM table)
     */
    private QueryResult executeSimpleAggregation(JfrTable sourceTable, QueryExecutionContext context) 
            throws PlanExecutionException {
        
        // Build result table structure
        List<JfrTable.Column> resultColumns = new ArrayList<>();
        List<CellValue> resultRow = new ArrayList<>();
        
        // Process each SELECT item
        for (int i = 0; i < selectItems.size(); i++) {
            SelectItemNode selectItem = selectItems.get(i);
            ExpressionNode expr = selectItem.expression();
            String columnName = selectItem.alias() != null ? selectItem.alias() : 
                               extractColumnName(expr, i);
            
            CellValue aggregateValue = computeAggregateValue(expr, sourceTable, context);
            
            // Determine column type from the result value
            CellType columnType = aggregateValue.getType();
            resultColumns.add(new JfrTable.Column(columnName, columnType));
            resultRow.add(aggregateValue);
        }
        
        // Compute additional aggregates from ORDER BY and HAVING clauses
        // For now, compute all of them - optimization can be added later
        // Create the result table first so we can reference SELECT aliases
        StandardJfrTable resultTable = new StandardJfrTable(resultColumns);
        resultTable.addRow(resultRow.toArray(new CellValue[0]));
        
        for (ExpressionNode additionalAggregate : additionalAggregates) {
            CellValue aggregateValue = computeAdditionalAggregateValue(additionalAggregate, sourceTable, resultTable, context);
            context.setComputedAggregate(additionalAggregate, aggregateValue);
        }
        
        return QueryResult.success(resultTable);
    }
    
    /**
     * Execute GROUP BY aggregation
     */
    private QueryResult executeGroupByAggregation(JfrTable sourceTable, QueryExecutionContext context) 
            throws PlanExecutionException {
        
        // Build result columns based on SELECT items
        List<JfrTable.Column> resultColumns = new ArrayList<>();
        
        for (int i = 0; i < selectItems.size(); i++) {
            SelectItemNode selectItem = selectItems.get(i);
            ExpressionNode expr = selectItem.expression();
            String columnName = selectItem.alias() != null ? selectItem.alias() : 
                               extractColumnName(expr, resultColumns.size());
            
            CellType columnType = inferColumnType(expr, sourceTable, context);
            resultColumns.add(new JfrTable.Column(columnName, columnType));
        }
        
        // Note: Additional aggregates are stored in computedAggregates map only, 
        // not as visible columns in the result table
        
        // Group rows by GROUP BY expressions using streaming iteration
        Map<List<CellValue>, List<JfrTable.Row>> groups = new LinkedHashMap<>();
        PlanExpressionEvaluator evaluator = new PlanExpressionEvaluator(context);
        
        for (JfrTable.Row row : sourceTable.getRows()) {
            List<CellValue> groupKey = new ArrayList<>();
            
            for (ExpressionNode groupExpr : groupByNode.fields()) {
                CellValue groupValue = evaluator.evaluateExpressionWithRowContext(groupExpr, row, sourceTable.getColumns());
                groupKey.add(groupValue);
            }
            
            groups.computeIfAbsent(groupKey, k -> new ArrayList<>()).add(row);
        }
        
        // Create result table
        StandardJfrTable resultTable = new StandardJfrTable(resultColumns);
        
        // Process each group
        for (Map.Entry<List<CellValue>, List<JfrTable.Row>> entry : groups.entrySet()) {
            List<JfrTable.Row> groupRows = entry.getValue();
            
            List<CellValue> resultRow = new ArrayList<>();
            Map<ExpressionKey, CellValue> computedAggregates = new HashMap<>();
            
            // Add values for each SELECT item in order
            for (int i = 0; i < selectItems.size(); i++) {
                SelectItemNode selectItem = selectItems.get(i);
                ExpressionNode expr = selectItem.expression();
                
                CellValue value = computeGroupAggregateValueFromRows(expr, sourceTable, groupRows, context);
                resultRow.add(value);
                
                // If this is an aggregate expression, also store it in computed aggregates
                if (QueryEvaluatorUtils.containsAggregateFunction(expr)) {
                    ExpressionKey key = new ExpressionKey(expr);
                    computedAggregates.put(key, value);
                }
            }
            
            // Compute additional aggregates from ORDER BY and HAVING clauses
            // Store them ONLY in the row's computed aggregates map, not as visible columns
            for (ExpressionNode additionalAggregate : additionalAggregates) {
                // Convert group rows to indices for compatibility with existing methods
                List<Integer> rowIndices = new ArrayList<>();
                for (int i = 0; i < sourceTable.getRowCount(); i++) {
                    if (!groupRows.isEmpty()) {
                        // This is a simplified approach - we'll create dummy indices
                        // for now to maintain compatibility
                        rowIndices.add(i);
                        break; // Only need one index per group for most aggregates
                    }
                }
                
                CellValue aggregateValue = computeGroupAdditionalAggregateValue(additionalAggregate, sourceTable, resultColumns, resultRow, rowIndices, context);
                
                // Store in row's computed aggregates (but don't add to resultRow)
                ExpressionKey key = new ExpressionKey(additionalAggregate);
                computedAggregates.put(key, aggregateValue);
                
                // Also store in context for backward compatibility
                context.setComputedAggregate(additionalAggregate, aggregateValue);
            }

            // Create Row with computed aggregates and add to table
            JfrTable.Row row = new JfrTable.Row(resultRow, null, computedAggregates);
            resultTable.addRow(row);
        }

        return QueryResult.success(resultTable);
    }
    
    /**
     * Evaluate expressions containing aggregates by recursively handling aggregate parts.
     * Uses exhaustive pattern matching to ensure all expression types are explicitly handled.
     */
    private CellValue evaluateAggregateExpression(ExpressionNode expr, JfrTable sourceTable, 
                                                 List<Integer> rowIndices, QueryExecutionContext context,
                                                 PlanExpressionEvaluator evaluator) throws PlanExecutionException {
        
        return switch (expr) {
            case FunctionCallNode functionCall when QueryEvaluatorUtils.isAggregateFunction(functionCall.functionName()) -> {
                // Direct aggregate function - evaluate in group context
                String functionName = functionCall.functionName().toUpperCase();
                
                // Handle COUNT(*) specially
                if ("COUNT".equals(functionName) && functionCall.arguments().size() == 1 && 
                    functionCall.arguments().get(0) instanceof StarNode) {
                    yield new CellValue.NumberValue((long) rowIndices.size());
                }
                
                // For other aggregate functions, collect values from the specified rows
                if (functionCall.arguments().size() == 1) {
                    ExpressionNode argExpr = functionCall.arguments().get(0);
                    List<CellValue> columnValues = new ArrayList<>();
                    
                    PlanExpressionEvaluator simpleEvaluator = new PlanExpressionEvaluator(context);
                    
                    for (int rowIndex : rowIndices) {
                        CellValue value = simpleEvaluator.evaluateExpression(argExpr, sourceTable, rowIndex);
                        columnValues.add(value);
                    }
                    
                    // Handle DISTINCT modifier - remove duplicates if distinct is true
                    if (functionCall.distinct()) {
                        Set<CellValue> distinctValues = new LinkedHashSet<>(columnValues);
                        columnValues = new ArrayList<>(distinctValues);
                    }
                    
                    // Call the aggregate function
                    List<CellValue> arguments = List.of(new CellValue.ArrayValue(columnValues));
                    AggregateFunctions.EvaluationContext evalContext = context.getEvaluationContext();
                    
                    try {
                        yield functionRegistry.evaluateFunction(functionName, arguments, evalContext);
                    } catch (Exception e) {
                        throw new PlanExecutionException(
                            "Failed to evaluate aggregate function '" + functionName + "': " + e.getMessage() +
                            ". Function may not be supported or arguments may be invalid.", 
                            null, e
                        );
                    }
                }
                
                // Handle functions with different argument counts
                throw new PlanExecutionException(
                    "Unsupported aggregate function signature: " + functionName + 
                    " with " + functionCall.arguments().size() + " arguments. " +
                    "Currently only single-argument aggregate functions are supported."
                );
            }
            
            case FunctionCallNode functionCall when QueryEvaluatorUtils.containsAggregateFunction(expr) -> {
                // Non-aggregate function that contains aggregates (like ABS(COUNT(*) - 3))
                // Evaluate arguments recursively first, then apply the function
                List<CellValue> evaluatedArgs = new ArrayList<>();
                for (ExpressionNode arg : functionCall.arguments()) {
                    CellValue argValue = evaluateAggregateExpression(arg, sourceTable, rowIndices, context, evaluator);
                    evaluatedArgs.add(argValue);
                }
                
                // Create a new function call with literal arguments
                List<ExpressionNode> literalArgs = evaluatedArgs.stream()
                    .map(value -> new LiteralNode(value, null))
                    .collect(Collectors.toList());
                FunctionCallNode tempFunctionCall = new FunctionCallNode(functionCall.functionName(), literalArgs, functionCall.distinct(), null);
                
                // Use a simple evaluator without hooks to avoid recursion
                PlanExpressionEvaluator simpleEvaluator = new PlanExpressionEvaluator(context);
                yield simpleEvaluator.evaluateExpression(tempFunctionCall, sourceTable, 0);
            }
            
            case FunctionCallNode functionCall -> {
                // Non-aggregate function without aggregates - evaluate directly
                PlanExpressionEvaluator simpleEvaluator = new PlanExpressionEvaluator(context);
                yield simpleEvaluator.evaluateExpression(functionCall, sourceTable, 0);
            }
            
            case BinaryExpressionNode binaryExpr -> {
                // For binary expressions like "SUM(a) - SUM(b)", evaluate both sides recursively
                CellValue left = evaluateAggregateExpression(binaryExpr.left(), sourceTable, rowIndices, context, evaluator);
                CellValue right = evaluateAggregateExpression(binaryExpr.right(), sourceTable, rowIndices, context, evaluator);
                
                // Create temporary literals and evaluate the binary operation
                LiteralNode leftLiteral = new LiteralNode(left, null);
                LiteralNode rightLiteral = new LiteralNode(right, null);
                BinaryExpressionNode tempExpr = new BinaryExpressionNode(leftLiteral, binaryExpr.operator(), rightLiteral, null);
                
                // Use a simple evaluator without hooks to avoid recursion
                PlanExpressionEvaluator simpleEvaluator = new PlanExpressionEvaluator(context);
                yield simpleEvaluator.evaluateExpression(tempExpr, sourceTable, 0);
            }
            
            case UnaryExpressionNode unaryExpr -> {
                // For unary expressions like "NOT SUM(a) > 0"
                CellValue operand = evaluateAggregateExpression(unaryExpr.operand(), sourceTable, rowIndices, context, evaluator);
                
                LiteralNode operandLiteral = new LiteralNode(operand, null);
                UnaryExpressionNode tempExpr = new UnaryExpressionNode(unaryExpr.operator(), operandLiteral, null);
                
                PlanExpressionEvaluator simpleEvaluator = new PlanExpressionEvaluator(context);
                yield simpleEvaluator.evaluateExpression(tempExpr, sourceTable, 0);
            }
            
            case CaseExpressionNode caseExpr -> {
                // Handle CASE expressions that may contain aggregates
                // This requires special handling of the WHEN clauses and ELSE expression
                ExpressionNode testExpression = caseExpr.expression();
                if (testExpression != null && QueryEvaluatorUtils.containsAggregateFunction(testExpression)) {
                    testExpression = new LiteralNode(evaluateAggregateExpression(testExpression, sourceTable, rowIndices, context, evaluator), null);
                }
                
                List<WhenClauseNode> evaluatedWhenClauses = new ArrayList<>();
                for (WhenClauseNode whenClause : caseExpr.whenClauses()) {
                    ExpressionNode condition = whenClause.condition();
                    ExpressionNode result = whenClause.result();
                    
                    if (QueryEvaluatorUtils.containsAggregateFunction(condition)) {
                        condition = new LiteralNode(evaluateAggregateExpression(condition, sourceTable, rowIndices, context, evaluator), null);
                    }
                    if (QueryEvaluatorUtils.containsAggregateFunction(result)) {
                        result = new LiteralNode(evaluateAggregateExpression(result, sourceTable, rowIndices, context, evaluator), null);
                    }
                    
                    evaluatedWhenClauses.add(new WhenClauseNode(condition, result, whenClause.location()));
                }
                
                ExpressionNode elseExpression = caseExpr.elseExpression();
                if (elseExpression != null && QueryEvaluatorUtils.containsAggregateFunction(elseExpression)) {
                    elseExpression = new LiteralNode(evaluateAggregateExpression(elseExpression, sourceTable, rowIndices, context, evaluator), null);
                }
                
                CaseExpressionNode tempCaseExpr = new CaseExpressionNode(testExpression, evaluatedWhenClauses, elseExpression, caseExpr.location());
                
                // Create a dummy table for evaluation since all aggregates are now literals
                List<JfrTable.Column> dummyColumns = List.of(new JfrTable.Column("dummy", CellType.STRING));
                StandardJfrTable dummyTable = new StandardJfrTable(dummyColumns);
                dummyTable.addRow(new CellValue.StringValue("dummy"));
                
                PlanExpressionEvaluator simpleEvaluator = new PlanExpressionEvaluator(context);
                yield simpleEvaluator.evaluateExpression(tempCaseExpr, dummyTable, 0);
            }
            
            // PercentileFunctionNode and PercentileSelectionNode removed as they are not properly supported
            
            case ArrayLiteralNode arrayLiteral -> {
                // Handle array literals that may contain aggregates
                List<ExpressionNode> evaluatedElements = new ArrayList<>();
                for (ExpressionNode element : arrayLiteral.elements()) {
                    if (QueryEvaluatorUtils.containsAggregateFunction(element)) {
                        evaluatedElements.add(new LiteralNode(evaluateAggregateExpression(element, sourceTable, rowIndices, context, evaluator), null));
                    } else {
                        evaluatedElements.add(element);
                    }
                }
                
                ArrayLiteralNode tempArrayLiteral = new ArrayLiteralNode(evaluatedElements, arrayLiteral.location());
                PlanExpressionEvaluator simpleEvaluator = new PlanExpressionEvaluator(context);
                yield simpleEvaluator.evaluateExpression(tempArrayLiteral, sourceTable, 0);
            }
            
            case VariableAssignmentExpressionNode varAssignment -> {
                // Handle variable assignment expressions that may contain aggregates
                ExpressionNode value = varAssignment.value();
                if (QueryEvaluatorUtils.containsAggregateFunction(value)) {
                    value = new LiteralNode(evaluateAggregateExpression(value, sourceTable, rowIndices, context, evaluator), null);
                }
                
                VariableAssignmentExpressionNode tempVarAssignment = new VariableAssignmentExpressionNode(
                    varAssignment.variable(), value, varAssignment.location());
                PlanExpressionEvaluator simpleEvaluator = new PlanExpressionEvaluator(context);
                yield simpleEvaluator.evaluateExpression(tempVarAssignment, sourceTable, 0);
            }
            
            // Expression types that don't contain sub-expressions with aggregates
            case LiteralNode literal -> {
                // For literal values, return the value directly if possible to avoid unnecessary evaluation
                if (literal.value() != null) {
                    yield literal.value();
                } else {
                    // If no direct value, evaluate through the expression evaluator
                    PlanExpressionEvaluator simpleEvaluator = new PlanExpressionEvaluator(context);
                    yield simpleEvaluator.evaluateExpression(literal, sourceTable, 0);
                }
            }
            
            case IdentifierNode identifier -> {
                // Column references - evaluate directly
                PlanExpressionEvaluator simpleEvaluator = new PlanExpressionEvaluator(context);
                yield simpleEvaluator.evaluateExpression(identifier, sourceTable, 0);
            }
            
            case FieldAccessNode fieldAccess -> {
                // Field access - evaluate directly
                PlanExpressionEvaluator simpleEvaluator = new PlanExpressionEvaluator(context);
                yield simpleEvaluator.evaluateExpression(fieldAccess, sourceTable, 0);
            }
            
            case StarNode star -> {
                // Star expressions - evaluate directly
                PlanExpressionEvaluator simpleEvaluator = new PlanExpressionEvaluator(context);
                yield simpleEvaluator.evaluateExpression(star, sourceTable, 0);
            }
            
            case NestedQueryNode nestedQuery -> {
                // Nested queries - evaluate directly (they don't contain aggregates from current context)
                PlanExpressionEvaluator simpleEvaluator = new PlanExpressionEvaluator(context);
                yield simpleEvaluator.evaluateExpression(nestedQuery, sourceTable, 0);
            }
            
            case WhenClauseNode whenClause -> {
                // WhenClauseNode should not be encountered directly in normal expression evaluation
                // It should only be processed as part of CaseExpressionNode
                // This could indicate a malformed AST or improper CASE expression construction
                throw new PlanExecutionException(
                    "WhenClauseNode encountered directly in expression evaluation. " +
                    "WhenClauseNode should only appear as part of a CaseExpressionNode. " +
                    "This may indicate a malformed AST or improper CASE expression construction. " +
                    "Location: " + (whenClause.location() != null ? whenClause.location() : "unknown")
                );
            }
            
            // The default case should never be reached due to exhaustive pattern matching
            // But we include it for safety and to make the intent clear
            default -> {
                String expressionType = expr.getClass().getSimpleName();
                String expressionInfo = expr.toString();
                
                // Log the unsupported expression for debugging
                System.err.println("WARNING: Unsupported expression type in aggregate context: " + expressionType);
                System.err.println("Expression details: " + expressionInfo);
                
                throw new PlanExecutionException(
                    "Unsupported expression type in aggregate context: " + expressionType + 
                    ". Expression: " + expressionInfo +
                    ". This indicates either a missing case in the exhaustive pattern matching " +
                    "or a new AST node type that needs to be added to the switch statement."
                );
            }
        };
    }
    
    /**
     * Infer column type from expression by computing a sample value
     */
    private CellType inferColumnType(ExpressionNode expr, JfrTable sourceTable, QueryExecutionContext context) 
            throws PlanExecutionException {
        
        if (sourceTable.getRowCount() == 0) {
            // If no data, fall back to function registry defaults
            if (expr instanceof FunctionCallNode functionCall) {
                CellType defaultType = functionRegistry.getDefaultReturnType(functionCall.functionName());
                return defaultType != null ? defaultType : CellType.STRING;
            }
            return CellType.STRING;
        }
        
        // Compute a sample value using the first few rows to determine the actual type
        List<Integer> sampleRows = new ArrayList<>();
        int sampleSize = Math.min(10, sourceTable.getRowCount());
        for (int i = 0; i < sampleSize; i++) {
            sampleRows.add(i);
        }
        
        try {
            CellValue sampleValue = computeGroupAggregateValue(expr, sourceTable, sampleRows, context);
            return sampleValue.getType();
        } catch (Exception e) {
            // If computation fails, fall back to function registry defaults
            if (expr instanceof FunctionCallNode functionCall) {
                CellType defaultType = functionRegistry.getDefaultReturnType(functionCall.functionName());
                return defaultType != null ? defaultType : CellType.STRING;
            }
            return CellType.STRING;
        }
    }
    
    /**
     * Evaluate an aggregate expression using direct row access instead of row indices.
     */
    private CellValue evaluateAggregateExpressionFromRows(ExpressionNode expr, JfrTable sourceTable, 
                                                         List<JfrTable.Row> groupRows, QueryExecutionContext context,
                                                         PlanExpressionEvaluator evaluator) throws PlanExecutionException {
        
        return switch (expr) {
            case FunctionCallNode functionCall when QueryEvaluatorUtils.isAggregateFunction(functionCall.functionName()) -> {
                // Direct aggregate function - evaluate in group context
                String functionName = functionCall.functionName().toUpperCase();
                
                // Handle COUNT(*) specially
                if ("COUNT".equals(functionName) && functionCall.arguments().size() == 1 && 
                    functionCall.arguments().get(0) instanceof StarNode) {
                    yield new CellValue.NumberValue((long) groupRows.size());
                }
                
                // For other aggregate functions, collect values from the specified rows
                if (functionCall.arguments().size() == 1) {
                    ExpressionNode argExpr = functionCall.arguments().get(0);
                    List<CellValue> columnValues = new ArrayList<>();
                    
                    PlanExpressionEvaluator simpleEvaluator = new PlanExpressionEvaluator(context);
                    
                    for (JfrTable.Row row : groupRows) {
                        CellValue value = simpleEvaluator.evaluateExpressionWithRowContext(argExpr, row, sourceTable.getColumns());
                        columnValues.add(value);
                    }
                    
                    // Handle DISTINCT modifier - remove duplicates if distinct is true
                    if (functionCall.distinct()) {
                        Set<CellValue> distinctValues = new LinkedHashSet<>(columnValues);
                        columnValues = new ArrayList<>(distinctValues);
                    }
                    
                    // Call the aggregate function
                    List<CellValue> arguments = List.of(new CellValue.ArrayValue(columnValues));
                    AggregateFunctions.EvaluationContext evalContext = context.getEvaluationContext();
                    
                    try {
                        yield functionRegistry.evaluateFunction(functionName, arguments, evalContext);
                    } catch (Exception e) {
                        throw new PlanExecutionException(
                            "Error evaluating aggregate function " + functionName + ": " + e.getMessage(), e);
                    }
                }
                
                throw new PlanExecutionException("Unsupported aggregate function: " + functionCall);
            }
            
            // For non-aggregate expressions, just evaluate with the first row (like GROUP BY behavior)
            default -> {
                if (!groupRows.isEmpty()) {
                    yield evaluator.evaluateExpressionWithRowContext(expr, groupRows.get(0), sourceTable.getColumns());
                }
                yield new CellValue.StringValue("");
            }
        };
    }

    /**
     * Compute aggregate value for an expression over a group of rows using direct row access.
     */
    private CellValue computeGroupAggregateValueFromRows(ExpressionNode expr, JfrTable sourceTable, 
                                                        List<JfrTable.Row> groupRows, QueryExecutionContext context) 
            throws PlanExecutionException {
        
        // Check if this expression contains aggregates
        if (QueryEvaluatorUtils.containsAggregateFunction(expr)) {
            // Use the exhaustive aggregate expression evaluator with rows
            PlanExpressionEvaluator dummyEvaluator = new PlanExpressionEvaluator(context);
            return evaluateAggregateExpressionFromRows(expr, sourceTable, groupRows, context, dummyEvaluator);
        }
        
        // For non-aggregate expressions in GROUP BY context, return the first value
        if (!groupRows.isEmpty()) {
            PlanExpressionEvaluator evaluator = new PlanExpressionEvaluator(context);
            return evaluator.evaluateExpressionWithRowContext(expr, groupRows.get(0), sourceTable.getColumns());
        }
        
        return new CellValue.StringValue("");
    }

    private CellValue computeGroupAggregateValue(ExpressionNode expr, JfrTable sourceTable, 
                                               List<Integer> rowIndices, QueryExecutionContext context) 
            throws PlanExecutionException {
        
        // Check if this expression contains aggregates
        if (QueryEvaluatorUtils.containsAggregateFunction(expr)) {
            // Use the exhaustive aggregate expression evaluator
            PlanExpressionEvaluator dummyEvaluator = new PlanExpressionEvaluator(context);
            return evaluateAggregateExpression(expr, sourceTable, rowIndices, context, dummyEvaluator);
        }
        
        // For non-aggregate expressions in GROUP BY context, return the first value
        if (!rowIndices.isEmpty()) {
            PlanExpressionEvaluator evaluator = new PlanExpressionEvaluator(context);
            return evaluator.evaluateExpression(expr, sourceTable, rowIndices.get(0));
        }
        
        return new CellValue.StringValue("");
    }
    
    /**
     * Compute aggregate value for an expression over all rows in the source table.
     */
    private CellValue computeAggregateValue(ExpressionNode expr, JfrTable sourceTable, QueryExecutionContext context) 
            throws PlanExecutionException {
        
        if (expr instanceof FunctionCallNode functionCall && 
            QueryEvaluatorUtils.isAggregateFunction(functionCall.functionName())) {
            
            String functionName = functionCall.functionName().toUpperCase();
            List<CellValue> arguments = new ArrayList<>();
            
            // Handle COUNT(*) specially
            if ("COUNT".equals(functionName) && functionCall.arguments().size() == 1 && 
                functionCall.arguments().get(0) instanceof StarNode) {
                // COUNT(*) - return the row count
                return new CellValue.NumberValue((long) sourceTable.getRowCount());
            }
            
            // For other aggregate functions, collect all values from the specified column(s)
            if (functionCall.arguments().size() == 1) {
                ExpressionNode argExpr = functionCall.arguments().get(0);
                List<CellValue> columnValues = new ArrayList<>();
                
                // Collect all values for this expression across all rows
                for (int rowIndex = 0; rowIndex < sourceTable.getRowCount(); rowIndex++) {
                    PlanExpressionEvaluator evaluator = new PlanExpressionEvaluator(context);
                    CellValue value = evaluator.evaluateExpression(argExpr, sourceTable, rowIndex);
                    columnValues.add(value);
                }
                
                // Create array argument for the aggregate function
                arguments.add(new CellValue.ArrayValue(columnValues));
            }
            
            // Call the aggregate function
            AggregateFunctions.EvaluationContext evalContext = context.getEvaluationContext();
            return functionRegistry.evaluateFunction(functionName, arguments, evalContext);
        }
        
        // For non-aggregate expressions, this is only allowed in GROUP BY context
        // In simple aggregation context, this is an error
        if (groupByNode == null) {
            // Provide more specific error messages based on expression type
            String errorMessage = "Non-aggregate expression in aggregation without GROUP BY: ";
            if (expr instanceof IdentifierNode identifier) {
                errorMessage += "column '" + identifier.name() + "'. ";
                errorMessage += "When using aggregate functions like COUNT, SUM, etc., all non-aggregate columns must be included in a GROUP BY clause.";
            } else if (expr instanceof FieldAccessNode fieldAccess) {
                errorMessage += "field '" + fieldAccess.field() + "'. ";
                errorMessage += "When using aggregate functions, all non-aggregate fields must be included in a GROUP BY clause.";
            } else {
                errorMessage += expr.getClass().getSimpleName() + ". ";
                errorMessage += "All expressions in SELECT must be aggregate functions when no GROUP BY is specified.";
            }
            
            throw new PlanExecutionException(errorMessage);
        }
        
        // In GROUP BY context, non-aggregate expressions should be handled differently
        // For now, return the first value (this will be refined in executeGroupByAggregation)
        if (sourceTable.getRowCount() > 0) {
            PlanExpressionEvaluator evaluator = new PlanExpressionEvaluator(context);
            return evaluator.evaluateExpression(expr, sourceTable, 0);
        }
        
        return new CellValue.StringValue("");
    }
    
    /**
     * Compute additional aggregate value for ORDER BY and HAVING expressions.
     * These can reference either SELECT aliases (from resultTable) or aggregate functions (computed from sourceTable).
     */
    private CellValue computeAdditionalAggregateValue(ExpressionNode expr, JfrTable sourceTable, 
                                                    JfrTable resultTable, QueryExecutionContext context) 
            throws PlanExecutionException {
        
        // First, check if this expression contains aggregate functions
        if (QueryEvaluatorUtils.containsAggregateFunction(expr)) {
            // If it contains aggregates, compute it against the source table
            return computeAggregateValue(expr, sourceTable, context);
        }
        
        // If it doesn't contain aggregates, try to evaluate it against the result table first
        // This allows ORDER BY to reference SELECT aliases
        try {
            PlanExpressionEvaluator evaluator = new PlanExpressionEvaluator(context);
            return evaluator.evaluateExpression(expr, resultTable, 0);
        } catch (Exception e) {
            // If that fails, try against the source table
            try {
                PlanExpressionEvaluator evaluator = new PlanExpressionEvaluator(context);
                return evaluator.evaluateExpression(expr, sourceTable, 0);
            } catch (Exception e2) {
                throw new PlanExecutionException(
                    "Failed to evaluate ORDER BY/HAVING expression: " + expr + 
                    ". Could not find column in result table (" + e.getMessage() + 
                    ") or source table (" + e2.getMessage() + ")", null, e2);
            }
        }
    }
    
    /**
     * Compute additional aggregate value for ORDER BY and HAVING expressions in GROUP BY context.
     * These can reference either SELECT aliases or aggregate functions computed for the current group.
     */
    private CellValue computeGroupAdditionalAggregateValue(ExpressionNode expr, JfrTable sourceTable, 
                                                         List<JfrTable.Column> resultColumns, List<CellValue> resultRow,
                                                         List<Integer> rowIndices, QueryExecutionContext context) 
            throws PlanExecutionException {
        
        // First, check if this expression contains aggregate functions
        if (QueryEvaluatorUtils.containsAggregateFunction(expr)) {
            // If it contains aggregates, compute it for this group
            return computeGroupAggregateValue(expr, sourceTable, rowIndices, context);
        }
        
        // If it doesn't contain aggregates, try to evaluate it against a temporary result table first
        // This allows ORDER BY to reference SELECT aliases
        try {
            StandardJfrTable tempResultTable = new StandardJfrTable(resultColumns);
            tempResultTable.addRow(resultRow.toArray(new CellValue[0]));
            
            PlanExpressionEvaluator evaluator = new PlanExpressionEvaluator(context);
            return evaluator.evaluateExpression(expr, tempResultTable, 0);
        } catch (Exception e) {
            // If that fails, try against the source table using the first row of the group
            try {
                if (!rowIndices.isEmpty()) {
                    PlanExpressionEvaluator evaluator = new PlanExpressionEvaluator(context);
                    return evaluator.evaluateExpression(expr, sourceTable, rowIndices.get(0));
                }
                return new CellValue.StringValue("");
            } catch (Exception e2) {
                throw new PlanExecutionException(
                    "Failed to evaluate GROUP BY ORDER BY/HAVING expression: " + expr + 
                    ". Could not find column in result (" + e.getMessage() + 
                    ") or source table (" + e2.getMessage() + ")", null, e2);
            }
        }
    }
    
    /**
     * Extract column name from expression.
     * For anonymous columns, use $0, $1, $2, etc.
     */
    private String extractColumnName(ExpressionNode expr, int columnIndex) {
        if (expr instanceof FunctionCallNode functionCall) {
            // Check if this is an aggregate function
            if (QueryEvaluatorUtils.isAggregateFunction(functionCall.functionName())) {
                if (functionCall.arguments().size() == 1 && functionCall.arguments().get(0) instanceof StarNode) {
                    return functionCall.functionName().toUpperCase() + "(*)";
                } else {
                    return functionCall.functionName().toUpperCase() + "(...)";
                }
            } else {
                // Non-aggregate functions are treated as anonymous expressions
                return "$" + columnIndex;
            }
        } else if (expr instanceof FieldAccessNode fieldAccess) {
            return fieldAccess.field();
        } else if (expr instanceof IdentifierNode identifier) {
            return identifier.name();
        } else {
            // For anonymous/complex expressions, use $0, $1, $2, etc.
            return "$" + columnIndex;
        }
    }
    
    @Override
    public String toString() {
        return "AggregationPlan{selectItems=" + selectItems.size() + "}";
    }

    @Override
    public String explain() {
        StringBuilder sb = new StringBuilder();
        sb.append("AggregationPlan:\n");
        sb.append("  Source Plan: ").append(sourcePlan.explain()).append("\n");
        sb.append("  Select Items:\n");
        for (SelectItemNode item : selectItems) {
            sb.append("    - ").append(item.toString()).append("\n");
        }
        return sb.toString();
    }
    
    /**
     * Collect all aggregate expressions from GROUP BY, ORDER BY, and HAVING clauses.
     * These need to be computed during aggregation and stored for later use.
     */
    public static List<ExpressionNode> collectAdditionalAggregates(GroupByNode groupByNode, OrderByNode orderByNode, HavingNode havingNode) {
        List<ExpressionNode> additionalAggregates = new ArrayList<>();
        
        // Collect aggregates from GROUP BY clause
        if (groupByNode != null) {
            for (ExpressionNode groupField : groupByNode.fields()) {
                collectAggregatesFromExpression(groupField, additionalAggregates);
            }
        }
        
        // Collect aggregates from ORDER BY clause
        if (orderByNode != null) {
            for (OrderFieldNode orderField : orderByNode.fields()) {
                ExpressionNode fieldExpr = orderField.field();
                
                // If the field expression contains aggregates, add it as a complex expression
                if (QueryEvaluatorUtils.containsAggregateFunction(fieldExpr)) {
                    additionalAggregates.add(fieldExpr);
                }
                
                // Also collect individual aggregate components
                collectAggregatesFromExpression(fieldExpr, additionalAggregates);
            }
        }
        
        // Collect aggregates from HAVING clause
        if (havingNode != null) {
            ConditionNode condition = havingNode.condition();
            
            // If the condition contains aggregates, add it as a complex expression
            if (condition instanceof ExpressionConditionNode exprCondition &&
                QueryEvaluatorUtils.containsAggregateFunction(exprCondition.expression())) {
                additionalAggregates.add(exprCondition.expression());
            }
            
            // Also collect individual aggregate components
            collectAggregatesFromCondition(condition, additionalAggregates);
        }
        
        return additionalAggregates;
    }
    
    /**
     * Recursively collect aggregate function calls from an expression.
     * Uses exhaustive pattern matching to ensure all expression types are covered at compile time.
     */
    private static void collectAggregatesFromExpression(ExpressionNode expr, List<ExpressionNode> aggregates) {
        if (expr == null) return;
        
        switch (expr) {
            case FunctionCallNode functionCall -> {
                if (QueryEvaluatorUtils.isAggregateFunction(functionCall.functionName())) {
                    aggregates.add(functionCall);
                }
                // Also check function arguments for nested aggregates
                for (ExpressionNode arg : functionCall.arguments()) {
                    collectAggregatesFromExpression(arg, aggregates);
                }
            }
            
            case BinaryExpressionNode binaryExpr -> {
                collectAggregatesFromExpression(binaryExpr.left(), aggregates);
                collectAggregatesFromExpression(binaryExpr.right(), aggregates);
            }
            
            case UnaryExpressionNode unaryExpr -> {
                collectAggregatesFromExpression(unaryExpr.operand(), aggregates);
            }
            
            case CaseExpressionNode caseExpr -> {
                // Check test expression
                if (caseExpr.expression() != null) {
                    collectAggregatesFromExpression(caseExpr.expression(), aggregates);
                }
                
                // Check WHEN clauses
                for (WhenClauseNode whenClause : caseExpr.whenClauses()) {
                    collectAggregatesFromExpression(whenClause.condition(), aggregates);
                    collectAggregatesFromExpression(whenClause.result(), aggregates);
                }
                
                // Check ELSE expression
                if (caseExpr.elseExpression() != null) {
                    collectAggregatesFromExpression(caseExpr.elseExpression(), aggregates);
                }
            }
            
            // PercentileFunctionNode and PercentileSelectionNode removed as they are not properly supported
            
            case ArrayLiteralNode arrayLiteral -> {
                for (ExpressionNode element : arrayLiteral.elements()) {
                    collectAggregatesFromExpression(element, aggregates);
                }
            }
            
            case VariableAssignmentExpressionNode varAssignment -> {
                collectAggregatesFromExpression(varAssignment.value(), aggregates);
            }
            
            // Expression types that don't contain sub-expressions
            case LiteralNode literal -> {
                // Literals don't contain aggregates
            }
            
            case IdentifierNode identifier -> {
                // Column references don't contain aggregates
            }
            
            case FieldAccessNode fieldAccess -> {
                // Field access doesn't contain aggregates
            }
            
            case StarNode star -> {
                // Star expressions don't contain aggregates
            }
            
            case NestedQueryNode nestedQuery -> {
                // Nested queries are handled separately - they have their own scope
                // Don't traverse into them for aggregate collection
            }
            
            case WhenClauseNode whenClause -> {
                // WhenClauseNode should only be processed as part of CaseExpressionNode
                // But handle it gracefully if encountered directly
                collectAggregatesFromExpression(whenClause.condition(), aggregates);
                collectAggregatesFromExpression(whenClause.result(), aggregates);
            }
            
            // The default case should never be reached due to exhaustive pattern matching
            // But we include it for safety and to make the intent clear
            default -> {
                String expressionType = expr.getClass().getSimpleName();
                String expressionInfo = expr.toString();
                
                // Log the unsupported expression for debugging
                System.err.println("WARNING: Unsupported expression type in aggregate collection: " + expressionType);
                System.err.println("Expression details: " + expressionInfo);
                
                throw new ExpressionEvaluationException.UnsupportedExpressionTypeException(
                    expressionType + ". Expression: " + expressionInfo +
                    ". This indicates either a missing case in the exhaustive pattern matching " +
                    "or a new AST node type that needs to be added to the switch statement."
                );
            }
        }
    }
    
    /**
     * Recursively collect aggregate function calls from a condition.
     * Uses exhaustive pattern matching to ensure all condition types are covered at compile time.
     */
    private static void collectAggregatesFromCondition(ConditionNode condition, List<ExpressionNode> aggregates) {
        if (condition == null) return;
        
        switch (condition) {
            case ExpressionConditionNode exprCondition -> {
                collectAggregatesFromExpression(exprCondition.expression(), aggregates);
            }
            
            case VariableDeclarationNode varDeclaration -> {
                collectAggregatesFromExpression(varDeclaration.value(), aggregates);
            }
            
            case WithinConditionNode withinCondition -> {
                collectAggregatesFromExpression(withinCondition.value(), aggregates);
                collectAggregatesFromExpression(withinCondition.timeWindow(), aggregates);
                collectAggregatesFromExpression(withinCondition.referenceTime(), aggregates);
            }
            
            // The default case should never be reached due to exhaustive pattern matching
            // But we include it for safety and to make the intent clear
            default -> {
                throw new ExpressionEvaluationException.UnsupportedExpressionTypeException(
                    condition.getClass().getSimpleName() + 
                    ". This indicates a missing case in the exhaustive pattern matching.");
            }
        }
    }
    
    /**
     * Apply precision rounding to numeric results to handle floating-point precision issues.
     * This is especially important for aggregate calculations like SUM, AVG that may accumulate
     * small floating-point errors.
     */
    @SuppressWarnings("unused")
    private static CellValue applyPrecisionRounding(CellValue value) {
        if (value instanceof CellValue.NumberValue numberValue) {
            Number numVal = numberValue.value();
            if (numVal instanceof Double doubleVal) {
                // Round to 10 decimal places to avoid precision issues
                double rounded = Math.round(doubleVal * 1e10) / 1e10;
                return new CellValue.NumberValue(rounded);
            } else if (numVal instanceof Float floatVal) {
                // Round to 6 decimal places for float
                float rounded = Math.round(floatVal * 1e6f) / 1e6f;
                return new CellValue.NumberValue(rounded);
            }
        }
        return value;
    }
}
