package me.bechberger.jfr.extended.plan;

import me.bechberger.jfr.extended.ast.ASTNodes.*;
import me.bechberger.jfr.extended.engine.RawJfrQueryExecutor;
import me.bechberger.jfr.extended.engine.QueryEvaluatorUtils;
import me.bechberger.jfr.extended.plan.plans.StreamingQueryPlan;
import me.bechberger.jfr.extended.plan.plans.*;
import me.bechberger.jfr.extended.table.CellValue;

import java.util.List;

/**
 * Converts AST nodes to streaming query plans.
 * 
 * This converter bridges the gap between the AST representation and the
 * streaming execution engine. It analyzes AST nodes and produces optimized
 * streaming query plans that can be executed efficiently.
 * 
 * @author JFR Query Engine Team
 * @since 2.0 (Streaming Architecture)
 */
public class AstToPlanConverter {
    
    private final RawJfrQueryExecutor rawJfrExecutor;
    
    /**
     * Creates a new AST to plan converter.
     * 
     * @param rawJfrExecutor the raw JFR executor for fallback operations
     */
    public AstToPlanConverter(RawJfrQueryExecutor rawJfrExecutor) {
        this.rawJfrExecutor = rawJfrExecutor;
    }
    
    /**
     * Get the raw JFR executor used by this converter.
     */
    public RawJfrQueryExecutor getRawExecutor() {
        return rawJfrExecutor;
    }
    
    /**
     * Converts a query node to a streaming query plan.
     * 
     * @param queryNode the query AST node
     * @param context the execution context for variable resolution (can be null)
     * @return streaming query plan for execution
     * @throws PlanExecutionException if conversion fails
     */
    public StreamingQueryPlan convertToPlan(QueryNode queryNode, QueryExecutionContext context) throws PlanExecutionException {
        if (queryNode == null) {
            throw new PlanExecutionException("Cannot convert null query node", null, null);
        }
        
        try {
            // Check if this is a raw query (starts with SELECT without @)
            String queryString = queryNode.format();
            if (!queryNode.isExtended()) {
                // Raw query - create a RawJfrQueryNode and use RawQueryPlan
                RawJfrQueryNode rawQueryNode = new RawJfrQueryNode(queryString, queryNode.location());
                return new RawQueryPlan(rawQueryNode, queryString, rawJfrExecutor);
            }
            
            // Extended query - analyze and build streaming plan
            return buildSelectPlan(queryNode, context);
            
        } catch (Exception e) {
            JFRErrorContext errorContext = new JFRErrorContext(
                queryNode,
                "Failed to convert query to streaming plan",
                null
            );
            throw new PlanExecutionException("AST to plan conversion failed: " + e.getMessage(), errorContext, e);
        }
    }
    
    /**
     * Converts a query node to a streaming query plan (without context).
     * 
     * @param queryNode the query AST node
     * @return streaming query plan for execution
     * @throws PlanExecutionException if conversion fails
     */
    public StreamingQueryPlan convertToPlan(QueryNode queryNode) throws PlanExecutionException {
        return convertToPlan(queryNode, null);
    }
    
    /**
     * Build a streaming plan for a SELECT query.
     */
    private StreamingQueryPlan buildSelectPlan(QueryNode queryNode, QueryExecutionContext context) throws PlanExecutionException {
        try {
            // Step 1: Build source plan (FROM clause)
            StreamingQueryPlan plan = buildSourcePlan(queryNode.from(), context);
            
            // Step 2: Apply WHERE filtering
            plan = applyWhereClause(plan, queryNode.where());
            
            // Step 3: Apply SELECT projection or aggregation with ORDER BY
            plan = applySelectClause(plan, queryNode);
            
            return plan;
            
        } catch (Exception e) {
            throw new PlanExecutionException("Failed to build SELECT plan: " + e.getMessage(), 
                new JFRErrorContext(queryNode, "buildSelectPlan", "Failed to build SELECT plan"), e);
        }
    }
    
    /**
     * Build a streaming plan for a SELECT query (without context).
     */
    private StreamingQueryPlan buildSelectPlan(QueryNode queryNode) throws PlanExecutionException {
        return buildSelectPlan(queryNode, null);
    }
    
    /**
     * Apply WHERE clause filtering if present.
     */
    private StreamingQueryPlan applyWhereClause(StreamingQueryPlan plan, WhereNode whereNode) {
        if (whereNode != null) {
            return new FilterPlan(whereNode, plan);
        }
        return plan;
    }
    
    /**
     * Apply SELECT clause with proper handling of aggregation and ORDER BY.
     */
    private StreamingQueryPlan applySelectClause(StreamingQueryPlan plan, QueryNode queryNode) {
        boolean hasAggregates = hasAggregateFunction(queryNode.select());
        boolean hasGroupBy = queryNode.groupBy() != null;
        
        if (hasAggregates || hasGroupBy) {
            return buildAggregationPlan(plan, queryNode);
        } else {
            return buildProjectionPlan(plan, queryNode);
        }
    }
    
    /**
     * Build aggregation plan: GROUP BY → ORDER BY
     */
    private StreamingQueryPlan buildAggregationPlan(StreamingQueryPlan plan, QueryNode queryNode) {
        // Collect additional aggregates from GROUP BY, ORDER BY and HAVING clauses
        List<ExpressionNode> additionalAggregates = AggregationPlan.collectAdditionalAggregates(
            queryNode.groupBy(),
            queryNode.orderBy(), 
            queryNode.having()
        );
        
        // Apply aggregation first
        StreamingQueryPlan aggregatedPlan = new AggregationPlan(
            queryNode.select(), 
            plan, 
            queryNode.select().items(),
            queryNode.groupBy(),
            additionalAggregates
        );
        
        // Add HAVING filter after aggregation
        if (queryNode.having() != null) {
            aggregatedPlan = new HavingPlan(queryNode.having(), aggregatedPlan);
        }
        
        // Add ORDER BY after aggregation
        if (queryNode.orderBy() != null) {
            aggregatedPlan = new SortPlan(queryNode.orderBy(), aggregatedPlan);
        }
        
        return aggregatedPlan;
    }
    
    /**
     * Build projection plan: projection → ORDER BY
     * 
     * This follows standard SQL semantics where ORDER BY can reference SELECT aliases,
     * which means ORDER BY must come after the projection is computed.
     */
    private StreamingQueryPlan buildProjectionPlan(StreamingQueryPlan plan, QueryNode queryNode) {
        // Apply projection first
        StreamingQueryPlan projectedPlan = new ProjectionPlan(
            queryNode.select(), 
            plan, 
            queryNode.select().items()
        );
        
        // Apply ORDER BY after projection (to have access to SELECT aliases)
        if (queryNode.orderBy() != null) {
            projectedPlan = new SortPlan(queryNode.orderBy(), projectedPlan);
        }
        
        return projectedPlan;
    }
    
    /**
     * Build a source plan from the FROM clause.
     */
    private StreamingQueryPlan buildSourcePlan(FromNode fromNode, QueryExecutionContext context) throws PlanExecutionException {
        if (fromNode == null || fromNode.sources().isEmpty()) {
            throw new PlanExecutionException("Query missing FROM clause", null, null);
        }
        
        List<SourceNodeBase> sources = fromNode.sources();
        
        // Handle single source (no joins)
        if (sources.size() == 1) {
            return buildSingleSourcePlan(sources.get(0));
        }
        
        // Handle multiple sources with joins
        StreamingQueryPlan leftPlan = buildSingleSourcePlan(sources.get(0));
        
        for (int i = 1; i < sources.size(); i++) {
            SourceNodeBase joinSource = sources.get(i);
            
            if (joinSource instanceof StandardJoinSourceNode standardJoin) {
                StreamingQueryPlan rightPlan = buildSingleSourcePlan(
                    new SourceNode(standardJoin.source(), standardJoin.alias(), standardJoin.location())
                );
                
                // Support CROSS, INNER, and LEFT JOINs in the streaming system
                if (standardJoin.joinType() == StandardJoinType.CROSS) {
                    leftPlan = new NestedLoopJoinPlan(standardJoin, leftPlan, rightPlan, StandardJoinType.CROSS, null, null);
                } else if (standardJoin.joinType() == StandardJoinType.INNER) {
                    // INNER JOIN requires join conditions
                    leftPlan = new NestedLoopJoinPlan(standardJoin, leftPlan, rightPlan, StandardJoinType.INNER, 
                        standardJoin.leftJoinField(), standardJoin.rightJoinField());
                } else if (standardJoin.joinType() == StandardJoinType.LEFT) {
                    // LEFT JOIN requires join conditions
                    leftPlan = new NestedLoopJoinPlan(standardJoin, leftPlan, rightPlan, StandardJoinType.LEFT, 
                        standardJoin.leftJoinField(), standardJoin.rightJoinField());
                } else {
                    throw new PlanExecutionException("Join type " + standardJoin.joinType() + " not yet supported in streaming plans", null, null);
                }
            } else if (joinSource instanceof FuzzyJoinSourceNode fuzzyJoin) {
                // Handle fuzzy join
                StreamingQueryPlan rightPlan = buildSingleSourcePlan(
                    new SourceNode(fuzzyJoin.source(), fuzzyJoin.alias(), fuzzyJoin.location())
                );
                
                // Create fuzzy join plan
                leftPlan = new FuzzyJoinPlan(
                    fuzzyJoin, 
                    leftPlan, 
                    rightPlan, 
                    fuzzyJoin.joinType(), 
                    fuzzyJoin.joinField(),
                    evaluateConstantExpression(fuzzyJoin.tolerance(), context),
                    evaluateConstantExpression(fuzzyJoin.threshold(), context)
                );
            } else {
                // Regular source - treat as cross join
                StreamingQueryPlan rightPlan = buildSingleSourcePlan(joinSource);
                leftPlan = new NestedLoopJoinPlan(joinSource, leftPlan, rightPlan, StandardJoinType.CROSS, null, null);
            }
        }
        
        return leftPlan;
    }
    
    /**
     * Build a source plan from the FROM clause (without context).
     */
    private StreamingQueryPlan buildSourcePlan(FromNode fromNode) throws PlanExecutionException {
        return buildSourcePlan(fromNode, null);
    }
    
    private StreamingQueryPlan buildSingleSourcePlan(SourceNodeBase source) throws PlanExecutionException {
        if (source instanceof SourceNode sourceNode) {
            return new ScanPlan(sourceNode, sourceNode.source(), sourceNode.alias(), rawJfrExecutor);
        } else if (source instanceof SubquerySourceNode subquerySource) {
            // Recursively convert the subquery
            if (subquerySource.query() instanceof QueryNode subquery) {
                return convertToPlan(subquery);
            } else if (subquerySource.query() instanceof RawJfrQueryNode rawQuery) {
                // Handle raw JFR queries in subqueries
                return new RawQueryPlan(rawQuery, rawQuery.rawQuery(), rawJfrExecutor);
            } else {
                throw new PlanExecutionException("Subquery is not a QueryNode or RawJfrQueryNode: " + subquerySource.query().getClass().getSimpleName(), 
                    null, null);
            }
        } else {
            throw new PlanExecutionException("Unsupported source type: " + source.getClass().getSimpleName(), 
                null, null);
        }
    }
    
    /**
     * Converts any statement node to a streaming query plan.
     * 
     * @param statement the statement AST node
     * @return streaming query plan for execution
     * @throws PlanExecutionException if conversion fails
     */
    public StreamingQueryPlan convertStatementToPlan(StatementNode statement) throws PlanExecutionException {
        return switch (statement) {
            case QueryNode queryNode -> convertToPlan(queryNode);
            case AssignmentNode assignment -> convertToPlan(assignment.query());
            case ViewDefinitionNode viewDef -> convertToPlan(viewDef.query());
            case ShowEventsNode showEventsNode -> new ShowEventsPlan(showEventsNode);
            case ShowFieldsNode showFieldsNode -> new ShowFieldsPlan(showFieldsNode);
            case HelpNode helpNode -> new HelpPlan(helpNode);
            case HelpFunctionNode helpFunctionNode -> new HelpFunctionPlan(helpFunctionNode);
            case HelpGrammarNode helpGrammarNode -> new HelpGrammarPlan(helpGrammarNode);
            case GlobalVariableAssignmentNode globalVarNode -> new GlobalVariableAssignmentPlan(globalVarNode);
            case RawJfrQueryNode rawQueryNode -> new RawQueryPlan(rawQueryNode, rawQueryNode.rawQuery(), rawJfrExecutor);
            default -> throw new PlanExecutionException("Statement type not supported in streaming plans: " + statement.getClass().getSimpleName(), null, null);
        };
    }
    
    /**
     * Check if a SELECT node contains aggregate functions.
     */
    private boolean hasAggregateFunction(SelectNode selectNode) {
        if (selectNode == null) {
            return false;
        }
        
        for (SelectItemNode item : selectNode.items()) {
            if (containsAggregateFunction(item.expression())) {
                return true;
            }
        }
        return false;
    }
    
    /**
     * Recursively check if an expression contains aggregate functions.
     */
    private boolean containsAggregateFunction(ExpressionNode expr) {
        if (expr instanceof FunctionCallNode functionCall) {
            String functionName = functionCall.functionName().toUpperCase();
            if (QueryEvaluatorUtils.isAggregateFunction(functionName)) {
                return true;
            }
            // Check arguments recursively
            for (ExpressionNode arg : functionCall.arguments()) {
                if (containsAggregateFunction(arg)) {
                    return true;
                }
            }
        } else if (expr instanceof BinaryExpressionNode binaryOp) {
            return containsAggregateFunction(binaryOp.left()) || containsAggregateFunction(binaryOp.right());
        } else if (expr instanceof UnaryExpressionNode unaryOp) {
            return containsAggregateFunction(unaryOp.operand());
        }
        return false;
    }
    
    /**
     * Evaluate a constant expression to a CellValue.
     * This is used for expressions that should be constant at plan time.
     * 
     * @param expression the expression to evaluate
     * @param context the execution context for variable resolution (can be null)
     * @return the constant cell value
     * @throws PlanExecutionException if the expression is not constant
     */
    private CellValue evaluateConstantExpression(ExpressionNode expression, QueryExecutionContext context) throws PlanExecutionException {
        if (expression == null) {
            return null;
        }
        
        if (expression instanceof LiteralNode literal) {
            return literal.value();
        } else if (expression instanceof IdentifierNode identifier && context != null) {
            // Try to resolve as a global variable
            Object value = context.getVariable(identifier.name());
            if (value instanceof CellValue cellValue) {
                return cellValue;
            } else if (value != null) {
                throw new PlanExecutionException("Variable '" + identifier.name() + "' in fuzzy join parameters is not a valid cell value", null, null);
            } else {
                throw new PlanExecutionException("Undefined variable in fuzzy join parameters: " + identifier.name(), null, null);
            }
        } else {
            throw new PlanExecutionException("Non-constant expression in fuzzy join parameters: " + 
                expression.getClass().getSimpleName(), null, null);
        }
    }
}
