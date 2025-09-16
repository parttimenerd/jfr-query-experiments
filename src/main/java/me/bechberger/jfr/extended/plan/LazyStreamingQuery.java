package me.bechberger.jfr.extended.plan;

import me.bechberger.jfr.extended.ast.ASTNodes.QueryNode;
import me.bechberger.jfr.extended.plan.plans.StreamingQueryPlan;
import me.bechberger.jfr.extended.table.JfrTable;

/**
 * Represents a lazily evaluated streaming query that is only executed when accessed.
 * This allows for efficient handling of variable assignments where the query
 * is only evaluated when the variable is actually used.
 * 
 * This is the streaming plan equivalent of LazyQuery, providing lazy evaluation
 * with better memory efficiency through streaming execution.
 * 
 * @author JFR Query Plan Architecture
 * @since 2.0
 */
public class LazyStreamingQuery {
    private final QueryNode queryNode;
    private final AstToPlanConverter planConverter;
    private final QueryExecutionContext context;
    private StreamingQueryPlan cachedPlan;
    private QueryResult cachedResult;
    private boolean planGenerated;
    private boolean hasBeenEvaluated;
    
    public LazyStreamingQuery(QueryNode queryNode, AstToPlanConverter planConverter, QueryExecutionContext context) {
        this.queryNode = queryNode;
        this.planConverter = planConverter;
        this.context = context;
        this.planGenerated = false;
        this.hasBeenEvaluated = false;
        this.cachedPlan = null;
        this.cachedResult = null;
    }
    
    /**
     * Generates the streaming plan if not already generated.
     * This is separate from evaluation to allow plan inspection without execution.
     */
    public synchronized StreamingQueryPlan getPlan() throws PlanExecutionException {
        if (!planGenerated) {
            cachedPlan = planConverter.convertToPlan(queryNode, context);
            planGenerated = true;
        }
        return cachedPlan;
    }
    
    /**
     * Evaluates the query if not already evaluated and returns the result.
     * Subsequent calls return the cached result.
     */
    public synchronized QueryResult evaluate() throws PlanExecutionException {
        if (!hasBeenEvaluated) {
            StreamingQueryPlan plan = getPlan();
            cachedResult = plan.execute(context);
            hasBeenEvaluated = true;
        }
        return cachedResult;
    }
    
    /**
     * Evaluates the query and returns the result table.
     * Throws an exception if the evaluation fails.
     */
    public synchronized JfrTable evaluateTable() throws PlanExecutionException {
        QueryResult result = evaluate();
        if (result.isSuccess()) {
            return result.getTable();
        } else {
            throw new PlanExecutionException("Lazy query evaluation failed", null, result.getError());
        }
    }
    
    /**
     * Checks if the plan has been generated yet.
     */
    public boolean isPlanGenerated() {
        return planGenerated;
    }
    
    /**
     * Checks if the query has been evaluated yet.
     */
    public boolean isEvaluated() {
        return hasBeenEvaluated;
    }
    
    /**
     * Gets the underlying query node.
     */
    public QueryNode getQueryNode() {
        return queryNode;
    }
    
    /**
     * Invalidates the cached result, forcing re-evaluation on next access.
     * The plan is preserved to avoid re-compilation.
     */
    public synchronized void invalidateResult() {
        hasBeenEvaluated = false;
        cachedResult = null;
    }
    
    /**
     * Invalidates both the plan and result, forcing complete regeneration.
     */
    public synchronized void invalidateAll() {
        planGenerated = false;
        hasBeenEvaluated = false;
        cachedPlan = null;
        cachedResult = null;
    }
    
    /**
     * Gets a string representation of this lazy query for debugging.
     */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("LazyStreamingQuery{");
        sb.append("query=").append(queryNode.format());
        sb.append(", planGenerated=").append(planGenerated);
        sb.append(", evaluated=").append(hasBeenEvaluated);
        if (cachedResult != null) {
            sb.append(", success=").append(cachedResult.isSuccess());
        }
        sb.append('}');
        return sb.toString();
    }
}
