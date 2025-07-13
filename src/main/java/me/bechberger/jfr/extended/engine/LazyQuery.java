package me.bechberger.jfr.extended.engine;

import me.bechberger.jfr.extended.ast.ASTNodes.QueryNode;
import me.bechberger.jfr.extended.table.JfrTable;

/**
 * Represents a lazily evaluated query that is only executed when accessed.
 * This allows for efficient handling of variable assignments where the query
 * is only evaluated when the variable is actually used.
 */
public class LazyQuery {
    private final QueryNode queryNode;
    private final QueryEvaluator evaluator;
    private JfrTable cachedResult;
    private boolean hasBeenEvaluated;
    
    public LazyQuery(QueryNode queryNode, QueryEvaluator evaluator) {
        this.queryNode = queryNode;
        this.evaluator = evaluator;
        this.hasBeenEvaluated = false;
        this.cachedResult = null;
    }
    
    /**
     * Evaluates the query if not already evaluated and returns the result.
     * Subsequent calls return the cached result.
     */
    public synchronized JfrTable evaluate() {
        if (!hasBeenEvaluated) {
            cachedResult = queryNode.accept(evaluator);
            hasBeenEvaluated = true;
        }
        return cachedResult;
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
     * Clears the cached result, forcing re-evaluation on next access.
     */
    public synchronized void invalidate() {
        hasBeenEvaluated = false;
        cachedResult = null;
    }
    
    @Override
    public String toString() {
        return "LazyQuery{" +
               "evaluated=" + hasBeenEvaluated +
               ", query=" + queryNode.format() +
               "}";
    }
}
