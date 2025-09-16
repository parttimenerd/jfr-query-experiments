package me.bechberger.jfr.extended.plan.plans;

import me.bechberger.jfr.extended.ast.ASTNode;
import me.bechberger.jfr.extended.plan.PlanExecutionException;
import me.bechberger.jfr.extended.plan.QueryExecutionContext;
import me.bechberger.jfr.extended.plan.QueryResult;
import me.bechberger.jfr.extended.table.JfrTable;

import java.util.Set;

/**
 * Core interface for streaming query plans.
 * 
 * This interface defines the contract for all query plan implementations
 * in the streaming architecture. It provides methods for execution,
 * analysis, and optimization of query plans.
 * 
 * @author JFR Query Plan Architecture
 * @since 2.0
 */
public interface StreamingQueryPlan {
    
    /**
     * Execute the query plan and return the result.
     * 
     * @param context the execution context
     * @return the query result
     * @throws PlanExecutionException if execution fails
     */
    QueryResult execute(QueryExecutionContext context) throws PlanExecutionException;
    
    /**
     * Generate a human-readable explanation of the query plan.
     * 
     * @return a string describing the plan execution strategy
     */
    String explain();
    
    /**
     * Get the AST node that this plan was created from.
     * 
     * @return the source AST node
     */
    ASTNode getSourceNode();
    
    /**
     * Check if this plan supports streaming execution.
     * Some plans may need to materialize all data before processing.
     * 
     * @return true if the plan can stream results
     */
    default boolean supportsStreaming() {
        return true;
    }

    
}
