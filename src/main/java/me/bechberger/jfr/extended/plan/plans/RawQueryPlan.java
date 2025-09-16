package me.bechberger.jfr.extended.plan.plans;

import me.bechberger.jfr.extended.ast.ASTNode;
import me.bechberger.jfr.extended.ast.ASTNodes;
import me.bechberger.jfr.extended.ast.ASTNodes.*;
import me.bechberger.jfr.extended.plan.*;
import me.bechberger.jfr.extended.engine.RawJfrQueryExecutor;
import me.bechberger.jfr.extended.table.JfrTable;

/**
 * Streaming plan for executing raw queries using the RawJfrQueryExecutor.
 * 
 * This plan handles queries that start with "SELECT" (without @)
 * by delegating to the existing RawJfrQueryExecutor infrastructure.
 * 
 * @author JFR Query Plan Architecture
 * @since 2.0
 */
public class RawQueryPlan extends AbstractStreamingPlan {
    
    private final String rawQuery;
    private final RawJfrQueryExecutor rawExecutor;
    
    /**
     * Creates a raw query plan for executing raw SQL queries.
     * 
     * @param selectNode the SELECT AST node
     * @param rawQuery the raw query string
     * @param rawExecutor the raw JFR query executor
     */
    public RawQueryPlan(ASTNodes.RawJfrQueryNode queryNode, String rawQuery, RawJfrQueryExecutor rawExecutor) {
        super(queryNode);
        this.rawQuery = rawQuery;
        this.rawExecutor = rawExecutor;
    }
    
    @Override
    public QueryResult execute(QueryExecutionContext context) throws PlanExecutionException {
        try {
            
            // Use the RawJfrQueryExecutor to execute the query
            JfrTable result = rawExecutor.execute((RawJfrQueryNode) sourceNode);
            return QueryResult.success(result);
            
        } catch (Exception e) {
            JFRErrorContext errorContext = createErrorContext("raw_query", 
                "Failed to execute raw query: " + e.getMessage());
            return QueryResult.failure(new PlanExecutionException(
                "Raw query execution failed", errorContext, e));
        }
    }
    
    @Override
    public String explain() {
        StringBuilder sb = new StringBuilder();
        sb.append("RawQueryPlan:\n");
        sb.append("  Query Type: Raw SQL Query\n");
        sb.append("  Query: ").append(rawQuery).append("\n");
        sb.append("  Executor: RawJfrQueryExecutor\n");
        return sb.toString();
    }
    
    @Override
    public ASTNode getSourceNode() {
        return sourceNode;
    }
    
    /**
     * Get the raw query string.
     */
    public String getRawQuery() {
        return rawQuery;
    }
}
