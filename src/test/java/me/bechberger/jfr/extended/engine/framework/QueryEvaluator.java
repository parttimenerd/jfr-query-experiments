package me.bechberger.jfr.extended.engine.framework;

import me.bechberger.jfr.extended.engine.RawJfrQueryExecutor;
import me.bechberger.jfr.extended.ast.ASTNodes.RawJfrQueryNode;
import me.bechberger.jfr.extended.ast.Location;
import me.bechberger.jfr.extended.table.JfrTable;

/**
 * Query evaluator for processing and executing queries
 */
public class QueryEvaluator {
    private final RawJfrQueryExecutor executor;
    private ExecutionContext context;
    
    public QueryEvaluator(RawJfrQueryExecutor executor) {
        this.executor = executor;
    }
    
    public void setContext(ExecutionContext context) {
        this.context = context;
    }
    
    public ExecutionContext getContext() {
        return context;
    }
    
    public JfrTable query(String query) throws Exception {
        // For simplicity, assume single query execution without parameters
        RawJfrQueryNode queryNode = new RawJfrQueryNode(query, new Location(1, 1));
        return executor.execute(queryNode);
    }
}
