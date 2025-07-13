package me.bechberger.jfr.extended.engine;

import me.bechberger.jfr.extended.table.JfrTable;
import me.bechberger.jfr.extended.ast.ASTNodes.RawJfrQueryNode;

/**
 * Interface for executing raw JFR queries.
 * 
 * This interface defines the contract for executing raw JFR queries
 * and returning the results as JfrTable objects.
 * 
 * @author JFR Extended Query Engine
 * @since 1.0
 */
@FunctionalInterface
public interface RawJfrQueryExecutor {
    /**
     * Execute a raw JFR query and return the result as a table.
     * 
     * @param queryNode the raw JFR query node to execute
     * @return the query result as a JfrTable
     * @throws Exception if the query execution fails
     */
    JfrTable execute(RawJfrQueryNode queryNode) throws Exception;
}
