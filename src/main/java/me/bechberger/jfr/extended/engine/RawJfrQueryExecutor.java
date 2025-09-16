package me.bechberger.jfr.extended.engine;

import me.bechberger.jfr.extended.table.JfrTable;
import me.bechberger.jfr.extended.ast.ASTNodes.RawJfrQueryNode;
import jdk.jfr.EventType;

import java.util.List;

/**
 * Interface for executing raw JFR queries.
 * 
 * This interface defines the contract for executing raw JFR queries
 * and returning the results as JfrTable objects.
 * 
 * @author JFR Extended Query Engine
 * @since 1.0
 */
public interface RawJfrQueryExecutor {
    /**
     * Execute a raw JFR query and return the result as a table.
     * 
     * @param queryNode the raw JFR query node to execute
     * @return the query result as a JfrTable
     * @throws Exception if the query execution fails
     */
    JfrTable execute(RawJfrQueryNode queryNode) throws Exception;
    
    /**
     * Get available event types from the JFR source.
     * 
     * @return list of available event types
     * @throws Exception if event types cannot be retrieved
     */
    List<EventType> getEventTypes() throws Exception;
}
