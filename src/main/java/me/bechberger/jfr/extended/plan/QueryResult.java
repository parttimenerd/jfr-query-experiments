package me.bechberger.jfr.extended.plan;

import me.bechberger.jfr.extended.table.JfrTable;

/**
 * Result wrapper for query execution with success/error states.
 * 
 * This class encapsulates the result of a query plan execution,
 * providing information about success/failure and carrying either
 * the result table or error information.
 * 
 * @author JFR Query Plan Architecture
 * @since 2.0
 */
public class QueryResult {
    private final JfrTable table;
    private final boolean success;
    private final Exception error;
    
    public QueryResult(JfrTable table, boolean success, Exception error) {
        this.table = table;
        this.success = success;
        this.error = error;
    }
    
    /**
     * Create a successful query result.
     */
    public static QueryResult success(JfrTable table) {
        return new QueryResult(table, true, null);
    }
    
    /**
     * Create a failed query result.
     */
    public static QueryResult failure(Exception error) {
        return new QueryResult(null, false, error);
    }
    
    /**
     * Check if the query execution was successful.
     */
    public boolean isSuccess() {
        return success;
    }
    
    /**
     * Get the result table (only valid if success is true).
     */
    public JfrTable getTable() {
        return table;
    }
    
    /**
     * Get the error (only valid if success is false).
     */
    public Exception getError() {
        return error;
    }
    
    @Override
    public String toString() {
        if (success) {
            return "QueryResult{success=true, table=" + table + "}";
        } else {
            return "QueryResult{success=false, error=" + error + "}";
        }
    }
}
