package me.bechberger.jfr.extended.plan.plans;

import me.bechberger.jfr.extended.ast.ASTNodes.*;
import me.bechberger.jfr.extended.plan.*;
import me.bechberger.jfr.extended.plan.exception.PlanExceptionFactory;
import me.bechberger.jfr.extended.table.JfrTable;
import me.bechberger.jfr.extended.table.StandardJfrTable;
import me.bechberger.jfr.extended.table.CellType;
import me.bechberger.jfr.extended.engine.RawJfrQueryExecutor;

import java.util.Set;
import java.util.HashSet;
import java.util.List;
import java.util.ArrayList;

/**
 * Streaming plan for scanning JFR event data.
 * 
 * This plan implements the fundamental data source operation,
 * reading and filtering JFR events from the underlying data source.
 * It supports both full table scans and filtered scans based on event types.
 * 
 * @author JFR Query Plan Architecture
 * @since 2.0
 */
public class ScanPlan extends AbstractStreamingPlan {
    
    private final String tableName;
    private final String alias;
    private final Set<String> requiredColumns;
    private final RawJfrQueryExecutor rawExecutor;
    
    /**
     * Creates a scan plan for a specific table/event type.
     * 
     * @param sourceNode the AST node that represents this scan
     * @param tableName the name of the table or event type to scan
     * @param alias the alias for this scan (may be null)
     * @param rawExecutor the raw JFR executor for table access
     */
    public ScanPlan(SourceNode sourceNode, String tableName, String alias, RawJfrQueryExecutor rawExecutor) {
        super(sourceNode);
        this.tableName = tableName;
        this.alias = alias;
        this.requiredColumns = new HashSet<>();
        this.rawExecutor = rawExecutor;
    }
    
    /**
     * Creates a scan plan with specific column requirements.
     */
    public ScanPlan(SourceNode sourceNode, String tableName, String alias, Set<String> requiredColumns, RawJfrQueryExecutor rawExecutor) {
        this(sourceNode, tableName, alias, rawExecutor);
        this.requiredColumns.addAll(requiredColumns);
    }
    
    @Override
    public QueryResult execute(QueryExecutionContext context) throws PlanExecutionException {
        context.recordPlanStart("ScanPlan", explain());
        try {
            // 1. Check if this is a table alias registered in the current scope
            try {
                JfrTable aliasTable = context.resolveAlias(tableName, sourceNode);
                context.recordPlanSuccess("ScanPlan", explain(), aliasTable.getRowCount());
                return QueryResult.success(aliasTable);
            } catch (PlanExecutionException e) {
                // Alias not found, continue to other resolution methods
            }
            
            // 2. Check if this is a variable (view or computed table)
            Object tableVariable = context.getVariable(tableName);
            if (tableVariable instanceof JfrTable virtualTable) {
                context.recordPlanSuccess("ScanPlan", explain(), virtualTable.getRowCount());
                return QueryResult.success(virtualTable);
            }
            
            // 3. Try to get the table from the raw JFR executor
            // This is how registered mock tables and real JFR tables are accessed
            try {
                RawJfrQueryNode queryNode = new RawJfrQueryNode("SELECT * FROM " + tableName, null);
                JfrTable table = rawExecutor.execute(queryNode);
                if (table != null) {
                    context.recordPlanSuccess("ScanPlan", explain(), table.getRowCount());
                    return QueryResult.success(table);
                }
            } catch (Exception e) {
                // Table access failed, continue to error handling
            }
            
            // 4. Handle unknown tables - return error with helpful message
            // Get available tables from context metadata instead of hardcoding assumptions
            String[] availableTables;
            try {
                // Get actual available event types from the JFR metadata
                availableTables = context.getMetadata().eventTypes().toArray(new String[0]);
            } catch (Exception e) {
                // Fallback to empty array if we can't determine available tables
                availableTables = new String[0];
            }
            
            me.bechberger.jfr.extended.plan.exception.DataAccessException tableNotFound = 
                PlanExceptionFactory.createTableNotFoundError(tableName, availableTables, this);
            context.recordPlanFailure("ScanPlan", explain(), "Table not found: " + tableName);
            return QueryResult.failure(new PlanExecutionException(
                tableNotFound.getMessage(), null, tableNotFound));
            
        } catch (Exception e) {
            context.recordPlanFailure("ScanPlan", explain(), e.getMessage());
            if (e instanceof PlanExecutionException) {
                throw e; // Re-throw plan execution exceptions as-is
            }
            
            // Wrap exception with plan context
            me.bechberger.jfr.extended.plan.exception.PlanException planException = 
                PlanExceptionFactory.createScanError(tableName, e, this);
            return QueryResult.failure(new PlanExecutionException(
                planException.getMessage(), planException.getErrorContext(), planException));
        }
    }
    
    

    
    
    
    @Override
    public String explain() {
        StringBuilder sb = new StringBuilder();
        sb.append("ScanPlan:\n");
        sb.append("  Table: ").append(tableName).append("\n");
        if (alias != null) {
            sb.append("  Alias: ").append(alias).append("\n");
        }
        sb.append("  Type: ").append("Table Scan").append("\n");
        sb.append("  Required Columns: ").append(requiredColumns.isEmpty() ? "ALL" : requiredColumns).append("\n");
        
        return sb.toString();
    }
    
    @Override
    public SourceNode getSourceNode() {
        return (SourceNode) sourceNode;
    }
    
    /**
     * Get the table name being scanned.
     */
    public String getTableName() {
        return tableName;
    }
    
    /**
     * Get the alias for this scan.
     */
    public String getAlias() {
        return alias;
    }
    
    /**
     * Check if this scan requires specific columns.
     */
    public boolean hasColumnRequirements() {
        return !requiredColumns.isEmpty();
    }
    
    /**
     * Get the required columns for this scan.
     */
    public Set<String> getRequiredColumns() {
        return new HashSet<>(requiredColumns);
    }
    
    /**
     * Add a column requirement to this scan.
     */
    public void addRequiredColumn(String columnName) {
        this.requiredColumns.add(columnName);
    }


}
