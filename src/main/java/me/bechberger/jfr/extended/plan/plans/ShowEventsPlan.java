package me.bechberger.jfr.extended.plan.plans;

import me.bechberger.jfr.extended.ast.ASTNodes.ShowEventsNode;
import me.bechberger.jfr.extended.plan.PlanExecutionException;
import me.bechberger.jfr.extended.plan.QueryExecutionContext;
import me.bechberger.jfr.extended.plan.QueryResult;
import me.bechberger.jfr.extended.table.JfrTable;
import me.bechberger.jfr.extended.table.SingleCellTable;
import me.bechberger.jfr.extended.table.StandardJfrTable;
import me.bechberger.jfr.extended.table.CellType;
import me.bechberger.jfr.extended.table.CellValue;

import java.util.Set;
import java.util.List;

/**
 * Plan for executing SHOW EVENTS statements.
 * 
 * This plan lists all available JFR event types in the context.
 * 
 * @author JFR Query Plan Architecture
 * @since 2.0
 */
public class ShowEventsPlan extends AbstractStreamingPlan {
    
    public ShowEventsPlan(ShowEventsNode sourceNode) {
        super(sourceNode);
    }
    
    @Override
    public QueryResult execute(QueryExecutionContext context) throws PlanExecutionException {
        try {
            // Get all available event types from the metadata
            Set<String> eventTypeNames = context.getMetadata().eventTypes();
            
            if (eventTypeNames.isEmpty()) {
                // Return a table with a message if no events are available
                return QueryResult.success(SingleCellTable.of("eventType", "No event types available"));
            }
            
            // Create a table with event type names
            List<JfrTable.Column> columns = List.of(new JfrTable.Column("eventType", CellType.STRING));
            JfrTable eventTypeTable = new StandardJfrTable(columns);
            
            for (String eventTypeName : eventTypeNames) {
                List<CellValue> rowValues = List.of(CellValue.of(eventTypeName));
                eventTypeTable.addRow(new JfrTable.Row(rowValues));
            }
            
            return QueryResult.success(eventTypeTable);
            
        } catch (Exception e) {
            throw new PlanExecutionException("Failed to show events", createErrorContext("execution", "Error retrieving event types"), e);
        }
    }
    
    @Override
    public String explain() {
        return formatPlanName() + " - List all available JFR event types";
    }
    
    @Override
    public boolean supportsStreaming() {
        return false; // This is a metadata operation, not streaming
    }
}
