package me.bechberger.jfr.extended.plan.plans;

import me.bechberger.jfr.extended.ast.ASTNodes.ShowFieldsNode;
import me.bechberger.jfr.extended.plan.PlanExecutionException;
import me.bechberger.jfr.extended.plan.QueryExecutionContext;
import me.bechberger.jfr.extended.plan.QueryResult;
import me.bechberger.jfr.extended.table.SingleCellTable;

/**
 * Plan for executing SHOW FIELDS statements.
 * 
 * This plan shows fields for a specific JFR event type.
 * 
 * @author JFR Query Plan Architecture
 * @since 2.0
 */
public class ShowFieldsPlan extends AbstractStreamingPlan {
    
    private final String eventType;
    
    public ShowFieldsPlan(ShowFieldsNode sourceNode) {
        super(sourceNode);
        this.eventType = sourceNode.eventType();
    }
    
    @Override
    public QueryResult execute(QueryExecutionContext context) throws PlanExecutionException {
        try {
            // For now, return a simple message about the event type
            // In a full implementation, this would query the actual JFR metadata
            // to get field information for the specified event type
            
            if (!context.getMetadata().hasEventType(eventType)) {
                return QueryResult.success(SingleCellTable.of("error", "Event type '" + eventType + "' not found"));
            }
            
            // Create a simple response for now
            String message = "Fields for event type '" + eventType + "' (metadata query not yet implemented)";
            return QueryResult.success(SingleCellTable.of("fields", message));
            
        } catch (Exception e) {
            throw new PlanExecutionException("Failed to show fields for event type: " + eventType, 
                createErrorContext("execution", "Error retrieving fields for event type"), e);
        }
    }
    
    @Override
    public String explain() {
        return formatPlanName() + " - Show fields for event type: " + eventType;
    }
    
    @Override
    public boolean supportsStreaming() {
        return false; // This is a metadata operation, not streaming
    }
}
