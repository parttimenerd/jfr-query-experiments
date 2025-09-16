package me.bechberger.jfr.extended.plan.plans;

import me.bechberger.jfr.extended.ast.ASTNodes.HelpNode;
import me.bechberger.jfr.extended.engine.HelpProvider;
import me.bechberger.jfr.extended.plan.PlanExecutionException;
import me.bechberger.jfr.extended.plan.QueryExecutionContext;
import me.bechberger.jfr.extended.plan.QueryResult;
import me.bechberger.jfr.extended.table.SingleCellTable;

/**
 * Plan for executing HELP statements.
 * 
 * This plan provides general help content for the JFR query language.
 * 
 * @author JFR Query Plan Architecture
 * @since 2.0
 */
public class HelpPlan extends AbstractStreamingPlan {
    
    public HelpPlan(HelpNode sourceNode) {
        super(sourceNode);
    }
    
    @Override
    public QueryResult execute(QueryExecutionContext context) throws PlanExecutionException {
        try {
            String helpContent = HelpProvider.getGeneralHelp();
            return QueryResult.success(SingleCellTable.of("help", helpContent));
            
        } catch (Exception e) {
            throw new PlanExecutionException("Failed to generate help content", 
                createErrorContext("execution", "Error generating help"), e);
        }
    }
    
    @Override
    public String explain() {
        return formatPlanName() + " - Provide general help for JFR query language";
    }
    
    @Override
    public boolean supportsStreaming() {
        return false; // This is a help operation, not streaming
    }
}
