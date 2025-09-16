package me.bechberger.jfr.extended.plan.plans;

import me.bechberger.jfr.extended.ast.ASTNodes.HelpFunctionNode;
import me.bechberger.jfr.extended.engine.HelpProvider;
import me.bechberger.jfr.extended.plan.PlanExecutionException;
import me.bechberger.jfr.extended.plan.QueryExecutionContext;
import me.bechberger.jfr.extended.plan.QueryResult;
import me.bechberger.jfr.extended.table.SingleCellTable;

/**
 * Plan for executing HELP FUNCTION statements.
 * 
 * This plan provides help content for a specific function.
 * 
 * @author JFR Query Plan Architecture
 * @since 2.0
 */
public class HelpFunctionPlan extends AbstractStreamingPlan {
    
    private final String functionName;
    
    public HelpFunctionPlan(HelpFunctionNode sourceNode) {
        super(sourceNode);
        this.functionName = sourceNode.functionName();
    }
    
    @Override
    public QueryResult execute(QueryExecutionContext context) throws PlanExecutionException {
        try {
            String helpContent = HelpProvider.getFunctionHelp(functionName);
            return QueryResult.success(SingleCellTable.of("help", helpContent));
            
        } catch (Exception e) {
            throw new PlanExecutionException("Failed to generate function help for: " + functionName, 
                createErrorContext("execution", "Error generating function help"), e);
        }
    }
    
    @Override
    public String explain() {
        return formatPlanName() + " - Provide help for function: " + functionName;
    }
    
    @Override
    public boolean supportsStreaming() {
        return false; // This is a help operation, not streaming
    }
}
