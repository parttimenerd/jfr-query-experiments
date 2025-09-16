package me.bechberger.jfr.extended.plan.plans;

import me.bechberger.jfr.extended.ast.ASTNodes.HelpGrammarNode;
import me.bechberger.jfr.extended.engine.HelpProvider;
import me.bechberger.jfr.extended.plan.PlanExecutionException;
import me.bechberger.jfr.extended.plan.QueryExecutionContext;
import me.bechberger.jfr.extended.plan.QueryResult;
import me.bechberger.jfr.extended.table.SingleCellTable;

/**
 * Plan for executing HELP GRAMMAR statements.
 * 
 * This plan provides complete grammar documentation for the JFR query language.
 * 
 * @author JFR Query Plan Architecture
 * @since 2.0
 */
public class HelpGrammarPlan extends AbstractStreamingPlan {
    
    public HelpGrammarPlan(HelpGrammarNode sourceNode) {
        super(sourceNode);
    }
    
    @Override
    public QueryResult execute(QueryExecutionContext context) throws PlanExecutionException {
        try {
            String grammarContent = HelpProvider.getGrammarHelp();
            return QueryResult.success(SingleCellTable.of("grammar", grammarContent));
            
        } catch (Exception e) {
            throw new PlanExecutionException("Failed to generate grammar help", 
                createErrorContext("execution", "Error generating grammar help"), e);
        }
    }
    
    @Override
    public String explain() {
        return formatPlanName() + " - Provide complete grammar documentation";
    }
    
    @Override
    public boolean supportsStreaming() {
        return false; // This is a help operation, not streaming
    }
}
