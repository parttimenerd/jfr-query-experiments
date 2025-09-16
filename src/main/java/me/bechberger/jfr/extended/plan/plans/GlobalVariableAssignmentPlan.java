package me.bechberger.jfr.extended.plan.plans;

import me.bechberger.jfr.extended.ast.ASTNodes.GlobalVariableAssignmentNode;
import me.bechberger.jfr.extended.table.CellValue;
import me.bechberger.jfr.extended.plan.QueryExecutionContext;
import me.bechberger.jfr.extended.plan.QueryResult;
import me.bechberger.jfr.extended.plan.PlanExecutionException;
import me.bechberger.jfr.extended.plan.JFRErrorContext;
import me.bechberger.jfr.extended.plan.evaluator.PlanExpressionEvaluator;

/**
 * Plan for executing global variable assignments.
 * Global variables are evaluated immediately and stored in the global context.
 * 
 * @since 2.0
 */
public class GlobalVariableAssignmentPlan extends AbstractStreamingPlan {
    
    private final GlobalVariableAssignmentNode globalVarNode;
    
    /**
     * Creates a global variable assignment plan.
     * 
     * @param globalVarNode the global variable assignment AST node
     */
    public GlobalVariableAssignmentPlan(GlobalVariableAssignmentNode globalVarNode) {
        super(globalVarNode);
        this.globalVarNode = globalVarNode;
    }
    
    @Override
    public QueryResult execute(QueryExecutionContext context) throws PlanExecutionException {
        context.recordPlanStart("GlobalVariableAssignmentPlan", explain());
        try {
            System.err.println("DEBUG GlobalVariableAssignmentPlan: Assigning variable " + globalVarNode.variable());
            // Use PlanExpressionEvaluator for robust expression evaluation
            PlanExpressionEvaluator evaluator = new PlanExpressionEvaluator(context);
            CellValue value = evaluator.evaluateExpression(globalVarNode.expression());
            
            System.err.println("DEBUG GlobalVariableAssignmentPlan: Evaluated value for " + globalVarNode.variable() + " = " + value);
            // Store the evaluated value directly in context for global access
            context.setVariable(globalVarNode.variable(), value);
            System.err.println("DEBUG GlobalVariableAssignmentPlan: Successfully set variable " + globalVarNode.variable());
            
            context.recordPlanSuccess("GlobalVariableAssignmentPlan", explain(), 1);
            // Return success result for assignment statements
            return QueryResult.success(null);
            
        } catch (Exception e) {
            context.recordPlanFailure("GlobalVariableAssignmentPlan", explain(), e.getMessage());
            JFRErrorContext errorContext = createErrorContext("global_variable_assignment", 
                "Failed to evaluate global variable: " + globalVarNode.variable());
            return QueryResult.failure(new PlanExecutionException(
                "Global variable assignment failed", errorContext, e));
        }
    }
    
    @Override
    public String explain() {
        return formatPlanName() + " - Assign expression to global variable: " + globalVarNode.variable();
    }
    
    @Override
    public boolean supportsStreaming() {
        return true;
    }
}
