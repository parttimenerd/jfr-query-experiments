package me.bechberger.jfr.extended.plan.plans;

import me.bechberger.jfr.extended.ast.ASTNode;
import me.bechberger.jfr.extended.plan.JFRErrorContext;
import me.bechberger.jfr.extended.plan.QueryExecutionContext;

import java.util.Set;
import java.util.HashSet;

/**
 * Abstract base class for streaming query plans.
 * 
 * This class provides common functionality for all plan implementations
 * including AST node association, basic memory estimation, and
 * utility methods for plan composition.
 * 
 * @author JFR Query Plan Architecture
 * @since 2.0
 */
public abstract class AbstractStreamingPlan implements StreamingQueryPlan {
    protected final ASTNode sourceNode;
    
    /**
     * Create a new plan with the given source AST node.
     */
    protected AbstractStreamingPlan(ASTNode sourceNode) {
        this.sourceNode = sourceNode;
    }
    
    @Override
    public ASTNode getSourceNode() {
        return sourceNode;
    }
   
    /**
     * Create an error context for this plan.
     */
    protected JFRErrorContext createErrorContext(String executionPhase, String message) {
        return new JFRErrorContext(sourceNode, executionPhase, message);
    }
    
    /**
     * Create an error context with additional context information.
     */
    protected JFRErrorContext createErrorContext(String executionPhase, String message, Object additionalContext) {
        return new JFRErrorContext(sourceNode, executionPhase, message, additionalContext);
    }
    
    /**
     * Format the plan name for explain() output.
     */
    protected String formatPlanName() {
        return this.getClass().getSimpleName().replace("Plan", "");
    }
    
    @Override
    public String toString() {
        return getClass().getSimpleName();
    }
}
