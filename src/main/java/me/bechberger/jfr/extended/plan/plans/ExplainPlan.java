package me.bechberger.jfr.extended.plan.plans;

import me.bechberger.jfr.extended.ast.ASTNode;
import me.bechberger.jfr.extended.plan.PlanExecutionException;
import me.bechberger.jfr.extended.plan.QueryExecutionContext;
import me.bechberger.jfr.extended.plan.QueryResult;
import me.bechberger.jfr.extended.plan.visualizer.QueryPlanVisualizer;
import me.bechberger.jfr.extended.table.JfrTable;
import me.bechberger.jfr.extended.table.StandardJfrTable;
import me.bechberger.jfr.extended.table.CellValue;
import me.bechberger.jfr.extended.table.CellType;

import java.util.List;

/**
 * Plan for executing EXPLAIN commands that display detailed query execution plans.
 * 
 * This plan provides comprehensive visualization of query execution plans
 * including performance metrics, optimization opportunities, and ASCII art
 * representations for debugging and understanding query behavior.
 * 
 * @author JFR Query Plan Architecture
 * @since 3.0
 */
public class ExplainPlan extends AbstractStreamingPlan {
    
    private final StreamingQueryPlan targetPlan;
    private final ExplainType explainType;
    
    /**
     * Types of EXPLAIN output formats
     */
    public enum ExplainType {
        SIMPLE,     // Simple tree view
        VERBOSE,    // Detailed analysis with performance metrics
        ASCII_ART,  // Fun ASCII art visualization
        PERFORMANCE // Performance-focused analysis
    }
    
    /**
     * Create a new EXPLAIN plan.
     * 
     * @param sourceNode The AST node this plan was created from
     * @param targetPlan The query plan to explain
     * @param explainType The type of explanation to generate
     */
    public ExplainPlan(ASTNode sourceNode, StreamingQueryPlan targetPlan, ExplainType explainType) {
        super(sourceNode);
        this.targetPlan = targetPlan;
        this.explainType = explainType;
    }
    
    /**
     * Create a new EXPLAIN plan with default verbose output.
     * 
     * @param sourceNode The AST node this plan was created from
     * @param targetPlan The query plan to explain
     */
    public ExplainPlan(ASTNode sourceNode, StreamingQueryPlan targetPlan) {
        this(sourceNode, targetPlan, ExplainType.VERBOSE);
    }
    
    @Override
    public QueryResult execute(QueryExecutionContext context) throws PlanExecutionException {
        try {
            // Generate the appropriate explanation based on type
            String explanation = switch (explainType) {
                case SIMPLE -> QueryPlanVisualizer.visualizeSimple(targetPlan);
                case VERBOSE -> QueryPlanVisualizer.visualizeVerbose(targetPlan);
                case ASCII_ART -> QueryPlanVisualizer.visualizeAsciiArt(targetPlan);
                case PERFORMANCE -> QueryPlanVisualizer.visualizePerformance(targetPlan);
            };
            
            // Create result table with the explanation
            JfrTable.Column explanationColumn = new JfrTable.Column("Explanation", CellType.STRING);
            StandardJfrTable resultTable = new StandardJfrTable(List.of(explanationColumn));
            
            // Split the explanation into lines for better display
            String[] lines = explanation.split("\n");
            for (String line : lines) {
                resultTable.addRow(new CellValue.StringValue(line));
            }
            
            return QueryResult.success(resultTable);
            
        } catch (Exception e) {
            throw new PlanExecutionException("EXPLAIN execution failed: " + e.getMessage(), e);
        }
    }
    
    @Override
    public String explain() {
        return "ExplainPlan(target: " + targetPlan.getClass().getSimpleName() + ", type: " + explainType + ")";
    }
    
    @Override
    public boolean supportsStreaming() {
        return true; // Plan explanation is always streaming
    }
    
    /**
     * Get the target plan being explained.
     * 
     * @return The target plan
     */
    public StreamingQueryPlan getTargetPlan() {
        return targetPlan;
    }
    
    /**
     * Get the explanation type.
     * 
     * @return The type of explanation
     */
    public ExplainType getExplainType() {
        return explainType;
    }
}
