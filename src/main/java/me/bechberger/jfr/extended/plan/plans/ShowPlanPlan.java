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
 * Plan for executing SHOW PLAN commands that display query execution plans.
 * 
 * This plan displays the structure and execution flow of a query plan
 * in a simple tree format, useful for understanding query optimization
 * and execution strategy.
 * 
 * @author JFR Query Plan Architecture
 * @since 3.0
 */
public class ShowPlanPlan extends AbstractStreamingPlan {
    
    private final StreamingQueryPlan targetPlan;
    private final boolean verbose;
    
    /**
     * Create a new SHOW PLAN plan.
     * 
     * @param sourceNode The AST node this plan was created from
     * @param targetPlan The query plan to visualize
     * @param verbose Whether to show verbose output
     */
    public ShowPlanPlan(ASTNode sourceNode, StreamingQueryPlan targetPlan, boolean verbose) {
        super(sourceNode);
        this.targetPlan = targetPlan;
        this.verbose = verbose;
    }
    
    /**
     * Create a new SHOW PLAN plan with default simple output.
     * 
     * @param sourceNode The AST node this plan was created from
     * @param targetPlan The query plan to visualize
     */
    public ShowPlanPlan(ASTNode sourceNode, StreamingQueryPlan targetPlan) {
        this(sourceNode, targetPlan, false);
    }
    
    @Override
    public QueryResult execute(QueryExecutionContext context) throws PlanExecutionException {
        try {
            // Generate the plan visualization
            String planOutput = verbose ? 
                QueryPlanVisualizer.visualizeVerbose(targetPlan) :
                QueryPlanVisualizer.visualizeSimple(targetPlan);
            
            // Create result table with the plan output
            JfrTable.Column planColumn = new JfrTable.Column("Query Plan", CellType.STRING);
            StandardJfrTable resultTable = new StandardJfrTable(List.of(planColumn));
            
            // Split the plan output into lines for better display
            String[] lines = planOutput.split("\n");
            for (String line : lines) {
                resultTable.addRow(new CellValue.StringValue(line));
            }
            
            return QueryResult.success(resultTable);
            
        } catch (Exception e) {
            throw new PlanExecutionException("SHOW PLAN execution failed: " + e.getMessage(), e);
        }
    }
    
    @Override
    public String explain() {
        return "ShowPlanPlan(target: " + targetPlan.getClass().getSimpleName() + ", verbose: " + verbose + ")";
    }
    
    @Override
    public boolean supportsStreaming() {
        return true; // Plan visualization is always streaming
    }
    
    /**
     * Get the target plan being visualized.
     * 
     * @return The target plan
     */
    public StreamingQueryPlan getTargetPlan() {
        return targetPlan;
    }
    
    /**
     * Check if this is a verbose plan display.
     * 
     * @return true if verbose output is enabled
     */
    public boolean isVerbose() {
        return verbose;
    }
}
