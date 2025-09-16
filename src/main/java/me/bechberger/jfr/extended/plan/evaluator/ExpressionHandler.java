package me.bechberger.jfr.extended.plan.evaluator;

import me.bechberger.jfr.extended.ast.ASTNodes.ExpressionNode;
import me.bechberger.jfr.extended.table.JfrTable;
import me.bechberger.jfr.extended.table.CellValue;

/**
 * Custom expression handler interface for specialized evaluation contexts.
 * Allows plans to provide custom handling for specific expression types.
 * 
 * This is particularly useful for HAVING clauses where aggregate functions
 * need to be evaluated with special context (grouped rows) rather than
 * the standard single-row context.
 * 
 * @author JFR Query Plan Architecture
 * @since 2.0
 */
@FunctionalInterface
public interface ExpressionHandler {
    /**
     * Attempt to handle a specific expression with custom logic.
     * 
     * @param expression The expression to evaluate
     * @param table The table context
     * @param rowIndex The row index
     * @param evaluator The parent evaluator for delegating standard evaluation
     * @return The evaluated result, or null if this handler cannot process the expression
     */
    CellValue handleExpression(ExpressionNode expression, JfrTable table, int rowIndex, 
                              PlanExpressionEvaluator evaluator);
}
