package me.bechberger.jfr.extended.plan;

import me.bechberger.jfr.extended.ast.ASTNodes.*;
import me.bechberger.jfr.extended.plan.plans.StreamingQueryPlan;
import me.bechberger.jfr.extended.table.CellValue;
import me.bechberger.jfr.extended.plan.exception.ErrorMessageEnhancer;
import me.bechberger.jfr.extended.plan.exception.StatementExecutionException;
import me.bechberger.jfr.extended.plan.exception.VariableEvaluationException;
import me.bechberger.jfr.extended.plan.exception.ExpressionEvaluationException;

/**
 * Visitor for handling different statement types in multi-statement execution.
 * 
 * This visitor converts different types of statements into appropriate actions:
 * - QueryNode: Execute as streaming query plan
 * - AssignmentNode: Create lazy variable assignment
 * - ViewDefinitionNode: Create lazy variable assignment (views are just variables)
 * - Other statements: Execute as streaming plans
 * 
 * @author JFR Query Plan Architecture
 * @since 2.0
 */
public class StatementExecutionVisitor {
    
    private final AstToPlanConverter planConverter;
    private final QueryExecutionContext context;
    
    public StatementExecutionVisitor(AstToPlanConverter planConverter, QueryExecutionContext context) {
        this.planConverter = planConverter;
        this.context = context;
    }
    
    /**
     * Execute a statement and return the result.
     */
    public QueryResult executeStatement(StatementNode statement) throws PlanExecutionException {
        try {
            if (statement instanceof QueryNode queryNode) {
                return executeQuery(queryNode);
                
            } else if (statement instanceof AssignmentNode assignmentNode) {
                return executeAssignment(assignmentNode);
                
            } else if (statement instanceof GlobalVariableAssignmentNode globalVarNode) {
                return executeGlobalVariableAssignment(globalVarNode);
                
            } else if (statement instanceof ViewDefinitionNode viewDefNode) {
                return executeViewDefinition(viewDefNode);
                
            } else {
                // Handle other statement types by converting to streaming plan
                return executeGenericStatement(statement);
            }
        } catch (Exception e) {
            // Create enhanced error message with statement context
            String enhancedMessage = ErrorMessageEnhancer.createConciseMessage(
                "Statement execution failed", e);
            return QueryResult.failure(new StatementExecutionException(enhancedMessage, e));
        }
    }
    
    /**
     * Execute a regular query.
     */
    private QueryResult executeQuery(QueryNode queryNode) throws PlanExecutionException {
        StreamingQueryPlan plan = planConverter.convertToPlan(queryNode, context);
        return plan.execute(context);
    }
    
    /**
     * Execute a variable assignment by creating a lazy query.
     */
    private QueryResult executeAssignment(AssignmentNode assignmentNode) {
        // Create lazy query for the assigned expression
        LazyStreamingQuery lazyQuery = new LazyStreamingQuery(assignmentNode.query(), planConverter, context);
        
        // Store ONLY as lazy variable - this will be properly evaluated when accessed
        context.setLazyVariable(assignmentNode.variable(), lazyQuery);
        
        // Return success result for assignment statements
        return QueryResult.success(null);
    }
    
    /**
     * Execute a global variable assignment by evaluating the expression immediately.
     */
    private QueryResult executeGlobalVariableAssignment(GlobalVariableAssignmentNode globalVarNode) {
        try {
            // For simple expressions, use direct evaluation for efficiency
            if (isSimpleExpression(globalVarNode.expression())) {
                CellValue value = evaluateExpressionEagerly(globalVarNode.expression(), context);
                context.setVariable(globalVarNode.variable(), value);
                return QueryResult.success(null);
            } else {
                // For complex expressions (with subqueries), use the proper plan execution
                StreamingQueryPlan plan = planConverter.convertStatementToPlan(globalVarNode);
                return plan.execute(context);
            }
        } catch (Exception e) {
            return QueryResult.failure(new PlanExecutionException("Failed to evaluate global variable: " + globalVarNode.variable(), null, e));
        }
    }
    
    /**
     * Execute a view definition by treating it as a variable assignment.
     * Views are just another way of assigning variables.
     */
    private QueryResult executeViewDefinition(ViewDefinitionNode viewDefNode) {
        // Create lazy query for the view
        LazyStreamingQuery lazyQuery = new LazyStreamingQuery(viewDefNode.query(), planConverter, context);
        
        // Store as lazy variable with the view name
        context.setLazyVariable(viewDefNode.viewName(), lazyQuery);
        
        // Also store in regular variables
        context.setVariable(viewDefNode.viewName(), lazyQuery);
        
        // Return success result for view definitions
        return QueryResult.success(null);
    }
    
    /**
     * Execute other statement types by converting to streaming plan.
     */
    private QueryResult executeGenericStatement(StatementNode statement) throws PlanExecutionException {
        StreamingQueryPlan plan = planConverter.convertStatementToPlan(statement);
        return plan.execute(context);
    }
    
    /**
     * Check if an expression is simple enough for direct evaluation.
     * Simple expressions include literals, identifiers, and basic binary operations.
     * Complex expressions include function calls and subqueries.
     */
    private boolean isSimpleExpression(ExpressionNode expression) {
        if (expression instanceof LiteralNode) {
            return true;
        } else if (expression instanceof IdentifierNode) {
            return true;
        } else if (expression instanceof BinaryExpressionNode binaryExpr) {
            return isSimpleExpression(binaryExpr.left()) && isSimpleExpression(binaryExpr.right());
        } else {
            // Function calls, subqueries, etc. are complex
            return false;
        }
    }
    
    /**
     * Eagerly evaluate an expression by using a simplified evaluation approach.
     * For simple expressions like literals and basic operations, this handles them directly.
     * For more complex expressions, this could be enhanced to use the full query engine.
     */
    private CellValue evaluateExpressionEagerly(ExpressionNode expression, QueryExecutionContext context) throws Exception {
        if (expression instanceof LiteralNode literalNode) {
            return literalNode.value();
        } else if (expression instanceof IdentifierNode identifierNode) {
            // Look up variable value
            Object value = context.getVariable(identifierNode.name());
            if (value instanceof CellValue cellValue) {
                return cellValue;
            } else if (value != null) {
                return CellValue.of(value);
            } else {
                throw new VariableEvaluationException("Unknown variable: " + identifierNode.name());
            }
        } else if (expression instanceof BinaryExpressionNode binaryExpr) {
            // Handle basic binary operations
            CellValue left = evaluateExpressionEagerly(binaryExpr.left(), context);
            CellValue right = evaluateExpressionEagerly(binaryExpr.right(), context);
            
            return switch (binaryExpr.operator()) {
                case ADD -> CellValue.of(left.extractNumericValue() + right.extractNumericValue());
                case SUBTRACT -> CellValue.of(left.extractNumericValue() - right.extractNumericValue());
                case MULTIPLY -> CellValue.of(left.extractNumericValue() * right.extractNumericValue());
                case DIVIDE -> CellValue.of(left.extractNumericValue() / right.extractNumericValue());
                default -> throw new ExpressionEvaluationException("Unsupported binary operator in global variable assignment: " + binaryExpr.operator());
            };
        } else {
            throw new ExpressionEvaluationException("Unsupported expression type in global variable assignment: " + expression.getClass().getSimpleName() + ". Complex expressions not yet supported for global variables.");
        }
    }
}
