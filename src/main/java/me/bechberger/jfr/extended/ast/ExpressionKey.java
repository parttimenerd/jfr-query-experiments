package me.bechberger.jfr.extended.ast;

import me.bechberger.jfr.extended.ast.ASTNodes.*;
import java.util.Objects;
import java.util.List;

/**
 * A wrapper for ExpressionNode that provides proper equals() and hashCode() implementation
 * while ignoring location information. This is used for caching and comparing expressions
 * based on their semantic content rather than their source location.
 */
public class ExpressionKey {
    
    private final ExpressionNode expression;
    private final int cachedHashCode;
    
    public ExpressionKey(ExpressionNode expression) {
        this.expression = expression;
        this.cachedHashCode = computeHashCode(expression);
    }
    
    public ExpressionNode getExpression() {
        return expression;
    }
    
    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        
        ExpressionKey that = (ExpressionKey) obj;
        return expressionsEqual(this.expression, that.expression);
    }
    
    @Override
    public int hashCode() {
        return cachedHashCode;
    }
    
    @Override
    public String toString() {
        return "ExpressionKey{" + expression + "}";
    }
    
    /**
     * Compare two expressions for semantic equality, ignoring location information.
     */
    private boolean expressionsEqual(ExpressionNode expr1, ExpressionNode expr2) {
        if (expr1 == expr2) return true;
        if (expr1 == null || expr2 == null) return false;
        if (expr1.getClass() != expr2.getClass()) return false;
        
        return switch (expr1) {
            case FunctionCallNode func1 when expr2 instanceof FunctionCallNode func2 -> 
                Objects.equals(func1.functionName(), func2.functionName()) &&
                func1.distinct() == func2.distinct() &&
                listsEqual(func1.arguments(), func2.arguments());
                
            case BinaryExpressionNode bin1 when expr2 instanceof BinaryExpressionNode bin2 ->
                bin1.operator() == bin2.operator() &&
                expressionsEqual(bin1.left(), bin2.left()) &&
                expressionsEqual(bin1.right(), bin2.right());
                
            case UnaryExpressionNode un1 when expr2 instanceof UnaryExpressionNode un2 ->
                un1.operator() == un2.operator() &&
                expressionsEqual(un1.operand(), un2.operand());
                
            case LiteralNode lit1 when expr2 instanceof LiteralNode lit2 ->
                Objects.equals(lit1.value(), lit2.value());
                
            case IdentifierNode id1 when expr2 instanceof IdentifierNode id2 ->
                Objects.equals(id1.name(), id2.name());
                
            case FieldAccessNode field1 when expr2 instanceof FieldAccessNode field2 ->
                Objects.equals(field1.field(), field2.field());
                
            case StarNode star1 when expr2 instanceof StarNode star2 ->
                true; // All star nodes are equal
                
            case CaseExpressionNode case1 when expr2 instanceof CaseExpressionNode case2 ->
                expressionsEqual(case1.expression(), case2.expression()) &&
                whenClausesEqual(case1.whenClauses(), case2.whenClauses()) &&
                expressionsEqual(case1.elseExpression(), case2.elseExpression());
                
            case ArrayLiteralNode arr1 when expr2 instanceof ArrayLiteralNode arr2 ->
                listsEqual(arr1.elements(), arr2.elements());
                
            case VariableAssignmentExpressionNode var1 when expr2 instanceof VariableAssignmentExpressionNode var2 ->
                Objects.equals(var1.variable(), var2.variable()) &&
                expressionsEqual(var1.value(), var2.value());
                
            case NestedQueryNode nested1 when expr2 instanceof NestedQueryNode nested2 ->
                // For nested queries, we'd need to implement QueryNode equality
                // For now, consider them equal only if they're the same object
                nested1 == nested2;
                
            case WhenClauseNode when1 when expr2 instanceof WhenClauseNode when2 ->
                expressionsEqual(when1.condition(), when2.condition()) &&
                expressionsEqual(when1.result(), when2.result());
                
            default -> false;
        };
    }
    
    /**
     * Check if two lists of expressions are equal.
     */
    private boolean listsEqual(List<ExpressionNode> list1, List<ExpressionNode> list2) {
        if (list1 == list2) return true;
        if (list1 == null || list2 == null) return false;
        if (list1.size() != list2.size()) return false;
        
        for (int i = 0; i < list1.size(); i++) {
            if (!expressionsEqual(list1.get(i), list2.get(i))) {
                return false;
            }
        }
        return true;
    }
    
    /**
     * Check if two lists of WHEN clauses are equal.
     */
    private boolean whenClausesEqual(List<WhenClauseNode> list1, List<WhenClauseNode> list2) {
        if (list1 == list2) return true;
        if (list1 == null || list2 == null) return false;
        if (list1.size() != list2.size()) return false;
        
        for (int i = 0; i < list1.size(); i++) {
            WhenClauseNode when1 = list1.get(i);
            WhenClauseNode when2 = list2.get(i);
            if (!expressionsEqual(when1.condition(), when2.condition()) ||
                !expressionsEqual(when1.result(), when2.result())) {
                return false;
            }
        }
        return true;
    }
    
    /**
     * Compute hash code for an expression, ignoring location information.
     */
    private int computeHashCode(ExpressionNode expr) {
        if (expr == null) return 0;
        
        return switch (expr) {
            case FunctionCallNode func -> 
                Objects.hash(func.getClass(), func.functionName(), func.distinct(), 
                           computeListHashCode(func.arguments()));
                           
            case BinaryExpressionNode bin ->
                Objects.hash(bin.getClass(), bin.operator(), 
                           computeHashCode(bin.left()), computeHashCode(bin.right()));
                           
            case UnaryExpressionNode un ->
                Objects.hash(un.getClass(), un.operator(), computeHashCode(un.operand()));
                
            case LiteralNode lit ->
                Objects.hash(lit.getClass(), lit.value());
                
            case IdentifierNode id ->
                Objects.hash(id.getClass(), id.name());
                
            case FieldAccessNode field ->
                Objects.hash(field.getClass(), field.field());
                
            case StarNode star ->
                Objects.hash(star.getClass());
                
            case CaseExpressionNode caseExpr ->
                Objects.hash(caseExpr.getClass(), 
                           computeHashCode(caseExpr.expression()),
                           computeWhenClausesHashCode(caseExpr.whenClauses()),
                           computeHashCode(caseExpr.elseExpression()));
                           
            case ArrayLiteralNode arr ->
                Objects.hash(arr.getClass(), computeListHashCode(arr.elements()));
                
            case VariableAssignmentExpressionNode var ->
                Objects.hash(var.getClass(), var.variable(), computeHashCode(var.value()));
                
            case NestedQueryNode nested ->
                // For nested queries, use object identity for now
                Objects.hash(nested.getClass(), System.identityHashCode(nested));
                
            case WhenClauseNode when ->
                Objects.hash(when.getClass(), 
                           computeHashCode(when.condition()), 
                           computeHashCode(when.result()));
                           
            default ->
                Objects.hash(expr.getClass(), System.identityHashCode(expr));
        };
    }
    
    /**
     * Compute hash code for a list of expressions.
     */
    private int computeListHashCode(List<ExpressionNode> list) {
        if (list == null) return 0;
        
        int hash = 1;
        for (ExpressionNode expr : list) {
            hash = 31 * hash + computeHashCode(expr);
        }
        return hash;
    }
    
    /**
     * Compute hash code for a list of WHEN clauses.
     */
    private int computeWhenClausesHashCode(List<WhenClauseNode> list) {
        if (list == null) return 0;
        
        int hash = 1;
        for (WhenClauseNode when : list) {
            hash = 31 * hash + Objects.hash(computeHashCode(when.condition()), 
                                          computeHashCode(when.result()));
        }
        return hash;
    }
    
    /**
     * Get a safe column name based on this expression key.
     * This creates a name that can be used as a hidden column identifier.
     */
    public String toColumnName() {
        // Use a prefix to identify aggregate columns and make the name more readable
        return "__agg_" + Math.abs(cachedHashCode);
    }
}
