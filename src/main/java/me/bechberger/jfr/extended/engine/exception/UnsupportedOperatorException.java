package me.bechberger.jfr.extended.engine.exception;

import me.bechberger.jfr.extended.ast.ASTNode;

/**
 * Exception thrown when an unsupported operator is used during query execution.
 * 
 * This exception provides specific information about the operator that failed,
 * including the operator type, operand types, and context about the operation.
 * 
 * @author Query Engine Team
 * @since 1.0
 */
public class UnsupportedOperatorException extends QueryExecutionException {
    
    private final String operator;
    private final String leftType;
    private final String rightType;
    private final String operatorCategory;
    
    /**
     * Creates a new UnsupportedOperatorException for binary operators.
     * 
     * @param operator The operator that is not supported
     * @param leftType Type of the left operand
     * @param rightType Type of the right operand
     * @param errorNode The AST node where the error occurred (can be null)
     */
    public UnsupportedOperatorException(String operator, String leftType, String rightType, ASTNode errorNode) {
        super(
            String.format("Unsupported operator '%s' for types %s and %s", operator, leftType, rightType),
            errorNode,
            buildBinaryContext(operator, leftType, rightType),
            buildBinaryHint(operator, leftType, rightType),
            null
        );
        this.operator = operator;
        this.leftType = leftType;
        this.rightType = rightType;
        this.operatorCategory = "binary";
    }
    
    /**
     * Creates a new UnsupportedOperatorException for unary operators.
     * 
     * @param operator The operator that is not supported
     * @param operandType Type of the operand
     * @param errorNode The AST node where the error occurred (can be null)
     */
    public UnsupportedOperatorException(String operator, String operandType, ASTNode errorNode) {
        super(
            String.format("Unsupported unary operator '%s' for type %s", operator, operandType),
            errorNode,
            buildUnaryContext(operator, operandType),
            buildUnaryHint(operator, operandType),
            null
        );
        this.operator = operator;
        this.leftType = operandType;
        this.rightType = null;
        this.operatorCategory = "unary";
    }
    
    /**
     * Creates a new UnsupportedOperatorException with a custom message.
     */
    private UnsupportedOperatorException(String message, String operator, String context, String hint, ASTNode errorNode) {
        super(message, errorNode, context, hint, null);
        this.operator = operator;
        this.leftType = null;
        this.rightType = null;
        this.operatorCategory = "unknown";
    }
    
    /**
     * Factory method for creating an exception with a custom message.
     */
    public static UnsupportedOperatorException withMessage(String message, String operator, ASTNode errorNode) {
        return new UnsupportedOperatorException(
            message,
            operator,
            String.format("Operator '%s' is not supported in this context", operator),
            "Check the documentation for supported operators and their compatible types.",
            errorNode
        );
    }
    
    private static String buildBinaryContext(String operator, String leftType, String rightType) {
        return String.format("Binary operator evaluation failed: %s %s %s is not supported", 
            leftType, operator, rightType);
    }
    
    private static String buildUnaryContext(String operator, String operandType) {
        return String.format("Unary operator evaluation failed: %s%s is not supported", 
            operator, operandType);
    }
    
    private static String buildBinaryHint(String operator, String leftType, String rightType) {
        switch (operator) {
            case "+", "-", "*", "/", "%":
                return "Arithmetic operators require numeric types. Use CAST or CONVERT functions to ensure both operands are numbers.";
            case "=", "!=", "<", ">", "<=", ">=":
                return "Comparison operators require compatible types. Consider using type conversion functions or check for null values.";
            case "AND", "OR":
                return "Logical operators require boolean values. Use comparison expressions or boolean functions.";
            case "||":
                return "String concatenation requires string types. Use CAST to convert values to strings first.";
            default:
                return String.format("The operator '%s' may not be supported for %s and %s types. Check the documentation for valid type combinations.", 
                    operator, leftType, rightType);
        }
    }
    
    private static String buildUnaryHint(String operator, String operandType) {
        switch (operator) {
            case "-":
                return "Numeric negation requires a numeric type. Use CAST to convert the value to a number.";
            case "NOT":
                return "Logical NOT requires a boolean value. Use comparison expressions or boolean functions.";
            default:
                return String.format("The unary operator '%s' may not be supported for %s type. Check the documentation for valid operators.", 
                    operator, operandType);
        }
    }
    
    // Getters
    public String getOperator() { return operator; }
    public String getLeftType() { return leftType; }
    public String getRightType() { return rightType; }
    public String getOperatorCategory() { return operatorCategory; }
}
