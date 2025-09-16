package me.bechberger.jfr.extended.plan.evaluator;

import me.bechberger.jfr.extended.ast.ASTNodes.ExpressionNode;

/**
 * Base exception for expression evaluation errors.
 */
public class ExpressionEvaluationException extends RuntimeException {
    
    private final ExpressionNode errorNode;
    
    public ExpressionEvaluationException(String message) {
        super(message);
        this.errorNode = null;
    }
    
    public ExpressionEvaluationException(String message, ExpressionNode errorNode) {
        super(message);
        this.errorNode = errorNode;
    }
    
    public ExpressionEvaluationException(String message, Throwable cause) {
        super(message, cause);
        this.errorNode = null;
    }
    
    public ExpressionEvaluationException(String message, ExpressionNode errorNode, Throwable cause) {
        super(message, cause);
        this.errorNode = errorNode;
    }
    
    public ExpressionNode getErrorNode() {
        return errorNode;
    }
    
    @Override
    public String getMessage() {
        String baseMessage = super.getMessage();
        if (errorNode != null) {
            return baseMessage + " (at AST node: " + errorNode.getClass().getSimpleName() + ")";
        }
        return baseMessage;
    }
    
    /**
     * Exception thrown when a variable or column cannot be resolved.
     */
    public static class UndefinedIdentifierException extends ExpressionEvaluationException {
        private final String identifier;
        private final String[] availableOptions;
        
        public UndefinedIdentifierException(String identifier, String[] availableOptions) {
            super(buildMessage(identifier, availableOptions));
            this.identifier = identifier;
            this.availableOptions = availableOptions;
        }
        
        public UndefinedIdentifierException(String identifier, String[] availableOptions, ExpressionNode errorNode) {
            super(buildMessage(identifier, availableOptions), errorNode);
            this.identifier = identifier;
            this.availableOptions = availableOptions;
        }
        
        private static String buildMessage(String identifier, String[] availableOptions) {
            StringBuilder message = new StringBuilder("Undefined variable or column: '")
                .append(identifier).append("'");
            
            if (availableOptions != null && availableOptions.length > 0) {
                message.append(". Available options: ")
                    .append(String.join(", ", availableOptions));
            }
            
            return message.toString();
        }
        
        public String getIdentifier() {
            return identifier;
        }
        
        public String[] getAvailableOptions() {
            return availableOptions;
        }
    }
    
    /**
     * Exception thrown when an unsupported operation is attempted.
     */
    public static class UnsupportedOperationException extends ExpressionEvaluationException {
        private final String operation;
        private final String leftType;
        private final String rightType;
        
        public UnsupportedOperationException(String operation, String leftType, String rightType) {
            super("Cannot " + operation + " " + leftType + 
                  (rightType != null ? " and " + rightType : ""));
            this.operation = operation;
            this.leftType = leftType;
            this.rightType = rightType;
        }
        
        public UnsupportedOperationException(String operation, String leftType, String rightType, ExpressionNode errorNode) {
            super("Cannot " + operation + " " + leftType + 
                  (rightType != null ? " and " + rightType : ""), errorNode);
            this.operation = operation;
            this.leftType = leftType;
            this.rightType = rightType;
        }
        
        public UnsupportedOperationException(String operation, String type) {
            this(operation, type, null);
        }
        
        public String getOperation() {
            return operation;
        }
        
        public String getLeftType() {
            return leftType;
        }
        
        public String getRightType() {
            return rightType;
        }
    }
    
    /**
     * Exception thrown when a function cannot be found.
     */
    public static class UnknownFunctionException extends ExpressionEvaluationException {
        private final String functionName;
        
        public UnknownFunctionException(String functionName) {
            super("Unknown function: " + functionName);
            this.functionName = functionName;
        }
        
        public UnknownFunctionException(String functionName, ExpressionNode errorNode) {
            super("Unknown function: " + functionName, errorNode);
            this.functionName = functionName;
        }
        
        public String getFunctionName() {
            return functionName;
        }
    }
    
    /**
     * Exception for type mismatches in operations
     */
    public static class TypeMismatchException extends ExpressionEvaluationException {
        private final String operation;
        private final String leftType;
        private final String rightType;
        
        public TypeMismatchException(String message, ExpressionNode errorNode) {
            super(message, errorNode);
            this.operation = null;
            this.leftType = null;
            this.rightType = null;
        }
        
        public TypeMismatchException(String operation, String leftType, String rightType, ExpressionNode errorNode) {
            super("Cannot " + operation + " " + leftType + " and " + rightType, errorNode);
            this.operation = operation;
            this.leftType = leftType;
            this.rightType = rightType;
        }
        
        public String getOperation() { return operation; }
        public String getLeftType() { return leftType; }
        public String getRightType() { return rightType; }
    }
    
    /**
     * Exception thrown when a column cannot be found.
     */
    public static class ColumnNotFoundException extends ExpressionEvaluationException {
        private final String columnName;
        
        public ColumnNotFoundException(String columnName) {
            super("Column not found: " + columnName);
            this.columnName = columnName;
        }
        
        public String getColumnName() {
            return columnName;
        }
    }
    
    /**
     * Exception thrown when an expression type is not supported.
     */
    public static class UnsupportedExpressionTypeException extends ExpressionEvaluationException {
        private final String expressionType;
        
        public UnsupportedExpressionTypeException(String expressionType) {
            super("Unsupported expression type: " + expressionType);
            this.expressionType = expressionType;
        }
        
        public String getExpressionType() {
            return expressionType;
        }
    }
}
