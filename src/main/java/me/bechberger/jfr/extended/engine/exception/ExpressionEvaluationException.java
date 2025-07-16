package me.bechberger.jfr.extended.engine.exception;

import me.bechberger.jfr.extended.ast.ASTNode;
import me.bechberger.jfr.extended.ast.ASTNodes.BinaryOperator;
import me.bechberger.jfr.extended.ast.ASTNodes.UnaryOperator;
import me.bechberger.jfr.extended.table.CellValue;

/**
 * Exception thrown when expression evaluation fails during query execution.
 * 
 * This exception provides detailed information about the expression that failed,
 * the operands involved, and the specific type of evaluation error.
 * 
 * @author Query Engine Team
 * @since 1.0
 */
public class ExpressionEvaluationException extends QueryEvaluationException {
    
    private final String expression;
    private final ExpressionType expressionType;
    private final CellValue leftOperand;
    private final CellValue rightOperand;
    private final BinaryOperator binaryOperator;
    private final UnaryOperator unaryOperator;
    
    // Enhanced node information
    private final String nodeLocation;
    private final String nodeType;
    private final String operationContext;
    private final String sourceCode;
    
    /**
     * Enumeration of different expression types.
     */
    public enum ExpressionType {
        BINARY_OPERATION("binary operation"),
        UNARY_OPERATION("unary operation"),
        FIELD_ACCESS("field access"),
        ARRAY_ACCESS("array access"),
        CASE_EXPRESSION("CASE expression"),
        CAST_EXPRESSION("type cast"),
        LITERAL_EVALUATION("literal evaluation");
        
        private final String description;
        
        ExpressionType(String description) {
            this.description = description;
        }
        
        public String getDescription() {
            return description;
        }
    }
    
    /**
     * Creates a new ExpressionEvaluationException for binary operations.
     */
    public ExpressionEvaluationException(String expression, BinaryOperator operator,
                                       CellValue leftOperand, CellValue rightOperand,
                                       ASTNode errorNode, Throwable cause) {
        super(
            buildEvaluationContext(expression, ExpressionType.BINARY_OPERATION),
            buildProblematicValue(operator, leftOperand, rightOperand),
            determineErrorType(cause),
            errorNode,
            cause
        );
        this.expression = expression;
        this.expressionType = ExpressionType.BINARY_OPERATION;
        this.leftOperand = leftOperand;
        this.rightOperand = rightOperand;
        this.binaryOperator = operator;
        this.unaryOperator = null;
        
        // Initialize enhanced node information
        this.nodeLocation = extractNodeLocation(errorNode);
        this.nodeType = extractNodeType(errorNode);
        this.operationContext = "binary operation with operator " + operator;
        this.sourceCode = extractSourceCode(errorNode);
    }
    
    /**
     * Creates a new ExpressionEvaluationException for unary operations.
     */
    public ExpressionEvaluationException(String expression, UnaryOperator operator,
                                       CellValue operand, ASTNode errorNode, Throwable cause) {
        super(
            buildEvaluationContext(expression, ExpressionType.UNARY_OPERATION),
            buildProblematicValue(operator, operand),
            determineErrorType(cause),
            errorNode,
            cause
        );
        this.expression = expression;
        this.expressionType = ExpressionType.UNARY_OPERATION;
        this.leftOperand = operand;
        this.rightOperand = null;
        this.binaryOperator = null;
        this.unaryOperator = operator;
        
        // Initialize enhanced node information
        this.nodeLocation = extractNodeLocation(errorNode);
        this.nodeType = extractNodeType(errorNode);
        this.operationContext = "unary operation with operator " + operator;
        this.sourceCode = extractSourceCode(errorNode);
    }
    
    /**
     * Creates a new ExpressionEvaluationException for general expression errors.
     */
    public ExpressionEvaluationException(String expression, ExpressionType type,
                                       Object problematicValue, ASTNode errorNode, Throwable cause) {
        super(
            buildEvaluationContext(expression, type),
            problematicValue,
            determineErrorType(cause),
            errorNode,
            cause
        );
        this.expression = expression;
        this.expressionType = type;
        this.leftOperand = null;
        this.rightOperand = null;
        this.binaryOperator = null;
        this.unaryOperator = null;
        
        // Initialize enhanced node information
        this.nodeLocation = extractNodeLocation(errorNode);
        this.nodeType = extractNodeType(errorNode);
        this.operationContext = type.getDescription();
        this.sourceCode = extractSourceCode(errorNode);
    }
    
    /**
     * Creates an ExpressionEvaluationException for arithmetic errors.
     */
    public static ExpressionEvaluationException forArithmeticError(String expression,
                                                                 BinaryOperator operator,
                                                                 CellValue left, CellValue right,
                                                                 ASTNode errorNode, Throwable cause) {
        return new ExpressionEvaluationException(expression, operator, left, right, errorNode, cause);
    }
    
    /**
     * Creates an ExpressionEvaluationException for type conversion errors with operation context.
     */
    public static ExpressionEvaluationException forTypeConversionError(String expression,
                                                                     CellValue value,
                                                                     String targetType,
                                                                     ASTNode errorNode,
                                                                     String operationContext) {
        // Enhanced error message with operation context and detailed conversion info
        String actualType = value != null ? value.getType().toString() : "null";
        String actualValue = value != null ? formatValueForError(value) : "null";
        
        String detailedMessage = String.format(
            "Invalid type conversion during %s: %s\n" +
            "  Conversion failed: %s (%s) -> %s\n" +
            "  Actual value: %s\n" +
            "  Context: %s\n" +
            "  Node location: %s\n" +
            "  Source code: %s\n" +
            "  Suggestion: Ensure data types are compatible for conversion. Use explicit conversion functions where needed.",
            operationContext,
            expression,
            actualType,
            actualValue,
            targetType,
            actualValue,
            expression,
            extractNodeLocation(errorNode),
            extractSourceCode(errorNode)
        );
        
        return new ExpressionEvaluationException(
            expression, 
            ExpressionType.CAST_EXPRESSION,
            detailedMessage,
            errorNode,
            new ClassCastException("Type conversion failed during " + operationContext + ": " + actualType + " -> " + targetType)
        );
    }

    /**
     * Creates an ExpressionEvaluationException for type conversion errors.
     */
    public static ExpressionEvaluationException forTypeConversionError(String expression,
                                                                     CellValue value,
                                                                     String targetType,
                                                                     ASTNode errorNode) {
        // Enhanced error message with more context about the conversion failure
        String actualType = value != null ? value.getType().toString() : "null";
        String actualValue = value != null ? formatValueForError(value) : "null";
        
        String detailedMessage = String.format(
            "Invalid type conversion during type cast: %s\n" +
            "  Conversion failed: %s (%s) -> %s\n" +
            "  Actual value: %s\n" +
            "  Context: %s\n" +
            "  Node location: %s\n" +
            "  Source code: %s\n" +
            "  Suggestion: Ensure data types are compatible for conversion. Use explicit conversion functions where needed.",
            expression,
            actualType,
            actualValue,
            targetType,
            actualValue,
            expression,
            extractNodeLocation(errorNode),
            extractSourceCode(errorNode)
        );
        
        return new ExpressionEvaluationException(
            expression, 
            ExpressionType.CAST_EXPRESSION,
            detailedMessage,
            errorNode,
            new ClassCastException("Type conversion failed: " + actualType + " -> " + targetType)
        );
    }
    
    /**
     * Format a CellValue for error messages, truncating long values.
     */
    private static String formatValueForError(CellValue value) {
        if (value == null) {
            return "null";
        }
        
        String valueStr = value.toString();
        
        // Truncate very long values for readability
        if (valueStr.length() > 100) {
            return valueStr.substring(0, 100) + "... (truncated)";
        }
        
        return valueStr;
    }
    
    /**
     * Creates an ExpressionEvaluationException for field access errors.
     */
    public static ExpressionEvaluationException forFieldAccessError(String fieldName,
                                                                   String[] availableFields,
                                                                   ASTNode errorNode) {
        return new ExpressionEvaluationException(
            "field access: " + fieldName,
            ExpressionType.FIELD_ACCESS,
            "Available fields: " + String.join(", ", availableFields),
            errorNode,
            new NoSuchFieldException("Field not found: " + fieldName)
        );
    }
    
    /**
     * Builds the evaluation context string.
     */
    private static String buildEvaluationContext(String expression, ExpressionType type) {
        return String.format("%s: %s", type.getDescription(), expression);
    }
    
    /**
     * Builds the problematic value for binary operations.
     */
    private static String buildProblematicValue(BinaryOperator operator, CellValue left, CellValue right) {
        return String.format("%s %s %s", 
            left != null ? left.getType() : "null",
            operator,
            right != null ? right.getType() : "null");
    }
    
    /**
     * Builds the problematic value for unary operations.
     */
    private static String buildProblematicValue(UnaryOperator operator, CellValue operand) {
        return String.format("%s %s", operator, operand != null ? operand.getType() : "null");
    }
    
    /**
     * Determines the appropriate error type based on the cause.
     */
    private static EvaluationErrorType determineErrorType(Throwable cause) {
        if (cause instanceof ArithmeticException) {
            return EvaluationErrorType.DIVISION_BY_ZERO;
        } else if (cause instanceof NullPointerException) {
            return EvaluationErrorType.NULL_POINTER;
        } else if (cause instanceof ClassCastException) {
            return EvaluationErrorType.INVALID_CONVERSION;
        } else if (cause instanceof IndexOutOfBoundsException) {
            return EvaluationErrorType.OUT_OF_BOUNDS;
        }
        return EvaluationErrorType.UNSUPPORTED_OPERATION;
    }
    
    // Getters for accessing specific error information
    
    public String getExpression() {
        return expression;
    }
    
    public ExpressionType getExpressionType() {
        return expressionType;
    }
    
    public CellValue getLeftOperand() {
        return leftOperand;
    }
    
    public CellValue getRightOperand() {
        return rightOperand;
    }
    
    public BinaryOperator getBinaryOperator() {
        return binaryOperator;
    }
    
    public UnaryOperator getUnaryOperator() {
        return unaryOperator;
    }
    
    /**
     * Returns true if this is a binary operation error.
     */
    public boolean isBinaryOperation() {
        return expressionType == ExpressionType.BINARY_OPERATION;
    }
    
    /**
     * Returns true if this is a unary operation error.
     */
    public boolean isUnaryOperation() {
        return expressionType == ExpressionType.UNARY_OPERATION;
    }
    
    /**
     * Returns a detailed description of the operation that failed.
     */
    public String getOperationDescription() {
        if (isBinaryOperation() && binaryOperator != null) {
            return String.format("%s %s %s", 
                leftOperand != null ? leftOperand.getType() : "null",
                binaryOperator,
                rightOperand != null ? rightOperand.getType() : "null");
        } else if (isUnaryOperation() && unaryOperator != null) {
            return String.format("%s %s", unaryOperator, leftOperand != null ? leftOperand.getType() : "null");
        }
        return expression;
    }
    
    // ===== ENHANCED NODE INFORMATION ACCESSORS =====
    
    /**
     * Returns the node location information if available.
     */
    public String getNodeLocation() {
        return nodeLocation;
    }
    
    /**
     * Returns the node type information.
     */
    public String getNodeType() {
        return nodeType;
    }
    
    /**
     * Returns the operation context string.
     */
    public String getOperationContext() {
        return operationContext;
    }
    
    /**
     * Returns the source code fragment if available.
     */
    public String getSourceCode() {
        return sourceCode;
    }
    
    // ===== PRIVATE HELPER METHODS FOR NODE INFORMATION EXTRACTION =====
    
    /**
     * Extract location information from an AST node.
     */
    private static String extractNodeLocation(ASTNode node) {
        if (node == null) {
            return "unknown location";
        }
        
        try {
            // Get location information from the node
            int line = node.getLine();
            int column = node.getColumn();
            return String.format("line %d, column %d", line, column);
        } catch (Exception e) {
            // Ignore exceptions when extracting location
            return "location extraction failed: " + e.getMessage();
        }
    }
    
    /**
     * Extract node type information from an AST node.
     */
    private static String extractNodeType(ASTNode node) {
        if (node == null) {
            return "unknown node type";
        }
        
        return node.getClass().getSimpleName();
    }
    
    /**
     * Extract source code fragment from an AST node.
     */
    private static String extractSourceCode(ASTNode node) {
        if (node == null) {
            return "source code not available";
        }
        
        try {
            // Try to get formatted source code representation
            return node.format();
        } catch (Exception e) {
            // Fallback to toString if format() fails
            try {
                return node.toString();
            } catch (Exception e2) {
                return "source code extraction failed: " + e.getMessage();
            }
        }
    }
}
