package me.bechberger.jfr.extended.engine.exception;

import me.bechberger.jfr.extended.ast.ASTNode;
import me.bechberger.jfr.extended.table.CellValue;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Exception thrown when a function evaluation fails during query execution.
 * 
 * This exception provides detailed information about the function that failed,
 * the arguments that were passed, and the context in which the failure occurred.
 * 
 * @author Query Engine Team
 * @since 1.0
 */
public class FunctionEvaluationException extends QueryEvaluationException {
    
    private final String functionName;
    private final List<CellValue> arguments;
    private final FunctionContext functionContext;
    private final String expectedSignature;
    
    /**
     * Enumeration of different function evaluation contexts.
     */
    public enum FunctionContext {
        ROW_CONTEXT("row-level evaluation"),
        AGGREGATE_CONTEXT("aggregate evaluation"),
        GROUP_BY_CONTEXT("GROUP BY evaluation"),
        HAVING_CONTEXT("HAVING clause evaluation"),
        WHERE_CONTEXT("WHERE clause evaluation"),
        SELECT_CONTEXT("SELECT clause evaluation"),
        ORDER_BY_CONTEXT("ORDER BY evaluation");
        
        private final String description;
        
        FunctionContext(String description) {
            this.description = description;
        }
        
        public String getDescription() {
            return description;
        }
    }
    
    /**
     * Creates a new FunctionEvaluationException with detailed information.
     * 
     * @param functionName The name of the function that failed
     * @param arguments The arguments passed to the function
     * @param functionContext The context in which the function was evaluated
     * @param expectedSignature The expected function signature (can be null)
     * @param errorNode The AST node where the error occurred
     * @param cause The underlying cause of the error
     */
    public FunctionEvaluationException(String functionName, List<CellValue> arguments, 
                                     FunctionContext functionContext, String expectedSignature,
                                     ASTNode errorNode, Throwable cause) {
        super(
            buildEvaluationContext(functionName, functionContext),
            buildProblematicValue(arguments),
            EvaluationErrorType.UNSUPPORTED_OPERATION,
            errorNode,
            cause
        );
        this.functionName = functionName;
        this.arguments = arguments != null ? List.copyOf(arguments) : List.of();
        this.functionContext = functionContext;
        this.expectedSignature = expectedSignature;
    }
    
    /**
     * Creates a new FunctionEvaluationException for argument type mismatch.
     */
    public static FunctionEvaluationException forArgumentTypeMismatch(String functionName, 
                                                                     List<CellValue> arguments,
                                                                     String expectedSignature,
                                                                     ASTNode errorNode) {
        return new FunctionEvaluationException(
            functionName, arguments, FunctionContext.ROW_CONTEXT, 
            expectedSignature, errorNode, 
            new IllegalArgumentException("Argument type mismatch")
        );
    }
    
    /**
     * Creates a new FunctionEvaluationException for runtime errors.
     */
    public static FunctionEvaluationException forRuntimeError(String functionName,
                                                             List<CellValue> arguments,
                                                             FunctionContext context,
                                                             ASTNode errorNode,
                                                             Throwable cause) {
        return new FunctionEvaluationException(functionName, arguments, context, null, errorNode, cause);
    }
    
    /**
     * Builds the main error message.
     */
    private static String buildEvaluationContext(String functionName, FunctionContext context) {
        return String.format("function '%s' evaluation in %s", functionName, context.getDescription());
    }
    
    /**
     * Builds the problematic value representation.
     */
    private static Object buildProblematicValue(List<CellValue> arguments) {
        if (arguments == null || arguments.isEmpty()) {
            return "no arguments";
        }
        return arguments.stream()
            .map(arg -> arg.getType().toString())
            .collect(Collectors.joining(", "));
    }
    
    // Getters for accessing specific error information
    
    public String getFunctionName() {
        return functionName;
    }
    
    public List<CellValue> getArguments() {
        return arguments;
    }
    
    public FunctionContext getFunctionContext() {
        return functionContext;
    }
    
    public String getExpectedSignature() {
        return expectedSignature;
    }
    
    /**
     * Returns a string representation of the actual function signature.
     */
    public String getActualSignature() {
        String argString = arguments.stream()
            .map(arg -> arg.getType().toString())
            .collect(Collectors.joining(", "));
        return functionName + "(" + argString + ")";
    }
    
    /**
     * Returns true if this exception has information about expected signature.
     */
    public boolean hasExpectedSignature() {
        return expectedSignature != null && !expectedSignature.isEmpty();
    }
}
