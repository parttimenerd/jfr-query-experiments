package me.bechberger.jfr.extended;

import me.bechberger.jfr.extended.ast.Location;
import me.bechberger.jfr.extended.table.CellValue;
import me.bechberger.jfr.extended.ast.ASTNodes.*;

/**
 * Utility class for handling repetitive error patterns in the parser.
 * 
 * <p>This class provides a common abstraction for the try/catch/recover pattern
 * that appears frequently throughout the parser. It helps eliminate code duplication
 * and ensures consistent error handling behavior.
 */
public class ParserErrorRecovery {
    
    /**
     * Functional interface for parser operations that may throw ParserException.
     */
    @FunctionalInterface
    public interface ThrowingSupplier<T> {
        T get() throws ParserException;
    }
    
    private final ParserErrorHandler errorHandler;
    
    public ParserErrorRecovery(ParserErrorHandler errorHandler) {
        this.errorHandler = errorHandler;
    }
    
    /**
     * Parse with error recovery, providing a fallback value if parsing fails.
     * 
     * @param <T> the type of value being parsed
     * @param parser the parsing logic that may throw ParserException
     * @param fallbackValue the value to return if parsing fails
     * @param errorMessage the error message to use if parsing fails
     * @param errorToken the token where the error occurred
     * @return the parsed value or fallback value
     */
    public <T> T parseWithFallback(ThrowingSupplier<T> parser, 
                                   T fallbackValue, 
                                   String errorMessage, 
                                   Token errorToken) {
        try {
            return parser.get();
        } catch (ParserException e) {
            ParserErrorHandler.ParserError error = errorHandler.createExpressionError(
                errorToken, errorMessage + ": " + e.getMessage());
            errorHandler.addError(error);
            return fallbackValue;
        }
    }
    
    /**
     * Parse with error recovery, providing a synthetic expression if parsing fails.
     * 
     * @param parser the parsing logic that may throw ParserException
     * @param errorMessage the error message to use if parsing fails
     * @param errorToken the token where the error occurred
     * @return the parsed expression or a synthetic boolean literal
     */
    public ExpressionNode parseExpressionWithSyntheticFallback(ThrowingSupplier<ExpressionNode> parser,
                                                               String errorMessage,
                                                               Token errorToken) {
        return parseWithFallback(
            parser,
            new LiteralNode(new CellValue.BooleanValue(true), new Location(errorToken.line(), errorToken.column())),
            errorMessage,
            errorToken
        );
    }
    
    /**
     * Parse with error recovery, providing a synthetic condition if parsing fails.
     * 
     * @param parser the parsing logic that may throw ParserException
     * @param errorMessage the error message to use if parsing fails
     * @param errorToken the token where the error occurred
     * @return the parsed condition or a synthetic true condition
     */
    public ConditionNode parseConditionWithSyntheticFallback(ThrowingSupplier<ConditionNode> parser,
                                                             String errorMessage,
                                                             Token errorToken) {
        return parseWithFallback(
            parser,
            new ExpressionConditionNode(
                new LiteralNode(new CellValue.BooleanValue(true), new Location(errorToken.line(), errorToken.column())),
                new Location(errorToken.line(), errorToken.column())
            ),
            errorMessage,
            errorToken
        );
    }
    
    /**
     * Parse a list with error recovery, adding successful items and continuing on errors.
     * 
     * @param <T> the type of items being parsed
     * @param items the list to add items to
     * @param parser the parsing logic for individual items
     * @param errorMessage the error message to use if parsing fails
     * @param errorToken the token where the error occurred
     * @param separatorSupplier optional logic to handle separators between items
     */
    public <T> void parseListWithRecovery(java.util.List<T> items,
                                          ThrowingSupplier<T> parser,
                                          String errorMessage,
                                          Token errorToken,
                                          Runnable separatorSupplier) {
        try {
            T item = parser.get();
            items.add(item);
            if (separatorSupplier != null) {
                separatorSupplier.run();
            }
        } catch (ParserException e) {
            ParserErrorHandler.ParserError error = errorHandler.createExpressionError(
                errorToken, errorMessage + ": " + e.getMessage());
            errorHandler.addError(error);
            // Continue parsing without adding this item
            if (separatorSupplier != null) {
                try {
                    separatorSupplier.run();
                } catch (Exception ignored) {
                    // Ignore separator errors during recovery
                }
            }
        }
    }
    
    /**
     * Parse with error recovery and custom recovery logic.
     * 
     * @param <T> the type of value being parsed
     * @param parser the parsing logic that may throw ParserException
     * @param errorMessage the error message to use if parsing fails
     * @param errorToken the token where the error occurred
     * @param recoveryAction custom recovery logic to execute on error
     * @return the parsed value or null if parsing failed
     */
    public <T> T parseWithCustomRecovery(ThrowingSupplier<T> parser,
                                         String errorMessage,
                                         Token errorToken,
                                         Runnable recoveryAction) {
        try {
            return parser.get();
        } catch (ParserException e) {
            ParserErrorHandler.ParserError error = errorHandler.createExpressionError(
                errorToken, errorMessage + ": " + e.getMessage());
            errorHandler.addError(error);
            if (recoveryAction != null) {
                recoveryAction.run();
            }
            return null;
        }
    }
}
