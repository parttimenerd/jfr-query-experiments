package me.bechberger.jfr.extended;

/**
 * Helper class for generating context-aware parser error suggestions.
 * 
 * <p>This class provides utility methods to create helpful suggestions for common
 * parsing errors, making the parser more user-friendly by suggesting likely fixes.</p>
 */
public class ParserSuggestionHelper {
    
    /**
     * Create a context-aware suggestion for double parentheses errors in function calls.
     * 
     * <p>Provides function-specific suggestions by removing the extra parentheses
     * and showing the correct syntax.</p>
     * 
     * @param functionName The name of the function with double parentheses
     * @param argumentText The text inside the double parentheses (can be extracted from tokens)
     * @return A helpful suggestion string
     */
    public static String createDoubleParenthesesSuggestion(String functionName, String argumentText) {
        if (functionName == null || functionName.trim().isEmpty()) {
            functionName = "function";
        }
        
        String normalizedFunctionName = functionName.toUpperCase();
        String argument = (argumentText == null || argumentText.trim().isEmpty()) ? "expression" : argumentText;
        
        // Provide function-specific suggestions
        return switch (normalizedFunctionName) {
            case "COUNT" -> {
                if (argumentText == null || argumentText.trim().isEmpty()) {
                    yield "Try: COUNT(*) instead of COUNT(())";
                } else {
                    yield "Try: COUNT(" + argumentText + ") instead of COUNT((" + argumentText + "))";
                }
            }
            case "SUM" -> "Try: SUM(" + argument + ") instead of SUM((" + argument + "))";
            case "AVG", "AVERAGE" -> "Try: AVG(" + argument + ") instead of AVG((" + argument + "))";
            case "MIN" -> "Try: MIN(" + argument + ") instead of MIN((" + argument + "))";
            case "MAX" -> "Try: MAX(" + argument + ") instead of MAX((" + argument + "))";
            case "P90", "P95", "P99", "P999" -> "Try: " + functionName + "(" + argument + ") instead of " + functionName + "((" + argument + "))";
            case "PERCENTILE" -> {
                if (argumentText == null || argumentText.trim().isEmpty()) {
                    yield "Try: PERCENTILE(90, duration) instead of PERCENTILE(())";
                } else {
                    yield "Try: PERCENTILE(" + argumentText + ") instead of PERCENTILE((" + argumentText + "))";
                }
            }
            default -> "Remove the extra parentheses: use " + functionName + "(" + argument + ") instead of " + functionName + "((" + argument + "))";
        };
    }
    
    /**
     * Create a suggestion for the COUNT function specifically.
     * This handles the most common case of COUNT((duration)) -> COUNT(duration).
     * 
     * @param argumentText The argument inside the double parentheses
     * @return A specific suggestion for COUNT function
     */
    public static String createCountFunctionSuggestion(String argumentText) {
        if (argumentText == null || argumentText.trim().isEmpty()) {
            return "Try: COUNT(*) instead of COUNT(())";
        }
        
        return "Try: COUNT(" + argumentText + ") instead of COUNT((" + argumentText + "))";
    }
    
    /**
     * Create a suggestion for unknown operators that might be typos.
     * 
     * @param unexpectedOperator The operator that caused the error
     * @param context Additional context about where the error occurred
     * @return A helpful suggestion
     */
    public static String createOperatorSuggestion(String unexpectedOperator, String context) {
        return switch (unexpectedOperator) {
            case "/" -> "Use '/' for division in expressions. Check if you meant to divide: " + context.replace("/", " / ");
            case "*" -> "Use '*' for multiplication in expressions. Check if you meant to multiply: " + context.replace("*", " * ");
            case "%" -> "Use '%' for modulo operations in expressions. Check if you meant modulo: " + context.replace("%", " % ");
            default -> "Check the operator syntax. Expected a valid operator or semicolon (;) to separate statements.";
        };
    }
    
    /**
     * Create a suggestion for malformed expressions with consecutive operators.
     * 
     * <p>Detects patterns like "5 * / 3" and suggests the corrected form "5 / 3"
     * by identifying and removing the likely incorrect operator.</p>
     * 
     * @param expression The malformed expression containing consecutive operators
     * @param errorToken The specific token that caused the parsing error
     * @return A "Did you mean" style suggestion with the corrected expression
     */
    public static String createMalformedExpressionSuggestion(String expression, String errorToken) {
        if (expression == null || expression.trim().isEmpty()) {
            return null;
        }
        
        // Clean up the expression for analysis
        String cleanExpression = expression.trim();
        
        // Pattern: number/identifier operator operator number/identifier
        // Examples: "5 * / 3", "duration + - 100", "x / * y"
        if (cleanExpression.matches(".*\\s*[+\\-*/]\\s*[+\\-*/]\\s*.*")) {
            return createConsecutiveOperatorSuggestion(cleanExpression, errorToken);
        }
        
        // Pattern: repeated operators like "/ /" or "* *"
        if (cleanExpression.matches(".*\\s*([+\\-*/])\\s*\\1\\s*.*")) {
            return createRepeatedOperatorSuggestion(cleanExpression);
        }
        
        return null;
    }
    
    /**
     * Create a suggestion for consecutive different operators.
     * 
     * @param expression The expression with consecutive operators
     * @param errorToken The token that caused the error
     * @return A suggestion string
     */
    private static String createConsecutiveOperatorSuggestion(String expression, String errorToken) {
        // Common patterns and their likely fixes
        if (expression.contains("* /")) {
            String fixed = expression.replaceFirst("\\*\\s*/", "/");
            return "Did you mean: " + fixed.trim() + "?";
        }
        if (expression.contains("/ *")) {
            String fixed = expression.replaceFirst("/\\s*\\*", "*");
            return "Did you mean: " + fixed.trim() + "?";
        }
        if (expression.contains("+ -")) {
            String fixed = expression.replaceFirst("\\+\\s*-", "-");
            return "Did you mean: " + fixed.trim() + "?";
        }
        if (expression.contains("- +")) {
            String fixed = expression.replaceFirst("-\\s*\\+", "+");
            return "Did you mean: " + fixed.trim() + "?";
        }
        if (expression.contains("* +")) {
            String fixed = expression.replaceFirst("\\*\\s*\\+", "+");
            return "Did you mean: " + fixed.trim() + "?";
        }
        if (expression.contains("/ +")) {
            String fixed = expression.replaceFirst("/\\s*\\+", "+");
            return "Did you mean: " + fixed.trim() + "?";
        }
        
        // Generic fix: remove the first operator in consecutive pairs
        String fixed = expression.replaceFirst("([+\\-*/])\\s*([+\\-*/])", "$2");
        return "Did you mean: " + fixed.trim() + "?";
    }
    
    /**
     * Create a suggestion for repeated operators like "/ /" or "* *".
     * 
     * @param expression The expression with repeated operators
     * @return A suggestion string
     */
    private static String createRepeatedOperatorSuggestion(String expression) {
        // Remove duplicate operators
        String fixed = expression.replaceAll("([+\\-*/])\\s*\\1", "$1");
        return "Did you mean: " + fixed.trim() + "?";
    }
    
    /**
     * Extract argument text from tokens between parentheses for suggestion purposes.
     * This is a simplified version that works with basic cases.
     * 
     * @param tokens The token stream positioned after the opening parenthesis
     * @return The argument text as a string, or empty string if no arguments
     */
    public static String extractArgumentText(TokenStream tokens) {
        if (tokens == null || tokens.isAtEnd()) {
            return "";
        }
        
        // For double parentheses case, we want to extract what's between the inner parentheses
        // This is a simple implementation - could be enhanced for complex expressions
        Token current = tokens.current();
        if (current != null && current.type() == TokenType.LPAREN) {
            // Skip the outer opening paren and get the next token
            tokens.advance();
            if (!tokens.isAtEnd() && tokens.current().type() == TokenType.IDENTIFIER) {
                return tokens.current().value();
            }
        } else if (current != null && current.type() == TokenType.IDENTIFIER) {
            return current.value();
        }
        
        return "";
    }
}
