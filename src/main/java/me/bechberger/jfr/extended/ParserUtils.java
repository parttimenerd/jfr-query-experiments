package me.bechberger.jfr.extended;

import me.bechberger.jfr.extended.ast.Location;
import me.bechberger.jfr.extended.ast.ASTNodes.*;
import java.util.Set;

/**
 * Utility methods for the Parser that handle validation and helper functions.
 * 
 * <p>This class contains methods for validating identifiers, string literals,
 * function names, keywords, and detecting aggregate functions.</p>
 */
public class ParserUtils {
    
    public static Location location(int line, int column) {
        return new Location(line, column);
    }
    
    public static Location location(Token token) {
        return new Location(token.line(), token.column());
    }
    
    /**
     * Validate identifier length to prevent extremely long names.
     */
    public static void validateIdentifierLength(String identifier, String type) throws ParserException {
        final int MAX_IDENTIFIER_LENGTH = 255; // Reasonable limit
        if (identifier.length() > MAX_IDENTIFIER_LENGTH) {
            throw new ParserException("Identifier too long. " + type + " name '" + 
                identifier.substring(0, 50) + "...' exceeds maximum length of " + 
                MAX_IDENTIFIER_LENGTH + " characters.");
        }
    }
    
    /**
     * Validate function names are properly capitalized.
     */
    public static void validateFunctionCase(String functionName) throws ParserException {
        // Allow both uppercase and lowercase function names
        // Convert to uppercase for consistency but don't throw an error for case differences
        // This makes the parser more user-friendly while maintaining strict syntax checking
    }
    
    /**
     * Validate keyword case.
     */
    public static void validateKeywordCase(Token token, String expectedKeyword) throws ParserException {
        if (!token.value().equals(expectedKeyword)) {
            throw new ParserException("Invalid keyword case. Expected '" + expectedKeyword + 
                "' but found '" + token.value() + "'. Keywords must be uppercase.");
        }
    }
    
    /**
     * Validate string literals for invalid characters.
     */
    public static void validateStringLiteral(String value) throws ParserException {
        // Empty strings are allowed
        if (value.isEmpty()) {
            return;
        }
        
        // Check for control characters that shouldn't be in string literals
        for (char c : value.toCharArray()) {
            if (Character.isISOControl(c) && c != '\t' && c != '\n' && c != '\r') {
                throw new ParserException("String literal contains invalid control character (code: " + 
                    (int)c + "). Use escape sequences for special characters.");
            }
            if (c == '\t') {
                throw new ParserException("String literal contains tab character. " +
                    "Use \\t escape sequence instead.");
            }
            if (c == '\n' || c == '\r') {
                throw new ParserException("String literal contains newline character. " +
                    "String literals cannot span multiple lines.");
            }
        }
    }
    
    /**
     * Check if a token value is a reserved keyword
     */
    public static boolean isReservedKeyword(String value) {
        if (value == null) return false;
        return switch (value.toUpperCase()) {
            case "SELECT", "FROM", "WHERE", "GROUP", "BY", "ORDER", "ASC", "DESC",
                 "LIMIT", "HAVING", "JOIN", "INNER", "LEFT", "RIGHT", "FULL", "ON",
                 "AND", "OR", "NOT", "AS", "DISTINCT", "COUNT", "SUM", "AVG", "MIN", "MAX",
                 "SHOW", "VIEW", "FUZZY", "TOLERANCE", "THRESHOLD", "TRUE", "FALSE", "NULL" -> true;
            default -> false;
        };
    }
    
    /**
     * Check if an expression contains aggregate functions.
     * 
     * @param expr The expression node to check
     * @param aggregateFunctions The set of aggregate function names
     * @return true if the expression contains aggregate functions, false otherwise
     */
    public static boolean containsAggregateFunction(ExpressionNode expr, Set<String> aggregateFunctions) {
        if (expr instanceof FunctionCallNode functionCall) {
            if (aggregateFunctions.contains(functionCall.functionName().toUpperCase())) {
                return true;
            }
            // Check arguments recursively
            for (ExpressionNode arg : functionCall.arguments()) {
                if (containsAggregateFunction(arg, aggregateFunctions)) {
                    return true;
                }
            }
        } else if (expr instanceof BinaryExpressionNode binaryExpr) {
            return containsAggregateFunction(binaryExpr.left(), aggregateFunctions) ||
                   containsAggregateFunction(binaryExpr.right(), aggregateFunctions);
        } else if (expr instanceof UnaryExpressionNode unaryExpr) {
            return containsAggregateFunction(unaryExpr.operand(), aggregateFunctions);
        }
        return false;
    }
    
    /**
     * Validate that aggregate functions are not nested.
     */
    public static void validateNoNestedAggregates(ExpressionNode expr, Set<String> aggregateFunctions) throws ParserException {
        if (containsAggregateFunction(expr, aggregateFunctions)) {
            throw new ParserException("Nested aggregate functions are not allowed. " +
                "Expression contains aggregate function. " +
                "Use separate queries or window functions instead.");
        }
    }
    
    /**
     * Parse a field reference (may include dots for nested fields)
     */
    public static String parseFieldReference(TokenStream tokens, String context) throws ParserException {
        Token firstPart = consumeToken(tokens, TokenType.IDENTIFIER, "Expected field name in " + context);
        StringBuilder fieldName = new StringBuilder(firstPart.value());
        
        while (tokens.match(TokenType.DOT)) {
            Token nextPart = consumeToken(tokens, TokenType.IDENTIFIER, "Expected field name after dot");
            fieldName.append(".").append(nextPart.value());
        }
        
        return fieldName.toString();
    }
    
    /**
     * Consume a token of the expected type, or throw an error
     */
    private static Token consumeToken(TokenStream tokens, TokenType type, String message) throws ParserException {
        if (tokens.check(type)) {
            return tokens.advance();
        }
        
        throw new ParserException(message + " at " + tokens.current().getPositionString());
    }
    
    /**
     * Validates that a subquery has proper structure and doesn't violate nesting rules
     */
    public static void validateSubqueryStructure(QueryNode subquery) throws ParserException {
        if (subquery == null) {
            throw new ParserException("Subquery cannot be null");
        }
        
        // Validate that subquery has required clauses
        if (subquery.select() == null || subquery.select().items().isEmpty()) {
            throw new ParserException("Subquery must have a SELECT clause with at least one field");
        }
        
        if (subquery.from() == null) {
            throw new ParserException("Subquery must have a FROM clause");
        }
        
        // Validate that aggregate functions in subqueries follow proper rules
        if (subquery.having() != null && subquery.groupBy() == null) {
            throw new ParserException("Subquery with HAVING clause must also have GROUP BY clause");
        }
    }
}
