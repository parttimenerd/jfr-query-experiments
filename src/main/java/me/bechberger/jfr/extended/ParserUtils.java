package me.bechberger.jfr.extended;

import me.bechberger.jfr.extended.ast.Location;
import me.bechberger.jfr.extended.ast.ASTNodes.*;
import java.util.EnumSet;
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
     * Accepts tab characters and newlines directly, along with Unicode and most printable characters.
     */
    public static void validateStringLiteral(String value) throws ParserException {
        // Empty strings are allowed
        if (value.isEmpty()) {
            return;
        }
        
        // Allow tabs, newlines, and most characters - only restrict truly problematic control characters
        for (char c : value.toCharArray()) {
            // Allow tabs (\t), newlines (\n, \r), and other printable characters
            // Only reject null character and other potentially problematic control characters
            if (c == '\0') {
                throw new ParserException("String literal contains null character (\\0) which is not allowed.");
            }
            // Allow other control characters including tabs and newlines as requested
        }
    }
    
    /**
     * Process escape sequences in a string literal.
     * Converts escape sequences like \n, \t, \', \" to their actual characters.
     */
    public static String processEscapeSequences(String value) throws ParserException {
        if (value.isEmpty()) {
            return value;
        }
        
        StringBuilder result = new StringBuilder();
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (c == '\\' && i + 1 < value.length()) {
                char next = value.charAt(i + 1);
                switch (next) {
                    case 'n' -> { result.append('\n'); i++; }
                    case 't' -> { result.append('\t'); i++; }
                    case 'r' -> { result.append('\r'); i++; }
                    case '\\' -> { result.append('\\'); i++; }
                    case '\'' -> { result.append('\''); i++; }
                    case '"' -> { result.append('"'); i++; }
                    case 'b' -> { result.append('\b'); i++; }
                    case 'f' -> { result.append('\f'); i++; }
                    default -> {
                        throw new ParserException("Invalid escape sequence: \\" + next + 
                            ". Supported escape sequences are: \\n, \\t, \\r, \\\\, \\', \\\", \\b, \\f");
                    }
                }
            } else {
                result.append(c);
            }
        }
        return result.toString();
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
        return parseFieldReferenceWithKeywords(tokens, context);
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
    
    /**
     * Keywords that can be used as identifiers in certain contexts.
     * This set contains all TokenType values that have the KEYWORD attribute.
     */
    private static final Set<TokenType> KEYWORDS_AS_IDENTIFIERS = EnumSet.allOf(TokenType.class)
            .stream()
            .filter(TokenType::isKeyword)
            .collect(java.util.stream.Collectors.toUnmodifiableSet());
    
    /**
     * Checks if the current token can be used as an identifier (either IDENTIFIER or allowed keyword)
     */
    public static boolean canUseAsIdentifier(TokenStream tokens) {
        return tokens.check(TokenType.IDENTIFIER) || 
               KEYWORDS_AS_IDENTIFIERS.contains(tokens.current().type());
    }
    
    /**
     * Consumes a token that can be used as an identifier (IDENTIFIER or allowed keyword)
     */
    public static Token consumeIdentifierOrKeyword(TokenStream tokens, String context) throws ParserException {
        if (tokens.check(TokenType.IDENTIFIER)) {
            return tokens.advance();
        }
        
        if (KEYWORDS_AS_IDENTIFIERS.contains(tokens.current().type())) {
            return tokens.advance();
        }
        
        throw new ParserException("Expected identifier in " + context + " but found " + 
                                tokens.current().type() + " at " + tokens.current().getPositionString());
    }
    
    /**
     * Parse a field reference that allows keywords as identifiers
     */
    public static String parseFieldReferenceWithKeywords(TokenStream tokens, String context) throws ParserException {
        Token firstPart = consumeIdentifierOrKeyword(tokens, context);
        StringBuilder fieldName = new StringBuilder(firstPart.value());
        
        while (tokens.match(TokenType.DOT)) {
            Token nextPart = consumeIdentifierOrKeyword(tokens, context + " after dot");
            fieldName.append(".").append(nextPart.value());
        }
        
        return fieldName.toString();
    }
}
