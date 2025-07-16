package me.bechberger.jfr.extended.engine.exception;

import me.bechberger.jfr.extended.ast.ASTNode;

/**
 * Exception thrown when a query syntax or semantic validation error occurs.
 * 
 * This exception provides specific information about syntax errors, semantic violations,
 * and helpful suggestions for fixing common query construction problems.
 * 
 * @author Query Engine Team
 * @since 1.0
 */
public class QuerySyntaxException extends QueryExecutionException {
    
    private final String syntaxRule;
    private final String queryFragment;
    private final SyntaxErrorType errorType;
    
    /**
     * Enumeration of different types of syntax errors.
     */
    public enum SyntaxErrorType {
        MISSING_CLAUSE("Missing required clause"),
        INVALID_OPERATOR("Invalid operator usage"),
        MALFORMED_EXPRESSION("Malformed expression"),
        AGGREGATE_WITHOUT_GROUP_BY("Aggregate function without GROUP BY"),
        INCOMPATIBLE_CLAUSES("Incompatible clause combination"),
        INVALID_IDENTIFIER("Invalid identifier"),
        PARSING_ERROR("Query parsing error"),
        SEMANTIC_VIOLATION("Semantic constraint violation");
        
        private final String description;
        
        SyntaxErrorType(String description) {
            this.description = description;
        }
        
        public String getDescription() {
            return description;
        }
    }
    
    /**
     * Creates a new QuerySyntaxException with detailed information.
     * 
     * @param syntaxRule The syntax rule that was violated
     * @param queryFragment The fragment of the query that caused the error
     * @param errorType The type of syntax error
     * @param errorNode The AST node where the error occurred (can be null)
     */
    public QuerySyntaxException(String syntaxRule, String queryFragment, SyntaxErrorType errorType, ASTNode errorNode) {
        super(
            String.format("%s: %s", errorType.getDescription(), syntaxRule),
            errorNode,
            buildContext(syntaxRule, queryFragment, errorType),
            buildUserHint(syntaxRule, errorType),
            null
        );
        this.syntaxRule = syntaxRule;
        this.queryFragment = queryFragment;
        this.errorType = errorType;
    }
    
    /**
     * Creates a new QuerySyntaxException with default error type.
     */
    public QuerySyntaxException(String syntaxRule, String queryFragment, ASTNode errorNode) {
        this(syntaxRule, queryFragment, SyntaxErrorType.SEMANTIC_VIOLATION, errorNode);
    }
    
    /**
     * Creates a QuerySyntaxException for aggregate function errors.
     */
    public static QuerySyntaxException forAggregateWithoutGroupBy(String functionName, ASTNode errorNode) {
        String rule = String.format("Aggregate function '%s' requires GROUP BY clause", functionName);
        String fragment = functionName + "()";
        return new QuerySyntaxException(rule, fragment, SyntaxErrorType.AGGREGATE_WITHOUT_GROUP_BY, errorNode);
    }
    
    /**
     * Creates a QuerySyntaxException for missing required clauses.
     */
    public static QuerySyntaxException forMissingClause(String requiredClause, String currentQuery, ASTNode errorNode) {
        String rule = String.format("Missing required %s clause", requiredClause);
        return new QuerySyntaxException(rule, currentQuery, SyntaxErrorType.MISSING_CLAUSE, errorNode);
    }
    
    /**
     * Creates a QuerySyntaxException for invalid operators.
     */
    public static QuerySyntaxException forInvalidOperator(String operator, String leftType, String rightType, ASTNode errorNode) {
        String rule = String.format("Operator '%s' cannot be applied to %s and %s", operator, leftType, rightType);
        String fragment = String.format("%s %s %s", leftType, operator, rightType);
        return new QuerySyntaxException(rule, fragment, SyntaxErrorType.INVALID_OPERATOR, errorNode);
    }
    
    /**
     * Creates a QuerySyntaxException for parsing errors.
     */
    public static QuerySyntaxException forParsingError(String parseError, String queryFragment, ASTNode errorNode) {
        return new QuerySyntaxException(parseError, queryFragment, SyntaxErrorType.PARSING_ERROR, errorNode);
    }
    
    /**
     * Builds the context string for this exception.
     */
    private static String buildContext(String syntaxRule, String queryFragment, SyntaxErrorType errorType) {
        StringBuilder context = new StringBuilder();
        context.append("Query validation failed: ").append(errorType.getDescription());
        
        if (queryFragment != null && !queryFragment.isEmpty()) {
            String fragment = queryFragment.length() > 100 ? 
                queryFragment.substring(0, 97) + "..." : queryFragment;
            context.append("\nQuery fragment: ").append(fragment);
        }
        
        return context.toString();
    }
    
    /**
     * Builds a helpful user hint for resolving the syntax error.
     */
    private static String buildUserHint(String syntaxRule, SyntaxErrorType errorType) {
        return switch (errorType) {
            case MISSING_CLAUSE -> "Add the required clause to complete the query. Check the query syntax documentation for proper clause ordering.";
            case INVALID_OPERATOR -> "Check operator compatibility with the data types. Some operators only work with specific types (e.g., arithmetic with numbers).";
            case MALFORMED_EXPRESSION -> "Review the expression syntax. Ensure proper parentheses, operator precedence, and valid identifiers.";
            case AGGREGATE_WITHOUT_GROUP_BY -> "When using aggregate functions (COUNT, SUM, AVG, etc.), add a GROUP BY clause or ensure it's a pure aggregate query.";
            case INCOMPATIBLE_CLAUSES -> "Some query clauses cannot be used together. Review the query structure and remove conflicting clauses.";
            case INVALID_IDENTIFIER -> "Check identifier names for invalid characters or reserved keywords. Use quotes if necessary.";
            case PARSING_ERROR -> "Review the query syntax for typos, missing keywords, or incorrect clause structure.";
            case SEMANTIC_VIOLATION -> "The query structure violates semantic rules. Check the relationships between clauses and expressions.";
        };
    }
    
    // Getters for accessing specific error information
    
    public String getSyntaxRule() {
        return syntaxRule;
    }
    
    public String getQueryFragment() {
        return queryFragment;
    }
    
    public SyntaxErrorType getErrorType() {
        return errorType;
    }
    
    /**
     * Returns true if this exception has a query fragment.
     */
    public boolean hasQueryFragment() {
        return queryFragment != null && !queryFragment.isEmpty();
    }
}
