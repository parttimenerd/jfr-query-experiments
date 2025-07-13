package me.bechberger.jfr.extended;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import me.bechberger.jfr.extended.evaluator.FunctionRegistry;

/**
 * Enhanced error handling for the JFR query parser.
 * Provides human-readable error messages with context and helpful suggestions.
 */
public class ParserErrorHandler {
    
    private final List<Token> tokens;
    private final String originalQuery;
    private final List<ParserError> errors = new ArrayList<>();
    
    public ParserErrorHandler(List<Token> tokens, String originalQuery) {
        this.tokens = tokens;
        this.originalQuery = originalQuery;
    }
    
    /**
     * Represents a parser error with enhanced information
     */
    public static class ParserError {
        private final String message;
        private final String suggestion;
        private final Token errorToken;
        private final String context;
        private final ErrorType type;
        private final String examples;
        
        public ParserError(String message, String suggestion, Token errorToken, String context, ErrorType type) {
            this(message, suggestion, errorToken, context, type, null);
        }
        
        public ParserError(String message, String suggestion, Token errorToken, String context, ErrorType type, String examples) {
            this.message = message;
            this.suggestion = suggestion;
            this.errorToken = errorToken;
            this.context = context;
            this.type = type;
            this.examples = examples;
        }
        
        public String getMessage() { return message; }
        public String getSuggestion() { return suggestion; }
        public Token getErrorToken() { return errorToken; }
        public String getContext() { return context; }
        public ErrorType getType() { return type; }
        public String getExamples() { return examples; }
        
        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("Syntax Error at ").append(errorToken.getPositionString()).append(":\n");
            if (context != null && !context.isEmpty()) {
                sb.append("Context: ").append(context).append("\n");
            }
            sb.append(message).append("\n");
            if (suggestion != null && !suggestion.isEmpty()) {
                sb.append("Suggestion: ").append(suggestion).append("\n");
            }
            if (examples != null && !examples.isEmpty()) {
                sb.append("Examples: ").append(examples).append("\n");
            }
            return sb.toString();
        }
    }
    
    /**
     * Types of parser errors for better categorization
     */
    public enum ErrorType {
        MISSING_TOKEN,
        UNEXPECTED_TOKEN, 
        MISSING_PARENTHESIS,
        MISSING_BRACKET,
        MISSING_COMMA,
        INVALID_IDENTIFIER,
        INVALID_FUNCTION_CALL,
        INVALID_EXPRESSION,
        DUPLICATE_CLAUSE,
        SYNTAX_ERROR
    }
    
    /**
     * Query parsing context to provide context-aware error suggestions
     */
    public enum QueryContext {
        SELECT_CLAUSE,      // In SELECT field list
        FROM_CLAUSE,        // In FROM table specification
        WHERE_CLAUSE,       // In WHERE condition
        GROUP_BY_CLAUSE,    // In GROUP BY field list
        ORDER_BY_CLAUSE,    // In ORDER BY field list
        HAVING_CLAUSE,      // In HAVING condition
        FUNCTION_CALL,      // Inside function call parameters
        EXPRESSION,         // General expression context
        UNKNOWN             // Context cannot be determined
    }
    
    /**
     * Detect the current parsing context based on error token position
     */
    public QueryContext detectQueryContext(Token errorToken) {
        int tokenIndex = findTokenIndex(errorToken);
        if (tokenIndex < 0 || tokens.isEmpty()) {
            return QueryContext.UNKNOWN;
        }
        
        // Look backwards to find the most recent clause keyword
        TokenType lastClauseKeyword = null;
        int functionDepth = 0;
        
        for (int i = tokenIndex - 1; i >= 0; i--) {
            Token token = tokens.get(i);
            
            // Track function call depth
            if (token.type() == TokenType.RPAREN) {
                functionDepth++;
            } else if (token.type() == TokenType.LPAREN) {
                functionDepth--;
                // If we're inside a function call, check if previous token is an identifier
                if (functionDepth < 0 && i > 0 && tokens.get(i - 1).type() == TokenType.IDENTIFIER) {
                    return QueryContext.FUNCTION_CALL;
                }
            }
            
            // Find clause keywords
            switch (token.type()) {
                case SELECT:
                    if (lastClauseKeyword == null) {
                        return QueryContext.SELECT_CLAUSE;
                    }
                    break;
                case FROM:
                    if (lastClauseKeyword == null) {
                        return QueryContext.FROM_CLAUSE;
                    }
                    break;
                case WHERE:
                    if (lastClauseKeyword == null) {
                        return QueryContext.WHERE_CLAUSE;
                    }
                    break;
                case GROUP_BY:
                    if (lastClauseKeyword == null) {
                        return QueryContext.GROUP_BY_CLAUSE;
                    }
                    break;
                case ORDER_BY:
                    if (lastClauseKeyword == null) {
                        return QueryContext.ORDER_BY_CLAUSE;
                    }
                    break;
                case HAVING:
                    if (lastClauseKeyword == null) {
                        return QueryContext.HAVING_CLAUSE;
                    }
                    break;
                default:
                    // Remember the first clause keyword we encounter going backwards
                    if (lastClauseKeyword == null && isClauseKeyword(token.type())) {
                        lastClauseKeyword = token.type();
                    }
                    break;
            }
        }
        
        // If we're inside a function call, return that context
        if (functionDepth < 0) {
            return QueryContext.FUNCTION_CALL;
        }
        
        // Default to expression if we can't determine specific context
        return QueryContext.EXPRESSION;
    }
    
    /**
     * Check if a token type represents a clause keyword
     */
    private boolean isClauseKeyword(TokenType type) {
        return type == TokenType.SELECT || type == TokenType.FROM || type == TokenType.WHERE ||
               type == TokenType.GROUP_BY || type == TokenType.ORDER_BY || type == TokenType.HAVING ||
               type == TokenType.LIMIT || type == TokenType.JOIN;
    }
    
    /**
     * Create a human-readable error for missing expected token
     */
    public ParserError createMissingTokenError(TokenType expected, Token actual) {
        QueryContext queryContext = detectQueryContext(actual);
        String message = createContextAwareMissingTokenMessage(expected, actual, queryContext);
        String suggestion = createContextAwareMissingTokenSuggestion(expected, actual, queryContext);
        String context = extractContext(actual);
        ErrorType type = determineErrorType(expected);
        
        return new ParserError(message, suggestion, actual, context, type);
    }
    
    /**
     * Create a human-readable error for unexpected token
     */
    public ParserError createUnexpectedTokenError(Token unexpected, String expectedDescription) {
        QueryContext queryContext = detectQueryContext(unexpected);
        String message = createContextAwareUnexpectedTokenMessage(unexpected, expectedDescription, queryContext);
        String suggestion = createContextAwareUnexpectedTokenSuggestion(unexpected, expectedDescription, queryContext);
        String context = extractContext(unexpected);
        
        return new ParserError(message, suggestion, unexpected, context, ErrorType.UNEXPECTED_TOKEN);
    }
    
    /**
     * Create a human-readable error for function call issues
     */
    public ParserError createFunctionCallError(Token functionToken, String issue) {
        QueryContext queryContext = detectQueryContext(functionToken);
        String message = createContextAwareFunctionMessage(functionToken, issue, queryContext);
        String suggestion = createContextAwareFunctionSuggestion(functionToken, issue, queryContext);
        String context = extractContext(functionToken);
        String examples = createFunctionCallExamples(functionToken);
        
        return new ParserError(message, suggestion, functionToken, context, ErrorType.INVALID_FUNCTION_CALL, examples);
    }
    
    /**
     * Create context-aware function call error message
     */
    private String createContextAwareFunctionMessage(Token functionToken, String issue, QueryContext queryContext) {
        String funcName = functionToken.value();
        return switch (queryContext) {
            case SELECT_CLAUSE -> "Invalid function call '" + funcName + "' in SELECT clause: " + issue;
            case WHERE_CLAUSE -> "Invalid function call '" + funcName + "' in WHERE condition: " + issue;
            case HAVING_CLAUSE -> "Invalid function call '" + funcName + "' in HAVING condition: " + issue;
            default -> "Invalid function call '" + funcName + "': " + issue;
        };
    }
    
    /**
     * Create context-aware function call suggestion
     */
    private String createContextAwareFunctionSuggestion(Token functionToken, String issue, QueryContext queryContext) {
        String funcName = functionToken.value().toUpperCase();
        FunctionRegistry registry = FunctionRegistry.getInstance();
        
        // Special case: if this is a double parentheses error, extract the suggestion from the issue message
        if (issue.contains("double parentheses") && "(".equals(functionToken.value())) {
            // Extract the suggestion that was added by ParserSuggestionHelper
            int suggestionStart = issue.indexOf("Try:");
            if (suggestionStart != -1) {
                return issue.substring(suggestionStart);
            }
            return ""; // Fallback if no suggestion found
        }
        
        // Check if function exists in registry
        if (!registry.isFunction(funcName)) {
            return generateContextAwareUnknownFunctionSuggestion(funcName, registry, queryContext);
        }
        
        // Get function definition from registry and provide detailed information
        FunctionRegistry.FunctionDefinition funcDef = registry.getFunction(funcName);
        if (funcDef != null) {
            return generateContextAwareFunctionUsageSuggestion(funcDef, issue, queryContext);
        }
        
        // Fallback for functions not in registry (shouldn't happen normally)
        return "Check the function syntax and ensure all arguments are provided correctly.";
    }
    
    private String generateContextAwareUnknownFunctionSuggestion(String funcName, FunctionRegistry registry, QueryContext queryContext) {
        // Find similar function names using string distance
        List<String> suggestions = findSimilarFunctionNames(funcName);
        
        StringBuilder sb = new StringBuilder();
        
        if (!suggestions.isEmpty()) {
            sb.append("Did you mean: ");
            for (int i = 0; i < Math.min(3, suggestions.size()); i++) {
                if (i > 0) sb.append(", ");
                sb.append(suggestions.get(i));
            }
            sb.append("? ");
        }
        
        // Add context-specific function suggestions
        String contextHelp = switch (queryContext) {
            case SELECT_CLAUSE -> "For SELECT clause, consider aggregate functions: COUNT, SUM, AVG, MIN, MAX";
            case WHERE_CLAUSE -> "For WHERE clause, consider comparison and mathematical functions";
            case HAVING_CLAUSE -> "For HAVING clause, use aggregate functions: COUNT, SUM, AVG, MIN, MAX";
            default -> "Available function categories: Aggregate, Mathematical, String, Date/Time, Conditional";
        };
        
        sb.append(contextHelp);
        
        return sb.toString();
    }
    
    private String generateContextAwareFunctionUsageSuggestion(FunctionRegistry.FunctionDefinition funcDef, String issue, QueryContext queryContext) {
        StringBuilder sb = new StringBuilder();
        
        // Add context-specific guidance first
        String contextGuidance = switch (queryContext) {
            case SELECT_CLAUSE -> "In SELECT clause: ";
            case WHERE_CLAUSE -> "In WHERE clause: ";
            case HAVING_CLAUSE -> "In HAVING clause: ";
            default -> "";
        };
        sb.append(contextGuidance);
        
        // Add function description
        if (funcDef.description() != null) {
            sb.append(funcDef.description()).append(". ");
        }
        
        // Add signature with parameter details
        sb.append("Usage: ").append(funcDef.signature());
        
        // Add parameter descriptions if available
        if (!funcDef.parameters().isEmpty()) {
            sb.append("\nParameters:");
            for (FunctionRegistry.ParameterDefinition param : funcDef.parameters()) {
                sb.append("\n  - ").append(param.name());
                if (param.optional()) sb.append(" (optional)");
                if (param.variadic()) sb.append(" (variadic)");
                sb.append(": ").append(param.type().name().toLowerCase());
                if (param.description() != null) {
                    sb.append(" - ").append(param.description());
                }
            }
        }
        
        // Add examples if available
        if (funcDef.examples() != null && !funcDef.examples().isEmpty()) {
            sb.append("\nExamples: ").append(funcDef.examples());
        }
        
        // Add specific issue guidance if provided
        if (issue != null && !issue.isEmpty()) {
            sb.append("\nIssue: ").append(issue);
        }
        
        return sb.toString();
    }
    
    /**
     * Create a human-readable error for expression issues
     */
    public ParserError createExpressionError(Token errorToken, String issue) {
        QueryContext queryContext = detectQueryContext(errorToken);
        String message = createContextAwareExpressionMessage(issue, queryContext);
        String suggestion = createContextAwareExpressionSuggestion(errorToken, issue, queryContext);
        String context = extractContext(errorToken);
        
        return new ParserError(message, suggestion, errorToken, context, ErrorType.INVALID_EXPRESSION);
    }
    
    /**
     * Create context-aware expression error message
     */
    private String createContextAwareExpressionMessage(String issue, QueryContext queryContext) {
        return switch (queryContext) {
            case SELECT_CLAUSE -> "Invalid expression in SELECT clause: " + issue;
            case FROM_CLAUSE -> "Invalid table specification in FROM clause: " + issue;
            case WHERE_CLAUSE -> "Invalid condition in WHERE clause: " + issue;
            case GROUP_BY_CLAUSE -> "Invalid field in GROUP BY clause: " + issue;
            case ORDER_BY_CLAUSE -> "Invalid field in ORDER BY clause: " + issue;
            case HAVING_CLAUSE -> "Invalid condition in HAVING clause: " + issue;
            case FUNCTION_CALL -> "Invalid function argument: " + issue;
            default -> "Invalid expression: " + issue;
        };
    }
    
    /**
     * Create context-aware expression error suggestion
     */
    private String createContextAwareExpressionSuggestion(Token errorToken, String issue, QueryContext queryContext) {
        // First check for context-specific suggestions
        String contextSuggestion = createExpressionContextSuggestion(errorToken, issue, queryContext);
        if (contextSuggestion != null) {
            return contextSuggestion;
        }
        
        // Fall back to general expression suggestions
        return createGeneralExpressionSuggestion(errorToken, issue);
    }
    
    /**
     * Create context-specific expression suggestions
     */
    private String createExpressionContextSuggestion(Token errorToken, String issue, QueryContext queryContext) {
        return switch (queryContext) {
            case SELECT_CLAUSE -> createSelectExpressionSuggestion(errorToken, issue);
            case FROM_CLAUSE -> createFromExpressionSuggestion(errorToken, issue);
            case WHERE_CLAUSE -> createWhereExpressionSuggestion(errorToken, issue);
            case FUNCTION_CALL -> createFunctionArgumentSuggestion(errorToken, issue);
            default -> null;
        };
    }
    
    private String createSelectExpressionSuggestion(Token errorToken, String issue) {
        if (issue.contains("comparison") || issue.contains("Unexpected token")) {
            return "SELECT clause should contain field names, expressions, or functions. If you have an incomplete expression with '(', make sure to close it with ')' and include a valid expression inside.";
        }
        if (errorToken.type() == TokenType.LPAREN) {
            return "Complete the expression inside parentheses. Example: SELECT (field1 + field2), field3 FROM table";
        }
        return "SELECT clause should contain valid field names, functions, or arithmetic expressions.";
    }
    
    private String createFromExpressionSuggestion(Token errorToken, String issue) {
        if (errorToken.type() == TokenType.MINUS || issue.contains("-")) {
            return "Table names cannot contain hyphens (-). Use underscores (_) instead or quoted names. Example: FROM GarbageCollection not FROM Garbage-Collection";
        }
        return "FROM clause should contain a valid table name. Available tables include: GarbageCollection, JVMInformation, etc.";
    }
    
    private String createWhereExpressionSuggestion(Token errorToken, String issue) {
        if (errorToken.type() == TokenType.EQUALS && errorToken.value().equals("==")) {
            return "Use single '=' for comparison, not '==' (double equals is not supported in this query language)";
        }
        return "WHERE clause should contain field comparisons. Example: WHERE duration > 1000 AND type = 'G1'";
    }
    
    private String createFunctionArgumentSuggestion(Token errorToken, String issue) {
        return "Function arguments should be valid expressions, field names, or constants. Check function documentation for required parameter types.";
    }
    
    private String createGeneralExpressionSuggestion(Token errorToken, String issue) {
        // First, check for malformed expressions with consecutive operators
        if (isOperatorToken(errorToken.type()) && issue.contains("Unexpected token")) {
            String expressionContext = extractExpressionContext(errorToken);
            String malformedSuggestion = ParserSuggestionHelper.createMalformedExpressionSuggestion(
                expressionContext, errorToken.value());
            if (malformedSuggestion != null) {
                return malformedSuggestion;
            }
        }
        
        // Check for common "==" confusion - if current token is EQUALS and the error mentions unexpected EQUALS
        if (errorToken.type() == TokenType.EQUALS && issue.contains("Unexpected token: EQUALS")) {
            return "Use single '=' for comparison, not '==' (double equals is not supported in this query language)";
        } else if (issue.contains("operator")) {
            return "Make sure operators are used correctly with operands on both sides. Example: field1 + field2, not +field1";
        } else {
            return "Complete the expression with valid operators and operands.";
        }
    }
    
    /**
     * Add an error to the collection with advanced deduplication and context filtering
     */
    public void addError(ParserError error) {
        // Check if we should add this error based on context and existing errors
        if (shouldAddErrorWithContextFilter(error)) {
            errors.add(error);
        }
    }
    
    /**
     * Enhanced error filtering that considers context and prevents misleading secondary errors
     */
    private boolean shouldAddErrorWithContextFilter(ParserError newError) {
        if (errors.isEmpty()) {
            return true;
        }
        
        // First check if this is a spurious error that should be filtered out
        if (isSpuriousError(newError)) {
            return false;
        }
        
        // Check for errors at the same position
        for (ParserError existingError : errors) {
            if (isSamePosition(existingError, newError)) {
                // If we have the same type of error at the same position, skip
                if (existingError.getType() == newError.getType()) {
                    return false;
                }
                
                // Prioritize specific errors over generic ones
                int existingPriority = getErrorPriority(existingError);
                int newPriority = getErrorPriority(newError);
                
                // If new error has lower priority, don't add it
                if (newPriority < existingPriority) {
                    return false;
                }
                
                // If new error has higher priority, remove the existing one
                if (newPriority > existingPriority) {
                    errors.remove(existingError);
                    return true;
                }
            }
        }
        
        return true;
    }
    
    /**
     * Detect spurious errors that should be filtered out based on query structure
     */
    private boolean isSpuriousError(ParserError error) {
        String message = error.getMessage().toLowerCase();
        
        // Don't filter "Missing FROM clause" - this is a legitimate structural error
        // that should always be shown when SELECT is used without FROM
        if (message.contains("missing from clause")) {
            return false; // Always show missing FROM clause errors
        }
        
        // Filter "Missing WHERE clause" when query structure doesn't require it
        if (message.contains("missing where clause")) {
            return !queryRequiresWhereClause();
        }
        
        // Filter "Missing SELECT" when we're clearly in the middle of parsing a SELECT statement
        if (message.contains("missing select")) {
            return tokens.stream().anyMatch(token -> 
                token.type() == TokenType.SELECT || 
                (token.type() == TokenType.IDENTIFIER && token.value().equalsIgnoreCase("SELECT"))
            );
        }
        
        // Filter duplicate "unexpected token" errors when we already have a primary error
        if (message.contains("unexpected token") && hasHigherPriorityErrorNearby(error)) {
            return true;
        }
        
        // Filter end-of-query errors when the main issue is earlier in the query
        if (isEndOfQueryError(error) && hasPrimaryErrorEarlier(error)) {
            return true;
        }
        
        // Filter errors that occur after major clause boundaries if there's already an error before them
        if (isAfterClauseBoundaryWithPriorError(error)) {
            return true;
        }
        
        return false;
    }
    
    /**
     * Check if error occurs after a major clause boundary and there's already a prior error
     */
    private boolean isAfterClauseBoundaryWithPriorError(ParserError error) {
        Token errorToken = error.getErrorToken();
        int errorPosition = errorToken.position();
        
        // Find the most recent clause boundary before this error
        Token lastClauseBoundary = null;
        for (Token token : tokens) {
            if (token.position() >= errorPosition) break;
            
            if (isClauseKeyword(token.type())) {
                lastClauseBoundary = token;
            }
        }
        
        // If there's no clause boundary, can't filter based on this rule
        if (lastClauseBoundary == null) {
            return false;
        }
        
        // Check if there's a prior error before the clause boundary
        for (ParserError existingError : errors) {
            if (existingError.getErrorToken().position() < lastClauseBoundary.position()) {
                // There's an error before the clause boundary, so this error after the boundary
                // might be spurious (cascading from the earlier error)
                return true;
            }
        }
        
        return false;
    }
    
    /**
     * Check if query structure requires a WHERE clause
     */
    private boolean queryRequiresWhereClause() {
        // Most queries don't require WHERE clause, so return false by default
        // Could be enhanced with more specific logic if needed
        return false;
    }
    
    /**
     * Check if there's a higher priority error nearby that makes this error redundant
     */
    private boolean hasHigherPriorityErrorNearby(ParserError error) {
        int errorPosition = error.getErrorToken().position();
        int priority = getErrorPriority(error);
        
        for (ParserError existingError : errors) {
            int existingPosition = existingError.getErrorToken().position();
            int existingPriority = getErrorPriority(existingError);
            
            // If there's a higher priority error within 10 characters, this error is redundant
            if (existingPriority > priority && Math.abs(existingPosition - errorPosition) <= 10) {
                return true;
            }
        }
        
        return false;
    }
    
    /**
     * Check if this error is at the end of the query (likely a cascading error)
     */
    private boolean isEndOfQueryError(ParserError error) {
        Token errorToken = error.getErrorToken();
        
        // Check if error is at EOF or near the end of the token stream
        if (errorToken.type() == TokenType.EOF) {
            return true;
        }
        
        int tokenIndex = findTokenIndex(errorToken);
        if (tokenIndex < 0) return false;
        
        // Consider it an end-of-query error if it's in the last 3 tokens
        return tokenIndex >= tokens.size() - 3;
    }
    
    /**
     * Check if there's a primary error earlier in the query
     */
    private boolean hasPrimaryErrorEarlier(ParserError error) {
        int errorPosition = error.getErrorToken().position();
        
        for (ParserError existingError : errors) {
            int existingPosition = existingError.getErrorToken().position();
            
            // If there's an error more than 5 characters earlier, consider it primary
            if (existingPosition < errorPosition - 5) {
                return true;
            }
        }
        
        return false;
    }
    
    /**
     * Check if two errors are at the same position
     */
    private boolean isSamePosition(ParserError error1, ParserError error2) {
        Token token1 = error1.getErrorToken();
        Token token2 = error2.getErrorToken();
        
        if (token1 == null || token2 == null) {
            return false;
        }
        
        return token1.line() == token2.line() && token1.column() == token2.column();
    }
    
    /**
     * Get error priority for deduplication (higher number = higher priority)
     */
    private int getErrorPriority(ParserError error) {
        return switch (error.getType()) {
            case INVALID_FUNCTION_CALL -> 100; // Specific function issues
            case INVALID_EXPRESSION -> 90;     // Expression-specific issues
            case UNEXPECTED_TOKEN -> 80;       // Token-specific issues
            case MISSING_TOKEN -> 70;          // Missing token issues
            case MISSING_PARENTHESIS -> 60;    // Parenthesis issues
            case MISSING_BRACKET -> 50;        // Bracket issues
            case MISSING_COMMA -> 40;          // Comma issues
            case INVALID_IDENTIFIER -> 30;     // Identifier issues
            case DUPLICATE_CLAUSE -> 20;       // Duplicate clause issues
            case SYNTAX_ERROR -> 10;           // Generic syntax errors
        };
    }
    
    /**
     * Get all collected errors
     */
    public List<ParserError> getErrors() {
        return new ArrayList<>(errors);
    }
    
    /**
     * Check if there are any errors
     */
    public boolean hasErrors() {
        return !errors.isEmpty();
    }
    
    /**
     * Create a comprehensive error message for all collected errors
     */
    public String createErrorReport() {
        if (errors.isEmpty()) {
            return "No parsing errors found.";
        }
        
        // Deduplicate and prioritize errors
        List<ParserError> processedErrors = deduplicateAndPrioritizeErrors();
        
        if (processedErrors.size() == 1) {
            // For single error, just return the error message without numbering
            return processedErrors.get(0).toString();
        } else {
            // For multiple errors, use numbered list but limit to most relevant ones
            StringBuilder report = new StringBuilder();
            int errorCount = Math.min(3, processedErrors.size()); // Limit to top 3 errors
            
            if (processedErrors.size() > 3) {
                report.append("Found ").append(processedErrors.size()).append(" parsing errors (showing top ").append(errorCount).append("):\n\n");
            } else {
                report.append("Found ").append(errorCount).append(" parsing errors:\n\n");
            }
            
            for (int i = 0; i < errorCount; i++) {
                report.append(i + 1).append(".\n").append(processedErrors.get(i).toString());
                if (i < errorCount - 1) {
                    report.append("\n--------------------------------------------------\n");
                }
            }
            
            if (processedErrors.size() > 3) {
                report.append("\n\n(Additional errors detected but not shown to avoid clutter)");
            }
            
            return report.toString();
        }
    }
    
    /**
     * Deduplicate and prioritize errors for cleaner reporting
     */
    private List<ParserError> deduplicateAndPrioritizeErrors() {
        if (errors.size() <= 1) {
            return new ArrayList<>(errors);
        }
        
        List<ParserError> result = new ArrayList<>();
        
        // Group errors by position and type
        java.util.Map<String, List<ParserError>> errorGroups = new java.util.HashMap<>();
        
        for (ParserError error : errors) {
            String key = getErrorGroupKey(error);
            errorGroups.computeIfAbsent(key, k -> new ArrayList<>()).add(error);
        }
        
        // For each group, select the highest priority error
        for (List<ParserError> group : errorGroups.values()) {
            if (group.size() == 1) {
                result.add(group.get(0));
            } else {
                // Find the highest priority error in the group
                ParserError bestError = group.stream()
                    .max((e1, e2) -> Integer.compare(getErrorPriority(e1), getErrorPriority(e2)))
                    .orElse(group.get(0));
                result.add(bestError);
            }
        }
        
        // Sort by position (line, then column)
        result.sort((e1, e2) -> {
            Token t1 = e1.getErrorToken();
            Token t2 = e2.getErrorToken();
            
            if (t1 == null && t2 == null) return 0;
            if (t1 == null) return 1;
            if (t2 == null) return -1;
            
            int lineCompare = Integer.compare(t1.line(), t2.line());
            if (lineCompare != 0) return lineCompare;
            
            return Integer.compare(t1.column(), t2.column());
        });
        
        return result;
    }
    
    /**
     * Create a group key for error deduplication
     */
    private String getErrorGroupKey(ParserError error) {
        Token token = error.getErrorToken();
        if (token == null) {
            return "unknown:0:0";
        }
        return token.line() + ":" + token.column() + ":" + error.getType();
    }
    
    // Helper methods for creating error messages
    
    /**
     * Create context-aware missing token message
     */
    private String createContextAwareMissingTokenMessage(TokenType expected, Token actual, QueryContext queryContext) {
        return switch (expected) {
            case LPAREN -> "Missing opening parenthesis '('";
            case RPAREN -> createMissingRParenMessage(queryContext);
            case LBRACKET -> "Missing opening bracket '['";
            case RBRACKET -> "Missing closing bracket ']'";
            case COMMA -> createMissingCommaMessage(queryContext);
            case SEMICOLON -> "Missing semicolon ';' to end statement";
            case FROM -> {
                // Only report missing FROM if we're not in SELECT clause with incomplete parentheses
                if (queryContext == QueryContext.SELECT_CLAUSE && hasIncompleteParentheses(actual)) {
                    yield "Missing closing parenthesis ')' in SELECT expression";
                } else {
                    yield "Missing FROM clause in query";
                }
            }
            case SELECT -> "Missing SELECT keyword to start query";
            case IDENTIFIER -> createMissingIdentifierMessage(queryContext);
            case EQUALS -> "Missing '=' in assignment or comparison";
            case AND, OR -> "Missing logical operator (AND/OR)";
            default -> "Missing " + getTokenDescription(expected);
        };
    }
    
    /**
     * Create context-aware missing token suggestion
     */
    private String createContextAwareMissingTokenSuggestion(TokenType expected, Token actual, QueryContext queryContext) {
        return switch (expected) {
            case RPAREN -> createMissingRParenSuggestion(actual, queryContext);
            case RBRACKET -> "Add ']' to close the array literal or bracket expression.";
            case COMMA -> createMissingCommaSuggestion(queryContext);
            case FROM -> {
                // Only suggest FROM clause if we're not dealing with incomplete parentheses
                if (queryContext == QueryContext.SELECT_CLAUSE && hasIncompleteParentheses(actual)) {
                    yield "Add ')' to close the expression in SELECT clause. Make sure the expression is complete.";
                } else {
                    yield "Add 'FROM table_name' to specify which table to query.";
                }
            }
            case SELECT -> "Start your query with 'SELECT' or '@SELECT' for extended queries.";
            case IDENTIFIER -> createMissingIdentifierSuggestion(queryContext);
            default -> "Add the missing " + getTokenDescription(expected) + " token.";
        };
    }
    
    /**
     * Check if there are incomplete/unbalanced parentheses around the error token
     */
    private boolean hasIncompleteParentheses(Token errorToken) {
        if (tokens == null || tokens.isEmpty()) {
            return false;
        }
        
        int parenCount = 0;
        int errorPosition = errorToken.position();
        
        // Count parentheses up to the error position
        for (Token token : tokens) {
            if (token.position() >= errorPosition) {
                break;
            }
            
            if (token.type() == TokenType.LPAREN) {
                parenCount++;
            } else if (token.type() == TokenType.RPAREN) {
                parenCount--;
            }
        }
        
        // If parenCount > 0, we have unmatched opening parentheses
        return parenCount > 0;
    }
    
    /**
     * Create context-aware unexpected token message
     */
    private String createContextAwareUnexpectedTokenMessage(Token unexpected, String expectedDescription, QueryContext queryContext) {
        String baseMessage = "Unexpected " + getTokenDescription(unexpected.type()) + " '" + unexpected.value() + "'";
        
        return switch (queryContext) {
            case SELECT_CLAUSE -> baseMessage + " in SELECT clause";
            case FROM_CLAUSE -> baseMessage + " in FROM clause";
            case WHERE_CLAUSE -> baseMessage + " in WHERE condition";
            case GROUP_BY_CLAUSE -> baseMessage + " in GROUP BY clause";
            case ORDER_BY_CLAUSE -> baseMessage + " in ORDER BY clause";
            case HAVING_CLAUSE -> baseMessage + " in HAVING condition";
            case FUNCTION_CALL -> baseMessage + " in function call";
            default -> baseMessage;
        };
    }
    
    /**
     * Create context-aware unexpected token suggestion
     */
    private String createContextAwareUnexpectedTokenSuggestion(Token unexpected, String expectedDescription, QueryContext queryContext) {
        // Handle context-specific suggestions first
        String contextSuggestion = createContextSpecificSuggestion(unexpected, queryContext);
        if (contextSuggestion != null) {
            return contextSuggestion;
        }
        
        // Fall back to general token-specific suggestions
        return createGeneralTokenSuggestion(unexpected, expectedDescription);
    }
    
    /**
     * Create context-specific suggestions for unexpected tokens
     */
    private String createContextSpecificSuggestion(Token unexpected, QueryContext queryContext) {
        return switch (queryContext) {
            case SELECT_CLAUSE -> createSelectClauseSuggestion(unexpected);
            case FROM_CLAUSE -> createFromClauseSuggestion(unexpected);
            case WHERE_CLAUSE -> createWhereClauseSuggestion(unexpected);
            case GROUP_BY_CLAUSE -> createGroupByClauseSuggestion(unexpected);
            case ORDER_BY_CLAUSE -> createOrderByClauseSuggestion(unexpected);
            case HAVING_CLAUSE -> createHavingClauseSuggestion(unexpected);
            case FUNCTION_CALL -> createFunctionCallContextSuggestion(unexpected);
            default -> null;
        };
    }
    
    // Context-specific helper methods for missing tokens
    
    private String createMissingRParenMessage(QueryContext context) {
        return switch (context) {
            case FUNCTION_CALL -> "Missing closing parenthesis ')' in function call";
            case SELECT_CLAUSE -> "Missing closing parenthesis ')' in SELECT expression";
            case WHERE_CLAUSE -> "Missing closing parenthesis ')' in WHERE condition";
            default -> "Missing closing parenthesis ')'";
        };
    }
    
    private String createMissingRParenSuggestion(Token actual, QueryContext context) {
        return switch (context) {
            case FUNCTION_CALL -> "Add ')' to close the function call. Check if all function arguments are properly separated by commas.";
            case SELECT_CLAUSE -> "Add ')' to close the expression in SELECT clause. Make sure the expression is complete.";
            case WHERE_CLAUSE -> "Add ')' to close the condition in WHERE clause.";
            default -> "Add ')' to close the parentheses. Make sure all opening parentheses have matching closing ones.";
        };
    }
    
    private String createMissingCommaMessage(QueryContext context) {
        return switch (context) {
            case SELECT_CLAUSE -> "Missing comma ',' between SELECT fields";
            case FUNCTION_CALL -> "Missing comma ',' between function arguments";
            case GROUP_BY_CLAUSE -> "Missing comma ',' between GROUP BY fields";
            case ORDER_BY_CLAUSE -> "Missing comma ',' between ORDER BY fields";
            default -> "Missing comma ',' between items";
        };
    }
    
    private String createMissingCommaSuggestion(QueryContext context) {
        return switch (context) {
            case SELECT_CLAUSE -> "Add ',' between fields in SELECT clause. Example: SELECT field1, field2, field3";
            case FUNCTION_CALL -> "Add ',' between function arguments. Example: FUNCTION(arg1, arg2, arg3)";
            case GROUP_BY_CLAUSE -> "Add ',' between fields in GROUP BY clause.";
            case ORDER_BY_CLAUSE -> "Add ',' between fields in ORDER BY clause.";
            default -> "Add ',' to separate items in the list.";
        };
    }
    
    private String createMissingIdentifierMessage(QueryContext context) {
        return switch (context) {
            case SELECT_CLAUSE -> "Missing field name or expression in SELECT clause";
            case FROM_CLAUSE -> "Missing table name in FROM clause";
            case WHERE_CLAUSE -> "Missing field name in WHERE condition";
            case FUNCTION_CALL -> "Missing function argument";
            default -> "Missing identifier or field name";
        };
    }
    
    private String createMissingIdentifierSuggestion(QueryContext context) {
        return switch (context) {
            case SELECT_CLAUSE -> "Provide field names, expressions, or functions in SELECT clause. Example: SELECT field1, COUNT(*), field2 + field3";
            case FROM_CLAUSE -> "Provide a table name. Example: FROM GarbageCollection, FROM JVMInformation";
            case WHERE_CLAUSE -> "Provide field names and conditions. Example: WHERE duration > 1000";
            case FUNCTION_CALL -> "Provide valid function arguments. Check function documentation for required parameters.";
            default -> "Provide a valid field name, table name, or identifier.";
        };
    }
    
    // Context-specific suggestion methods for unexpected tokens
    
    private String createSelectClauseSuggestion(Token unexpected) {
        return switch (unexpected.type()) {
            case LPAREN -> "Opening parenthesis in SELECT clause should contain a complete expression. Make sure to close it with ')'.";
            case FROM -> "FROM keyword found in SELECT clause. Complete the SELECT field list first. Example: SELECT field1, field2 FROM table";
            case WHERE -> "WHERE keyword found in SELECT clause. Complete the SELECT and FROM clauses first.";
            case COMMA -> "Extra comma in SELECT clause. Remove it or add a field name after the previous comma.";
            case MINUS -> "Invalid '-' in FROM clause. Table names cannot contain '-'. Use underscores '_' or quoted names.";
            default -> "Invalid token in SELECT clause. SELECT should contain field names, expressions, or functions.";
        };
    }
    
    private String createFromClauseSuggestion(Token unexpected) {
        return switch (unexpected.type()) {
            case MINUS -> "Invalid '-' in FROM clause. Table names cannot contain hyphens. Use underscores '_' instead: 'GarbageCollection' not 'Garbage-Collection'.";
            case LPAREN -> "Unexpected parenthesis in FROM clause. If you need a subquery, use WITH or VIEW statement instead.";
            case COMMA -> "Multiple tables in FROM not supported directly. Use JOIN syntax or separate queries.";
            case WHERE -> "WHERE keyword found too early. Complete the FROM clause with a valid table name first.";
            default -> "Invalid token in FROM clause. FROM should be followed by a table name.";
        };
    }
    
    private String createWhereClauseSuggestion(Token unexpected) {
        return switch (unexpected.type()) {
            case EQUALS -> {
                if (unexpected.value().equals("==")) {
                    yield "Use single '=' for comparison, not '==' (double equals is not supported in this query language).";
                } else {
                    yield "Unexpected '=' in WHERE clause. Check the condition syntax.";
                }
            }
            case COMMA -> "Comma not allowed in WHERE clause. Use AND/OR to combine conditions.";
            case FROM -> "FROM keyword not allowed in WHERE clause. Complete the WHERE condition first.";
            default -> "Invalid token in WHERE clause. WHERE should contain field comparisons with AND/OR operators.";
        };
    }
    
    private String createGroupByClauseSuggestion(Token unexpected) {
        return switch (unexpected.type()) {
            case LPAREN -> "Parentheses not typically needed in GROUP BY. Use field names directly: GROUP BY field1, field2";
            case EQUALS -> "Assignment not allowed in GROUP BY. Use field names only.";
            case WHERE -> "WHERE keyword found too early. Complete the GROUP BY clause with field names first.";
            default -> "Invalid token in GROUP BY clause. GROUP BY should contain field names separated by commas.";
        };
    }
    
    private String createOrderByClauseSuggestion(Token unexpected) {
        return switch (unexpected.type()) {
            case LPAREN -> "Parentheses not typically needed in ORDER BY. Use field names with ASC/DESC.";
            case EQUALS -> "Assignment not allowed in ORDER BY. Use field names with optional ASC/DESC.";
            case WHERE -> "WHERE keyword found too early. Complete the ORDER BY clause with field names first.";
            default -> "Invalid token in ORDER BY clause. ORDER BY should contain field names with optional ASC/DESC.";
        };
    }
    
    private String createHavingClauseSuggestion(Token unexpected) {
        return switch (unexpected.type()) {
            case COMMA -> "Comma not allowed in HAVING clause. Use AND/OR to combine conditions.";
            case WHERE -> "WHERE keyword found too early. Complete the HAVING clause with aggregate conditions first.";
            default -> "Invalid token in HAVING clause. HAVING should contain aggregate function conditions with AND/OR operators.";
        };
    }
    
    private String createFunctionCallContextSuggestion(Token unexpected) {
        return switch (unexpected.type()) {
            case SEMICOLON -> "Semicolon not allowed inside function call. Close the function with ')' first.";
            case FROM -> "FROM keyword inside function call. Close the function with ')' before continuing the query.";
            case WHERE -> "WHERE keyword inside function call. Close the function with ')' before continuing the query.";
            default -> "Invalid token inside function call. Check function syntax and close with ')'.";
        };
    }
    
    /**
     * Create general token suggestions when context-specific ones don't apply
     */
    private String createGeneralTokenSuggestion(Token unexpected, String expectedDescription) {
        // First, check for malformed expressions with consecutive operators
        if (isOperatorToken(unexpected.type())) {
            String expressionContext = extractExpressionContext(unexpected);
            String malformedSuggestion = ParserSuggestionHelper.createMalformedExpressionSuggestion(
                expressionContext, unexpected.value());
            if (malformedSuggestion != null) {
                return malformedSuggestion;
            }
        }
        
        return switch (unexpected.type()) {
            case COMMA -> {
                if (expectedDescription.contains("parenthesis")) {
                    yield "You may be missing a closing parenthesis ')' before this comma.";
                } else {
                    yield "Check if this comma is in the right place. " + expectedDescription;
                }
            }
            case RPAREN -> "You may have an extra closing parenthesis or be missing an opening one.";
            case IDENTIFIER -> {
                if (isReservedKeyword(unexpected.value())) {
                    yield "'" + unexpected.value() + "' is a reserved keyword. Use a different name or quote it if needed.";
                } else {
                    yield expectedDescription;
                }
            }
            case STAR, DIVIDE, PLUS, MINUS, MODULO -> {
                yield "Operator '" + unexpected.value() + "' used incorrectly. " +
                      "Make sure it's between two operands: operand1 " + unexpected.value() + " operand2";
            }
            default -> expectedDescription;
        };
    }
    
    /**
     * Check if a token type represents an operator.
     */
    private boolean isOperatorToken(TokenType type) {
        return type == TokenType.STAR || type == TokenType.DIVIDE || 
               type == TokenType.PLUS || type == TokenType.MINUS ||
               type == TokenType.MODULO;
    }
    
    /**
     * Extract the expression context around an error token for analysis.
     * This looks for tokens before and after to reconstruct the expression.
     */
    private String extractExpressionContext(Token errorToken) {
        if (tokens == null || tokens.isEmpty()) {
            return "";
        }
        
        // Find the position of the error token in the token list
        int errorPos = -1;
        for (int i = 0; i < tokens.size(); i++) {
            Token token = tokens.get(i);
            if (token.position() == errorToken.position()) {
                errorPos = i;
                break;
            }
        }
        
        if (errorPos == -1) {
            return errorToken.value(); // Fallback if token not found
        }
        
        // Try to find the expression context by looking at surrounding tokens
        StringBuilder context = new StringBuilder();
        
        // Look backwards for the start of the expression
        int start = Math.max(0, errorPos - 3); // Look back a few tokens
        int end = Math.min(tokens.size() - 1, errorPos + 3); // Look ahead a few tokens
        
        for (int i = start; i <= end; i++) {
            Token token = tokens.get(i);
            if (token != null) {
                if (context.length() > 0) {
                    context.append(" ");
                }
                context.append(token.value());
            }
        }
        
        return context.toString().trim();
    }
    // Helper methods for generating function suggestions (used by context-aware methods)
    
    
    /**
     * Recovery strategies for different parsing contexts
     */
    public enum RecoveryStrategy {
        SKIP_TO_NEXT_STATEMENT,
        SKIP_TO_NEXT_CLAUSE,
        INSERT_MISSING_TOKEN,
        SYNCHRONIZE_PARENTHESES,
        SYNCHRONIZE_BRACKETS,
        RECOVER_EXPRESSION,
        RECOVER_FUNCTION_CALL
    }
    
    // Enhanced helper methods
    
    private String generateRecoverySuggestion(Token errorToken, RecoveryStrategy strategy) {
        return switch (strategy) {
            case SKIP_TO_NEXT_STATEMENT -> 
                "Try completing the current statement or start a new one with SELECT/SHOW.";
            case SKIP_TO_NEXT_CLAUSE -> 
                "Try completing the current clause (SELECT, FROM, WHERE, etc.) or move to the next one.";
            case INSERT_MISSING_TOKEN -> 
                "Insert the missing token and continue parsing.";
            case SYNCHRONIZE_PARENTHESES -> 
                "Check that all parentheses are properly balanced. Every '(' needs a matching ')'.";
            case SYNCHRONIZE_BRACKETS -> 
                "Check that all brackets are properly balanced. Every '[' needs a matching ']'.";
            case RECOVER_EXPRESSION -> 
                "Complete the expression with valid operators and operands.";
            case RECOVER_FUNCTION_CALL -> 
                "Complete the function call: function_name(arg1, arg2, ...).";
        };
    }
    
    private String generateSQLMigrationSuggestion(String sqlPattern) {
        return switch (sqlPattern.toUpperCase()) {
            case "CREATE TABLE", "CREATE VIEW" -> 
                "Use VIEW statements instead: VIEW my_view AS @SELECT ...";
            case "INSERT", "UPDATE", "DELETE" -> 
                "JFR query language is read-only. Use SELECT queries to analyze data.";
            case "UNION", "INTERSECT", "EXCEPT" -> 
                "Use multiple statements separated by semicolons instead.";
            case "SUBQUERY_IN_SELECT" -> 
                "Move subqueries to the FROM clause or use variables.";
            default -> 
                "Check the JFR query language documentation for supported syntax.";
        };
    }
    
    /**
     * Generate suggestions for function argument mismatches
     */
    private String generateFunctionArgumentSuggestion(String functionName, int expected, int actual) {
        FunctionRegistry registry = FunctionRegistry.getInstance();
        FunctionRegistry.FunctionDefinition funcDef = registry.getFunction(functionName.toUpperCase());
        
        StringBuilder sb = new StringBuilder();
        
        if (actual < expected) {
            sb.append("Add ").append(expected - actual).append(" more argument(s) to ").append(functionName).append(" function.");
        } else if (actual > expected) {
            sb.append("Remove ").append(actual - expected).append(" argument(s) from ").append(functionName).append(" function.");
        }
        
        // Add function signature and parameter information if available
        if (funcDef != null) {
            sb.append(" Expected signature: ").append(funcDef.signature());
            
            if (!funcDef.parameters().isEmpty()) {
                sb.append("\nParameters:");
                for (int i = 0; i < funcDef.parameters().size(); i++) {
                    FunctionRegistry.ParameterDefinition param = funcDef.parameters().get(i);
                    sb.append("\n  ").append(i + 1).append(". ").append(param.name());
                    if (param.optional()) sb.append(" (optional)");
                    if (param.variadic()) sb.append(" (variadic)");
                    sb.append(": ").append(param.type().name().toLowerCase());
                    if (param.description() != null) {
                        sb.append(" - ").append(param.description());
                    }
                }
            }
            
            if (funcDef.examples() != null && !funcDef.examples().isEmpty()) {
                sb.append("\nExamples: ").append(funcDef.examples());
            }
        } else {
            sb.append(" Check function documentation for required parameters.");
        }
        
        return sb.toString();
    }
    
    private ErrorType inferErrorType(Token errorToken, String message) {
        String msg = message.toLowerCase();
        if (msg.contains("parenthesis") || msg.contains("(") || msg.contains(")")) {
            return ErrorType.MISSING_PARENTHESIS;
        } else if (msg.contains("bracket") || msg.contains("[") || msg.contains("]")) {
            return ErrorType.MISSING_BRACKET;
        } else if (msg.contains("comma") || msg.contains(",")) {
            return ErrorType.MISSING_COMMA;
        } else if (msg.contains("function")) {
            return ErrorType.INVALID_FUNCTION_CALL;
        } else if (msg.contains("expression")) {
            return ErrorType.INVALID_EXPRESSION;
        } else if (msg.contains("identifier")) {
            return ErrorType.INVALID_IDENTIFIER;
        }
        return ErrorType.SYNTAX_ERROR;
    }
    
    private String extractEnhancedContext(Token errorToken) {
        if (originalQuery == null || originalQuery.isEmpty()) {
            return extractSurroundingTokens(errorToken);
        }
        
        // Get more context - full line plus surrounding
        String[] lines = originalQuery.split("\n");
        int line = errorToken.line() - 1; // Convert to 0-based
        
        StringBuilder context = new StringBuilder();
        
        // Add line before if exists
        if (line > 0) {
            context.append("  ").append(line).append(": ").append(lines[line - 1]).append("\n");
        }
        
        // Add current line with error marker
        if (line >= 0 && line < lines.length) {
            context.append("→ ").append(line + 1).append(": ").append(lines[line]).append("\n");
            
            // Add column indicator
            int column = errorToken.column() - 1;
            context.append("  ").append(" ".repeat(String.valueOf(line + 1).length() + 2));
            if (column > 0) {
                context.append(" ".repeat(Math.max(0, column - 1)));
            }
            context.append("^^^").append("\n");
        }
        
        // Add line after if exists
        if (line + 1 < lines.length) {
            context.append("  ").append(line + 2).append(": ").append(lines[line + 1]).append("\n");
        }
        
        return context.toString().trim();
    }
    
    private String extractSurroundingTokens(Token errorToken) {
        int tokenIndex = findTokenIndex(errorToken);
        if (tokenIndex < 0) return "Token not found in stream";
        
        StringBuilder context = new StringBuilder();
        
        // Show 3 tokens before
        int start = Math.max(0, tokenIndex - 3);
        for (int i = start; i < tokenIndex; i++) {
            context.append(tokens.get(i).value()).append(" ");
        }
        
        // Mark error token
        context.append(">>>").append(errorToken.value()).append("<<< ");
        
        // Show 3 tokens after
        int end = Math.min(tokens.size(), tokenIndex + 4);
        for (int i = tokenIndex + 1; i < end; i++) {
            context.append(tokens.get(i).value()).append(" ");
        }
        
        return context.toString().trim();
    }
    
    /**
     * Create error with automatic recovery suggestions
     */
    public ParserError createErrorWithRecovery(Token errorToken, String message, RecoveryStrategy strategy) {
        String enhancedMessage = message;
        String suggestion = generateRecoverySuggestion(errorToken, strategy);
        String context = extractEnhancedContext(errorToken);
        ErrorType type = inferErrorType(errorToken, message);
        
        return new ParserError(enhancedMessage, suggestion, errorToken, context, type);
    }
    
    /**
     * Create error with contextual recovery based on parsing context
     */
    public ParserError createContextualError(Token errorToken, String message, String parsingContext) {
        RecoveryStrategy strategy = determineContextualRecoveryStrategy(parsingContext);
        return createErrorWithRecovery(errorToken, message, strategy);
    }
    
    /**
     * Create error for SQL compatibility issues with migration help
     */
    public ParserError createSQLMigrationError(Token errorToken, String sqlConstruct) {
        String message = "SQL construct '" + sqlConstruct + "' not supported in JFR query language";
        String suggestion = generateSQLMigrationSuggestion(sqlConstruct);
        String context = extractContext(errorToken);
        
        return new ParserError(message, suggestion, errorToken, context, ErrorType.SYNTAX_ERROR);
    }
    
    /**
     * Create error for function argument count mismatches
     */
    public ParserError createFunctionArgumentError(Token functionToken, String functionName, int expected, int actual) {
        String message = String.format("Function '%s' expects %d argument(s) but received %d", 
                                      functionName, expected, actual);
        String suggestion = generateFunctionArgumentSuggestion(functionName, expected, actual);
        String context = extractContext(functionToken);
        
        return new ParserError(message, suggestion, functionToken, context, ErrorType.INVALID_FUNCTION_CALL);
    }
    
    /**
     * Create error for bracket/parenthesis mismatches with detailed recovery
     */
    public ParserError createBracketMismatchError(Token errorToken, String expectedBracket, String foundBracket) {
        String message = String.format("Bracket mismatch: expected '%s' but found '%s'", 
                                      expectedBracket, foundBracket);
        RecoveryStrategy strategy = expectedBracket.equals(")") ? 
            RecoveryStrategy.SYNCHRONIZE_PARENTHESES : RecoveryStrategy.SYNCHRONIZE_BRACKETS;
        
        return createErrorWithRecovery(errorToken, message, strategy);
    }
    
    /**
     * Enhanced pattern detection for complex query issues
     */
    public List<String> detectAdvancedQueryIssues(List<Token> allTokens) {
        List<String> issues = new ArrayList<>();
        
        // Check for nested function calls depth
        int maxNestingDepth = calculateMaxNestingDepth(allTokens);
        if (maxNestingDepth > 5) {
            issues.add("Deep function nesting detected (depth: " + maxNestingDepth + "). Consider simplifying with variables.");
        }
        
        // Check for SQL-style syntax
        if (containsSQLPatterns(allTokens)) {
            issues.add("SQL-style syntax detected. JFR query language has different syntax - check documentation.");
        }
        
        // Check for common typos in function names
        for (Token token : allTokens) {
            if (token.type() == TokenType.IDENTIFIER) {
                String typoSuggestion = generateTypoSuggestions(token.value());
                if (typoSuggestion != null) {
                    issues.add("Possible typo in '" + token.value() + "': " + typoSuggestion);
                }
            }
        }
        
        // Check for unbalanced quotes
        if (hasUnbalancedQuotes(allTokens)) {
            issues.add("Unbalanced string quotes detected. Check that all string literals are properly closed.");
        }
        
        return issues;
    }
    
    /**
     * Calculate maximum nesting depth of parentheses/brackets
     */
    private int calculateMaxNestingDepth(List<Token> tokens) {
        int currentDepth = 0;
        int maxDepth = 0;
        
        for (Token token : tokens) {
            switch (token.type()) {
                case LPAREN, LBRACKET -> {
                    currentDepth++;
                    maxDepth = Math.max(maxDepth, currentDepth);
                }
                case RPAREN, RBRACKET -> currentDepth = Math.max(0, currentDepth - 1);
                default -> { /* No action needed for other token types */ }
            }
        }
        
        return maxDepth;
    }
    
    /**
     * Check for SQL-style patterns that don't work in JFR query language
     */
    private boolean containsSQLPatterns(List<Token> tokens) {
        for (int i = 0; i < tokens.size() - 1; i++) {
            Token current = tokens.get(i);
            Token next = tokens.get(i + 1);
            
            // Check for SQL-style CREATE TABLE, INSERT, UPDATE, DELETE
            if (current.type() == TokenType.IDENTIFIER) {
                String value = current.value().toUpperCase();
                if (value.equals("CREATE") || value.equals("INSERT") || 
                    value.equals("UPDATE") || value.equals("DELETE")) {
                    return true;
                }
                
                // Check for SQL-style INNER JOIN, LEFT JOIN etc.
                if (value.equals("INNER") || value.equals("LEFT") || value.equals("RIGHT")) {
                    if (next.type() == TokenType.IDENTIFIER && next.value().toUpperCase().equals("JOIN")) {
                        return true;
                    }
                }
            }
        }
        return false;
    }
    
    /**
     * Check for unbalanced quotes in string literals
     */
    private boolean hasUnbalancedQuotes(List<Token> tokens) {
        // This is a simple heuristic - in practice, the lexer should handle this
        return tokens.stream()
                .filter(t -> t.type() == TokenType.STRING)
                .anyMatch(t -> {
                    String value = t.value();
                    return !value.startsWith("\"") || !value.endsWith("\"") ||
                           !value.startsWith("'") || !value.endsWith("'");
                });
    }
    
    /**
     * Generate suggestions for common typos and mistakes
     */
    public String generateTypoSuggestions(String token) {
        String upperToken = token.toUpperCase();
        
        // First check if it's a known function with a typo
        List<String> similarFunctions = findSimilarFunctionNames(upperToken);
        if (!similarFunctions.isEmpty()) {
            return "Did you mean function '" + similarFunctions.get(0) + "'?";
        }
        
        // Common typos for keywords
        return switch (upperToken) {
            case "SELCT", "SLECT", "SELET" -> "Did you mean 'SELECT'?";
            case "FORM", "ROM" -> "Did you mean 'FROM'?";
            case "WERE", "WHE", "WHRE" -> "Did you mean 'WHERE'?";
            case "JION", "JOI" -> "Did you mean 'JOIN'?";
            case "GRUOP", "GROP" -> "Did you mean 'GROUP'?";
            case "ODER", "ORDR" -> "Did you mean 'ORDER'?";
            case "COUN", "COUT" -> "Did you mean 'COUNT'?";
            case "FUZZT", "FUZY" -> "Did you mean 'FUZZY'?";
            default -> null;
        };
    }
    
    /**
     * Check for common query pattern issues
     */
    public String detectQueryPatternIssues(List<Token> allTokens) {
        List<String> issues = new ArrayList<>();
        
        // Check for missing SELECT
        if (allTokens.isEmpty() || !allTokens.get(0).type().toString().contains("SELECT")) {
            issues.add("Query should start with SELECT or @SELECT");
        }
        
        // Check for unbalanced parentheses
        int parenBalance = 0;
        int bracketBalance = 0;
        for (Token token : allTokens) {
            switch (token.type()) {
                case LPAREN -> parenBalance++;
                case RPAREN -> parenBalance--;
                case LBRACKET -> bracketBalance++;
                case RBRACKET -> bracketBalance--;
                default -> { /* No action needed for other token types */ }
            }
        }
        
        if (parenBalance != 0) {
            issues.add(parenBalance > 0 ? "Missing closing parenthesis ')'" : "Extra closing parenthesis ')'");
        }
        
        if (bracketBalance != 0) {
            issues.add(bracketBalance > 0 ? "Missing closing bracket ']'" : "Extra closing bracket ']'");
        }
        
        // Check for multiple FROM clauses
        long fromCount = allTokens.stream()
                .filter(t -> t.type() == TokenType.FROM)
                .count();
        if (fromCount > 1) {
            issues.add("Multiple FROM clauses detected - use JOINs instead");
        }
        
        return issues.isEmpty() ? null : String.join("; ", issues);
    }
    
    /**
     * Extracts a snippet of the original query around the error position, highlighting the error.
     * Ensures the full error token and surrounding context are visible without truncation.
     */
    public String extractContext(Token errorToken) {
        if (originalQuery == null || originalQuery.isEmpty()) {
            return null;
        }
        
        // Find the error token in our token list to get its exact position
        int errorTokenIndex = findTokenIndex(errorToken);
        
        if (errorTokenIndex == -1) {
            // Fallback if token not found
            return extractContextFallback(errorToken);
        }
        
        // Determine how many tokens to show before and after
        int tokensToShow = 5; // Show 5 tokens before and after the error token
        int startTokenIndex = Math.max(0, errorTokenIndex - tokensToShow);
        int endTokenIndex = Math.min(tokens.size() - 1, errorTokenIndex + tokensToShow);
        
        // Build context using complete tokens
        StringBuilder contextBuilder = new StringBuilder();
        int contextStartPos = -1;
        boolean errorTokenProcessed = false;
        
        for (int i = startTokenIndex; i <= endTokenIndex; i++) {
            Token token = tokens.get(i);
            
            // Skip whitespace and comment tokens for cleaner context
            if (token.type() == TokenType.WHITESPACE || 
                token.type() == TokenType.COMMENT) {
                continue;
            }
            
            if (contextStartPos == -1) {
                contextStartPos = token.position();
            }
            
            if (contextBuilder.length() > 0) {
                contextBuilder.append(" ");
            }
            
            if (i == errorTokenIndex) {
                // Handle EOF and empty tokens specially
                String tokenValue = token.value();
                if (tokenValue == null || tokenValue.isEmpty() || token.type() == TokenType.EOF) {
                    // For EOF or empty tokens, show cursor at end of previous content
                    contextBuilder.append(">>><<<");
                } else {
                    contextBuilder.append(">>>").append(tokenValue).append("<<<");
                }
                errorTokenProcessed = true;
            } else {
                String tokenValue = token.value();
                if (tokenValue != null && !tokenValue.isEmpty() && token.type() != TokenType.EOF) {
                    contextBuilder.append(tokenValue);
                }
            }
        }
        
        // If error token wasn't processed (e.g., EOF at end), add cursor at end
        if (!errorTokenProcessed && errorToken.type() == TokenType.EOF) {
            if (contextBuilder.length() > 0) {
                contextBuilder.append(" ");
            }
            contextBuilder.append(">>><<<");
        }
        
        // If we didn't find meaningful tokens, use fallback
        if (contextBuilder.length() == 0) {
            return extractContextFallback(errorToken);
        }
        
        return "→ " + (contextStartPos + 1) + ": " + contextBuilder.toString();
    }
    
    /**
     * Fallback context extraction for cases where token-based approach fails
     */
    private String extractContextFallback(Token errorToken) {
        int tokenStart = errorToken.position();
        String tokenValue = errorToken.value();
        int tokenLength = tokenValue != null ? tokenValue.length() : 0;
        int tokenEnd = tokenStart + tokenLength;
        
        // For EOF or empty tokens, show position as cursor
        if (tokenLength == 0 || tokenStart >= originalQuery.length() || 
            errorToken.type() == TokenType.EOF) {
            int contextStart = Math.max(0, Math.min(tokenStart, originalQuery.length()) - 30);
            String beforeContext = originalQuery.substring(contextStart, Math.min(tokenStart, originalQuery.length()));
            return "→ " + (contextStart + 1) + ": " + beforeContext.trim() + " >>><<<";
        }
        
        // Context window: 30 chars before token start, 30 chars after token end
        int contextStart = Math.max(0, tokenStart - 30);
        int contextEnd = Math.min(originalQuery.length(), tokenEnd + 30);
        
        // Adjust start to avoid cutting tokens in half - find whitespace boundary
        while (contextStart > 0 && contextStart < tokenStart && 
               !Character.isWhitespace(originalQuery.charAt(contextStart - 1))) {
            contextStart--;
        }
        
        // Adjust end to avoid cutting tokens in half - find whitespace boundary  
        while (contextEnd < originalQuery.length() && contextEnd > tokenEnd &&
               !Character.isWhitespace(originalQuery.charAt(contextEnd))) {
            contextEnd++;
        }
        
        String context = originalQuery.substring(contextStart, contextEnd);
        
        // Add markers to highlight the error token within the context
        int errorPosInContext = tokenStart - contextStart;
        int errorLengthInContext = tokenEnd - tokenStart;
        
        if (errorPosInContext >= 0 && errorPosInContext < context.length()) {
            String before = context.substring(0, errorPosInContext);
            String errorPart = context.substring(errorPosInContext, 
                Math.min(context.length(), errorPosInContext + errorLengthInContext));
            String after = context.substring(Math.min(context.length(), errorPosInContext + errorLengthInContext));
            
            context = before + ">>>" + errorPart + "<<<" + after;
        }
        
        return "→ " + (contextStart + 1) + ": " + context.trim();
    }
    
    /**
     * Find the index of the given token in our token list using reference equality
     */

    /**
     * Maps a token type to a specific error type for better error categorization.
     */
    public ErrorType determineErrorType(TokenType expected) {
        return switch (expected) {
            case RPAREN, LPAREN -> ErrorType.MISSING_PARENTHESIS;
            case RBRACKET, LBRACKET -> ErrorType.MISSING_BRACKET;
            case COMMA -> ErrorType.MISSING_COMMA;
            case IDENTIFIER -> ErrorType.INVALID_IDENTIFIER;
            default -> ErrorType.MISSING_TOKEN;
        };
    }
    
    /**
     * Find similar function names using string distance algorithms
     */
    private List<String> findSimilarFunctionNames(String inputFunction) {
        FunctionRegistry registry = FunctionRegistry.getInstance();
        List<String> allFunctions = new ArrayList<>(registry.getFunctionNames());
        List<String> suggestions = new ArrayList<>();
        
        String input = inputFunction.toUpperCase();
        
        for (String funcName : allFunctions) {
            String func = funcName.toUpperCase();
            
            // Exact prefix match (high priority)
            if (func.startsWith(input) || input.startsWith(func)) {
                suggestions.add(funcName);
                continue;
            }
            
            // Check for common abbreviation patterns
            if (isCommonAbbreviation(input, func)) {
                suggestions.add(funcName);
                continue;
            }
            
            // Levenshtein distance with adaptive threshold
            int maxDistance = Math.max(2, Math.min(input.length(), func.length()) / 3);
            if (calculateLevenshteinDistance(input, func) <= maxDistance) {
                suggestions.add(funcName);
            }
        }
        
        // Sort by similarity score and limit to top 3 suggestions
        return suggestions.stream()
            .sorted((a, b) -> Integer.compare(
                calculateSimilarityScore(input, a.toUpperCase()),
                calculateSimilarityScore(input, b.toUpperCase())
            ))
            .limit(3)
            .toList();
    }
    
    /**
     * Check if input looks like a common abbreviation or extension of the function
     */
    private boolean isCommonAbbreviation(String input, String func) {
        // Check if input is the function with common suffixes removed/added
        // e.g., "AVERAG" -> "AVG", "COUT" -> "COUNT", "SUMM" -> "SUM"
        
        // Input might be function + common suffix
        if (input.startsWith(func)) {
            String suffix = input.substring(func.length());
            return suffix.matches("^(E|ER|AGE|ING|S|ED|AL|IC|LY)?$");
        }
        
        // Function might be input + common suffix
        if (func.startsWith(input)) {
            String suffix = func.substring(input.length());
            return suffix.matches("^(E|ER|AGE|ING|S|ED|AL|IC|LY)?$");
        }
        
        // Special handling for common function name patterns
        // Handle cases like "AVERAG" -> "AVG" (user started typing "AVERAGE" but meant "AVG")
        if (input.length() > func.length()) {
            // Check for specific common patterns
            if (func.equals("AVG") && input.matches("^AVER(AG|AGE)$")) {
                return true;
            }
            if (func.equals("COUNT") && input.matches("^COUN?T?$")) {
                return true;
            }
            if (func.equals("SUM") && input.matches("^SUMM?$")) {
                return true;
            }
        }
        
        // Check if they share a significant common prefix (at least 2 characters for short functions)
        int commonPrefix = 0;
        int minLen = Math.min(input.length(), func.length());
        for (int i = 0; i < minLen; i++) {
            if (input.charAt(i) == func.charAt(i)) {
                commonPrefix++;
            } else {
                break;
            }
        }
        
        // For short functions (3 chars or less), require at least 2 matching prefix chars
        // For longer functions, require at least 75% of the shorter string
        if (minLen <= 3) {
            return commonPrefix >= 2;
        } else {
            return commonPrefix >= 3 && commonPrefix >= minLen * 0.75;
        }
    }
    
    /**
     * Calculate similarity score (lower is better)
     */
    private int calculateSimilarityScore(String input, String func) {
        // Prefer shorter functions with good prefix match
        int prefixScore = 0;
        int minLen = Math.min(input.length(), func.length());
        for (int i = 0; i < minLen; i++) {
            if (input.charAt(i) == func.charAt(i)) {
                prefixScore++;
            } else {
                break;
            }
        }
        
        int lengthDiff = Math.abs(input.length() - func.length());
        int levenshteinDist = calculateLevenshteinDistance(input, func);
        
        // Score combines distance with preference for prefix matches and similar length
        return levenshteinDist * 10 - prefixScore * 5 + lengthDiff;
    }
    
    /**
     * Calculate Levenshtein distance between two strings
     */
    private int calculateLevenshteinDistance(String s1, String s2) {
        int len1 = s1.length();
        int len2 = s2.length();
        
        int[][] dp = new int[len1 + 1][len2 + 1];
        
        for (int i = 0; i <= len1; i++) {
            dp[i][0] = i;
        }
        for (int j = 0; j <= len2; j++) {
            dp[0][j] = j;
        }
        
        for (int i = 1; i <= len1; i++) {
            for (int j = 1; j <= len2; j++) {
                if (s1.charAt(i - 1) == s2.charAt(j - 1)) {
                    dp[i][j] = dp[i - 1][j - 1];
                } else {
                    dp[i][j] = 1 + Math.min(dp[i - 1][j], Math.min(dp[i][j - 1], dp[i - 1][j - 1]));
                }
            }
        }
        
        return dp[len1][len2];
    }
    
    /**
     * Apply recovery strategy and return suggested token position
     */
    public int applyRecoveryStrategy(RecoveryStrategy strategy, Token errorToken, List<Token> tokens) {
        int tokenIndex = findTokenIndex(errorToken);
        if (tokenIndex < 0) return tokens.size(); // Skip to end if token not found
        
        return switch (strategy) {
            case SKIP_TO_NEXT_STATEMENT -> recoverToNextStatement(tokenIndex, tokens);
            case SKIP_TO_NEXT_CLAUSE -> recoverToNextClause(tokenIndex, tokens);
            case INSERT_MISSING_TOKEN -> tokenIndex; // Stay at current position for insertion
            case SYNCHRONIZE_PARENTHESES -> synchronizeParentheses(tokenIndex, tokens);
            case SYNCHRONIZE_BRACKETS -> synchronizeBrackets(tokenIndex, tokens);
            case RECOVER_EXPRESSION -> recoverExpression(tokenIndex, tokens);
            case RECOVER_FUNCTION_CALL -> recoverFunctionCall(tokenIndex, tokens);
        };
    }
    
    private int recoverToNextStatement(int tokenIndex, List<Token> tokens) {
        for (int i = tokenIndex; i < tokens.size(); i++) {
            Token token = tokens.get(i);
            if (token.type() == TokenType.SEMICOLON || 
                token.type() == TokenType.SELECT || 
                token.type() == TokenType.SHOW ||
                token.type() == TokenType.VIEW ||
                (token.type() == TokenType.IDENTIFIER && i + 1 < tokens.size() && 
                 tokens.get(i + 1).type() == TokenType.ASSIGN)) {
                return i;
            }
        }
        return tokens.size();
    }
    
    private int recoverToNextClause(int tokenIndex, List<Token> tokens) {
        for (int i = tokenIndex; i < tokens.size(); i++) {
            Token token = tokens.get(i);
            if (token.type() == TokenType.FROM || 
                token.type() == TokenType.WHERE ||
                token.type() == TokenType.GROUP_BY ||
                token.type() == TokenType.ORDER_BY ||
                token.type() == TokenType.LIMIT ||
                token.type() == TokenType.SEMICOLON) {
                return i;
            }
        }
        return tokens.size();
    }
    
    private int synchronizeParentheses(int tokenIndex, List<Token> tokens) {
        int parenCount = 0;
        boolean foundOpenParen = false;
        
        // Look backwards to find unmatched opening parenthesis
        for (int i = tokenIndex - 1; i >= 0; i--) {
            if (tokens.get(i).type() == TokenType.RPAREN) parenCount++;
            else if (tokens.get(i).type() == TokenType.LPAREN) {
                parenCount--;
                if (parenCount < 0) {
                    foundOpenParen = true;
                    break;
                }
            }
        }
        
        if (!foundOpenParen) return tokenIndex;
        
        // Look forward to find appropriate closing position
        parenCount = 1; // We have one unmatched opening paren
        for (int i = tokenIndex; i < tokens.size(); i++) {
            if (tokens.get(i).type() == TokenType.LPAREN) parenCount++;
            else if (tokens.get(i).type() == TokenType.RPAREN) parenCount--;
            
            if (parenCount == 0) {
                return i + 1; // Position after the closing paren
            }
            
            // Don't go past major boundaries
            if (tokens.get(i).type() == TokenType.SEMICOLON || 
                tokens.get(i).type() == TokenType.FROM ||
                tokens.get(i).type() == TokenType.WHERE) {
                return i;
            }
        }
        
        return tokens.size();
    }
    
    private int synchronizeBrackets(int tokenIndex, List<Token> tokens) {
        int bracketCount = 0;
        boolean foundOpenBracket = false;
        
        // Look backwards to find unmatched opening bracket
        for (int i = tokenIndex - 1; i >= 0; i--) {
            if (tokens.get(i).type() == TokenType.RBRACKET) bracketCount++;
            else if (tokens.get(i).type() == TokenType.LBRACKET) {
                bracketCount--;
                if (bracketCount < 0) {
                    foundOpenBracket = true;
                    break;
                }
            }
        }
        
        if (!foundOpenBracket) return tokenIndex;
        
        // Look forward to find appropriate closing position
        bracketCount = 1; // We have one unmatched opening bracket
        for (int i = tokenIndex; i < tokens.size(); i++) {
            if (tokens.get(i).type() == TokenType.LBRACKET) bracketCount++;
            else if (tokens.get(i).type() == TokenType.RBRACKET) bracketCount--;
            
            if (bracketCount == 0) {
                return i + 1; // Position after the closing bracket
            }
            
            // Don't go past major boundaries
            if (tokens.get(i).type() == TokenType.SEMICOLON || 
                tokens.get(i).type() == TokenType.FROM ||
                tokens.get(i).type() == TokenType.WHERE) {
                return i;
            }
        }
        
        return tokens.size();
    }
    
    private int recoverExpression(int tokenIndex, List<Token> tokens) {
        // Skip to the next comma, closing paren, or clause boundary
        for (int i = tokenIndex; i < tokens.size(); i++) {
            Token token = tokens.get(i);
            if (token.type() == TokenType.COMMA ||
                token.type() == TokenType.RPAREN ||
                token.type() == TokenType.RBRACKET ||
                token.type() == TokenType.AND ||
                token.type() == TokenType.OR ||
                token.type() == TokenType.FROM ||
                token.type() == TokenType.WHERE ||
                token.type() == TokenType.GROUP_BY ||
                token.type() == TokenType.ORDER_BY ||
                token.type() == TokenType.LIMIT ||
                token.type() == TokenType.SEMICOLON) {
                return i;
            }
        }
        return tokens.size();
    }
    
    private int recoverFunctionCall(int tokenIndex, List<Token> tokens) {
        // Look for the end of the function call (closing parenthesis)
        int parenCount = 0;
        boolean inFunction = false;
        
        for (int i = tokenIndex; i < tokens.size(); i++) {
            Token token = tokens.get(i);
            
            if (token.type() == TokenType.LPAREN) {
                parenCount++;
                inFunction = true;
            } else if (token.type() == TokenType.RPAREN) {
                parenCount--;
                if (inFunction && parenCount == 0) {
                    return i + 1; // Position after closing parenthesis
                }
            }
            
            // Don't go past major boundaries
            if (token.type() == TokenType.SEMICOLON ||
                token.type() == TokenType.FROM ||
                token.type() == TokenType.WHERE) {
                return i;
            }
        }
        
        return tokens.size();
    }
    
    private String createFunctionCallExamples(Token functionToken) {
        String funcName = functionToken.value().toUpperCase();
        FunctionRegistry registry = FunctionRegistry.getInstance();
        FunctionRegistry.FunctionDefinition funcDef = registry.getFunction(funcName);
        
        if (funcDef != null && funcDef.examples() != null && !funcDef.examples().isEmpty()) {
            return funcDef.examples();
        }
        
        // Generate basic examples for common functions
        return switch (funcName) {
            case "COUNT" -> "COUNT(*), COUNT(field)";
            case "SUM" -> "SUM(field), SUM(duration)";
            case "AVG" -> "AVG(field), AVG(duration)";
            case "MIN" -> "MIN(field), MIN(timestamp)";
            case "MAX" -> "MAX(field), MAX(duration)";
            case "UPPER" -> "UPPER(fieldName)";
            case "LOWER" -> "LOWER(fieldName)";
            case "YEAR" -> "YEAR(timestamp)";
            case "MONTH" -> "MONTH(timestamp)";
            case "DAY" -> "DAY(timestamp)";
            default -> null;
        };
    }
    
    private boolean isReservedKeyword(String value) {
        // Check against common reserved keywords
        Set<String> keywords = Set.of(
            "SELECT", "FROM", "WHERE", "GROUP", "BY", "ORDER", "LIMIT", "AS", 
            "AND", "OR", "IN", "NOT", "LIKE", "JOIN", "LEFT", "RIGHT", "INNER", 
            "FULL", "ON", "WITH", "SHOW", "VIEW", "FUZZY", "NEAREST", "PREVIOUS", "AFTER"
        );
        return keywords.contains(value.toUpperCase());
    }
    
    private int findTokenIndex(Token token) {
        for (int i = 0; i < tokens.size(); i++) {
            if (tokens.get(i) == token) {
                return i;
            }
        }
        return -1;
    }
    
    private String getTokenDescription(TokenType type) {
        return switch (type) {
            case IDENTIFIER -> "identifier";
            case NUMBER -> "number";
            case STRING -> "string";
            case LPAREN -> "opening parenthesis '('";
            case RPAREN -> "closing parenthesis ')'";
            case LBRACKET -> "opening bracket '['";
            case RBRACKET -> "closing bracket ']'";
            case COMMA -> "comma ','";
            case SEMICOLON -> "semicolon ';'";

            case EQUALS -> "equals sign '='";
            case FROM -> "FROM keyword";
            case SELECT -> "SELECT keyword";
            case WHERE -> "WHERE keyword";
            case AND -> "AND keyword";
            case OR -> "OR keyword";
            default -> type.toString().toLowerCase();
        };
    }
    
    /**
     * Create context-aware error with automatic context detection and recovery suggestions
     */
    public ParserError createContextAwareError(Token errorToken, String message, String expectedDescription) {
        QueryContext queryContext = detectQueryContext(errorToken);
        String contextAwareMessage = enhanceMessageWithContext(message, queryContext);
        String contextAwareSuggestion = createContextSpecificSuggestion(errorToken, queryContext);
        
        // If no context-specific suggestion, fall back to general suggestion
        if (contextAwareSuggestion == null) {
            contextAwareSuggestion = createGeneralTokenSuggestion(errorToken, expectedDescription);
        }
        
        String context = extractContext(errorToken);
        ErrorType type = inferErrorType(errorToken, message);
        
        return new ParserError(contextAwareMessage, contextAwareSuggestion, errorToken, context, type);
    }
    
    /**
     * Enhance error message with context information
     */
    private String enhanceMessageWithContext(String message, QueryContext queryContext) {
        String contextSuffix = switch (queryContext) {
            case SELECT_CLAUSE -> " in SELECT clause";
            case FROM_CLAUSE -> " in FROM clause";
            case WHERE_CLAUSE -> " in WHERE condition";
            case GROUP_BY_CLAUSE -> " in GROUP BY clause";
            case ORDER_BY_CLAUSE -> " in ORDER BY clause";
            case HAVING_CLAUSE -> " in HAVING condition";
            case FUNCTION_CALL -> " in function call";
            default -> "";
        };
        
        return message + contextSuffix;
    }
    
    /**
     * Advanced recovery strategy determination based on parsing context
     */
    private RecoveryStrategy determineContextualRecoveryStrategy(String context) {
        String ctx = context.toLowerCase();
        if (ctx.contains("function") || ctx.contains("call")) {
            return RecoveryStrategy.RECOVER_FUNCTION_CALL;
        } else if (ctx.contains("expression") || ctx.contains("operator")) {
            return RecoveryStrategy.RECOVER_EXPRESSION;
        } else if (ctx.contains("statement") || ctx.contains("query")) {
            return RecoveryStrategy.SKIP_TO_NEXT_STATEMENT;
        } else if (ctx.contains("clause") || ctx.contains("select") || ctx.contains("from") || ctx.contains("where")) {
            return RecoveryStrategy.SKIP_TO_NEXT_CLAUSE;
        } else {
            return RecoveryStrategy.SKIP_TO_NEXT_CLAUSE;
        }
    }
}
