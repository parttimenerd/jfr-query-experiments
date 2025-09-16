package me.bechberger.jfr.extended;

import me.bechberger.jfr.extended.ast.ASTNodes.*;
import me.bechberger.jfr.extended.engine.util.StringSimilarity;
import me.bechberger.jfr.extended.evaluator.FunctionRegistry;
import me.bechberger.jfr.extended.evaluator.FunctionRegistry.FunctionDefinition;
import me.bechberger.jfr.extended.evaluator.FunctionRegistry.ParameterDefinition;

import java.util.*;

/**
 * Generates intelligent error messages based on grammar rules and function information.
 * This class leverages all available information about the JFR query language to provide
 * helpful, context-aware error messages and suggestions.
 */
public class QueryErrorMessageGenerator {
    
    private final FunctionRegistry functionRegistry;
    
    // Grammar context for better error messages
    public enum QueryContext {
        SELECT_CLAUSE,
        FROM_CLAUSE,
        WHERE_CLAUSE,
        GROUP_BY_CLAUSE,
        HAVING_CLAUSE,
        ORDER_BY_CLAUSE,
        FUNCTION_CALL,
        EXPRESSION,
        CONDITION
    }
    
    public QueryErrorMessageGenerator() {
        this.functionRegistry = FunctionRegistry.getInstance();
    }
    
    /**
     * Generate intelligent function suggestion based on context and similarity
     */
    public String generateFunctionSuggestion(String invalidName, List<ExpressionNode> arguments, QueryContext context) {
        // Analyze the context to determine what type of function the user likely wants
        FunctionRegistry.FunctionType likelyType = inferFunctionTypeFromContext(arguments, context);
        
        // First, check for exact misspellings
        String exactMatch = findSimilarFunctionName(invalidName);
        if (exactMatch != null) {
            FunctionDefinition exactFunc = functionRegistry.getFunction(exactMatch);
            if (exactFunc != null && isContextAppropriate(exactFunc, context)) {
                return createFunctionSuggestionMessage(invalidName, exactMatch, exactFunc, arguments.size());
            }
        }
        
        // Get functions of the likely type
        List<FunctionDefinition> candidateFunctions = functionRegistry.getFunctionsByType(likelyType);
        if (candidateFunctions.isEmpty()) {
            candidateFunctions = new ArrayList<>(functionRegistry.getAllFunctions());
        }
        
        // Filter by context appropriateness and sort by similarity
        candidateFunctions = candidateFunctions.stream()
            .filter(f -> isContextAppropriate(f, context))
            .filter(f -> isArgumentCountCompatible(f, arguments.size()))
            .sorted((f1, f2) -> {
                int similarity1 = StringSimilarity.levenshteinDistanceIgnoreCase(invalidName, f1.name());
                int similarity2 = StringSimilarity.levenshteinDistanceIgnoreCase(invalidName, f2.name());
                return Integer.compare(similarity1, similarity2);
            })
            .limit(3)
            .toList();
        
        if (!candidateFunctions.isEmpty()) {
            return createMultipleSuggestionsMessage(invalidName, candidateFunctions, context);
        }
        
        // If no close matches, suggest by category
        return createCategorySuggestionMessage(likelyType, arguments.size(), context);
    }
    
    /**
     * Generate detailed argument validation error message
     */
    public String generateArgumentValidationError(FunctionDefinition function, List<ExpressionNode> arguments, 
                                                QueryContext context, ArgumentValidationError errorType) {
        return switch (errorType) {
            case TOO_FEW_ARGUMENTS -> createInsufficientArgumentsMessage(function, arguments.size(), context);
            case TOO_MANY_ARGUMENTS -> createTooManyArgumentsMessage(function, arguments.size(), context);
            case INVALID_ARGUMENT_TYPE -> createInvalidArgumentTypeMessage(function, arguments, context);
            case AGGREGATE_IN_WHERE -> createAggregateInWhereMessage(function, context);
        };
    }
    
    /**
     * Generate grammar-aware error message for syntax errors
     */
    public String generateGrammarError(String issue, QueryContext context, Token errorToken) {
        StringBuilder message = new StringBuilder();
        
        // Add context-specific guidance
        String contextGuidance = getContextSpecificGuidance(context);
        if (contextGuidance != null) {
            message.append(contextGuidance).append(" ");
        }
        
        message.append(issue);
        
        // Add recovery suggestions based on context
        String recoverySuggestion = getRecoverySuggestion(context, errorToken);
        if (recoverySuggestion != null) {
            message.append(" ").append(recoverySuggestion);
        }
        
        return message.toString();
    }
    
    /**
     * Check if an aggregate function is being used inappropriately in WHERE clause
     */
    public boolean isAggregateInWhere(String functionName, QueryContext context) {
        if (context != QueryContext.WHERE_CLAUSE) {
            return false;
        }
        
        FunctionDefinition function = functionRegistry.getFunction(functionName);
        return function != null && function.type() == FunctionRegistry.FunctionType.AGGREGATE;
    }
    
    /**
     * Generate error message for aggregate function in WHERE clause
     */
    public String createAggregateInWhereMessage(FunctionDefinition function, QueryContext context) {
        StringBuilder message = new StringBuilder();
        message.append("Aggregate function ").append(function.name()).append("() cannot be used in WHERE clause. ");
        message.append("Use it in SELECT or HAVING clause instead.\n");
        
        // Provide specific examples on new lines
        if ("COUNT".equals(function.name())) {
            message.append("Example: SELECT COUNT(*) FROM table HAVING COUNT(*) > 5");
        } else if ("AVG".equals(function.name())) {
            message.append("Example: SELECT AVG(field) FROM table GROUP BY category HAVING AVG(field) > 100");
        } else {
            message.append("Example: SELECT ").append(function.name()).append("(field) FROM table GROUP BY category");
        }
        
        return message.toString();
    }
    
    /**
     * Generate ParserErrorHandler.ParserError for function suggestion
     */
    public ParserErrorHandler.ParserError generateFunctionSuggestionError(String invalidName, List<ExpressionNode> arguments, 
                                                       QueryContext context, Token errorToken) {
        String message = "Unknown function '" + invalidName + "'";
        String suggestion = generateFunctionSuggestion(invalidName, arguments, context);
        String contextInfo = generateContextInfo(errorToken, invalidName);
        
        return new ParserErrorHandler.ParserError(message, suggestion, errorToken, contextInfo, ParserErrorHandler.ErrorType.INVALID_FUNCTION_CALL);
    }
    
    /**
     * Generate ParserErrorHandler.ParserError for argument validation
     */
    public ParserErrorHandler.ParserError generateArgumentValidationParserError(FunctionDefinition function, 
                                                             List<ExpressionNode> arguments, 
                                                             QueryContext context, 
                                                             ArgumentValidationError errorType,
                                                             Token errorToken) {
        String message = generateArgumentValidationError(function, arguments, context, errorType);
        String suggestion = generateArgumentSuggestion(function, arguments, errorType);
        String contextInfo = generateContextInfo(errorToken, function.name());
        
        return new ParserErrorHandler.ParserError(message, suggestion, errorToken, contextInfo, ParserErrorHandler.ErrorType.INVALID_FUNCTION_CALL);
    }
    
    /**
     * Generate ParserErrorHandler.ParserError for grammar errors
     */
    public ParserErrorHandler.ParserError generateGrammarParserError(String issue, QueryContext context, Token errorToken) {
        String message = generateGrammarError(issue, context, errorToken);
        String suggestion = getRecoverySuggestion(context, errorToken);
        String contextInfo = generateContextInfo(errorToken, null);
        
        return new ParserErrorHandler.ParserError(message, suggestion, errorToken, contextInfo, ParserErrorHandler.ErrorType.SYNTAX_ERROR);
    }
    
    /**
     * Generate ParserErrorHandler.ParserError for aggregate function in WHERE clause
     */
    public ParserErrorHandler.ParserError generateAggregateInWhereError(String functionName, Token errorToken) {
        String message = "Aggregate function '" + functionName + "' cannot be used in WHERE clause";
        String suggestion = "Use aggregate functions in SELECT or HAVING clause instead";
        String contextInfo = generateContextInfo(errorToken, functionName);
        
        return new ParserErrorHandler.ParserError(message, suggestion, errorToken, contextInfo, ParserErrorHandler.ErrorType.INVALID_FUNCTION_CALL);
    }
    
    /**
     * Generate context information showing the error location
     */
    private String generateContextInfo(Token errorToken, String highlightText) {
        if (errorToken == null) return null;
        
        // This would ideally show the query with the error location highlighted
        // For now, return a simple context indicator
        if (highlightText != null) {
            return "Near: " + highlightText + " <<<";
        }
        return "At position: line " + errorToken.line() + ", column " + errorToken.column();
    }
    
    /**
     * Generate argument-specific suggestions
     */
    private String generateArgumentSuggestion(FunctionDefinition function, List<ExpressionNode> arguments, 
                                            ArgumentValidationError errorType) {
        return switch (errorType) {
            case TOO_FEW_ARGUMENTS -> "Add the required arguments. Check function documentation for parameter details.";
            case TOO_MANY_ARGUMENTS -> "Remove extra arguments or check if you meant a different function.";
            case INVALID_ARGUMENT_TYPE -> "Check argument types match function expectations.";
            case AGGREGATE_IN_WHERE -> "Move aggregate functions to SELECT or HAVING clause.";
        };
    }

    // ===== PRIVATE HELPER METHODS =====
    
    /**
     * Infer the likely function type based on usage context
     */
    private FunctionRegistry.FunctionType inferFunctionTypeFromContext(List<ExpressionNode> arguments, QueryContext context) {
        // Context-based inference
        if (context == QueryContext.WHERE_CLAUSE) {
            // WHERE clauses typically use comparison, string, or mathematical functions
            return inferTypeFromArguments(arguments, FunctionRegistry.FunctionType.MATHEMATICAL);
        } else if (context == QueryContext.SELECT_CLAUSE) {
            // SELECT clauses can use aggregate functions
            return inferTypeFromArguments(arguments, FunctionRegistry.FunctionType.AGGREGATE);
        } else if (context == QueryContext.HAVING_CLAUSE) {
            // HAVING clauses typically use aggregate functions
            return FunctionRegistry.FunctionType.AGGREGATE;
        }
        
        return inferTypeFromArguments(arguments, FunctionRegistry.FunctionType.MATHEMATICAL);
    }
    
    /**
     * Infer function type from arguments with a default fallback
     */
    private FunctionRegistry.FunctionType inferTypeFromArguments(List<ExpressionNode> arguments, FunctionRegistry.FunctionType defaultType) {
        if (arguments.isEmpty()) {
            return FunctionRegistry.FunctionType.AGGREGATE; // COUNT(), etc.
        }
        
        if (arguments.size() == 1) {
            ExpressionNode arg = arguments.get(0);
            
            // Check if it's a field reference (likely aggregate)
            if (arg instanceof IdentifierNode || arg instanceof FieldAccessNode) {
                return FunctionRegistry.FunctionType.AGGREGATE;
            }
            
            // Check if it's a string literal (likely string function)
            if (arg instanceof LiteralNode literal && 
                literal.value() instanceof me.bechberger.jfr.extended.table.CellValue.StringValue) {
                return FunctionRegistry.FunctionType.STRING;
            }
            
            // Check if it's a numeric literal (likely mathematical function)
            if (arg instanceof LiteralNode literal && 
                literal.value() instanceof me.bechberger.jfr.extended.table.CellValue.NumberValue) {
                return FunctionRegistry.FunctionType.MATHEMATICAL;
            }
        }
        
        // Multiple arguments might be mathematical, string, or datetime
        if (arguments.size() >= 2) {
            return FunctionRegistry.FunctionType.MATHEMATICAL;
        }
        
        return defaultType;
    }
    
    /**
     * Check if a function is appropriate for the given context
     */
    private boolean isContextAppropriate(FunctionDefinition function, QueryContext context) {
        return switch (context) {
            case WHERE_CLAUSE -> function.type() != FunctionRegistry.FunctionType.AGGREGATE;
            case HAVING_CLAUSE -> true; // All functions allowed in HAVING
            case SELECT_CLAUSE -> true; // All functions allowed in SELECT
            case GROUP_BY_CLAUSE -> function.type() != FunctionRegistry.FunctionType.AGGREGATE;
            case ORDER_BY_CLAUSE -> true; // All functions allowed in ORDER BY
            default -> true;
        };
    }
    
    /**
     * Check if a function's argument count is compatible
     */
    private boolean isArgumentCountCompatible(FunctionDefinition function, int argCount) {
        List<ParameterDefinition> params = function.parameters();
        int requiredCount = (int) params.stream().filter(p -> !p.optional()).count();
        int maxCount = params.size();
        boolean hasVariadic = params.stream().anyMatch(ParameterDefinition::variadic);
        
        if (argCount < requiredCount) return false;
        if (!hasVariadic && argCount > maxCount) return false;
        
        return true;
    }
    
    /**
     * Find similar function name using Levenshtein distance
     */
    private String findSimilarFunctionName(String invalidName) {
        String upperInvalidName = invalidName.toUpperCase();
        
        // Check for common misspellings first
        Map<String, String> commonMisspellings = Map.of(
            "AVERAG", "AVG",
            "AVARAGE", "AVG", 
            "AVRAGE", "AVG",
            "UPPPER", "UPPER",
            "PERCENTILEX", "PERCENTILE",
            "YEAAR", "YEAR",
            "MOUNTH", "MONTH",
            "SECOUND", "SECOND"
        );
        
        if (commonMisspellings.containsKey(upperInvalidName)) {
            return commonMisspellings.get(upperInvalidName);
        }
        
        // Find closest match using Levenshtein distance
        String[] functionNames = functionRegistry.getAllFunctions().stream()
            .map(FunctionDefinition::name)
            .toArray(String[]::new);
        return StringSimilarity.findClosestMatch(invalidName, functionNames, 3, true);
    }

    /**
     * Create suggestion message for a single function
     */
    private String createFunctionSuggestionMessage(String invalidName, String suggestion, 
                                                 FunctionDefinition function, int argCount) {
        StringBuilder message = new StringBuilder();
        message.append("Did you mean '").append(suggestion).append("'? ");
        
        // Add brief description
        if (function.description() != null && !function.description().isEmpty()) {
            message.append(function.description()).append(" ");
        }
        
        // Add usage example
        if (function.examples() != null && !function.examples().isEmpty()) {
            message.append("Usage: ").append(function.examples());
        } else {
            message.append("Usage: ").append(generateBasicUsageExample(function));
        }
        
        return message.toString();
    }
    
    /**
     * Create message for multiple suggestions
     */
    private String createMultipleSuggestionsMessage(String invalidName, List<FunctionDefinition> suggestions, 
                                                  QueryContext context) {
        if (suggestions.size() == 1) {
            return createFunctionSuggestionMessage(invalidName, suggestions.get(0).name(), 
                                                 suggestions.get(0), 0);
        }
        
        StringBuilder message = new StringBuilder();
        message.append("Did you mean one of: ");
        
        List<String> functionNames = suggestions.stream()
            .map(f -> f.name() + "()")
            .toList();
        
        message.append(String.join(", ", functionNames)).append("?");
        
        return message.toString();
    }
    
    /**
     * Create category-based suggestion message
     */
    private String createCategorySuggestionMessage(FunctionRegistry.FunctionType type, int argCount, 
                                                 QueryContext context) {
        List<FunctionDefinition> functions = functionRegistry.getFunctionsByType(type).stream()
            .filter(f -> isContextAppropriate(f, context))
            .filter(f -> isArgumentCountCompatible(f, argCount))
            .limit(3)
            .toList();
        
        if (!functions.isEmpty()) {
            String typeDesc = switch (type) {
                case AGGREGATE -> "aggregate";
                case DATA_ACCESS -> "data access";
                case DATE_TIME -> "date/time";
                case STRING -> "string";
                case MATHEMATICAL -> "mathematical";
                case CONDITIONAL -> "conditional";
                case CONVERSION -> "conversion";
            };
            
            List<String> functionNames = functions.stream()
                .map(f -> f.name() + "()")
                .toList();
            
            return "Try " + typeDesc + " functions: " + String.join(", ", functionNames);
        }
        
        return "Use function help for available functions.";
    }
    
    /**
     * Create error message for insufficient arguments
     */
    private String createInsufficientArgumentsMessage(FunctionDefinition function, int provided, 
                                                     QueryContext context) {
        List<ParameterDefinition> parameters = function.parameters();
        int required = (int) parameters.stream().filter(p -> !p.optional()).count();
        
        StringBuilder message = new StringBuilder();
        message.append(function.name()).append("() requires ");
        
        if (required == 1) {
            message.append("1 argument");
        } else {
            message.append(required).append(" arguments");
        }
        
        message.append(", got ").append(provided).append(".\n");
        
        // Add parameter details on new lines
        if (!parameters.isEmpty()) {
            message.append("Parameters:\n");
            for (ParameterDefinition param : parameters) {
                message.append("  - ").append(param.name());
                if (param.optional()) {
                    message.append(" (optional)");
                }
                if (param.variadic()) {
                    message.append(" (variadic)");
                }
                if (param.type() != null) {
                    message.append(": ").append(param.type());
                }
                if (param.description() != null && !param.description().isEmpty()) {
                    message.append(" - ").append(param.description());
                }
                message.append("\n");
            }
        }
        
        // Add context-specific examples on new lines
        if (function.examples() != null && !function.examples().isEmpty()) {
            message.append("Examples:\n");
            String[] examples = function.examples().split(",\\s*");
            for (String example : examples) {
                message.append("  ").append(example.trim()).append("\n");
            }
        } else {
            message.append("Usage: ").append(generateBasicUsageExample(function));
        }
        
        return message.toString();
    }
    
    /**
     * Create error message for too many arguments
     */
    private String createTooManyArgumentsMessage(FunctionDefinition function, int provided, 
                                               QueryContext context) {
        int maxAllowed = function.parameters().size();
        
        StringBuilder message = new StringBuilder();
        message.append(function.name()).append("() accepts at most ");
        
        if (maxAllowed == 1) {
            message.append("1 argument");
        } else {
            message.append(maxAllowed).append(" arguments");
        }
        
        message.append(", got ").append(provided).append(".\n");
        
        // Add examples on new lines
        if (function.examples() != null && !function.examples().isEmpty()) {
            message.append("Examples:\n");
            String[] examples = function.examples().split(",\\s*");
            for (String example : examples) {
                message.append("  ").append(example.trim()).append("\n");
            }
        } else {
            message.append("Usage: ").append(generateBasicUsageExample(function));
        }
        
        return message.toString();
    }
    
    /**
     * Create error message for invalid argument types
     */
    private String createInvalidArgumentTypeMessage(FunctionDefinition function, List<ExpressionNode> arguments, 
                                                   QueryContext context) {
        // This is a simplified version - in a full implementation you'd want more sophisticated type checking
        StringBuilder message = new StringBuilder();
        message.append("Invalid argument types for ").append(function.name()).append("(). ");
        
        if (function.examples() != null && !function.examples().isEmpty()) {
            message.append("Expected usage: ").append(function.examples());
        }
        
        return message.toString();
    }
    
    /**
     * Generate a basic usage example from function parameters
     */
    private String generateBasicUsageExample(FunctionDefinition function) {
        StringBuilder example = new StringBuilder();
        example.append(function.name()).append("(");
        
        List<ParameterDefinition> params = function.parameters();
        for (int i = 0; i < params.size(); i++) {
            if (i > 0) example.append(", ");
            ParameterDefinition param = params.get(i);
            
            if (param.optional()) {
                example.append("[").append(param.name()).append("]");
            } else {
                example.append(param.name());
            }
        }
        example.append(")");
        
        return example.toString();
    }
    
    /**
     * Get context-specific guidance for grammar errors
     */
    private String getContextSpecificGuidance(QueryContext context) {
        return switch (context) {
            case SELECT_CLAUSE -> "In SELECT clause:";
            case WHERE_CLAUSE -> "In WHERE clause:";
            case FROM_CLAUSE -> "In FROM clause:";
            case GROUP_BY_CLAUSE -> "In GROUP BY clause:";
            case HAVING_CLAUSE -> "In HAVING clause:";
            case ORDER_BY_CLAUSE -> "In ORDER BY clause:";
            case FUNCTION_CALL -> "In function call:";
            default -> null;
        };
    }
    
    /**
     * Get recovery suggestion based on context
     */
    private String getRecoverySuggestion(QueryContext context, Token errorToken) {
        return switch (context) {
            case SELECT_CLAUSE -> "Check field names and function syntax.";
            case WHERE_CLAUSE -> "Ensure conditions use comparison operators (=, >, <, etc.).";
            case FROM_CLAUSE -> "Specify a valid table or event type name.";
            case FUNCTION_CALL -> "Check function name spelling and argument count.";
            default -> "Review the query syntax.";
        };
    }
    
    /**
     * Enum for different types of argument validation errors
     */
    public enum ArgumentValidationError {
        TOO_FEW_ARGUMENTS,
        TOO_MANY_ARGUMENTS,
        INVALID_ARGUMENT_TYPE,
        AGGREGATE_IN_WHERE
    }
}
