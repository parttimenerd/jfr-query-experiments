package me.bechberger.jfr.extended.engine;

import me.bechberger.jfr.extended.evaluator.FunctionRegistry;
import me.bechberger.jfr.extended.Grammar;

/**
 * Provides help content for the JFR query language HELP commands.
 * This class generates formatted documentation for general help, function-specific help,
 * and grammar documentation.
 */
public class HelpProvider {
    
    /**
     * Generate general help content showing available functions and basic usage
     */
    public static String getGeneralHelp() {
        StringBuilder help = new StringBuilder();
        
        help.append("JFR Query Language Help\n");
        help.append("======================\n\n");
        
        help.append("BASIC USAGE:\n");
        help.append("  SELECT [fields] FROM [table] [WHERE condition] [GROUP BY fields] [ORDER BY fields] [LIMIT count]\n");
        help.append("  @SELECT ...  (extended syntax for advanced features)\n");
        help.append("  SHOW EVENTS  (list available event types)\n");
        help.append("  SHOW FIELDS event_type  (show fields for a specific event type)\n");
        help.append("  HELP FUNCTION function_name  (get help for a specific function)\n");
        help.append("  HELP GRAMMAR  (show complete grammar documentation)\n\n");
        
        help.append("COMMON OPERATORS:\n");
        help.append("  =, !=, <, >, <=, >=  (comparison)\n");
        help.append("  AND, OR, NOT  (logical)\n");
        help.append("  IN, LIKE  (text matching)\n");
        help.append("  WITHIN ... OF ...  (time proximity)\n\n");
        
        help.append("DURATION FORMATS:\n");
        help.append("  1s, 100ms, 5m, 2h, 1d  (seconds, milliseconds, minutes, hours, days)\n\n");
        
        help.append("MEMORY FORMATS:\n");
        help.append("  1KB, 1MB, 1GB, 1TB  (kilobytes, megabytes, gigabytes, terabytes)\n\n");
        
        // Add function categories
        FunctionRegistry registry = FunctionRegistry.getInstance();
        help.append("AVAILABLE FUNCTIONS BY CATEGORY:\n");
        
        for (FunctionRegistry.FunctionType type : FunctionRegistry.FunctionType.values()) {
            var funcsOfType = registry.getFunctionsByType(type);
            if (!funcsOfType.isEmpty()) {
                help.append("  ").append(formatTypeName(type)).append(": ");
                var names = funcsOfType.stream()
                    .map(FunctionRegistry.FunctionDefinition::name)
                    .limit(10) // Limit to avoid overwhelming output
                    .toList();
                help.append(String.join(", ", names));
                if (funcsOfType.size() > 10) {
                    help.append("... (").append(funcsOfType.size() - 10).append(" more)");
                }
                help.append("\n");
            }
        }
        
        help.append("\nEXAMPLES:\n");
        help.append("  SELECT * FROM GarbageCollection WHERE duration > 100ms\n");
        help.append("  SELECT COUNT(*), AVG(duration) FROM ThreadSleep GROUP BY eventType\n");
        help.append("  SELECT * FROM JavaExceptionThrow WHERE message LIKE '%error%'\n");
        help.append("  @SELECT eventType, stackTrace FROM * WHERE duration WITHIN 1s OF startTime\n\n");
        
        help.append("Use 'HELP FUNCTION function_name' for detailed function help.\n");
        help.append("Use 'HELP GRAMMAR' for complete syntax documentation.\n");
        
        return help.toString();
    }
    
    /**
     * Generate help content for a specific function
     */
    public static String getFunctionHelp(String functionName) {
        FunctionRegistry registry = FunctionRegistry.getInstance();
        String upperFunctionName = functionName.toUpperCase();
        
        if (!registry.isFunction(upperFunctionName)) {
            StringBuilder help = new StringBuilder();
            help.append("Function '").append(functionName).append("' not found.\n\n");
            
            // Find similar function names
            var suggestions = findSimilarFunctionNames(upperFunctionName, registry);
            if (!suggestions.isEmpty()) {
                help.append("Did you mean: ").append(String.join(", ", suggestions)).append("?\n\n");
            }
            
            help.append("Use 'HELP' to see all available functions.\n");
            return help.toString();
        }
        
        FunctionRegistry.FunctionDefinition funcDef = registry.getFunction(upperFunctionName);
        StringBuilder help = new StringBuilder();
        
        help.append("FUNCTION: ").append(funcDef.name()).append("\n");
        help.append("TYPE: ").append(formatTypeName(funcDef.type())).append("\n\n");
        
        if (funcDef.description() != null) {
            help.append("DESCRIPTION:\n");
            help.append("  ").append(funcDef.description()).append("\n\n");
        }
        
        help.append("SIGNATURE:\n");
        help.append("  ").append(funcDef.signature()).append("\n\n");
        
        if (!funcDef.parameters().isEmpty()) {
            help.append("PARAMETERS:\n");
            for (FunctionRegistry.ParameterDefinition param : funcDef.parameters()) {
                help.append("  - ").append(param.name());
                if (param.optional()) help.append(" (optional)");
                if (param.variadic()) help.append(" (variadic)");
                help.append(": ").append(param.type().name().toLowerCase());
                if (param.description() != null) {
                    help.append(" - ").append(param.description());
                }
                help.append("\n");
            }
            help.append("\n");
        }
        
        if (funcDef.examples() != null && !funcDef.examples().isEmpty()) {
            help.append("EXAMPLES:\n");
            help.append("  ").append(funcDef.examples()).append("\n\n");
        }
        
        return help.toString();
    }
    
    /**
     * Generate complete grammar documentation
     */
    public static String getGrammarHelp() {
        return Grammar.getGrammarText();
    }
    
    /**
     * Format function type name for display
     */
    private static String formatTypeName(FunctionRegistry.FunctionType type) {
        return switch (type) {
            case AGGREGATE -> "Aggregate";
            case DATA_ACCESS -> "Data Access";
            case MATHEMATICAL -> "Mathematical";
            case STRING -> "String";
            case DATE_TIME -> "Date/Time";
            case CONDITIONAL -> "Conditional";
        };
    }
    
    /**
     * Find similar function names for suggestions
     */
    private static java.util.List<String> findSimilarFunctionNames(String input, FunctionRegistry registry) {
        java.util.List<String> allFunctions = new java.util.ArrayList<>(registry.getFunctionNames());
        java.util.List<String> suggestions = new java.util.ArrayList<>();
        
        for (String funcName : allFunctions) {
            String func = funcName.toUpperCase();
            
            // Exact prefix match (high priority)
            if (func.startsWith(input) || input.startsWith(func)) {
                suggestions.add(funcName);
                continue;
            }
            
            // Levenshtein distance with adaptive threshold
            int maxDistance = Math.max(2, Math.min(input.length(), func.length()) / 3);
            if (calculateLevenshteinDistance(input, func) <= maxDistance) {
                suggestions.add(funcName);
            }
        }
        
        return suggestions.stream().limit(3).toList();
    }
    
    /**
     * Calculate Levenshtein distance between two strings
     */
    private static int calculateLevenshteinDistance(String s1, String s2) {
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
}
