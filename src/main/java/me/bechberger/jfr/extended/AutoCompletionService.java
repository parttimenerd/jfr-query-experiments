package me.bechberger.jfr.extended;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.HashSet;

/**
 * Auto-completion service for the extended JFR query language
 */
public class AutoCompletionService {
    
    /**
     * Represents an auto-completion suggestion
     */
    public record CompletionItem(
        String text,
        CompletionType type,
        String description,
        String documentation,
        int priority
    ) {
    }
    
    /**
     * Types of completion items
     */
    public enum CompletionType {
        KEYWORD,
        FUNCTION,
        EVENT_TYPE,
        FIELD,
        VARIABLE,
        OPERATOR,
        SNIPPET
    }
    
    /**
     * Context information for auto-completion
     */
    public record CompletionContext(
        String query,
        int cursorPosition,
        String currentToken,
        List<String> availableEventTypes,
        List<String> availableFields,
        List<String> variables
    ) {
    }
    
    // Known JFR event types (this would be populated from actual JFR data)
    private static final Set<String> DEFAULT_EVENT_TYPES = Set.of(
        "jdk.GarbageCollection",
        "jdk.G1GarbageCollection",
        "jdk.ParallelOldGarbageCollection",
        "jdk.SerialOldGarbageCollection",
        "jdk.PSGarbageCollection",
        "jdk.ZGarbageCollection",
        "jdk.ShenandoahGarbageCollection",
        "jdk.ExecutionSample",
        "jdk.NativeMethodSample",
        "jdk.JavaMonitorEnter",
        "jdk.JavaMonitorWait",
        "jdk.ThreadSleep",
        "jdk.ThreadPark",
        "jdk.ObjectAllocationInNewTLAB",
        "jdk.ObjectAllocationOutsideTLAB",
        "jdk.ObjectAllocationSample",
        "jdk.ClassLoad",
        "jdk.ClassDefine",
        "jdk.Compilation",
        "jdk.CompilerInlining",
        "jdk.SystemGC",
        "jdk.HeapSummary",
        "jdk.MetaspaceSummary",
        "jdk.PSHeapSummary",
        "jdk.G1HeapSummary",
        "jdk.GCHeapSummary",
        "jdk.YoungGenerationConfiguration",
        "jdk.GCConfiguration",
        "jdk.GCTLABConfiguration",
        "jdk.GCSurvivorConfiguration",
        "jdk.ObjectCount",
        "jdk.GCReferenceStatistics",
        "jdk.OldObjectSample",
        "jdk.DumpReason",
        "jdk.DataLoss",
        "jdk.CPULoad",
        "jdk.ThreadCPULoad",
        "jdk.SystemProcess",
        "jdk.InitialSystemProperty",
        "jdk.ThreadStart",
        "jdk.ThreadEnd",
        "jdk.ThreadContextSwitchRate",
        "jdk.NetworkUtilization",
        "jdk.JavaThreadStatistics",
        "jdk.PhysicalMemory",
        "jdk.SafepointBegin",
        "jdk.SafepointEnd",
        "jdk.ExecuteVMOperation",
        "jdk.Shutdown",
        "jdk.VMInfo",
        "jdk.OSInformation",
        "jdk.CPUInformation",
        "jdk.ThreadAllocationStatistics",
        "jdk.FinalizerStatistics",
        "jdk.CodeCacheStatistics",
        "jdk.CodeCacheConfiguration",
        "jdk.CodeSweeperStatistics",
        "jdk.CodeSweeperConfiguration",
        "jdk.StringDeduplicationStatistics",
        "jdk.FileForce",
        "jdk.FileRead",
        "jdk.FileWrite",
        "jdk.SocketRead",
        "jdk.SocketWrite"
    );
    
    // Common field names for JFR events
    private static final Set<String> DEFAULT_FIELDS = Set.of(
        "duration",
        "startTime",
        "endTime",
        "stackTrace",
        "thread",
        "eventThread",
        "cause",
        "id",
        "size",
        "count",
        "name",
        "type",
        "value",
        "gcId",
        "when",
        "sumOfPauses",
        "longestPause",
        "collectorName",
        "heapUsed",
        "heapSize",
        "youngGenUsed",
        "youngGenSize",
        "oldGenUsed",
        "oldGenSize",
        "committedSize",
        "reservedSize",
        "nonNMethodSize",
        "profiledSize",
        "nonProfiledSize",
        "reservedTopSize",
        "codeBlob",
        "methodName",
        "className",
        "sampledThread",
        "state",
        "javaThreadId",
        "osThreadId",
        "cpuTime",
        "userTime",
        "blockedTime",
        "waitedTime",
        "allocationRate",
        "tlabSize",
        "objectClass",
        "arrayElements",
        "address",
        "description",
        "reason",
        "vmOperationType",
        "safepoint",
        "caller",
        "safepointId",
        "globalTime",
        "nanoTime",
        "machineTotal",
        "jvmUser",
        "jvmSystem",
        "percentage",
        "path",
        "fileSize",
        "bytesRead",
        "bytesWritten",
        "host",
        "port",
        "timeout",
        "socketAddress",
        "remoteHost",
        "remotePort"
    );
    
    /**
     * Get auto-completion suggestions for the given context
     */
    public List<CompletionItem> getCompletions(CompletionContext context) {
        List<CompletionItem> completions = new ArrayList<>();
        
        // Determine the completion context
        String currentToken = context.currentToken(); // Keep original case
        String beforeCursor = "";
        
        if (context.cursorPosition() <= context.query().length()) {
            beforeCursor = context.query().substring(0, context.cursorPosition()).toLowerCase();
        }
        
        // Add keyword completions
        if (shouldSuggestKeywords(beforeCursor)) {
            completions.addAll(getKeywordCompletions(currentToken));
        }
        
        // Add function completions
        if (shouldSuggestFunctions(beforeCursor)) {
            completions.addAll(getFunctionCompletions(currentToken));
        }
        
        // Add event type completions
        if (shouldSuggestEventTypes(beforeCursor)) {
            completions.addAll(getEventTypeCompletions(currentToken, context.availableEventTypes()));
        }
        
        // Add field completions
        if (shouldSuggestFields(beforeCursor)) {
            completions.addAll(getFieldCompletions(currentToken, context.availableFields()));
        }
        
        // Add variable completions
        if (shouldSuggestVariables(beforeCursor)) {
            completions.addAll(getVariableCompletions(currentToken, context.variables()));
        }
        
        // Add operator completions
        if (shouldSuggestOperators(beforeCursor)) {
            completions.addAll(getOperatorCompletions(currentToken));
        }
        
        // Add snippet completions
        completions.addAll(getSnippetCompletions(currentToken, beforeCursor));
        
        // Sort by priority and filter by current token
        return completions.stream()
            .filter(item -> matchesPrefix(item.text(), currentToken))
            .sorted((a, b) -> Integer.compare(b.priority(), a.priority()))
            .limit(50) // Limit to top 50 suggestions
            .toList();
    }
    
    /**
     * Check if keywords should be suggested
     */
    private boolean shouldSuggestKeywords(String beforeCursor) {
        // Always suggest keywords - they will be filtered by prefix matching
        return true;
    }
    
    /**
     * Check if functions should be suggested
     */
    private boolean shouldSuggestFunctions(String beforeCursor) {
        // Always suggest functions - they will be filtered by prefix matching
        return true;
    }
    
    /**
     * Check if event types should be suggested
     */
    private boolean shouldSuggestEventTypes(String beforeCursor) {
        // Always suggest event types - they will be filtered by prefix matching
        return true;
    }
    
    /**
     * Check if fields should be suggested
     */
    private boolean shouldSuggestFields(String beforeCursor) {
        // Always suggest fields - they will be filtered by prefix matching
        return true;
    }
    
    /**
     * Check if variables should be suggested
     */
    private boolean shouldSuggestVariables(String beforeCursor) {
        return beforeCursor.contains("where") || 
               beforeCursor.contains("select") ||
               beforeCursor.contains("from");
    }
    
    /**
     * Check if operators should be suggested
     */
    private boolean shouldSuggestOperators(String beforeCursor) {
        // Always suggest operators - they will be filtered by prefix matching
        return true;
    }
    
    /**
     * Get keyword completions
     */
    private List<CompletionItem> getKeywordCompletions(String prefix) {
        List<CompletionItem> keywords = new ArrayList<>();
        
        keywords.add(new CompletionItem("SELECT", CompletionType.KEYWORD, "Select columns", "SELECT clause to specify columns", 100));
        keywords.add(new CompletionItem("FROM", CompletionType.KEYWORD, "From table", "FROM clause to specify source", 100));
        keywords.add(new CompletionItem("WHERE", CompletionType.KEYWORD, "Filter rows", "WHERE clause to filter results", 90));
        keywords.add(new CompletionItem("GROUP BY", CompletionType.KEYWORD, "Group results", "GROUP BY clause to group results", 80));
        keywords.add(new CompletionItem("ORDER BY", CompletionType.KEYWORD, "Sort results", "ORDER BY clause to sort results", 80));
        keywords.add(new CompletionItem("LIMIT", CompletionType.KEYWORD, "Limit results", "LIMIT clause to limit number of results", 70));
        keywords.add(new CompletionItem("AS", CompletionType.KEYWORD, "Alias", "AS keyword for aliases", 60));
        keywords.add(new CompletionItem("AND", CompletionType.KEYWORD, "Logical AND", "AND operator for conditions", 60));
        keywords.add(new CompletionItem("OR", CompletionType.KEYWORD, "Logical OR", "OR operator for conditions", 60));
        keywords.add(new CompletionItem("ASC", CompletionType.KEYWORD, "Ascending order", "ASC for ascending sort order", 50));
        keywords.add(new CompletionItem("DESC", CompletionType.KEYWORD, "Descending order", "DESC for descending sort order", 50));
        keywords.add(new CompletionItem("SHOW EVENTS", CompletionType.KEYWORD, "Show available events", "SHOW EVENTS to list all available event types", 70));
        keywords.add(new CompletionItem("SHOW FIELDS", CompletionType.KEYWORD, "Show event fields", "SHOW FIELDS <event> to list fields for an event type", 70));
        keywords.add(new CompletionItem("VIEW", CompletionType.KEYWORD, "Create view", "VIEW to create a reusable view", 60));
        keywords.add(new CompletionItem("COLUMN", CompletionType.KEYWORD, "Column format", "COLUMN clause to specify column formatting", 50));
        keywords.add(new CompletionItem("FORMAT", CompletionType.KEYWORD, "Output format", "FORMAT clause to specify output formatting", 50));
        
        return keywords;
    }
    
    /**
     * Get function completions
     */
    private List<CompletionItem> getFunctionCompletions(String prefix) {
        List<CompletionItem> functions = new ArrayList<>();
        
        functions.add(new CompletionItem("COUNT(*)", CompletionType.FUNCTION, "Count rows", "COUNT(*) - Count all rows", 100));
        functions.add(new CompletionItem("COUNT(", CompletionType.FUNCTION, "Count values", "COUNT(field) - Count non-null values", 95));
        functions.add(new CompletionItem("SUM(", CompletionType.FUNCTION, "Sum values", "SUM(field) - Sum of numeric values", 90));
        functions.add(new CompletionItem("AVG(", CompletionType.FUNCTION, "Average values", "AVG(field) - Average of numeric values", 90));
        functions.add(new CompletionItem("MIN(", CompletionType.FUNCTION, "Minimum value", "MIN(field) - Minimum value", 85));
        functions.add(new CompletionItem("MAX(", CompletionType.FUNCTION, "Maximum value", "MAX(field) - Maximum value", 85));
        functions.add(new CompletionItem("MEDIAN(", CompletionType.FUNCTION, "Median value", "MEDIAN(field) - Median value", 80));
        functions.add(new CompletionItem("STDDEV(", CompletionType.FUNCTION, "Standard deviation", "STDDEV(field) - Standard deviation", 75));
        functions.add(new CompletionItem("FIRST(", CompletionType.FUNCTION, "First value", "FIRST(field) - First value in group", 75));
        functions.add(new CompletionItem("LAST(", CompletionType.FUNCTION, "Last value", "LAST(field) - Last value in group", 75));
        functions.add(new CompletionItem("UNIQUE(", CompletionType.FUNCTION, "Unique count", "UNIQUE(field) - Count of unique values", 70));
        functions.add(new CompletionItem("LIST(", CompletionType.FUNCTION, "List values", "LIST(field) - Comma-separated list of values", 70));
        functions.add(new CompletionItem("DIFF(", CompletionType.FUNCTION, "Difference", "DIFF(field) - Difference between first and last", 65));
        functions.add(new CompletionItem("LAST_BATCH(", CompletionType.FUNCTION, "Last batch", "LAST_BATCH(field) - Last batch of values", 60));
        functions.add(new CompletionItem("P90(", CompletionType.FUNCTION, "90th percentile", "P90(field) - 90th percentile", 85));
        functions.add(new CompletionItem("P95(", CompletionType.FUNCTION, "95th percentile", "P95(field) - 95th percentile", 85));
        functions.add(new CompletionItem("P99(", CompletionType.FUNCTION, "99th percentile", "P99(field) - 99th percentile", 85));
        functions.add(new CompletionItem("P999(", CompletionType.FUNCTION, "99.9th percentile", "P999(field) - 99.9th percentile", 80));
        
        return functions;
    }
    
    /**
     * Get event type completions
     */
    private List<CompletionItem> getEventTypeCompletions(String prefix, List<String> availableEventTypes) {
        List<CompletionItem> eventTypes = new ArrayList<>();
        
        // Use provided event types if available, otherwise use defaults
        Set<String> eventTypeSet = availableEventTypes != null && !availableEventTypes.isEmpty() 
            ? new HashSet<>(availableEventTypes) 
            : DEFAULT_EVENT_TYPES;
        
        for (String eventType : eventTypeSet) {
            String description = getEventTypeDescription(eventType);
            eventTypes.add(new CompletionItem(eventType, CompletionType.EVENT_TYPE, description, 
                                            "Event type: " + eventType, 80));
        }
        
        return eventTypes;
    }
    
    /**
     * Get field completions
     */
    private List<CompletionItem> getFieldCompletions(String prefix, List<String> availableFields) {
        List<CompletionItem> fields = new ArrayList<>();
        
        // Use provided fields if available, otherwise use defaults
        Set<String> fieldSet = availableFields != null && !availableFields.isEmpty() 
            ? new HashSet<>(availableFields) 
            : DEFAULT_FIELDS;
        
        for (String field : fieldSet) {
            String description = getFieldDescription(field);
            fields.add(new CompletionItem(field, CompletionType.FIELD, description, 
                                        "Field: " + field, 70));
        }
        
        return fields;
    }
    
    /**
     * Get variable completions
     */
    private List<CompletionItem> getVariableCompletions(String prefix, List<String> variables) {
        List<CompletionItem> completions = new ArrayList<>();
        
        if (variables != null) {
            for (String variable : variables) {
                completions.add(new CompletionItem(variable, CompletionType.VARIABLE, 
                                                 "Variable: " + variable, 
                                                 "User-defined variable", 65));
            }
        }
        
        return completions;
    }
    
    /**
     * Get operator completions
     */
    private List<CompletionItem> getOperatorCompletions(String prefix) {
        List<CompletionItem> operators = new ArrayList<>();
        
        operators.add(new CompletionItem("=", CompletionType.OPERATOR, "Equals", "Equality comparison", 95));
        operators.add(new CompletionItem("!=", CompletionType.OPERATOR, "Not equals", "Inequality comparison", 95));
        operators.add(new CompletionItem("<", CompletionType.OPERATOR, "Less than", "Less than comparison", 95));
        operators.add(new CompletionItem(">", CompletionType.OPERATOR, "Greater than", "Greater than comparison", 95));
        operators.add(new CompletionItem("<=", CompletionType.OPERATOR, "Less or equal", "Less than or equal comparison", 90));
        operators.add(new CompletionItem(">=", CompletionType.OPERATOR, "Greater or equal", "Greater than or equal comparison", 90));
        operators.add(new CompletionItem("LIKE", CompletionType.OPERATOR, "Pattern match", "Pattern matching with wildcards", 85));
        operators.add(new CompletionItem("IN", CompletionType.OPERATOR, "In set", "Check if value is in set", 85));
        
        return operators;
    }
    
    /**
     * Get snippet completions
     */
    private List<CompletionItem> getSnippetCompletions(String prefix, String beforeCursor) {
        List<CompletionItem> snippets = new ArrayList<>();
        
        // Common query patterns
        snippets.add(new CompletionItem("SELECT * FROM ", CompletionType.SNIPPET, 
                                      "Select all query", "Basic SELECT * FROM query", 95));
        snippets.add(new CompletionItem("SELECT COUNT(*) FROM ", CompletionType.SNIPPET, 
                                      "Count query", "Count all rows query", 90));
        snippets.add(new CompletionItem("WHERE duration > 10ms", CompletionType.SNIPPET, 
                                      "Duration filter", "Filter by duration greater than 10ms", 85));
        snippets.add(new CompletionItem("WHERE startTime > now() - 1h", CompletionType.SNIPPET, 
                                      "Time filter", "Filter by events in last hour", 80));
        snippets.add(new CompletionItem("GROUP BY eventThread", CompletionType.SNIPPET, 
                                      "Group by thread", "Group results by thread", 75));
        snippets.add(new CompletionItem("ORDER BY duration DESC", CompletionType.SNIPPET, 
                                      "Sort by duration", "Sort by duration descending", 75));
        snippets.add(new CompletionItem("LIMIT 100", CompletionType.SNIPPET, 
                                      "Limit results", "Limit to 100 results", 70));        
        return snippets;
    }
    
    /**
     * Get description for event type
     */
    private String getEventTypeDescription(String eventType) {
        return switch (eventType) {
            case "jdk.GarbageCollection" -> "Garbage collection events";
            case "jdk.ExecutionSample" -> "Execution profiling samples";
            case "jdk.ObjectAllocationInNewTLAB" -> "Object allocation in new TLAB";
            case "jdk.ObjectAllocationOutsideTLAB" -> "Object allocation outside TLAB";
            case "jdk.ThreadSleep" -> "Thread sleep events";
            case "jdk.ThreadPark" -> "Thread park events";
            case "jdk.JavaMonitorEnter" -> "Monitor enter events";
            case "jdk.JavaMonitorWait" -> "Monitor wait events";
            case "jdk.ClassLoad" -> "Class loading events";
            case "jdk.Compilation" -> "JIT compilation events";
            case "jdk.SafepointBegin" -> "Safepoint start events";
            case "jdk.SafepointEnd" -> "Safepoint end events";
            case "jdk.SystemGC" -> "System GC trigger events";
            case "jdk.CPULoad" -> "CPU load measurements";
            case "jdk.ThreadCPULoad" -> "Thread CPU load measurements";
            default -> "JFR event type";
        };
    }
    
    /**
     * Get description for field
     */
    private String getFieldDescription(String field) {
        return switch (field) {
            case "duration" -> "Duration of the event";
            case "startTime" -> "Start time of the event";
            case "endTime" -> "End time of the event";
            case "stackTrace" -> "Stack trace at event time";
            case "thread" -> "Thread associated with event";
            case "eventThread" -> "Thread that recorded the event";
            case "gcId" -> "Garbage collection ID";
            case "cause" -> "Cause of the event";
            case "size" -> "Size in bytes";
            case "count" -> "Count of occurrences";
            case "heapUsed" -> "Heap memory used";
            case "heapSize" -> "Total heap size";
            case "id" -> "Event identifier";
            case "name" -> "Name of the object/event";
            case "type" -> "Type of the object/event";
            case "value" -> "Value associated with event";
            default -> "Event field";
        };
    }
    
    /**
     * Check if the completion text matches the prefix (case-insensitive)
     */
    private boolean matchesPrefix(String text, String prefix) {
        if (prefix == null || prefix.isEmpty()) {
            return true;
        }
        String lowerText = text.toLowerCase();
        String lowerPrefix = prefix.toLowerCase();
        
        // Check if it starts with the prefix
        if (lowerText.startsWith(lowerPrefix)) {
            return true;
        }
        
        // For event types, also check if any part after dots starts with the prefix
        if (lowerText.contains(".")) {
            String[] parts = lowerText.split("\\.");
            for (String part : parts) {
                if (part.startsWith(lowerPrefix)) {
                    return true;
                }
            }
        }
        
        return false;
    }
}
