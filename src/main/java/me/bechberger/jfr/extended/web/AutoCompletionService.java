package me.bechberger.jfr.extended.web;

import me.bechberger.jfr.extended.evaluator.FunctionRegistry;
import me.bechberger.jfr.extended.Lexer;
import me.bechberger.jfr.extended.Token;
import me.bechberger.jfr.extended.TokenType;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Service for providing auto-completion suggestions for JFR queries
 */
public class AutoCompletionService {
    
    private final FunctionRegistry functionRegistry;
    private final EventTypeRegistry eventTypeRegistry;
    
    public AutoCompletionService() {
        this.functionRegistry = FunctionRegistry.getInstance();
        this.eventTypeRegistry = new EventTypeRegistry();
        initializeEventTypes();
    }
    
    /**
     * Auto-completion suggestion
     */
    public record CompletionItem(
        String text,
        CompletionItemType type,
        String description,
        String detail,
        int priority
    ) {}
    
    /**
     * Types of completion items
     */
    public enum CompletionItemType {
        KEYWORD,
        FUNCTION, 
        TABLE,
        COLUMN,
        VALUE,
        SNIPPET
    }
    
    /**
     * Auto-completion response
     */
    public record AutoCompletionResponse(
        List<CompletionItem> items,
        int cursorLine,
        int cursorColumn,
        boolean hasErrors,
        List<String> errors
    ) {}
    
    /**
     * Get auto-completion suggestions for a query at a specific cursor position
     */
    public AutoCompletionResponse getCompletions(String query, int cursorLine, int cursorColumn) {
        List<CompletionItem> items = new ArrayList<>();
        List<String> errors = new ArrayList<>();
        boolean hasErrors = false;
        
        try {
            // Parse the query up to the cursor position
            String queryUpToCursor = getQueryUpToCursor(query, cursorLine, cursorColumn);
            
            // Tokenize to understand context
            Lexer lexer = new Lexer(queryUpToCursor);
            List<Token> tokens = lexer.tokenize();
            
            // Determine completion context
            CompletionContext context = analyzeContext(tokens);
            
            // Generate suggestions based on context
            switch (context.getType()) {
                case AFTER_SELECT -> {
                    items.addAll(getFunctionCompletions());
                    items.addAll(getColumnCompletions(context));
                    items.add(new CompletionItem("*", CompletionItemType.VALUE, "Select all columns", "Wildcard selector", 10));
                }
                case AFTER_FROM -> {
                    items.addAll(getTableCompletions());
                    items.addAll(getSubqueryCompletions());
                }
                case AFTER_WHERE -> {
                    items.addAll(getColumnCompletions(context));
                    items.addAll(getFunctionCompletions());
                    items.addAll(getVariableCompletions());
                }
                case FUNCTION_PARAMETERS -> {
                    items.addAll(getColumnCompletions(context));
                    items.add(new CompletionItem("*", CompletionItemType.VALUE, "All rows", "Wildcard for COUNT function", 10));
                }
                case AFTER_OPERATOR -> {
                    items.addAll(getValueCompletions(context));
                    items.addAll(getFunctionCompletions());
                    items.addAll(getSubqueryCompletions());
                }
                case STATEMENT_START -> {
                    items.addAll(getKeywordCompletions());
                    items.addAll(getStatementSnippets());
                }
                default -> {
                    // Default suggestions
                    items.addAll(getKeywordCompletions());
                    items.addAll(getFunctionCompletions());
                }
            }
            
        } catch (Exception e) {
            hasErrors = true;
            errors.add("Auto-completion error: " + e.getMessage());
        }
        
        // Sort by priority and alphabetically
        items.sort((a, b) -> {
            int priorityCompare = Integer.compare(b.priority(), a.priority());
            return priorityCompare != 0 ? priorityCompare : a.text().compareToIgnoreCase(b.text());
        });
        
        return new AutoCompletionResponse(items, cursorLine, cursorColumn, hasErrors, errors);
    }
    
    private String getQueryUpToCursor(String query, int cursorLine, int cursorColumn) {
        String[] lines = query.split("\n");
        StringBuilder result = new StringBuilder();
        
        for (int i = 0; i < Math.min(cursorLine, lines.length); i++) {
            if (i == cursorLine - 1) {
                // Current line - only include up to cursor column
                String line = lines[i];
                int endCol = Math.min(cursorColumn - 1, line.length());
                result.append(line, 0, Math.max(0, endCol));
            } else {
                result.append(lines[i]).append("\n");
            }
        }
        
        return result.toString();
    }
    
    private CompletionContext analyzeContext(List<Token> tokens) {
        if (tokens.isEmpty() || tokens.size() == 1) {
            return new CompletionContext(CompletionContext.Type.STATEMENT_START);
        }
        
        // Remove EOF token
        List<Token> relevantTokens = tokens.stream()
            .filter(t -> t.type() != TokenType.EOF && t.type() != TokenType.WHITESPACE)
            .collect(Collectors.toList());
        
        if (relevantTokens.isEmpty()) {
            return new CompletionContext(CompletionContext.Type.STATEMENT_START);
        }
        
        Token lastToken = relevantTokens.get(relevantTokens.size() - 1);
        
        // Analyze based on last token and overall structure
        return switch (lastToken.type()) {
            case SELECT -> new CompletionContext(CompletionContext.Type.AFTER_SELECT);
            case FROM -> new CompletionContext(CompletionContext.Type.AFTER_FROM);
            case WHERE -> new CompletionContext(CompletionContext.Type.AFTER_WHERE);
            case LPAREN -> {
                // Check if we're in a function call
                if (relevantTokens.size() >= 2) {
                    Token beforeParen = relevantTokens.get(relevantTokens.size() - 2);
                    if (functionRegistry.isFunction(beforeParen.type()) || 
                        (beforeParen.type() == TokenType.IDENTIFIER && 
                         functionRegistry.isFunction(beforeParen.value()))) {
                        yield new CompletionContext(CompletionContext.Type.FUNCTION_PARAMETERS);
                    }
                }
                yield new CompletionContext(CompletionContext.Type.AFTER_OPERATOR);
            }
            case EQUALS, NOT_EQUALS, LESS_THAN, GREATER_THAN, LESS_EQUAL, GREATER_EQUAL, LIKE, IN 
                -> new CompletionContext(CompletionContext.Type.AFTER_OPERATOR);
            default -> new CompletionContext(CompletionContext.Type.GENERAL);
        };
    }
    
    private List<CompletionItem> getKeywordCompletions() {
        return List.of(
            new CompletionItem("SELECT", CompletionItemType.KEYWORD, "Select data", "SELECT clause", 20),
            new CompletionItem("FROM", CompletionItemType.KEYWORD, "Specify data source", "FROM clause", 20),
            new CompletionItem("WHERE", CompletionItemType.KEYWORD, "Filter conditions", "WHERE clause", 20),
            new CompletionItem("GROUP BY", CompletionItemType.KEYWORD, "Group results", "GROUP BY clause", 15),
            new CompletionItem("ORDER BY", CompletionItemType.KEYWORD, "Sort results", "ORDER BY clause", 15),
            new CompletionItem("LIMIT", CompletionItemType.KEYWORD, "Limit results", "LIMIT clause", 15),
            new CompletionItem("AS", CompletionItemType.KEYWORD, "Create alias", "Alias keyword", 10),
            new CompletionItem("AND", CompletionItemType.KEYWORD, "Logical AND", "Logical operator", 10),
            new CompletionItem("OR", CompletionItemType.KEYWORD, "Logical OR", "Logical operator", 10),
            new CompletionItem("SHOW EVENTS", CompletionItemType.KEYWORD, "Show available events", "List all event types", 25),
            new CompletionItem("SHOW FIELDS", CompletionItemType.KEYWORD, "Show event fields", "SHOW FIELDS <event_type>", 25)
        );
    }
    
    private List<CompletionItem> getFunctionCompletions() {
        return functionRegistry.getAllFunctions().stream()
            .map(func -> new CompletionItem(
                func.name() + "()",
                CompletionItemType.FUNCTION,
                func.description(),
                func.type().toString(),
                18
            ))
            .collect(Collectors.toList());
    }
    
    private List<CompletionItem> getTableCompletions() {
        return eventTypeRegistry.getEventTypes().stream()
            .map(event -> new CompletionItem(
                event.name(),
                CompletionItemType.TABLE,
                event.description(),
                "Event type with " + event.fieldCount() + " fields",
                15
            ))
            .collect(Collectors.toList());
    }
    
    private List<CompletionItem> getColumnCompletions(CompletionContext context) {
        // For now, return common JFR field names
        return List.of(
            new CompletionItem("duration", CompletionItemType.COLUMN, "Event duration", "Duration field", 12),
            new CompletionItem("startTime", CompletionItemType.COLUMN, "Event start time", "Timestamp field", 12),
            new CompletionItem("endTime", CompletionItemType.COLUMN, "Event end time", "Timestamp field", 12),
            new CompletionItem("stackTrace", CompletionItemType.COLUMN, "Stack trace", "Stack trace field", 12),
            new CompletionItem("eventThread", CompletionItemType.COLUMN, "Event thread", "Thread field", 12)
        );
    }
    
    private List<CompletionItem> getSubqueryCompletions() {
        return List.of(
            new CompletionItem("[SELECT * FROM jdk.GarbageCollection]", 
                CompletionItemType.SNIPPET, "Nested JFR query", "Bracketed subquery", 20),
            new CompletionItem("(SELECT * FROM events)", 
                CompletionItemType.SNIPPET, "Parenthesized subquery", "Subquery expression", 15)
        );
    }
    
    private List<CompletionItem> getValueCompletions(CompletionContext context) {
        return List.of(
            new CompletionItem("true", CompletionItemType.VALUE, "Boolean true", "Boolean literal", 10),
            new CompletionItem("false", CompletionItemType.VALUE, "Boolean false", "Boolean literal", 10),
            new CompletionItem("10s", CompletionItemType.VALUE, "10 seconds", "Duration literal", 10),
            new CompletionItem("1MB", CompletionItemType.VALUE, "1 megabyte", "Memory size literal", 10)
        );
    }
    
    private List<CompletionItem> getVariableCompletions() {
        return List.of(
            new CompletionItem("x := 10", CompletionItemType.SNIPPET, "Variable assignment", "Declare variable", 15)
        );
    }
    
    private List<CompletionItem> getStatementSnippets() {
        return List.of(
            new CompletionItem("SELECT * FROM ${table}", 
                CompletionItemType.SNIPPET, "Select all from table", "Basic SELECT statement", 25),
            new CompletionItem("SELECT COUNT(*) FROM ${table} GROUP BY ${field}", 
                CompletionItemType.SNIPPET, "Count by field", "Aggregation query", 20),
            new CompletionItem("SELECT AVG(duration) FROM ${table} WHERE duration > ${threshold}", 
                CompletionItemType.SNIPPET, "Average duration with filter", "Performance analysis", 20)
        );
    }
    
    private void initializeEventTypes() {
        // Initialize with common JFR event types
        eventTypeRegistry.addEventType(new EventType("jdk.ExecutionSample", "Java execution sampling", 10));
        eventTypeRegistry.addEventType(new EventType("jdk.GarbageCollection", "Garbage collection events", 8));
        eventTypeRegistry.addEventType(new EventType("jdk.ThreadSleep", "Thread sleep events", 5));
        eventTypeRegistry.addEventType(new EventType("jdk.SocketRead", "Socket read operations", 6));
        eventTypeRegistry.addEventType(new EventType("jdk.FileRead", "File read operations", 6));
        eventTypeRegistry.addEventType(new EventType("jdk.CPULoad", "CPU load measurements", 4));
    }
    
    /**
     * Context information for auto-completion
     */
    private static class CompletionContext {
        public enum Type {
            STATEMENT_START,
            AFTER_SELECT,
            AFTER_FROM,
            AFTER_WHERE,
            FUNCTION_PARAMETERS,
            AFTER_OPERATOR,
            GENERAL
        }
        
        private final Type type;
        
        public CompletionContext(Type type) {
            this.type = type;
        }
        
        public Type getType() {
            return type;
        }
    }
    
    /**
     * Registry for event types
     */
    private static class EventTypeRegistry {
        private final List<EventType> eventTypes = new ArrayList<>();
        
        public void addEventType(EventType eventType) {
            eventTypes.add(eventType);
        }
        
        public List<EventType> getEventTypes() {
            return Collections.unmodifiableList(eventTypes);
        }
    }
    
    /**
     * Event type information
     */
    public record EventType(String name, String description, int fieldCount) {}
}
