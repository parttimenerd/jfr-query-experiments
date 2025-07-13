package me.bechberger.jfr.extended.web;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * REST endpoint handlers for JFR query language services
 */
public class JfrQueryEndpoints {
    
    private final SyntaxHighlightingService syntaxHighlighter;
    private final AutoCompletionService autoCompletion;
    
    public JfrQueryEndpoints() {
        this.syntaxHighlighter = new SyntaxHighlightingService();
        this.autoCompletion = new AutoCompletionService();
    }
    
    /**
     * Simple JSON serialization helper
     */
    private String toJson(Object obj) {
        if (obj instanceof Map) {
            @SuppressWarnings("unchecked")
            Map<String, Object> map = (Map<String, Object>) obj;
            return "{" + map.entrySet().stream()
                .map(entry -> "\"" + entry.getKey() + "\":" + toJson(entry.getValue()))
                .collect(Collectors.joining(",")) + "}";
        } else if (obj instanceof String) {
            return "\"" + obj.toString().replace("\"", "\\\"") + "\"";
        } else {
            return obj.toString();
        }
    }
    
    /**
     * Syntax highlighting endpoint
     * 
     * GET /api/syntax-highlight?query=SELECT * FROM events
     * 
     * @param query The JFR query to highlight
     * @return JSON response with highlighted tokens
     */
    public String handleSyntaxHighlight(String query) {
        try {
            if (query == null || query.trim().isEmpty()) {
                return createErrorResponse("Query parameter is required");
            }
            
            SyntaxHighlightingService.SyntaxHighlightResponse response = 
                syntaxHighlighter.highlight(query);
            
            return toJson(response);
            
        } catch (Exception e) {
            return createErrorResponse("Error processing syntax highlighting: " + e.getMessage());
        }
    }
    
    /**
     * Auto-completion endpoint
     * 
     * GET /api/autocomplete?query=SELECT * FROM ev&cursor_line=1&cursor_column=18
     * 
     * @param query The partial JFR query
     * @param cursorLine The cursor line position (1-based)
     * @param cursorColumn The cursor column position (1-based)
     * @return JSON response with completion suggestions
     */
    public String handleAutoComplete(String query, int cursorLine, int cursorColumn) {
        try {
            if (query == null) {
                query = "";
            }
            
            if (cursorLine < 1) cursorLine = 1;
            if (cursorColumn < 1) cursorColumn = 1;
            
            AutoCompletionService.AutoCompletionResponse response = 
                autoCompletion.getCompletions(query, cursorLine, cursorColumn);
            
            return toJson(response);
            
        } catch (Exception e) {
            return createErrorResponse("Error processing auto-completion: " + e.getMessage());
        }
    }
    
    /**
     * CSS classes endpoint for syntax highlighting
     * 
     * GET /api/css-classes
     * 
     * @return JSON response with available CSS classes and their descriptions
     */
    public String handleCssClasses() {
        try {
            return toJson(syntaxHighlighter.getCssClasses());
        } catch (Exception e) {
            return createErrorResponse("Error retrieving CSS classes: " + e.getMessage());
        }
    }
    
    /**
     * Health check endpoint
     * 
     * GET /api/health
     * 
     * @return JSON response with service status
     */
    public String handleHealthCheck() {
        try {
            Map<String, Object> health = new HashMap<>();
            health.put("status", "healthy");
            health.put("services", Map.of(
                "syntax_highlighting", "available",
                "auto_completion", "available"
            ));
            health.put("version", "1.0.0");
            
            return toJson(health);
        } catch (Exception e) {
            return createErrorResponse("Error checking health: " + e.getMessage());
        }
    }
    
    /**
     * API documentation endpoint
     * 
     * GET /api/docs
     * 
     * @return JSON response with API documentation
     */
    public String handleApiDocs() {
        try {
            Map<String, Object> docs = new HashMap<>();
            docs.put("title", "JFR Query Language API");
            docs.put("version", "1.0.0");
            docs.put("description", "REST API for JFR query language services");
            
            docs.put("endpoints", Map.of(
                "/api/syntax-highlight", Map.of(
                    "method", "GET",
                    "parameters", Map.of(
                        "query", "The JFR query to highlight (required)"
                    ),
                    "description", "Returns syntax highlighting information for a JFR query",
                    "example", "/api/syntax-highlight?query=SELECT * FROM jdk.ExecutionSample"
                ),
                "/api/autocomplete", Map.of(
                    "method", "GET", 
                    "parameters", Map.of(
                        "query", "The partial JFR query (optional)",
                        "cursor_line", "Cursor line position, 1-based (required)",
                        "cursor_column", "Cursor column position, 1-based (required)"
                    ),
                    "description", "Returns auto-completion suggestions for a JFR query",
                    "example", "/api/autocomplete?query=SELECT * FROM&cursor_line=1&cursor_column=15"
                ),
                "/api/css-classes", Map.of(
                    "method", "GET",
                    "parameters", Map.of(),
                    "description", "Returns available CSS classes for syntax highlighting"
                ),
                "/api/health", Map.of(
                    "method", "GET",
                    "parameters", Map.of(),
                    "description", "Returns service health status"
                )
            ));
            
            return toJson(docs);
            
        } catch (Exception e) {
            return createErrorResponse("Error generating API documentation: " + e.getMessage());
        }
    }
    
    private String createErrorResponse(String message) {
        Map<String, Object> error = new HashMap<>();
        error.put("error", true);
        error.put("message", message);
        return toJson(error);
    }
    
    /**
     * Simple HTTP server for testing the endpoints
     */
    public static class SimpleHttpServer {
        private final JfrQueryEndpoints endpoints;
        
        public SimpleHttpServer() {
            this.endpoints = new JfrQueryEndpoints();
        }
        
        /**
         * Simulate handling an HTTP request
         */
        public String handleRequest(String path, Map<String, String> parameters) {
            return switch (path) {
                case "/api/syntax-highlight" -> 
                    endpoints.handleSyntaxHighlight(parameters.get("query"));
                
                case "/api/autocomplete" -> {
                    String query = parameters.get("query");
                    int line = parseIntParameter(parameters, "cursor_line", 1);
                    int column = parseIntParameter(parameters, "cursor_column", 1);
                    yield endpoints.handleAutoComplete(query, line, column);
                }
                
                case "/api/css-classes" -> endpoints.handleCssClasses();
                case "/api/health" -> endpoints.handleHealthCheck();
                case "/api/docs" -> endpoints.handleApiDocs();
                
                default -> endpoints.createErrorResponse("Unknown endpoint: " + path);
            };
        }
        
        private int parseIntParameter(Map<String, String> parameters, String name, int defaultValue) {
            String value = parameters.get(name);
            if (value == null) return defaultValue;
            
            try {
                return Integer.parseInt(value);
            } catch (NumberFormatException e) {
                return defaultValue;
            }
        }
    }
}
