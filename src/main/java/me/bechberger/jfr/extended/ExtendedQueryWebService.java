package me.bechberger.jfr.extended;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import me.bechberger.jfr.extended.AutoCompletionService.CompletionContext;
import me.bechberger.jfr.extended.AutoCompletionService.CompletionItem;
import me.bechberger.jfr.extended.SyntaxHighlighter.HighlightedToken;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

/**
 * Web service for the extended JFR query language
 * Provides auto-completion and syntax highlighting endpoints
 */
public class ExtendedQueryWebService implements Callable<Integer> {
    
    private final int port;
    private final String host;
    private final AutoCompletionService autoCompletionService;
    private final SyntaxHighlighter syntaxHighlighter;
    
    public ExtendedQueryWebService(String host, int port) {
        this.host = host;
        this.port = port;
        this.autoCompletionService = new AutoCompletionService();
        this.syntaxHighlighter = new SyntaxHighlighter();
    }
    
    @Override
    public Integer call() {
        try {
            HttpServer server = HttpServer.create(new InetSocketAddress(host, port), 0);
            
            // Auto-completion endpoint
            server.createContext("/autocomplete", new AutoCompleteHandler());
            
            // Syntax highlighting endpoint
            server.createContext("/highlight", new SyntaxHighlightHandler());
            
            // Grammar endpoint
            server.createContext("/grammar", new GrammarHandler());
            
            // Event types endpoint (would be populated from JFR data)
            server.createContext("/events", new EventsHandler());
            
            // Event fields endpoint (would be populated from JFR data)
            server.createContext("/fields", new FieldsHandler());
            
            // CSS endpoint for syntax highlighting
            server.createContext("/css", new CSSHandler());
            
            // Health check endpoint
            server.createContext("/health", new HealthHandler());
            
            server.start();
            System.out.printf("Extended JFR Query Web Service started at http://%s:%d%n", host, port);
            System.out.println("Available endpoints:");
            System.out.println("  /autocomplete?query=SELECT&cursor=6");
            System.out.println("  /highlight?query=SELECT * FROM GarbageCollection");
            System.out.println("  /grammar");
            System.out.println("  /events");
            System.out.println("  /fields?event=jdk.GarbageCollection");
            System.out.println("  /css");
            System.out.println("  /health");
            
            return 0;
        } catch (Exception e) {
            System.err.println("Error starting web service: " + e.getMessage());
            return 1;
        }
    }
    
    /**
     * Auto-completion handler
     */
    private class AutoCompleteHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!"GET".equals(exchange.getRequestMethod())) {
                sendError(exchange, 405, "Method not allowed");
                return;
            }
            
            try {
                Map<String, String> params = parseQueryParameters(exchange.getRequestURI());
                String query = params.get("query");
                String cursorStr = params.get("cursor");
                
                if (query == null) {
                    sendError(exchange, 400, "Missing 'query' parameter");
                    return;
                }
                
                int cursor = cursorStr != null ? Integer.parseInt(cursorStr) : query.length();
                cursor = Math.max(0, Math.min(cursor, query.length()));
                
                // Extract current token at cursor position
                String currentToken = extractCurrentToken(query, cursor);
                
                // Get available event types and fields (in a real implementation, these would come from JFR data)
                List<String> availableEventTypes = getAvailableEventTypes();
                List<String> availableFields = getAvailableFields(null);
                List<String> variables = extractVariables(query);
                
                CompletionContext context = new CompletionContext(
                    query, cursor, currentToken, availableEventTypes, availableFields, variables
                );
                
                List<CompletionItem> completions = autoCompletionService.getCompletions(context);
                
                // Convert to JSON
                String json = completionsToJson(completions);
                
                exchange.getResponseHeaders().set("Content-Type", "application/json");
                exchange.getResponseHeaders().set("Access-Control-Allow-Origin", "*");
                sendResponse(exchange, 200, json);
                
            } catch (Exception e) {
                sendError(exchange, 500, "Internal server error: " + e.getMessage());
            }
        }
    }
    
    /**
     * Syntax highlighting handler
     */
    private class SyntaxHighlightHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!"GET".equals(exchange.getRequestMethod())) {
                sendError(exchange, 405, "Method not allowed");
                return;
            }
            
            try {
                Map<String, String> params = parseQueryParameters(exchange.getRequestURI());
                String query = params.get("query");
                
                if (query == null) {
                    sendError(exchange, 400, "Missing 'query' parameter");
                    return;
                }
                
                List<HighlightedToken> tokens = syntaxHighlighter.highlight(query);
                
                // Convert to JSON
                String json = highlightTokensToJson(tokens);
                
                exchange.getResponseHeaders().set("Content-Type", "application/json");
                exchange.getResponseHeaders().set("Access-Control-Allow-Origin", "*");
                sendResponse(exchange, 200, json);
                
            } catch (Exception e) {
                sendError(exchange, 500, "Internal server error: " + e.getMessage());
            }
        }
    }
    
    /**
     * Grammar handler
     */
    private class GrammarHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!"GET".equals(exchange.getRequestMethod())) {
                sendError(exchange, 405, "Method not allowed");
                return;
            }
            
            try {
                String grammar = Grammar.getGrammarText();
                
                exchange.getResponseHeaders().set("Content-Type", "text/plain");
                exchange.getResponseHeaders().set("Access-Control-Allow-Origin", "*");
                sendResponse(exchange, 200, grammar);
                
            } catch (Exception e) {
                sendError(exchange, 500, "Internal server error: " + e.getMessage());
            }
        }
    }
    
    /**
     * Events handler - returns available event types
     */
    private class EventsHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!"GET".equals(exchange.getRequestMethod())) {
                sendError(exchange, 405, "Method not allowed");
                return;
            }
            
            try {
                List<String> eventTypes = getAvailableEventTypes();
                
                // Convert to JSON array
                StringBuilder json = new StringBuilder();
                json.append("[");
                for (int i = 0; i < eventTypes.size(); i++) {
                    if (i > 0) json.append(",");
                    json.append("\"").append(escapeJson(eventTypes.get(i))).append("\"");
                }
                json.append("]");
                
                exchange.getResponseHeaders().set("Content-Type", "application/json");
                exchange.getResponseHeaders().set("Access-Control-Allow-Origin", "*");
                sendResponse(exchange, 200, json.toString());
                
            } catch (Exception e) {
                sendError(exchange, 500, "Internal server error: " + e.getMessage());
            }
        }
    }
    
    /**
     * Fields handler - returns available fields for an event type
     */
    private class FieldsHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!"GET".equals(exchange.getRequestMethod())) {
                sendError(exchange, 405, "Method not allowed");
                return;
            }
            
            try {
                Map<String, String> params = parseQueryParameters(exchange.getRequestURI());
                String eventType = params.get("event");
                
                List<String> fields = getAvailableFields(eventType);
                
                // Convert to JSON array
                StringBuilder json = new StringBuilder();
                json.append("[");
                for (int i = 0; i < fields.size(); i++) {
                    if (i > 0) json.append(",");
                    json.append("\"").append(escapeJson(fields.get(i))).append("\"");
                }
                json.append("]");
                
                exchange.getResponseHeaders().set("Content-Type", "application/json");
                exchange.getResponseHeaders().set("Access-Control-Allow-Origin", "*");
                sendResponse(exchange, 200, json.toString());
                
            } catch (Exception e) {
                sendError(exchange, 500, "Internal server error: " + e.getMessage());
            }
        }
    }
    
    /**
     * CSS handler - returns CSS for syntax highlighting
     */
    private class CSSHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!"GET".equals(exchange.getRequestMethod())) {
                sendError(exchange, 405, "Method not allowed");
                return;
            }
            
            try {
                String css = syntaxHighlighter.getCSS();
                
                exchange.getResponseHeaders().set("Content-Type", "text/css");
                exchange.getResponseHeaders().set("Access-Control-Allow-Origin", "*");
                sendResponse(exchange, 200, css);
                
            } catch (Exception e) {
                sendError(exchange, 500, "Internal server error: " + e.getMessage());
            }
        }
    }
    
    /**
     * Health check handler
     */
    private class HealthHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.getResponseHeaders().set("Access-Control-Allow-Origin", "*");
            sendResponse(exchange, 200, "{\"status\":\"ok\",\"service\":\"extended-jfr-query\"}");
        }
    }
    
    // Helper methods
    
    private void sendResponse(HttpExchange exchange, int statusCode, String response) throws IOException {
        byte[] responseBytes = response.getBytes(StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(statusCode, responseBytes.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(responseBytes);
        }
    }
    
    private void sendError(HttpExchange exchange, int statusCode, String message) throws IOException {
        String error = String.format("{\"error\":\"%s\"}", escapeJson(message));
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.getResponseHeaders().set("Access-Control-Allow-Origin", "*");
        sendResponse(exchange, statusCode, error);
    }
    
    private Map<String, String> parseQueryParameters(URI uri) {
        Map<String, String> params = new HashMap<>();
        String query = uri.getQuery();
        if (query != null) {
            String[] pairs = query.split("&");
            for (String pair : pairs) {
                String[] keyValue = pair.split("=", 2);
                if (keyValue.length == 2) {
                    String key = URLDecoder.decode(keyValue[0], StandardCharsets.UTF_8);
                    String value = URLDecoder.decode(keyValue[1], StandardCharsets.UTF_8);
                    params.put(key, value);
                }
            }
        }
        return params;
    }
    
    private String extractCurrentToken(String query, int cursor) {
        if (cursor <= 0 || cursor > query.length()) {
            return "";
        }
        
        int start = cursor - 1;
        int end = cursor;
        
        // Find token boundaries
        while (start > 0 && Character.isLetterOrDigit(query.charAt(start - 1))) {
            start--;
        }
        while (end < query.length() && Character.isLetterOrDigit(query.charAt(end))) {
            end++;
        }
        
        return query.substring(start, end);
    }
    
    private List<String> extractVariables(String query) {
        List<String> variables = new ArrayList<>();
        
        // Simple regex to find variable assignments (var := ...)
        String[] lines = query.split("\\n");
        for (String line : lines) {
            if (line.contains(":=")) {
                String[] parts = line.split(":=", 2);
                if (parts.length == 2) {
                    String varName = parts[0].trim();
                    if (!varName.isEmpty()) {
                        variables.add(varName);
                    }
                }
            }
        }
        
        return variables;
    }
    
    private List<String> getAvailableEventTypes() {
        // In a real implementation, this would query JFR data
        return List.of(
            "jdk.GarbageCollection",
            "jdk.G1GarbageCollection",
            "jdk.ExecutionSample",
            "jdk.ObjectAllocationInNewTLAB",
            "jdk.ObjectAllocationOutsideTLAB",
            "jdk.ThreadSleep",
            "jdk.ThreadPark",
            "jdk.JavaMonitorEnter",
            "jdk.JavaMonitorWait",
            "jdk.SystemGC",
            "jdk.CPULoad",
            "jdk.ThreadCPULoad",
            "jdk.ClassLoad",
            "jdk.Compilation",
            "jdk.SafepointBegin",
            "jdk.SafepointEnd"
        );
    }
    
    private List<String> getAvailableFields(String eventType) {
        // In a real implementation, this would query JFR data for the specific event type
        return List.of(
            "duration",
            "startTime",
            "endTime",
            "stackTrace",
            "thread",
            "eventThread",
            "gcId",
            "cause",
            "size",
            "count",
            "heapUsed",
            "heapSize",
            "id",
            "name",
            "type",
            "value"
        );
    }
    
    private String completionsToJson(List<CompletionItem> completions) {
        StringBuilder json = new StringBuilder();
        json.append("{\"completions\":[");
        
        for (int i = 0; i < completions.size(); i++) {
            if (i > 0) json.append(",");
            CompletionItem item = completions.get(i);
            json.append("{");
            json.append("\"text\":\"").append(escapeJson(item.text())).append("\",");
            json.append("\"type\":\"").append(item.type()).append("\",");
            json.append("\"description\":\"").append(escapeJson(item.description())).append("\",");
            json.append("\"documentation\":\"").append(escapeJson(item.documentation())).append("\",");
            json.append("\"priority\":").append(item.priority());
            json.append("}");
        }
        
        json.append("]}");
        return json.toString();
    }
    
    private String highlightTokensToJson(List<HighlightedToken> tokens) {
        StringBuilder json = new StringBuilder();
        json.append("{\"tokens\":[");
        
        for (int i = 0; i < tokens.size(); i++) {
            if (i > 0) json.append(",");
            HighlightedToken token = tokens.get(i);
            json.append("{");
            json.append("\"text\":\"").append(escapeJson(token.text())).append("\",");
            json.append("\"start\":").append(token.start()).append(",");
            json.append("\"end\":").append(token.end()).append(",");
            json.append("\"category\":\"").append(token.category()).append("\",");
            json.append("\"cssClass\":\"").append(syntaxHighlighter.getCSSClass(token.category())).append("\"");
            json.append("}");
        }
        
        json.append("]}");
        return json.toString();
    }
    
    private String escapeJson(String str) {
        if (str == null) return "";
        return str.replace("\\", "\\\\")
                  .replace("\"", "\\\"")
                  .replace("\n", "\\n")
                  .replace("\r", "\\r")
                  .replace("\t", "\\t");
    }
    
    public static void main(String[] args) {
        ExtendedQueryWebService service = new ExtendedQueryWebService("localhost", 8081);
        try {
            service.call();
            // Keep the server running
            Thread.currentThread().join();
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            System.exit(1);
        }
    }
}
