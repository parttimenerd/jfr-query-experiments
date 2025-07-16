package me.bechberger.jfr.extended.engine.exception;

import me.bechberger.jfr.extended.ast.ASTNode;
import me.bechberger.jfr.extended.ast.Location;

/**
 * Base exception for query execution errors in the JFR Extended Query Engine.
 * 
 * This exception provides rich context about where the error occurred in the query,
 * including the AST node that caused the error, location information, and detailed
 * error messages that are helpful for both developers and end users.
 * 
 * @author Query Engine Team
 * @since 1.0
 */
public class QueryExecutionException extends RuntimeException {
    
    private final ASTNode errorNode;
    private final Location location;
    private final String context;
    private final String userHint;
    
    /**
     * Creates a new QueryExecutionException with detailed context.
     * 
     * @param message The primary error message
     * @param errorNode The AST node where the error occurred (can be null)
     * @param context Additional context about what was being executed
     * @param userHint A helpful hint for the user on how to fix the issue
     * @param cause The underlying cause (can be null)
     */
    public QueryExecutionException(String message, ASTNode errorNode, String context, String userHint, Throwable cause) {
        super(buildDetailedMessage(message, errorNode, context, userHint), cause);
        this.errorNode = errorNode;
        this.location = errorNode != null ? new Location(errorNode.getLine(), errorNode.getColumn()) : null;
        this.context = context;
        this.userHint = userHint;
    }
    
    /**
     * Creates a new QueryExecutionException with context but no hint.
     */
    public QueryExecutionException(String message, ASTNode errorNode, String context, Throwable cause) {
        this(message, errorNode, context, null, cause);
    }
    
    /**
     * Creates a new QueryExecutionException with minimal context.
     */
    public QueryExecutionException(String message, ASTNode errorNode, String context) {
        this(message, errorNode, context, null, null);
    }
    
    /**
     * Creates a new QueryExecutionException without AST node context.
     */
    public QueryExecutionException(String message, String context, String userHint) {
        this(message, null, context, userHint, null);
    }
    
    /**
     * Creates a new QueryExecutionException with just a message and context.
     */
    public QueryExecutionException(String message, String context) {
        this(message, null, context, null, null);
    }
    
    /**
     * Builds a detailed, user-friendly error message.
     */
    private static String buildDetailedMessage(String message, ASTNode errorNode, String context, String userHint) {
        StringBuilder sb = new StringBuilder();
        
        // Clean up the primary message - remove redundant prefixes and verbose context
        String cleanMessage = message;
        if (message != null) {
            // Remove common redundant prefixes that might be added by callers
            cleanMessage = message.replaceFirst("^(Query Execution Error:|Error:|query parsing and execution:|Query evaluation failed:|Operation:)\\s*", "");
            // Remove redundant context patterns like "Unsupported operation during..."
            cleanMessage = cleanMessage.replaceFirst("^Unsupported operation during\\s+", "");
            // Capitalize first letter if not empty
            if (!cleanMessage.isEmpty()) {
                cleanMessage = Character.toUpperCase(cleanMessage.charAt(0)) + cleanMessage.substring(1);
            }
        }
        
        // Primary error message - just the clean message without redundant prefixes
        sb.append(cleanMessage);
        
        // Location information if available
        if (errorNode != null) {
            Location loc = new Location(errorNode.getLine(), errorNode.getColumn());
            sb.append("\n  Location: Line ").append(loc.line()).append(", Column ").append(loc.column());
        }
        
        // AST Node information
        if (errorNode != null) {
            sb.append("\n  Node Type: ").append(errorNode.getClass().getSimpleName());
            sb.append("\n  Node Content: ").append(formatNodeContent(errorNode));
        }
        
        // Context information (only if it's not redundant)
        if (context != null && !context.isEmpty() && !isRedundantContext(context, cleanMessage)) {
            sb.append("\n  Context: ").append(context);
        }
        
        // User hint
        if (userHint != null && !userHint.isEmpty()) {
            sb.append("\n  Suggestion: ").append(userHint);
        }
        
        return sb.toString();
    }
    
    /**
     * Check if context information would be redundant given the error message
     */
    private static boolean isRedundantContext(String context, String message) {
        if (context == null || message == null) return false;
        
        // Don't show context if it's just repeating what's in the message
        String lowerContext = context.toLowerCase();
        String lowerMessage = message.toLowerCase();
        
        return lowerMessage.contains(lowerContext) || 
               lowerContext.contains("query") || 
               lowerContext.contains("parsing") ||
               lowerContext.contains("execution");
    }
    
    /**
     * Formats the node content for display in error messages.
     */
    private static String formatNodeContent(ASTNode node) {
        if (node == null) return "null";
        
        String nodeStr = node.toString();
        if (nodeStr.length() > 100) {
            return nodeStr.substring(0, 97) + "...";
        }
        return nodeStr;
    }
    
    // Getters for accessing exception details
    
    public ASTNode getErrorNode() {
        return errorNode;
    }
    
    public Location getLocation() {
        return location;
    }
    
    public String getContext() {
        return context;
    }
    
    public String getUserHint() {
        return userHint;
    }
    
    /**
     * Returns a simplified error message for logging purposes.
     */
    public String getSimpleMessage() {
        String msg = getMessage();
        int firstNewline = msg.indexOf('\n');
        return firstNewline > 0 ? msg.substring(0, firstNewline) : msg;
    }
    
    /**
     * Returns true if this exception has location information.
     */
    public boolean hasLocation() {
        return location != null;
    }
    
    /**
     * Returns true if this exception has an associated AST node.
     */
    public boolean hasErrorNode() {
        return errorNode != null;
    }
}
