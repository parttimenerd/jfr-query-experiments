package me.bechberger.jfr.extended.plan;

import me.bechberger.jfr.extended.ast.ASTNode;
import me.bechberger.jfr.extended.ast.Location;

/**
 * Enhanced error context with AST and execution details.
 * 
 * This class provides rich error context information including
 * AST node positioning, execution state, and detailed error messages
 * for better debugging and error reporting.
 * 
 * @author JFR Query Plan Architecture
 * @since 2.0
 */
public class JFRErrorContext {
    private final ASTNode sourceNode;
    private final Location location;
    private final String executionPhase;
    private final String detailedMessage;
    private final Object additionalContext;
    
    public JFRErrorContext(ASTNode sourceNode, 
                          String executionPhase, 
                          String detailedMessage) {
        this(sourceNode, executionPhase, detailedMessage, null);
    }
    
    public JFRErrorContext(ASTNode sourceNode, 
                          String executionPhase, 
                          String detailedMessage, 
                          Object additionalContext) {
        this.sourceNode = sourceNode;
        this.location = sourceNode != null ? new Location(sourceNode.getLine(), sourceNode.getColumn()) : null;
        this.executionPhase = executionPhase;
        this.detailedMessage = detailedMessage;
        this.additionalContext = additionalContext;
    }
    
    /**
     * Get the AST node that caused the error.
     */
    public ASTNode getSourceNode() {
        return sourceNode;
    }
    
    /**
     * Get the location in the source query where the error occurred.
     */
    public Location getLocation() {
        return location;
    }
    
    /**
     * Get the execution phase during which the error occurred.
     */
    public String getExecutionPhase() {
        return executionPhase;
    }
    
    /**
     * Get the detailed error message.
     */
    public String getDetailedMessage() {
        return detailedMessage;
    }
    
    /**
     * Get any additional context information.
     */
    public Object getAdditionalContext() {
        return additionalContext;
    }
    
    /**
     * Create a comprehensive error message with context.
     */
    public String createContextualErrorMessage() {
        StringBuilder sb = new StringBuilder();
        sb.append("Error during ").append(executionPhase).append(": ");
        sb.append(detailedMessage);
        
        if (location != null) {
            sb.append(" (at line ").append(location.line())
              .append(", column ").append(location.column()).append(")");
        }
        
        if (additionalContext != null) {
            sb.append(" [").append(additionalContext).append("]");
        }
        
        return sb.toString();
    }
    
    @Override
    public String toString() {
        return createContextualErrorMessage();
    }
}
