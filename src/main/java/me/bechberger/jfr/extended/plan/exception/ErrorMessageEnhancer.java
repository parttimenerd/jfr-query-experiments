package me.bechberger.jfr.extended.plan.exception;

import me.bechberger.jfr.extended.ParserException;
import me.bechberger.jfr.extended.plan.PlanExecutionException;

import java.io.PrintWriter;
import java.io.StringWriter;

/**
 * Utility class for creating detailed error messages from exceptions.
 * Extracts and formats information from causing exceptions to provide
 * more helpful debugging information.
 */
public class ErrorMessageEnhancer {
    
    /**
     * Create an enhanced error message from an exception and context.
     * 
     * @param primaryMessage The main error message
     * @param query The query that failed (optional)
     * @param cause The underlying exception (optional)
     * @return Enhanced error message with context and details
     */
    public static String createEnhancedMessage(String primaryMessage, String query, Throwable cause) {
        StringBuilder enhanced = new StringBuilder();
        
        // Start with the primary message
        enhanced.append(primaryMessage);
        
        // Add query context if available
        if (query != null && !query.trim().isEmpty()) {
            enhanced.append("\n\nQuery: ").append(formatQuery(query));
        }
        
        // Add cause information if available
        if (cause != null) {
            enhanced.append("\n\nCause: ").append(extractCauseDetails(cause));
        }
        
        return enhanced.toString();
    }
    
    /**
     * Extract detailed information from a causing exception.
     */
    private static String extractCauseDetails(Throwable cause) {
        StringBuilder details = new StringBuilder();
        
        // Add the exception type and message
        details.append(cause.getClass().getSimpleName());
        if (cause.getMessage() != null) {
            details.append(": ").append(cause.getMessage());
        }
        
        // For parser exceptions, add specific formatting
        if (cause instanceof ParserException parserEx) {
            details.append("\n  Parser Error: ").append(parserEx.getMessage());
        }
        
        // Add root cause if different
        Throwable rootCause = findRootCause(cause);
        if (rootCause != null && rootCause != cause) {
            details.append("\n  Root Cause: ").append(rootCause.getClass().getSimpleName());
            if (rootCause.getMessage() != null) {
                details.append(": ").append(rootCause.getMessage());
            }
        }
        
        // Add a few relevant stack trace lines for debugging
        String stackSummary = extractRelevantStackTrace(cause);
        if (!stackSummary.isEmpty()) {
            details.append("\n  Stack Trace Summary:\n").append(stackSummary);
        }
        
        return details.toString();
    }
    
    /**
     * Find the root cause of an exception chain.
     */
    private static Throwable findRootCause(Throwable throwable) {
        Throwable rootCause = throwable;
        while (rootCause.getCause() != null && rootCause.getCause() != rootCause) {
            rootCause = rootCause.getCause();
        }
        return rootCause;
    }
    
    /**
     * Extract relevant lines from stack trace for debugging.
     * Focuses on application code rather than framework internals.
     */
    private static String extractRelevantStackTrace(Throwable cause) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        cause.printStackTrace(pw);
        
        String[] lines = sw.toString().split("\n");
        StringBuilder relevant = new StringBuilder();
        int count = 0;
        
        for (String line : lines) {
            // Include lines from our application packages
            if (line.contains("me.bechberger.jfr.extended") && 
                !line.contains("at java.") && 
                !line.contains("at sun.") &&
                count < 5) {
                relevant.append("    ").append(line.trim()).append("\n");
                count++;
            }
        }
        
        return relevant.toString();
    }
    
    /**
     * Format the query for better readability in error messages.
     */
    private static String formatQuery(String query) {
        String trimmed = query.trim();
        
        // If it's a short query, keep it on one line
        if (trimmed.length() <= 80) {
            return trimmed;
        }
        
        // For longer queries, add some basic formatting
        return trimmed.replace(" FROM ", "\n  FROM ")
                     .replace(" WHERE ", "\n  WHERE ")
                     .replace(" GROUP BY ", "\n  GROUP BY ")
                     .replace(" ORDER BY ", "\n  ORDER BY ")
                     .replace(" HAVING ", "\n  HAVING ");
    }
    
    /**
     * Create a concise error message for test failures.
     * Used when we want a shorter message without full details.
     */
    public static String createConciseMessage(String primaryMessage, Throwable cause) {
        if (cause == null) {
            return primaryMessage;
        }
        
        StringBuilder concise = new StringBuilder(primaryMessage);
        
        // Add the most relevant cause information
        if (cause instanceof ParserException) {
            concise.append(" (Parser: ").append(cause.getMessage()).append(")");
        } else if (cause.getMessage() != null && !cause.getMessage().isEmpty()) {
            concise.append(" (").append(cause.getClass().getSimpleName())
                   .append(": ").append(cause.getMessage()).append(")");
        } else {
            concise.append(" (").append(cause.getClass().getSimpleName()).append(")");
        }
        
        return concise.toString();
    }
    
    /**
     * Extract context-specific information based on exception type.
     */
    public static String extractContextualInfo(Throwable cause) {
        if (cause instanceof PlanExecutionException planEx) {
            // Extract information from plan execution context
            if (planEx.getErrorContext() != null) {
                return "Plan: " + planEx.getErrorContext().getExecutionPhase() + 
                       " | Context: " + planEx.getErrorContext().getDetailedMessage();
            }
        }
        
        if (cause instanceof ParserException) {
            return "Parser error at query position";
        }
        
        return "";
    }
}
