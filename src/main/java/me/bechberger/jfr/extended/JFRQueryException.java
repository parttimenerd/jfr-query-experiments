package me.bechberger.jfr.extended;

import java.util.ArrayList;
import java.util.List;

/**
 * Base exception class for all JFR query parsing errors (lexer and parser).
 * 
 * <p>This class implements the user's requirement that "LexerException should have the same 
 * structure as ParserExceptions and have the same parent class". It provides unified error 
 * handling with enhanced formatting and context analysis for both lexer and parser errors.</p>
 * 
 * <p><strong>Key Capabilities:</strong></p>
 * <ul>
 *   <li><strong>Unified error collection:</strong> Can hold multiple errors from different sources</li>
 *   <li><strong>Error categorization:</strong> Distinguishes between lexer and parser errors</li>
 *   <li><strong>Enhanced formatting:</strong> Provides detailed, formatted error messages</li>
 *   <li><strong>Structured error data:</strong> All errors use the unified QueryError structure</li>
 * </ul>
 * 
 * <p><strong>Error Origins:</strong></p>
 * <ul>
 *   <li>{@link ErrorOrigin#LEXER} - Tokenization errors (invalid characters, unclosed strings)</li>
 *   <li>{@link ErrorOrigin#PARSER} - Syntax errors (unexpected tokens, missing clauses)</li>
 * </ul>
 * 
 * <p><strong>Usage Example:</strong></p>
 * <pre>{@code
 * // Single error
 * QueryError error = new QueryError.Builder(ErrorOrigin.LEXER)...build();
 * JFRQueryException exception = new JFRQueryException(error, originalQuery);
 * 
 * // Multiple errors
 * List<QueryError> errors = Arrays.asList(error1, error2);
 * JFRQueryException exception = new JFRQueryException(errors, originalQuery);
 * }</pre>
 * 
 * @see QueryError The unified error representation used by this exception
 * @see LexerException Lexer-specific subclass
 * @see ParserException Parser-specific subclass
 */
public class JFRQueryException extends Exception {
    
    /**
     * Type of error - distinguishes between lexer and parser errors.
     * 
     * <p>This enum implements the unified error structure where both lexer and parser 
     * errors are categorized under the same system, addressing the requirement that
     * they "should be collected into the same structure".</p>
     */
    public enum ErrorOrigin {
        /** Errors during tokenization (lexical analysis) - invalid characters, unclosed literals */
        LEXER("Lexer"),
        /** Errors during parsing (syntax analysis) - unexpected tokens, missing clauses */
        PARSER("Parser");
        
        private final String displayName;
        
        ErrorOrigin(String displayName) {
            this.displayName = displayName;
        }
        
        /** @return Human-readable name for error messages */
        public String getDisplayName() {
            return displayName;
        }
    }
    
    /**
     * Category of syntax error for better organization and specific guidance.
     * 
     * <p>These categories help provide more targeted error messages and suggestions,
     * improving the user experience when debugging query syntax issues.</p>
     */
    public enum ErrorCategory {
        /** Invalid or unsupported characters in the input */
        UNEXPECTED_CHARACTER("Unexpected Character"),
        /** Required token is missing (e.g., missing FROM keyword) */
        MISSING_TOKEN("Missing Token"),
        /** Token found in wrong context (e.g., LIMIT before SELECT) */
        UNEXPECTED_TOKEN("Unexpected Token"),
        /** General syntax rule violations */
        INVALID_SYNTAX("Invalid Syntax"),
        /** String or comment not properly closed */
        UNCLOSED_LITERAL("Unclosed Literal"),
        /** Likely typo with suggested correction */
        TYPO_SUGGESTION("Possible Typo"),
        /** Error related to parsing context or state */
        CONTEXT_ERROR("Context Error"),
        /** Error related to meaning rather than syntax */
        SEMANTIC_ERROR("Semantic Error");
        
        private final String displayName;
        
        ErrorCategory(String displayName) {
            this.displayName = displayName;
        }
        
        /** @return Human-readable name for error categorization */
        public String getDisplayName() {
            return displayName;
        }
    }
    
    private final List<QueryError> errors;
    private final String originalQuery;
    
    public JFRQueryException(String message, String originalQuery) {
        super(message);
        this.originalQuery = originalQuery;
        this.errors = new ArrayList<>();
    }
    
    public JFRQueryException(QueryError error, String originalQuery) {
        super(error.getFormattedMessage());
        this.originalQuery = originalQuery;
        this.errors = new ArrayList<>();
        this.errors.add(error);
    }
    
    public JFRQueryException(List<QueryError> errors, String originalQuery) {
        super(formatMultipleErrors(errors));
        this.originalQuery = originalQuery;
        this.errors = new ArrayList<>(errors);
    }
    
    public List<QueryError> getErrors() {
        return new ArrayList<>(errors);
    }
    
    public boolean hasErrors() {
        return !errors.isEmpty();
    }
    
    public String getOriginalQuery() {
        return originalQuery;
    }
    
    /**
     * Check if this exception contains lexer errors
     */
    public boolean hasLexerErrors() {
        return errors.stream().anyMatch(error -> error.getOrigin() == ErrorOrigin.LEXER);
    }
    
    /**
     * Check if this exception contains parser errors
     */
    public boolean hasParserErrors() {
        return errors.stream().anyMatch(error -> error.getOrigin() == ErrorOrigin.PARSER);
    }
    
    /**
     * Get only the lexer errors
     */
    public List<QueryError> getLexerErrors() {
        return errors.stream()
                     .filter(error -> error.getOrigin() == ErrorOrigin.LEXER)
                     .toList();
    }
    
    /**
     * Get only the parser errors
     */
    public List<QueryError> getParserErrors() {
        return errors.stream()
                     .filter(error -> error.getOrigin() == ErrorOrigin.PARSER)
                     .toList();
    }
    
    /**
     * Add an error to this exception
     */
    public void addError(QueryError error) {
        errors.add(error);
    }
    
    /**
     * Format multiple errors into a single message
     */
    private static String formatMultipleErrors(List<QueryError> errors) {
        if (errors == null || errors.isEmpty()) {
            return "Unknown syntax error";
        }
        
        if (errors.size() == 1) {
            return errors.get(0).getFormattedMessage();
        }
        
        StringBuilder sb = new StringBuilder();
        sb.append("Multiple syntax errors found (").append(errors.size()).append("):");
        sb.append("\n").append("━".repeat(80)).append("\n");
        
        for (int i = 0; i < errors.size(); i++) {
            sb.append("\n").append(i + 1).append(". ");
            String errorMessage = errors.get(i).getFormattedMessage();
            
            // Indent each line of the error message
            String[] lines = errorMessage.split("\n");
            for (int j = 0; j < lines.length; j++) {
                if (j > 0) {
                    sb.append("   "); // Align with error number
                }
                sb.append(lines[j]);
                if (j < lines.length - 1) {
                    sb.append("\n");
                }
            }
            
            if (i < errors.size() - 1) {
                sb.append("\n").append("─".repeat(40));
            }
        }
        
        return sb.toString();
    }
    
    // Compatibility methods for existing code
    public Token getErrorToken() {
        return errors.isEmpty() ? null : errors.get(0).getErrorToken();
    }
}
