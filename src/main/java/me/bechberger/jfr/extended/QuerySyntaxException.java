package me.bechberger.jfr.extended;

import java.util.ArrayList;
import java.util.List;

/**
 * Unified exception for both lexer and parser errors, providing enhanced error messages
 * with context analysis, visual formatting, and intelligent suggestions.
 */
public class QuerySyntaxException extends Exception {
    private final List<QuerySyntaxError> errors;
    private final String originalQuery;
    
    public QuerySyntaxException(String message, String originalQuery) {
        super(message);
        this.originalQuery = originalQuery;
        this.errors = new ArrayList<>();
    }
    
    public QuerySyntaxException(QuerySyntaxError error, String originalQuery) {
        super(error.getFormattedMessage());
        this.originalQuery = originalQuery;
        this.errors = new ArrayList<>();
        this.errors.add(error);
    }
    
    public QuerySyntaxException(List<QuerySyntaxError> errors, String originalQuery) {
        super(formatMultipleErrors(errors));
        this.originalQuery = originalQuery;
        this.errors = new ArrayList<>(errors);
    }
    
    public List<QuerySyntaxError> getErrors() {
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
        return errors.stream().anyMatch(error -> error.getErrorType() == QuerySyntaxError.ErrorOrigin.LEXER);
    }
    
    /**
     * Check if this exception contains parser errors
     */
    public boolean hasParserErrors() {
        return errors.stream().anyMatch(error -> error.getErrorType() == QuerySyntaxError.ErrorOrigin.PARSER);
    }
    
    /**
     * Get only the lexer errors
     */
    public List<QuerySyntaxError> getLexerErrors() {
        return errors.stream()
                     .filter(error -> error.getErrorType() == QuerySyntaxError.ErrorOrigin.LEXER)
                     .toList();
    }
    
    /**
     * Get only the parser errors
     */
    public List<QuerySyntaxError> getParserErrors() {
        return errors.stream()
                     .filter(error -> error.getErrorType() == QuerySyntaxError.ErrorOrigin.PARSER)
                     .toList();
    }
    
    private static String formatMultipleErrors(List<QuerySyntaxError> errors) {
        if (errors == null || errors.isEmpty()) {
            return "Unknown syntax error";
        }
        
        if (errors.size() == 1) {
            return errors.get(0).getFormattedMessage();
        }
        
        StringBuilder sb = new StringBuilder();
        sb.append("Multiple syntax errors found (").append(errors.size()).append("):\n");
        sb.append("━".repeat(80)).append("\n");
        
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
    
    public List<ParserErrorHandler.ParserError> getParsingErrors() {
        // Convert QuerySyntaxError back to ParserError for compatibility
        List<ParserErrorHandler.ParserError> parserErrors = new ArrayList<>();
        for (QuerySyntaxError error : errors) {
            if (error.getErrorType() == QuerySyntaxError.ErrorOrigin.PARSER) {
                // Create a compatible ParserError
                parserErrors.add(new ParserErrorHandler.ParserError(
                    error.getProblemDescription(),
                    error.getSuggestion(),
                    error.getErrorToken(),
                    error.getContextDescription(),
                    ParserErrorHandler.ErrorType.SYNTAX_ERROR,
                    error.getExamples()
                ));
            }
        }
        return parserErrors;
    }
}
