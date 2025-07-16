package me.bechberger.jfr.extended;

import me.bechberger.jfr.extended.ast.Location;
import java.util.List;

/**
 * Exception thrown by the lexer when encountering invalid characters or syntax.
 * Extends JFRQueryException to provide unified error handling with Location objects.
 */
public class LexerException extends JFRQueryException {
    
    public LexerException(String message, String originalQuery) {
        super(message, originalQuery);
    }
    
    public LexerException(QueryError error, String originalQuery) {
        super(error, originalQuery);
    }
    
    public LexerException(List<QueryError> errors, String originalQuery) {
        super(errors, originalQuery);
    }
    
    /**
     * Compatibility constructor for legacy code that doesn't have original query
     */
    public LexerException(String message) {
        this(message, "");
    }
    
    /**
     * Create a lexer exception from a simple error message with Location
     */
    public static LexerException fromMessage(String message, String originalQuery, Location location) {
        QueryError error = new QueryError.Builder(ErrorOrigin.LEXER)
                .category(ErrorCategory.UNEXPECTED_CHARACTER)
                .errorMessage(message)
                .originalQuery(originalQuery)
                .build();
        return new LexerException(error, originalQuery);
    }
    
    /**
     * Create a lexer exception from a simple error message with line/column (legacy support)
     */
    public static LexerException fromMessage(String message, String originalQuery, int line, int column, int position) {
        Location location = new Location(line, column);
        return fromMessage(message, originalQuery, location);
    }
}
