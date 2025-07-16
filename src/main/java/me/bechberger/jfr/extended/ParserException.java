package me.bechberger.jfr.extended;

import java.util.List;
import java.util.ArrayList;

/**
 * Exception thrown by the parser when encountering syntax errors.
 * Extends JFRQueryException to provide unified error handling.
 */
public class ParserException extends JFRQueryException {
    
    private Token errorToken;
    
    public ParserException(String message, String originalQuery) {
        super(message, originalQuery);
    }
    
    public ParserException(QueryError error, String originalQuery) {
        super(error, originalQuery);
        this.errorToken = error.getErrorToken();
    }
    
    public ParserException(List<QueryError> errors, String originalQuery) {
        super(errors, originalQuery);
        this.errorToken = errors.isEmpty() ? null : errors.get(0).getErrorToken();
    }
    
    /**
     * Constructor for compatibility with existing code
     */
    public ParserException(String message, Token errorToken, String originalQuery) {
        super(message, originalQuery);
        this.errorToken = errorToken;
        
        // Create a QueryError and add it
        QueryError error = new QueryError.Builder(ErrorOrigin.PARSER)
                .category(ErrorCategory.INVALID_SYNTAX)
                .errorMessage(message)
                .errorToken(errorToken)
                .originalQuery(originalQuery)
                .build();
        addError(error);
    }
    
    /**
     * Constructor for compatibility with old ParserError objects
     */
    public ParserException(ParserErrorHandler.ParserError parsingError, String originalQuery) {
        super(convertParserErrorToMessage(parsingError), originalQuery);
        if (parsingError != null) {
            this.errorToken = parsingError.getErrorToken();
            QueryError error = convertParserErrorToQueryError(parsingError, originalQuery);
            addError(error);
        }
    }
    
    /**
     * Compatibility constructor that takes just a ParserError
     */
    public ParserException(ParserErrorHandler.ParserError parsingError) {
        this(parsingError, "");
    }
    
    /**
     * Constructor for compatibility with old ParserError list
     */
    public static ParserException fromParserErrors(List<ParserErrorHandler.ParserError> parsingErrors, String originalQuery) {
        ParserException exception = new ParserException(convertParserErrorsToQueryErrors(parsingErrors), originalQuery);
        exception.errorToken = parsingErrors.isEmpty() ? null : parsingErrors.get(0).getErrorToken();
        return exception;
    }
    
    /**
     * Create a parser exception from a simple error message
     */
    public static ParserException fromMessage(String message, Token errorToken, String originalQuery) {
        QueryError error = new QueryError.Builder(ErrorOrigin.PARSER)
                .category(ErrorCategory.INVALID_SYNTAX)
                .errorMessage(message)
                .errorToken(errorToken)
                .originalQuery(originalQuery)
                .build();
        return new ParserException(error, originalQuery);
    }
    
    /**
     * Get the error token (for compatibility)
     */
    @Override
    public Token getErrorToken() {
        if (errorToken != null) {
            return errorToken;
        }
        return super.getErrorToken();
    }
    
    /**
     * Legacy support - convert to old ParserError list
     */
    public List<ParserErrorHandler.ParserError> getParsingErrors() {
        List<ParserErrorHandler.ParserError> parserErrors = new ArrayList<>();
        for (QueryError error : getErrors()) {
            if (error.getOrigin() == ErrorOrigin.PARSER) {
                ParserErrorHandler.ParserError parserError = new ParserErrorHandler.ParserError(
                    error.getErrorMessage(),
                    error.getSuggestion(),
                    error.getErrorToken(),
                    error.getContext(),
                    ParserErrorHandler.ErrorType.SYNTAX_ERROR,
                    error.getExamples()
                );
                parserErrors.add(parserError);
            }
        }
        return parserErrors;
    }
    
    public boolean hasParsingErrors() {
        return hasErrors();
    }
    
    // Helper methods for conversion
    private static String convertParserErrorToMessage(ParserErrorHandler.ParserError parsingError) {
        return parsingError != null ? parsingError.toString() : "Unknown parsing error";
    }
    
    private static QueryError convertParserErrorToQueryError(ParserErrorHandler.ParserError parsingError, String originalQuery) {
        return new QueryError.Builder(ErrorOrigin.PARSER)
                .category(ErrorCategory.INVALID_SYNTAX)
                .errorMessage(parsingError.getMessage())
                .suggestion(parsingError.getSuggestion())
                .context(parsingError.getContext())
                .examples(parsingError.getExamples())
                .errorToken(parsingError.getErrorToken())
                .originalQuery(originalQuery)
                .build();
    }
    
    private static List<QueryError> convertParserErrorsToQueryErrors(List<ParserErrorHandler.ParserError> parsingErrors) {
        List<QueryError> queryErrors = new ArrayList<>();
        for (ParserErrorHandler.ParserError parsingError : parsingErrors) {
            queryErrors.add(convertParserErrorToQueryError(parsingError, ""));
        }
        return queryErrors;
    }
    
    /**
     * Compatibility constructor for legacy code that doesn't have original query
     */
    public ParserException(String message) {
        this(message, "");
    }
    
    /**
     * Compatibility constructor with cause exception
     */
    public ParserException(String message, Exception cause) {
        this(message, "");
        initCause(cause);
    }
}
