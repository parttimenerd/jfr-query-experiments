package me.bechberger.jfr.extended;

/**
 * A parser exception that includes position information about where the error occurred.
 */
public class PositionedParserException extends ParserException {
    
    public PositionedParserException(String message, Token errorToken) {
        super(new ParserErrorHandler.ParserError(
            message, 
            "Position error context", 
            errorToken, 
            "", 
            ParserErrorHandler.ErrorType.SYNTAX_ERROR
        ));
    }
    
    public PositionedParserException(String message, Token errorToken, Throwable cause) {
        super(new ParserErrorHandler.ParserError(
            message, 
            "Position error context", 
            errorToken, 
            "", 
            ParserErrorHandler.ErrorType.SYNTAX_ERROR
        ));
    }
}
