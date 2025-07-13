package me.bechberger.jfr.extended;

/**
 * Represents a parsing error with full metadata for diagnostics and reporting.
 * This class follows the same format as ParserErrorHandler.ParserError.
 */
public class ParsingError {
    private final String message;
    private final String suggestion;
    private final Token errorToken;
    private final String context;
    private final Throwable cause;

    public ParsingError(String context, String message, Token errorToken) {
        this(context, message, errorToken, null, null);
    }

    public ParsingError(String context, String message, Token errorToken, Throwable cause) {
        this(context, message, errorToken, null, cause);
    }

    public ParsingError(String context, String message, Token errorToken, String suggestion, Throwable cause) {
        this.context = context;
        this.message = message;
        this.errorToken = errorToken;
        this.suggestion = suggestion;
        this.cause = cause;
    }

    public String getMessage() {
        return message;
    }

    public String getSuggestion() {
        return suggestion;
    }

    public Token getErrorToken() {
        return errorToken;
    }

    public String getContext() {
        return context;
    }

    public Throwable getCause() {
        return cause;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Syntax Error at ").append(errorToken.getPositionString()).append(":\n");
        sb.append("  ").append(message).append("\n");
        if (context != null && !context.isEmpty()) {
            sb.append("  Context:\n").append(context).append("\n");
        }
        if (suggestion != null && !suggestion.isEmpty()) {
            sb.append("  Suggestion: ").append(suggestion).append("\n");
        }
        return sb.toString();
    }
}
