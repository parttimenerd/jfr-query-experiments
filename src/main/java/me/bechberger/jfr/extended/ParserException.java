package me.bechberger.jfr.extended;

import java.util.List;
import java.util.ArrayList;

/**
 * Exception thrown during parsing when syntax errors or other parsing problems are encountered.
 * Can wrap multiple ParserError instances for comprehensive diagnostics.
 */
public class ParserException extends Exception {
    private final List<ParserErrorHandler.ParserError> parsingErrors;

    public ParserException(String message) {
        super(message);
        this.parsingErrors = new ArrayList<>();
    }

    public ParserException(String message, Throwable cause) {
        super(message, cause);
        this.parsingErrors = new ArrayList<>();
    }

    public ParserException(ParserErrorHandler.ParserError parsingError) {
        super(parsingError != null ? parsingError.toString() : null);
        this.parsingErrors = new ArrayList<>();
        if (parsingError != null) {
            this.parsingErrors.add(parsingError);
        }
    }

    public ParserException(List<ParserErrorHandler.ParserError> parsingErrors) {
        super(formatErrorsMessage(parsingErrors));
        this.parsingErrors = new ArrayList<>(parsingErrors);
    }

    public List<ParserErrorHandler.ParserError> getParsingErrors() {
        return new ArrayList<>(parsingErrors);
    }

    public boolean hasParsingErrors() {
        return !parsingErrors.isEmpty();
    }

    // Legacy support for Token-based errors
    public Token getErrorToken() {
        return parsingErrors.isEmpty() ? null : parsingErrors.get(0).getErrorToken();
    }

    private static String formatErrorsMessage(List<ParserErrorHandler.ParserError> errors) {
        if (errors == null || errors.isEmpty()) {
            return "Unknown parsing error";
        }
        if (errors.size() == 1) {
            return errors.get(0).toString();
        }
        StringBuilder sb = new StringBuilder();
        sb.append("Multiple parsing errors (").append(errors.size()).append("):\n");
        for (int i = 0; i < errors.size(); i++) {
            sb.append("  ").append(i + 1).append(". ");
            String errorStr = errors.get(i).toString();
            // Indent each line of the error for multi-error display
            String[] lines = errorStr.split("\n");
            for (int j = 0; j < lines.length; j++) {
                if (j > 0) {
                    sb.append("     "); // Align with the error number
                }
                sb.append(lines[j]);
                if (j < lines.length - 1) {
                    sb.append("\n");
                }
            }
            if (i < errors.size() - 1) {
                sb.append("\n");
            }
        }
        return sb.toString();
    }
}
