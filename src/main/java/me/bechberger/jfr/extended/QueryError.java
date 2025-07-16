package me.bechberger.jfr.extended;

import me.bechberger.jfr.extended.ast.Location;

/**
 * Unified error representation for both lexer and parser errors.
 * 
 * <p>This class provides a consistent structure for all query parsing errors,
 * implementing the user's requirement that "LexerException should have the same 
 * structure as ParserExceptions and have the same parent class, they should be 
 * collected into the same structure".</p>
 * 
 * <p><strong>Key Features:</strong></p>
 * <ul>
 *   <li><strong>Token-based positioning:</strong> Position information is derived from tokens 
 *       rather than stored separately, eliminating duplicate position data as requested</li>
 *   <li><strong>Enhanced formatting:</strong> Provides "Error here" pointer functionality 
 *       for precise error location highlighting</li>
 *   <li><strong>Rich context:</strong> Includes error message, suggestions, context, and examples</li>
 *   <li><strong>Unified structure:</strong> Same format for both lexer and parser errors</li>
 * </ul>
 * 
 * <p><strong>Usage Example:</strong></p>
 * <pre>{@code
 * QueryError error = new QueryError.Builder(JFRQueryException.ErrorOrigin.LEXER)
 *     .category(JFRQueryException.ErrorCategory.UNEXPECTED_CHARACTER)
 *     .errorMessage("Double quotes are not supported")
 *     .suggestion("Use single quotes instead: 'text'")
 *     .errorToken(token)
 *     .originalQuery(query)
 *     .build();
 * }</pre>
 * 
 * @see JFRQueryException The common parent for all query exceptions
 * @see LexerException Lexer-specific exception using this error structure  
 * @see ParserException Parser-specific exception using this error structure
 */
public class QueryError {
    
    private final JFRQueryException.ErrorOrigin origin;
    private final JFRQueryException.ErrorCategory category;
    private final String errorMessage;
    private final String context;
    private final String suggestion;
    private final String examples;
    
    // Token-based location information - no need to duplicate positions
    private final Token errorToken;
    private final Token endToken; // Optional end token for range errors
    private final String originalQuery;
    
    private QueryError(Builder builder) {
        this.origin = builder.origin;
        this.category = builder.category;
        this.errorMessage = builder.errorMessage;
        this.context = builder.context;
        this.suggestion = builder.suggestion;
        this.examples = builder.examples;
        this.errorToken = builder.errorToken;
        this.endToken = builder.endToken;
        this.originalQuery = builder.originalQuery;
    }
    
    /**
     * Simple constructor for testing convenience
     */
    public QueryError(Token errorToken, JFRQueryException.ErrorOrigin origin, 
                     JFRQueryException.ErrorCategory category, String errorMessage, 
                     String context, String suggestion) {
        this.errorToken = errorToken;
        this.origin = origin;
        this.category = category;
        this.errorMessage = errorMessage;
        this.context = context;
        this.suggestion = suggestion;
        this.examples = null;
        this.endToken = null;
        this.originalQuery = null;
    }
    
    // Getters for all properties
    public JFRQueryException.ErrorOrigin getOrigin() { return origin; }
    public JFRQueryException.ErrorCategory getCategory() { return category; }
    public String getErrorMessage() { return errorMessage; }
    public String getContext() { return context; }
    public String getSuggestion() { return suggestion; }
    public String getExamples() { return examples; }
    public Token getErrorToken() { return errorToken; }
    public Token getEndToken() { return endToken; }
    public String getOriginalQuery() { return originalQuery; }
    
    // Derived location information from tokens - now using precise from/to positions
    public Location getFromLocation() { 
        return errorToken != null ? Location.from(errorToken) : Location.start(); 
    }
    
    public Location getToLocation() { 
        if (endToken != null) {
            return Location.from(endToken);
        } else if (errorToken != null) {
            // For single tokens, use the end position of the error token
            return new Location(errorToken.endLine(), errorToken.endColumn());
        } else {
            return getFromLocation();
        }
    }
    
    public int getFromPosition() { 
        return errorToken != null ? errorToken.fromPosition() : 0; 
    }
    
    public int getToPosition() { 
        if (endToken != null) {
            return endToken.toPosition();
        } else if (errorToken != null) {
            return errorToken.toPosition();
        } else {
            return 0;
        }
    }
    
    // Legacy getters for compatibility
    public int getLine() { return getFromLocation().line(); }
    public int getColumn() { return getFromLocation().column(); }
    public int getFromLine() { return getFromLocation().line(); }
    public int getFromColumn() { return getFromLocation().column(); }
    public int getToLine() { return getToLocation().line(); }
    public int getToColumn() { return getToLocation().column(); }
    public int getErrorPosition() { return getFromPosition(); }
    public String getProblemDescription() { return errorMessage; }
    public String getContextDescription() { return context; }
    
    /**
     * Generate the enhanced formatted error message with context highlighting.
     * 
     * <p>This method creates a comprehensive error message that includes:</p>
     * <ul>
     *   <li>Error type and location header with decorative separator</li>
     *   <li>Token description and unexpected value details</li>
     *   <li>Problem description explaining what went wrong</li>
     *   <li>Actionable suggestions for fixing the error</li>
     *   <li>Context snippet with "Error here" pointer for precise location</li>
     *   <li>Examples showing correct vs incorrect usage</li>
     * </ul>
     * 
     * <p>The formatting uses Unicode box drawing characters for visual clarity
     * and provides precise location highlighting using token positions.</p>
     * 
     * @return A fully formatted, multi-line error message ready for display
     */
    public String getFormattedMessage() {
        StringBuilder message = new StringBuilder();
        
        // Header with error type and location
        message.append(origin.getDisplayName()).append(" Error at ").append(getFromLocation().toString());
        message.append("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
        
        // Error description
        if (errorToken != null) {
            message.append("\nUnexpected ").append(getTokenDescription()).append(": '").append(errorToken.value()).append("'");
        }
        
        // Problem description (error message)
        if (errorMessage != null && !errorMessage.isEmpty()) {
            message.append("\n\nProblem: ").append(errorMessage);
        }
        
        // Main suggestion
        if (suggestion != null && !suggestion.isEmpty()) {
            message.append("\n\nSuggestion: ").append(suggestion);
        }
        
        // Context with highlighting
        String contextSnippet = generateContextSnippet();
        if (!contextSnippet.isEmpty()) {
            message.append("\n\nContext:\n").append(contextSnippet);
        }
        
        // Examples
        if (examples != null && !examples.isEmpty()) {
            message.append("\n\n📝 Examples:\n").append(examples);
        }
        
        return message.toString();
    }
    
    /**
     * Generate context snippet with error highlighting (the "Error here" pointer).
     * 
     * <p>This method handles various query types including:</p>
     * <ul>
     *   <li><strong>Single line queries:</strong> Shows the line with error pointer</li>
     *   <li><strong>Multiline queries:</strong> Shows context around the error line</li>
     *   <li><strong>Long queries:</strong> Truncates appropriately while preserving context</li>
     * </ul>
     * 
     * @return A formatted context snippet with error highlighting
     */
    public String generateContextSnippet(String query, int contextLines) {
        // Create a temporary QueryError with the provided query for context generation
        return new QueryError.Builder(this.origin)
            .category(this.category)
            .errorMessage(this.errorMessage)
            .context(this.context)
            .suggestion(this.suggestion)
            .errorToken(this.errorToken)
            .endToken(this.endToken)
            .originalQuery(query)
            .build()
            .generateContextSnippet();
    }
    
    /**
     * Generate the context snippet with "Error here" pointer highlighting.
     * 
     * <p>This method creates a visual representation of the error location within
     * the query text, showing:</p>
     * <ul>
     *   <li>Relevant lines of context around the error</li>
     *   <li>Line numbers for multiline queries</li>
     *   <li>Visual pointer using ^ and ~ characters to highlight the exact error range</li>
     *   <li>Character count for the error span</li>
     *   <li>Intelligent truncation for very long lines</li>
     * </ul>
     * 
     * <p>This method handles various query types including:</p>
     * <ul>
     *   <li><strong>Single line queries:</strong> Shows the line with error pointer</li>
     *   <li><strong>Multiline queries:</strong> Shows context around the error line</li>
     *   <li><strong>Long queries:</strong> Truncates appropriately while preserving context</li>
     * </ul>
     * 
     * @return A formatted context snippet with error highlighting
     */
    private String generateContextSnippet() {
        if (originalQuery == null || originalQuery.isEmpty()) {
            return "";
        }
        
        int position = getFromPosition();
        
        // Split query into lines for better handling of multiline queries
        String[] lines = originalQuery.split("\n", -1); // -1 to preserve trailing empty lines
        
        // Find which line contains the error
        int currentPosition = 0;
        int errorLine = 0;
        int errorColumn = position;
        
        for (int i = 0; i < lines.length; i++) {
            int lineLength = lines[i].length() + 1; // +1 for newline character
            if (currentPosition + lineLength > position) {
                errorLine = i;
                errorColumn = position - currentPosition;
                break;
            }
            currentPosition += lineLength;
        }
        
        // Ensure error column is within bounds of the line
        if (errorLine < lines.length) {
            errorColumn = Math.max(0, Math.min(errorColumn, lines[errorLine].length()));
        }
        
        StringBuilder snippet = new StringBuilder();
        
        // For multiline queries, show context around the error
        int contextLines = 2; // Number of lines to show before and after error
        int startLine = Math.max(0, errorLine - contextLines);
        int endLine = Math.min(lines.length - 1, errorLine + contextLines);
        
        // Add line numbers for multiline context
        boolean isMultilineContext = (endLine - startLine) > 0;
        int maxLineNumberWidth = String.valueOf(endLine + 1).length();
        
        for (int i = startLine; i <= endLine; i++) {
            String line = lines[i];
            
            // Truncate very long lines while preserving context around error
            String displayLine = line;
            int displayErrorColumn = errorColumn;
            
            if (line.length() > 120 && i == errorLine) {
                // For long lines with errors, show context around the error position
                int contextWidth = 50; // Characters to show on each side of error
                int start = Math.max(0, errorColumn - contextWidth);
                int end = Math.min(line.length(), errorColumn + contextWidth);
                
                displayLine = (start > 0 ? "..." : "") + 
                             line.substring(start, end) + 
                             (end < line.length() ? "..." : "");
                             
                displayErrorColumn = (start > 0 ? 3 : 0) + (errorColumn - start);
            } else if (line.length() > 120) {
                // For other long lines, just truncate
                displayLine = line.substring(0, 117) + "...";
            }
            
            // Add line number for multiline queries
            if (isMultilineContext) {
                snippet.append(String.format("%" + maxLineNumberWidth + "d: ", i + 1));
            } else {
                snippet.append("  ");
            }
            
            snippet.append(displayLine).append("\n");
            
            // Add error pointer for the error line
            if (i == errorLine) {
                // Add spacing for line number if present
                if (isMultilineContext) {
                    snippet.append(" ".repeat(maxLineNumberWidth + 2));
                } else {
                    snippet.append("  ");
                }
                
                // Add spaces to align with error position
                for (int j = 0; j < displayErrorColumn; j++) {
                    snippet.append(" ");
                }
                
                // Enhanced range highlighting for tokens
                int tokenLength = errorToken != null ? errorToken.length() : 1;
                boolean hasEndToken = endToken != null;
                
                if (hasEndToken) {
                    // For range errors, show start and end
                    int rangeLength = getToPosition() - getFromPosition();
                    snippet.append("^");
                    if (rangeLength > 1 && rangeLength <= 50) {
                        // Show underline for reasonable ranges
                        for (int j = 1; j < Math.min(rangeLength, displayLine.length() - displayErrorColumn); j++) {
                            snippet.append("~");
                        }
                    }
                    snippet.append("^ Range error");
                } else if (tokenLength > 1 && tokenLength <= 20) {
                    // For single multi-character tokens, show the full token span
                    snippet.append("^");
                    for (int j = 1; j < Math.min(tokenLength, displayLine.length() - displayErrorColumn); j++) {
                        snippet.append("~");
                    }
                    snippet.append(" Error here");
                } else {
                    // For single character tokens or very long tokens
                    snippet.append("^--- Error here");
                }
                
                snippet.append("\n");
            }
        }
        
        // Add summary for very long queries
        if (lines.length > 10) {
            snippet.append("\n  (Query has ").append(lines.length).append(" lines total)");
        }
        
        return snippet.toString();
    }
    
    /**
     * Get a human-readable description of the token type
     */
    private String getTokenDescription() {
        if (errorToken == null) {
            return "token";
        }
        
        switch (errorToken.type()) {
            case IDENTIFIER:
                return "identifier";
            case NUMBER:
                return "number";
            case STRING:
                return "string";
            case EOF:
                return "end of input";
            default:
                if (errorToken.type().name().contains("_")) {
                    return errorToken.type().name().toLowerCase().replace("_", " ");
                }
                return errorToken.type().name().toLowerCase();
        }
    }
    
    /**
     * Builder pattern for creating QueryError instances
     */
    public static class Builder {
        private JFRQueryException.ErrorOrigin origin;
        private JFRQueryException.ErrorCategory category;
        private String errorMessage;
        private String context;
        private String suggestion;
        private String examples;
        private Token errorToken;
        private Token endToken;
        private String originalQuery;
        
        public Builder(JFRQueryException.ErrorOrigin origin) {
            this.origin = origin;
        }
        
        public Builder category(JFRQueryException.ErrorCategory category) {
            this.category = category;
            return this;
        }
        
        public Builder errorMessage(String errorMessage) {
            this.errorMessage = errorMessage;
            return this;
        }
        
        public Builder context(String context) {
            this.context = context;
            return this;
        }
        
        public Builder suggestion(String suggestion) {
            this.suggestion = suggestion;
            return this;
        }
        
        public Builder examples(String examples) {
            this.examples = examples;
            return this;
        }
        
        public Builder errorToken(Token errorToken) {
            this.errorToken = errorToken;
            return this;
        }
        
        public Builder endToken(Token endToken) {
            this.endToken = endToken;
            return this;
        }
        
        public Builder originalQuery(String originalQuery) {
            this.originalQuery = originalQuery;
            return this;
        }
        
        // Legacy method names for compatibility
        public Builder problemDescription(String problemDescription) {
            return errorMessage(problemDescription);
        }
        
        public Builder contextDescription(String contextDescription) {
            return context(contextDescription);
        }
        
        public QueryError build() {
            return new QueryError(this);
        }
    }
}
