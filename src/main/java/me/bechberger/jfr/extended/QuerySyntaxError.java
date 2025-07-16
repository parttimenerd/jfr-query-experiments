package me.bechberger.jfr.extended;

/**
 * Unified error representation for both lexer and parser errors with enhanced formatting,
 * context analysis, and intelligent suggestions.
 */
public class QuerySyntaxError {
    
    /**
     * Origin of the error - lexer or parser
     */
    public enum ErrorOrigin {
        LEXER,
        PARSER
    }
    
    /**
     * Category of syntax error for better organization
     */
    public enum ErrorCategory {
        UNEXPECTED_CHARACTER,
        MISSING_TOKEN,
        UNEXPECTED_TOKEN,
        INVALID_SYNTAX,
        UNCLOSED_LITERAL,
        TYPO_SUGGESTION,
        CONTEXT_ERROR
    }
    
    private final ErrorOrigin errorType;
    private final ErrorCategory category;
    private final String problemDescription;
    private final String suggestion;
    private final String typoSuggestion;
    private final String contextDescription;
    private final String tips;
    private final String examples;
    private final Token errorToken;
    private final String originalQuery;
    private final int errorPosition;
    private final int line;
    private final int column;
    
    private QuerySyntaxError(Builder builder) {
        this.errorType = builder.errorType;
        this.category = builder.category;
        this.problemDescription = builder.problemDescription;
        this.suggestion = builder.suggestion;
        this.typoSuggestion = builder.typoSuggestion;
        this.contextDescription = builder.contextDescription;
        this.tips = builder.tips;
        this.examples = builder.examples;
        this.errorToken = builder.errorToken;
        this.originalQuery = builder.originalQuery;
        this.errorPosition = builder.errorPosition;
        this.line = builder.line;
        this.column = builder.column;
    }
    
    // Getters
    public ErrorOrigin getErrorType() { return errorType; }
    public ErrorCategory getCategory() { return category; }
    public String getProblemDescription() { return problemDescription; }
    public String getSuggestion() { return suggestion; }
    public String getTypoSuggestion() { return typoSuggestion; }
    public String getContextDescription() { return contextDescription; }
    public String getTips() { return tips; }
    public String getExamples() { return examples; }
    public Token getErrorToken() { return errorToken; }
    public String getOriginalQuery() { return originalQuery; }
    public int getErrorPosition() { return errorPosition; }
    public int getLine() { return line; }
    public int getColumn() { return column; }
    
    /**
     * Generate the enhanced formatted error message with context highlighting
     */
    public String getFormattedMessage() {
        StringBuilder message = new StringBuilder();
        
        // Header with error type and location
        String errorOrigin = errorType == ErrorOrigin.LEXER ? "Lexer" : "Parser";
        message.append(errorOrigin).append(" Error at line ").append(line).append(", column ").append(column);
        message.append("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
        
        // Error description
        if (errorToken != null) {
            message.append("\nUnexpected ").append(getTokenDescription()).append(": '").append(errorToken.value()).append("'");
        }
        
        // Problem description
        if (problemDescription != null && !problemDescription.isEmpty()) {
            message.append("\n\nProblem: ").append(problemDescription);
        }
        
        // Typo suggestion
        if (typoSuggestion != null && !typoSuggestion.isEmpty()) {
            message.append("\n\nDid you mean: ").append(typoSuggestion);
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
        
        // Tips section
        if (tips != null && !tips.isEmpty()) {
            message.append("\n\n💡 Tips:\n").append(tips);
        }
        
        // Examples
        if (examples != null && !examples.isEmpty()) {
            message.append("\n\n📝 Examples:\n").append(examples);
        }
        
        return message.toString();
    }
    
    /**
     * Generate context snippet with error highlighting (like the lexer's enhanced version)
     */
    private String generateContextSnippet() {
        if (originalQuery == null || originalQuery.isEmpty()) {
            return "";
        }
        
        int position = errorPosition >= 0 ? errorPosition : 
                      (errorToken != null ? errorToken.position() : 0);
        
        // Find the line boundaries
        int lineStart = originalQuery.lastIndexOf('\n', position - 1) + 1;
        int lineEnd = originalQuery.indexOf('\n', position);
        if (lineEnd == -1) lineEnd = originalQuery.length();
        
        // Extract the line containing the error
        String currentLine = originalQuery.substring(lineStart, lineEnd);
        int errorColumn = position - lineStart;
        
        // Ensure error column is within bounds
        errorColumn = Math.max(0, Math.min(errorColumn, currentLine.length()));
        
        StringBuilder snippet = new StringBuilder();
        snippet.append("  ").append(currentLine).append("\n");
        snippet.append("  ");
        
        // Add pointer to the error position
        for (int i = 0; i < errorColumn; i++) {
            snippet.append(" ");
        }
        snippet.append("^--- Error here");
        
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
     * Builder pattern for creating QuerySyntaxError instances
     */
    public static class Builder {
        private ErrorOrigin errorType;
        private ErrorCategory category;
        private String problemDescription;
        private String suggestion;
        private String typoSuggestion;
        private String contextDescription;
        private String tips;
        private String examples;
        private Token errorToken;
        private String originalQuery;
        private int errorPosition = -1;
        private int line = 1;
        private int column = 1;
        
        public Builder(ErrorOrigin errorType) {
            this.errorType = errorType;
        }
        
        public Builder category(ErrorCategory category) {
            this.category = category;
            return this;
        }
        
        public Builder problemDescription(String problemDescription) {
            this.problemDescription = problemDescription;
            return this;
        }
        
        public Builder suggestion(String suggestion) {
            this.suggestion = suggestion;
            return this;
        }
        
        public Builder typoSuggestion(String typoSuggestion) {
            this.typoSuggestion = typoSuggestion;
            return this;
        }
        
        public Builder contextDescription(String contextDescription) {
            this.contextDescription = contextDescription;
            return this;
        }
        
        public Builder tips(String tips) {
            this.tips = tips;
            return this;
        }
        
        public Builder examples(String examples) {
            this.examples = examples;
            return this;
        }
        
        public Builder errorToken(Token errorToken) {
            this.errorToken = errorToken;
            if (errorToken != null) {
                this.line = errorToken.line();
                this.column = errorToken.column();
                this.errorPosition = errorToken.position();
            }
            return this;
        }
        
        public Builder originalQuery(String originalQuery) {
            this.originalQuery = originalQuery;
            return this;
        }
        
        public Builder position(int line, int column, int position) {
            this.line = line;
            this.column = column;
            this.errorPosition = position;
            return this;
        }
        
        public QuerySyntaxError build() {
            return new QuerySyntaxError(this);
        }
    }
    
    /**
     * Create a lexer error from a LexerException
     */
    public static QuerySyntaxError fromLexerException(LexerException lexerException, String originalQuery) {
        return new Builder(ErrorOrigin.LEXER)
            .category(ErrorCategory.UNEXPECTED_CHARACTER)
            .problemDescription(lexerException.getMessage())
            .originalQuery(originalQuery)
            .build();
    }
    
    /**
     * Create a parser error from a ParserException
     */
    public static QuerySyntaxError fromParserException(ParserException parserException, String originalQuery) {
        Builder builder = new Builder(ErrorOrigin.PARSER)
            .category(ErrorCategory.INVALID_SYNTAX)
            .problemDescription(parserException.getMessage())
            .originalQuery(originalQuery);
            
        // Try to extract token information from parser exception
        if (parserException.getErrorToken() != null) {
            builder.errorToken(parserException.getErrorToken());
        }
        
        return builder.build();
    }
}
