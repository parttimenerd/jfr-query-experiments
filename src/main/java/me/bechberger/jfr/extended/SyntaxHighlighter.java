package me.bechberger.jfr.extended;

import java.util.ArrayList;
import java.util.List;
import me.bechberger.jfr.extended.evaluator.FunctionRegistry;

/**
 * Syntax highlighter for the extended JFR query language
 */
public class SyntaxHighlighter {
    
    /**
     * Represents a syntax-highlighted token
     */
    public record HighlightedToken(
        String text,
        int start,
        int end,
        TokenCategory category
    ) {
    }
    
    /**
     * Token categories for syntax highlighting
     */
    public enum TokenCategory {
        KEYWORD,
        IDENTIFIER,
        STRING,
        NUMBER,
        OPERATOR,
        PUNCTUATION,
        COMMENT,
        FUNCTION,
        FIELD,
        TYPE,
        ERROR
    }
    
    /**
     * Highlights the given query text and returns a list of highlighted tokens
     */
    public List<HighlightedToken> highlight(String query) {
        List<HighlightedToken> result = new ArrayList<>();
        
        try {
            Lexer lexer = new Lexer(query);
            List<Token> tokens = lexer.tokenize();
            
            for (Token token : tokens) {
                if (token.type() == TokenType.EOF) {
                    break;
                }
                
                TokenCategory category = categorizeToken(token);
                int start = token.position();
                int end = start + token.value().length();
                
                result.add(new HighlightedToken(token.value(), start, end, category));
            }
            
        } catch (Exception e) {
            // If lexing fails, mark the entire input as an error
            result.add(new HighlightedToken(query, 0, query.length(), TokenCategory.ERROR));
        }
        
        return result;
    }
    
    /**
     * Categorizes a token for syntax highlighting
     */
    private TokenCategory categorizeToken(Token token) {
        return switch (token.type()) {
            // Keywords
            case SELECT, FROM, WHERE, GROUP_BY, ORDER_BY, LIMIT, AS, AND, OR, ASC, DESC,
                 SHOW, VIEW, EXTENDED_QUERY -> TokenCategory.KEYWORD;
            
            // Operators
            case EQUALS, NOT_EQUALS, LESS_THAN, GREATER_THAN, LESS_EQUAL, GREATER_EQUAL,
                 LIKE, IN, PLUS, MINUS, STAR, DIVIDE, MODULO, ASSIGN -> TokenCategory.OPERATOR;
            
            // Punctuation
            case LPAREN, RPAREN, LBRACKET, RBRACKET, COMMA, SEMICOLON, DOT -> TokenCategory.PUNCTUATION;
            
            // Literals
            case STRING -> TokenCategory.STRING;
            case NUMBER -> TokenCategory.NUMBER;
            
            // Identifiers (could be fields, types, or variables)
            case IDENTIFIER -> categorizeIdentifier(token);
            
            // Units/Literals with units
            case DURATION_LITERAL, MEMORY_SIZE_LITERAL -> TokenCategory.NUMBER;
            
            // Comments and whitespace
            case COMMENT -> TokenCategory.COMMENT;
            case WHITESPACE -> TokenCategory.PUNCTUATION; // Usually ignored
            
            default -> TokenCategory.IDENTIFIER;
        };
    }
    
    /**
     * Categorizes an identifier token (field, type, or variable)
     */
    private TokenCategory categorizeIdentifier(Token token) {
        String value = token.value().toLowerCase();
        
        // Check if it's a function name (now that many are identifiers)
        if (isFunctionName(value)) {
            return TokenCategory.FUNCTION;
        }
        
        // Check if it's a keyword-like identifier (now that many keywords are identifiers)
        if (isKeywordLikeIdentifier(value)) {
            return TokenCategory.KEYWORD;
        }
        
        // Common JFR event types
        if (isEventType(value)) {
            return TokenCategory.TYPE;
        }
        
        // Common field names
        if (isFieldName(value)) {
            return TokenCategory.FIELD;
        }
        
        // Default to identifier
        return TokenCategory.IDENTIFIER;
    }
    
    /**
     * Checks if the value is a function name (aggregate, mathematical, string, etc.)
     */
    private boolean isFunctionName(String value) {
        // Use the FunctionRegistry to check for registered functions
        FunctionRegistry registry = FunctionRegistry.getInstance();
        return registry.isFunction(value);
    }
    
    /**
     * Checks if the value is a keyword-like identifier
     */
    private boolean isKeywordLikeIdentifier(String value) {
        return switch (value) {
            case "events", "fields", "column", "format", "min_time_slice" -> true;
            default -> false;
        };
    }
    
    /**
     * Checks if the value is a known JFR event type
     */
    private boolean isEventType(String value) {
        return value.contains("gc") || 
               value.contains("garbage") ||
               value.contains("allocation") ||
               value.contains("execution") ||
               value.contains("sample") ||
               value.contains("thread") ||
               value.contains("cpu") ||
               value.contains("memory") ||
               value.contains("io") ||
               value.contains("jdk.") ||
               value.contains("jvm.") ||
               value.contains("java.");
    }
    
    /**
     * Checks if the value is a known field name
     */
    private boolean isFieldName(String value) {
        return value.equals("duration") ||
               value.equals("starttime") ||
               value.equals("endtime") ||
               value.equals("stacktrace") ||
               value.equals("thread") ||
               value.equals("eventthread") ||
               value.equals("cause") ||
               value.equals("id") ||
               value.equals("size") ||
               value.equals("count") ||
               value.equals("name") ||
               value.equals("type") ||
               value.equals("value");
    }
    
    /**
     * Returns CSS classes for the token categories
     */
    public String getCSSClass(TokenCategory category) {
        return switch (category) {
            case KEYWORD -> "jfr-keyword";
            case IDENTIFIER -> "jfr-identifier";
            case STRING -> "jfr-string";
            case NUMBER -> "jfr-number";
            case OPERATOR -> "jfr-operator";
            case PUNCTUATION -> "jfr-punctuation";
            case COMMENT -> "jfr-comment";
            case FUNCTION -> "jfr-function";
            case FIELD -> "jfr-field";
            case TYPE -> "jfr-type";
            case ERROR -> "jfr-error";
        };
    }
    
    /**
     * Returns a CSS stylesheet for syntax highlighting
     */
    public String getCSS() {
        return """
            .jfr-keyword { color: #0066CC; font-weight: bold; }
            .jfr-identifier { color: #000000; }
            .jfr-string { color: #008000; }
            .jfr-number { color: #FF6600; }
            .jfr-operator { color: #666666; font-weight: bold; }
            .jfr-punctuation { color: #666666; }
            .jfr-comment { color: #999999; font-style: italic; }
            .jfr-function { color: #9900CC; font-weight: bold; }
            .jfr-field { color: #0066CC; }
            .jfr-type { color: #CC0000; font-weight: bold; }
            .jfr-error { color: #FF0000; background-color: #FFE6E6; }
            """;
    }
}
