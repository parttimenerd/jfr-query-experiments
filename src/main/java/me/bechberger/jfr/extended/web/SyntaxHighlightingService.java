package me.bechberger.jfr.extended.web;

import me.bechberger.jfr.extended.Lexer;
import me.bechberger.jfr.extended.Token;
import me.bechberger.jfr.extended.TokenType;

import java.util.List;
import java.util.ArrayList;

/**
 * Service for syntax highlighting JFR queries
 */
public class SyntaxHighlightingService {
    
    /**
     * Highlighted token with position and type information
     */
    public record HighlightedToken(
        String text,
        TokenType type,
        int startLine,
        int startColumn,
        int endLine,
        int endColumn,
        String cssClass
    ) {}
    
    /**
     * Response containing highlighted tokens
     */
    public record SyntaxHighlightResponse(
        List<HighlightedToken> tokens,
        boolean hasErrors,
        List<String> errors
    ) {}
    
    /**
     * Highlight a JFR query and return token information
     */
    public SyntaxHighlightResponse highlight(String query) {
        List<HighlightedToken> highlightedTokens = new ArrayList<>();
        List<String> errors = new ArrayList<>();
        boolean hasErrors = false;
        
        try {
            Lexer lexer = new Lexer(query);
            List<Token> tokens = lexer.tokenize();
            
            for (Token token : tokens) {
                if (token.type() == TokenType.EOF) {
                    continue;
                }
                
                String cssClass = getCssClass(token.type());
                
                // Calculate end position
                String[] lines = token.value().split("\n");
                int endLine = token.line();
                int endColumn = token.column();
                
                if (lines.length > 1) {
                    endLine += lines.length - 1;
                    endColumn = lines[lines.length - 1].length();
                } else {
                    endColumn += token.value().length();
                }
                
                highlightedTokens.add(new HighlightedToken(
                    token.value(),
                    token.type(),
                    token.line(),
                    token.column(),
                    endLine,
                    endColumn,
                    cssClass
                ));
            }
            
        } catch (Exception e) {
            hasErrors = true;
            errors.add("Lexer error: " + e.getMessage());
        }
        
        return new SyntaxHighlightResponse(highlightedTokens, hasErrors, errors);
    }
    
    /**
     * Map token types to CSS classes for highlighting
     */
    private String getCssClass(TokenType tokenType) {
        return switch (tokenType) {
            case SELECT, FROM, WHERE, GROUP_BY, ORDER_BY, LIMIT, AS, AND, OR, 
                 ASC, DESC, SHOW, VIEW -> "keyword";
            
            case NUMBER, BOOLEAN, TIME_LITERAL, TIMESTAMP_LITERAL -> "literal";
            case STRING -> "string";
            case IDENTIFIER -> getIdentifierCssClass(tokenType);
            
            case EQUALS, NOT_EQUALS, LESS_THAN, GREATER_THAN, LESS_EQUAL, 
                 GREATER_EQUAL, LIKE, IN, PLUS, MINUS, STAR, DIVIDE, 
                 MODULO, ASSIGN -> "operator";
            
            case LPAREN, RPAREN, LBRACKET, RBRACKET, COMMA, SEMICOLON, DOT -> "punctuation";
            
            case DURATION_LITERAL, MEMORY_SIZE_LITERAL, RATE_UNIT -> "unit";
            case EXTENDED_QUERY -> "special";
            case COMMENT -> "comment";
            case WHITESPACE -> "whitespace";
            
            default -> "unknown";
        };
    }
    
    /**
     * Get CSS class for identifier tokens, which may represent functions or keywords
     */
    private String getIdentifierCssClass(TokenType tokenType) {
        // For identifiers, we need to check the actual token value
        // This is a simplified approach - in practice, you'd pass the actual token
        return "identifier";
    }
    
    /**
     * Get available CSS classes and their descriptions
     */
    public List<CssClassInfo> getCssClasses() {
        return List.of(
            new CssClassInfo("keyword", "SQL keywords like SELECT, FROM, WHERE"),
            new CssClassInfo("function", "Percentile and other functions"),
            new CssClassInfo("literal", "Numeric, boolean, time literals"),
            new CssClassInfo("string", "String literals"),
            new CssClassInfo("identifier", "Table names, column names, aliases, function names"),
            new CssClassInfo("operator", "Operators like =, <, >, +, -"),
            new CssClassInfo("punctuation", "Punctuation like (, ), [, ], ,"),
            new CssClassInfo("unit", "Time, memory, rate units"),
            new CssClassInfo("special", "Special tokens like * and @"),
            new CssClassInfo("comment", "Comments"),
            new CssClassInfo("whitespace", "Whitespace"),
            new CssClassInfo("unknown", "Unknown tokens")
        );
    }
    
    public record CssClassInfo(String className, String description) {}
}
