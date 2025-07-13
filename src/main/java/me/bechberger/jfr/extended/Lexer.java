package me.bechberger.jfr.extended;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import me.bechberger.jfr.extended.evaluator.FunctionRegistry;

/**
 * Lexer for the extended JFR query language
 */
public class Lexer {
    
    private final String input;
    private int position;
    private int line;
    private int column;
    
    public Lexer(String input) {
        this.input = input;
        this.position = 0;
        this.line = 1;
        this.column = 1;
    }
    
    /**
     * Tokenizes the entire input and returns a list of tokens
     */
    public List<Token> tokenize() throws LexerException {
        List<Token> tokens = new ArrayList<>();
        
        while (position < input.length()) {
            Token token = nextToken();
            if (token.type() != TokenType.WHITESPACE && token.type() != TokenType.COMMENT) {
                tokens.add(token);
            }
        }
        
        // Add EOF token
        tokens.add(new Token(TokenType.EOF, "", line, column, position));
        
        return tokens;
    }
    
    /**
     * Returns the next token from the input
     */
    private Token nextToken() throws LexerException {
        if (position >= input.length()) {
            return new Token(TokenType.EOF, "", line, column, position);
        }
        
        // Skip whitespace but track position
        if (Character.isWhitespace(input.charAt(position))) {
            return readWhitespace();
        }
        
        // Check for comments
        if (position + 1 < input.length()) {
            char current = input.charAt(position);
            char next = input.charAt(position + 1);
            
            // Multi-line comments /* */
            if (current == '/' && next == '*') {
                return readMultilineComment();
            }
            
            // Single-line comments: --, //, #
            if ((current == '-' && next == '-') || 
                (current == '/' && next == '/')) {
                return readSingleLineComment();
            }
        }
        
        // Hash comments #
        if (input.charAt(position) == '#') {
            return readSingleLineComment();
        }
        
        // Handle square brackets
        if (input.charAt(position) == '[') {
            return new Token(TokenType.LBRACKET, "[", line, column, position++);
        }
        if (input.charAt(position) == ']') {
            return new Token(TokenType.RBRACKET, "]", line, column, position++);
        }
        
        // First, try to match keywords (to prioritize them over IDENTIFIER)
        for (TokenType tokenType : TokenType.values()) {
            if (tokenType == TokenType.EOF) continue;
            if (tokenType == TokenType.IDENTIFIER || tokenType == TokenType.WHITESPACE || tokenType == TokenType.COMMENT) continue; // Skip these for now
            
            Matcher matcher = tokenType.getPattern().matcher(input);
            matcher.region(position, input.length());
            
            if (matcher.lookingAt()) {
                String value = matcher.group();
                
                // For keywords, ensure word boundaries (not part of a larger identifier)
                if (isKeyword(tokenType)) {
                    if (position + value.length() < input.length()) {
                        char nextChar = input.charAt(position + value.length());
                        if (Character.isLetterOrDigit(nextChar) || nextChar == '_') {
                            continue; // This is part of a larger identifier
                        }
                    }
                }
                
                Token token = new Token(tokenType, value, line, column, position);
                advance(value.length());
                return token;
            }
        }
        
        // Then try IDENTIFIER and other general patterns
        for (TokenType tokenType : new TokenType[]{TokenType.IDENTIFIER, TokenType.NUMBER, TokenType.STRING, TokenType.WHITESPACE}) {
            Matcher matcher = tokenType.getPattern().matcher(input);
            matcher.region(position, input.length());
            
            if (matcher.lookingAt()) {
                String value = matcher.group();
                Token token = new Token(tokenType, value, line, column, position);
                advance(value.length());
                return token;
            }
        }
        
        // If no token matches, it's an error
        throw new LexerException("Unexpected character: '" + input.charAt(position) + 
                                "' at line " + line + ", column " + column);
    }
    
    /**
     * Check if a token type is a keyword
     */
    private boolean isKeyword(TokenType tokenType) {
        return switch (tokenType) {
            case SELECT, FROM, WHERE, GROUP_BY, ORDER_BY, LIMIT, AS, AND, OR, ASC, DESC,
                 SHOW, VIEW, JOIN, FUZZY, ON, WITH, INNER, LEFT, RIGHT, FULL, TOLERANCE, NEAREST, PREVIOUS, AFTER,
                 PERCENTILE, P90, P95, P99, P999, P90SELECT, P95SELECT, P99SELECT, P999SELECT,
                 PERCENTILE_SELECT, LIKE, IN, RATE_UNIT -> true;
            default -> FunctionRegistry.getInstance().isFunction(tokenType);
        };
    }
    
    /**
     * Reads whitespace and returns a whitespace token
     */
    private Token readWhitespace() {
        int start = position;
        int startLine = line;
        int startColumn = column;
        
        while (position < input.length() && Character.isWhitespace(input.charAt(position))) {
            if (input.charAt(position) == '\n') {
                line++;
                column = 1;
            } else {
                column++;
            }
            position++;
        }
        
        String value = input.substring(start, position);
        return new Token(TokenType.WHITESPACE, value, startLine, startColumn, start);
    }
    
    /**
     * Reads a multi-line comment and returns a comment token
     */
    private Token readMultilineComment() throws LexerException {
        int start = position;
        int startLine = line;
        int startColumn = column;
        
        // Skip the opening /*
        advance(2);
        
        // Look for the closing */
        while (position + 1 < input.length()) {
            if (input.charAt(position) == '*' && input.charAt(position + 1) == '/') {
                // Found closing */
                advance(2);
                String value = input.substring(start, position);
                return new Token(TokenType.COMMENT, value, startLine, startColumn, start);
            }
            advance(1);
        }
        
        // If we reach here, the comment is not closed
        throw new LexerException("Unclosed multi-line comment starting at line " + startLine + ", column " + startColumn);
    }
    
    /**
     * Reads a single-line comment and returns a comment token
     */
    private Token readSingleLineComment() {
        int start = position;
        int startLine = line;
        int startColumn = column;
        
        // Skip until end of line or end of input
        while (position < input.length() && input.charAt(position) != '\n' && input.charAt(position) != '\r') {
            position++;
            column++;
        }
        
        String value = input.substring(start, position);
        return new Token(TokenType.COMMENT, value, startLine, startColumn, start);
    }
    
    /**
     * Advances the position by the specified number of characters
     */
    private void advance(int count) {
        for (int i = 0; i < count; i++) {
            if (position < input.length() && input.charAt(position) == '\n') {
                line++;
                column = 1;
            } else {
                column++;
            }
            position++;
        }
    }
    
    /**
     * Returns the current position in the input
     */
    public int getPosition() {
        return position;
    }
    
    /**
     * Returns the current line number
     */
    public int getLine() {
        return line;
    }
    
    /**
     * Returns the current column number
     */
    public int getColumn() {
        return column;
    }
    
    /**
     * Exception thrown when the lexer encounters an unexpected character
     */
    public static class LexerException extends Exception {
        public LexerException(String message) {
            super(message);
        }
    }
}
