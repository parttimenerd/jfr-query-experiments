package me.bechberger.jfr.extended;

import java.util.List;

/**
 * Token stream management for the Parser.
 * 
 * <p>Provides methods for navigating through tokens during parsing,
 * including lookahead, consumption, and position tracking.</p>
 */
public class TokenStream {
    
    private final List<Token> tokens;
    private int current = 0;
    
    public TokenStream(List<Token> tokens) {
        this.tokens = tokens;
    }
    
    /**
     * Check if we've reached the end of the token stream
     */
    public boolean isAtEnd() {
        return current >= tokens.size() || peek().type() == TokenType.EOF;
    }
    
    /**
     * Get the current token without consuming it
     */
    public Token peek() {
        if (current >= tokens.size()) {
            // Return a synthetic EOF token if we're past the end
            Token lastToken = tokens.get(tokens.size() - 1);
            return new Token(TokenType.EOF, "", lastToken.line(), 
                           lastToken.column() + 1, lastToken.position() + 1);
        }
        return tokens.get(current);
    }
    
    /**
     * Get the current token (same as peek for consistency with existing code)
     */
    public Token current() {
        return peek();
    }
    
    /**
     * Get the previous token
     */
    public Token previous() {
        if (current == 0) {
            return tokens.get(0);
        }
        return tokens.get(current - 1);
    }
    
    /**
     * Check if the current token is of the given type
     */
    public boolean check(TokenType type) {
        if (isAtEnd()) return false;
        return peek().type() == type;
    }
    
    /**
     * Check if the next token is of the given type
     */
    public boolean checkNext(TokenType type) {
        if (current + 1 >= tokens.size()) return false;
        return tokens.get(current + 1).type() == type;
    }
    
    /**
     * Advance to the next token and return the current one
     */
    public Token advance() {
        if (!isAtEnd()) current++;
        return previous();
    }
    
    /**
     * Check if the current token matches any of the given types, and advance if so
     */
    public boolean match(TokenType... types) {
        for (TokenType type : types) {
            if (check(type)) {
                advance();
                return true;
            }
        }
        return false;
    }
    
    /**
     * Consume a token of the expected type, or throw an error
     */
    public Token consume(TokenType type, String message) throws ParserException {
        if (check(type)) {
            return advance();
        }
        
        ParserErrorHandler.ParserError error = new ParserErrorHandler.ParserError(
            message + ". Expected " + type + " but found " + current().type(),
            "Check for missing or misplaced tokens",
            current(),
            "Near: " + current().value(),
            ParserErrorHandler.ErrorType.UNEXPECTED_TOKEN
        );
        throw new ParserException(error);
    }
    
    /**
     * Skip tokens until we reach one of the recovery points
     */
    public void skipToRecoveryPoint(TokenType... recoveryTokens) {
        while (!isAtEnd()) {
            for (TokenType recoveryToken : recoveryTokens) {
                if (check(recoveryToken)) {
                    return;
                }
            }
            advance();
        }
    }
    
    /**
     * Check if we're at the end of a clause (next token starts a new clause)
     */
    public boolean isAtEndOfClause() {
        if (isAtEnd()) return true;
        
        TokenType type = current().type();
        return type == TokenType.WHERE || type == TokenType.GROUP_BY || type == TokenType.ORDER_BY ||
               type == TokenType.LIMIT || type == TokenType.HAVING || type == TokenType.SEMICOLON ||
               type == TokenType.EOF;
    }
    
    /**
     * Get current position in the token stream
     */
    public int getCurrentPosition() {
        return current;
    }
    
    /**
     * Set position in the token stream (for backtracking)
     */
    public void setPosition(int position) {
        this.current = Math.max(0, Math.min(position, tokens.size()));
    }
}
