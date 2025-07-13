package me.bechberger.jfr.extended;

/**
 * Represents a token in the extended JFR query language
 */
public record Token(
    TokenType type,
    String value,
    int line,
    int column,
    int position
) {
    
    /**
     * Creates a token with the given type and value at the specified position
     */
    public Token(TokenType type, String value, int line, int column, int position) {
        this.type = type;
        this.value = value;
        this.line = line;
        this.column = column;
        this.position = position;
    }
    
    /**
     * Returns true if this token is of the specified type
     */
    public boolean is(TokenType type) {
        return this.type == type;
    }
    
    /**
     * Returns true if this token is one of the specified types
     */
    public boolean isOneOf(TokenType... types) {
        for (TokenType type : types) {
            if (this.type == type) {
                return true;
            }
        }
        return false;
    }
    
    /**
     * Returns true if this token has the specified value (case-insensitive)
     */
    public boolean hasValue(String value) {
        return this.value.equalsIgnoreCase(value);
    }
    
    /**
     * Returns the position information as a string
     */
    public String getPositionString() {
        return "line " + line + ", column " + column;
    }
    
    @Override
    public String toString() {
        return String.format("%s('%s') at %s", type, value, getPositionString());
    }
}
