package me.bechberger.jfr.extended;

/**
 * Represents a token in the extended JFR query language.
 * 
 * <p>Each token now includes both start and end positions for more precise error reporting.
 * This enables better error messages that can highlight the exact span of problematic tokens,
 * especially useful for multi-character tokens like identifiers, strings, and numbers.</p>
 * 
 * <p><strong>Position Information:</strong></p>
 * <ul>
 *   <li><strong>fromPosition/toPosition:</strong> Character indices in the source text</li>
 *   <li><strong>line/column:</strong> Human-readable line and column numbers (1-based)</li>
 *   <li><strong>endLine/endColumn:</strong> End position for multi-line tokens</li>
 * </ul>
 */
public record Token(
    TokenType type,
    String value,
    int line,
    int column,
    int fromPosition,
    int toPosition,
    int endLine,
    int endColumn
) {
    
    /**
     * Creates a token with start and end positions
     */
    public Token(TokenType type, String value, int line, int column, int fromPosition, int toPosition, int endLine, int endColumn) {
        this.type = type;
        this.value = value;
        this.line = line;
        this.column = column;
        this.fromPosition = fromPosition;
        this.toPosition = toPosition;
        this.endLine = endLine;
        this.endColumn = endColumn;
    }
    
    /**
     * Creates a token with calculated end position (for single-line tokens)
     */
    public Token(TokenType type, String value, int line, int column, int fromPosition) {
        this(type, value, line, column, fromPosition, fromPosition + value.length() - 1, line, column + value.length() - 1);
    }
    
    /**
     * Legacy constructor for backward compatibility - calculates end position
     * @deprecated Use constructor with explicit end positions for better precision
     */
    @Deprecated
    public static Token legacy(TokenType type, String value, int line, int column, int position) {
        return new Token(type, value, line, column, position, position + value.length() - 1, line, column + value.length() - 1);
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
     * Legacy position getter for backward compatibility
     * @deprecated Use fromPosition for clarity
     */
    @Deprecated
    public int position() {
        return fromPosition;
    }
    
    /**
     * Returns the length of this token in characters
     */
    public int length() {
        return toPosition - fromPosition + 1;
    }
    
    /**
     * Returns true if this token spans multiple lines
     */
    public boolean isMultiline() {
        return endLine > line;
    }
    
    /**
     * Returns the start position information as a string
     */
    public String getPositionString() {
        return "line " + line + ", column " + column;
    }
    
    /**
     * Returns the complete position range as a string
     */
    public String getRangeString() {
        if (isMultiline()) {
            return String.format("lines %d-%d, columns %d-%d", line, endLine, column, endColumn);
        } else if (length() > 1) {
            return String.format("line %d, columns %d-%d", line, column, endColumn);
        } else {
            return getPositionString();
        }
    }
    
    @Override
    public String toString() {
        return String.format("%s('%s') at %s", type, value, getRangeString());
    }
}
