package me.bechberger.jfr.extended.ast;

import me.bechberger.jfr.extended.Token;

/**
 * Represents a source code location with line and column information.
 * 
 * This record encapsulates position information for AST nodes, providing
 * better type safety and convenience methods compared to separate int fields.
 */
public record Location(int line, int column) {
    
    /**
     * Creates a location at the beginning of the file
     */
    public static Location start() {
        return new Location(1, 1);
    }
    
    /**
     * Creates a location from a token
     */
    public static Location from(Token token) {
        return new Location(token.line(), token.column());
    }
    
    /**
     * Returns a human-readable representation of this location
     */
    @Override
    public String toString() {
        return "line " + line + ", column " + column;
    }
    
    /**
     * Check if this location comes before another location
     */
    public boolean isBefore(Location other) {
        if (this.line < other.line) {
            return true;
        }
        return this.line == other.line && this.column < other.column;
    }
    
    /**
     * Check if this location comes after another location
     */
    public boolean isAfter(Location other) {
        return other.isBefore(this);
    }
}
