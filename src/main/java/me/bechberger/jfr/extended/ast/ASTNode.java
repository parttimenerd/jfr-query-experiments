package me.bechberger.jfr.extended.ast;

/**
 * Base interface for all AST nodes in the extended JFR query language
 */
public interface ASTNode {
    
    /**
     * Accept method for the visitor pattern
     */
    <T> T accept(ASTVisitor<T> visitor);
    
    /**
     * Returns the line number where this node appears in the source
     */
    default int getLine() {
        return getLocation().line();
    }
    
    /**
     * Returns the column number where this node appears in the source
     */
    default int getColumn() {
        return getLocation().column();
    }
    
    /**
     * Format this node as a pretty-printed string
     */
    String format();

    Location getLocation();
}
