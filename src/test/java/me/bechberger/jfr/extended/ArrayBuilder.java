package me.bechberger.jfr.extended;

import me.bechberger.jfr.extended.ast.ASTNodes.*;
import me.bechberger.jfr.extended.ast.Location;
import me.bechberger.jfr.extended.ast.ASTNode;
import me.bechberger.jfr.extended.table.CellValue;
import java.util.List;
import java.util.Arrays;

/**
 * Helper class to extend ASTBuilder with methods for creating ArrayLiteralNode instances.
 * This is needed because ASTBuilder doesn't provide factory methods for array literals.
 */
public class ArrayBuilder {
    
    /**
     * Create an array literal node with the given elements
     * 
     * @param elements the array elements
     * @return an ArrayLiteralNode with the elements
     */
    public static me.bechberger.jfr.extended.ast.ASTNodes.ArrayLiteralNode arrayLiteral(ExpressionNode... elements) {
        return new me.bechberger.jfr.extended.ast.ASTNodes.ArrayLiteralNode(Arrays.asList(elements), new Location(1, 1));
    }
    
    /**
     * Create an array literal node with the given list of elements
     * 
     * @param elements the array elements as a list
     * @return an ArrayLiteralNode with the elements
     */
    public static me.bechberger.jfr.extended.ast.ASTNodes.ArrayLiteralNode arrayLiteral(List<ExpressionNode> elements) {
        return new me.bechberger.jfr.extended.ast.ASTNodes.ArrayLiteralNode(elements, new Location(1, 1));
    }
    
    /**
     * Create an integer array literal
     * 
     * @param values the integer values
     * @return an ArrayLiteralNode with integer literal elements
     */
    public static me.bechberger.jfr.extended.ast.ASTNodes.ArrayLiteralNode intArray(int... values) {
        ExpressionNode[] elements = new ExpressionNode[values.length];
        for (int i = 0; i < values.length; i++) {
            elements[i] = ASTBuilder.numberLiteral(values[i]);
        }
        return arrayLiteral(elements);
    }
    
    /**
     * Create a string array literal
     * 
     * @param values the string values
     * @return an ArrayLiteralNode with string literal elements
     */
    public static me.bechberger.jfr.extended.ast.ASTNodes.ArrayLiteralNode stringArray(String... values) {
        ExpressionNode[] elements = new ExpressionNode[values.length];
        for (int i = 0; i < values.length; i++) {
            elements[i] = ASTBuilder.stringLiteral(values[i]);
        }
        return arrayLiteral(elements);
    }
    
    /**
     * Create a boolean array literal
     * 
     * @param values the boolean values
     * @return an ArrayLiteralNode with boolean literal elements
     */
    public static me.bechberger.jfr.extended.ast.ASTNodes.ArrayLiteralNode booleanArray(boolean... values) {
        ExpressionNode[] elements = new ExpressionNode[values.length];
        for (int i = 0; i < values.length; i++) {
            elements[i] = ASTBuilder.literal(new CellValue.BooleanValue(values[i]));
        }
        return arrayLiteral(elements);
    }
    
    /**
     * Create a double array literal
     * 
     * @param values the double values
     * @return an ArrayLiteralNode with number literal elements
     */
    public static me.bechberger.jfr.extended.ast.ASTNodes.ArrayLiteralNode doubleArray(double... values) {
        ExpressionNode[] elements = new ExpressionNode[values.length];
        for (int i = 0; i < values.length; i++) {
            elements[i] = ASTBuilder.numberLiteral(values[i]);
        }
        return arrayLiteral(elements);
    }
    
    /**
     * Create a duration array literal
     * 
     * @param values the duration values as strings
     * @return an ArrayLiteralNode with duration literal elements
     */
    public static me.bechberger.jfr.extended.ast.ASTNodes.ArrayLiteralNode durationArray(String... values) {
        ExpressionNode[] elements = new ExpressionNode[values.length];
        for (int i = 0; i < values.length; i++) {
            elements[i] = ASTBuilder.durationLiteral(values[i]);
        }
        return arrayLiteral(elements);
    }
    
    /**
     * Create an empty array literal
     * 
     * @return an empty ArrayLiteralNode
     */
    public static me.bechberger.jfr.extended.ast.ASTNodes.ArrayLiteralNode emptyArray() {
        return arrayLiteral();
    }
    
    /**
     * Create a mixed type array literal with specified elements
     * 
     * @param elements the array elements of mixed types
     * @return an ArrayLiteralNode with the mixed elements
     */
    public static me.bechberger.jfr.extended.ast.ASTNodes.ArrayLiteralNode mixedArray(ExpressionNode... elements) {
        return arrayLiteral(elements);
    }
}
