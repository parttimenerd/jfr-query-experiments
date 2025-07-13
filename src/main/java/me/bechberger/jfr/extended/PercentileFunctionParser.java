package me.bechberger.jfr.extended;

import me.bechberger.jfr.extended.ast.ASTNodes.*;
import me.bechberger.jfr.extended.ast.Location;
import me.bechberger.jfr.extended.table.CellValue;

import java.util.List;

/**
 * Utility class for parsing percentile functions (P90, P95, P99, P999, PERCENTILE, etc.)
 * and their selection variants (P90SELECT, P95SELECT, etc.).
 * 
 * <p>This class handles the special syntax and semantics of percentile functions,
 * including their ability to work with or without parentheses and their selection variants.</p>
 */
public class PercentileFunctionParser {
    
    /**
     * Check if a function name is a percentile function
     */
    public static boolean isPercentileFunction(String functionName) {
        return switch (functionName.toUpperCase()) {
            case "P90", "P95", "P99", "P999", "PERCENTILE" -> true;
            default -> false;
        };
    }
    
    /**
     * Check if a function name is a percentile selection function
     */
    public static boolean isPercentileSelectionFunction(String functionName) {
        return switch (functionName.toUpperCase()) {
            case "P90SELECT", "P95SELECT", "P99SELECT", "P999SELECT", "PERCENTILE_SELECT" -> true;
            default -> false;
        };
    }
    
    /**
     * Get the percentile value for a percentile function name
     */
    public static double getPercentileValue(String functionName) {
        return switch (functionName.toUpperCase()) {
            case "P90" -> 90.0;
            case "P95" -> 95.0;
            case "P99" -> 99.0;
            case "P999" -> 99.9;
            default -> throw new IllegalArgumentException("Unknown percentile function: " + functionName);
        };
    }
    
    /**
     * Get the percentile value for a percentile selection function name
     */
    public static double getPercentileSelectionValue(String functionName) {
        return switch (functionName.toUpperCase()) {
            case "P90SELECT" -> 90.0;
            case "P95SELECT" -> 95.0;
            case "P99SELECT" -> 99.0;
            case "P999SELECT" -> 99.9;
            case "PERCENTILE_SELECT" -> 0.0; // Will be set from arguments
            default -> throw new IllegalArgumentException("Unknown percentile selection function: " + functionName);
        };
    }
    
    /**
     * Create a percentile function node from a function call
     */
    public static ExpressionNode createPercentileFunction(String functionName, List<ExpressionNode> arguments, Location location) throws ParserException {
        if ("PERCENTILE".equalsIgnoreCase(functionName)) {
            // PERCENTILE function requires a percentile value argument
            if (arguments.isEmpty()) {
                throw new ParserException("PERCENTILE function requires a percentile value argument");
            }
            
            ExpressionNode percentileArg = arguments.get(0);
            if (!(percentileArg instanceof LiteralNode literal) || 
                !(literal.value() instanceof CellValue.NumberValue numberValue)) {
                throw new ParserException("PERCENTILE function requires a numeric percentile value");
            }
            
            double percentile = numberValue.value();
            // Note: Removed range validation (0-100) to allow any numeric percentile value
            
            // Get the field/expression argument if provided
            ExpressionNode fieldExpr = arguments.size() > 1 ? arguments.get(1) : null;
            ExpressionNode timeSliceFilter = arguments.size() > 2 ? arguments.get(2) : null;
            
            return new PercentileFunctionNode(functionName, fieldExpr, percentile, timeSliceFilter, location);
        } else {
            // Fixed percentile functions (P90, P95, etc.)
            double percentile = getPercentileValue(functionName);
            ExpressionNode fieldExpr = arguments.isEmpty() ? null : arguments.get(0);
            ExpressionNode timeSliceFilter = arguments.size() > 1 ? arguments.get(1) : null;
            
            return new PercentileFunctionNode(functionName, fieldExpr, percentile, timeSliceFilter, location);
        }
    }
    
    /**
     * Create a percentile selection function node from a function call
     */
    public static ExpressionNode createPercentileSelectionFunction(String functionName, List<ExpressionNode> arguments, Location location) throws ParserException {
        if ("PERCENTILE_SELECT".equalsIgnoreCase(functionName)) {
            // PERCENTILE_SELECT function requires percentile, table, id field, and value expression
            if (arguments.size() < 4) {
                throw new ParserException("PERCENTILE_SELECT function requires 4 arguments: percentile, tableName, idField, valueExpression");
            }
            
            ExpressionNode percentileArg = arguments.get(0);
            if (!(percentileArg instanceof LiteralNode literal) || 
                !(literal.value() instanceof CellValue.NumberValue numberValue)) {
                throw new ParserException("PERCENTILE_SELECT function requires a numeric percentile value");
            }
            
            double percentile = numberValue.value();
            // Note: Removed range validation (0-100) to allow any numeric percentile value
            
            ExpressionNode tableExpr = arguments.get(1);
            ExpressionNode idFieldExpr = arguments.get(2);
            ExpressionNode valueExpression = arguments.get(3);
            
            // Extract table name and id field (should be identifiers)
            String tableName = tableExpr instanceof IdentifierNode id ? id.name() : tableExpr.toString();
            String idField = idFieldExpr instanceof IdentifierNode id ? id.name() : idFieldExpr.toString();
            
            return new PercentileSelectionNode(
                functionName,
                tableName,
                idField,
                valueExpression,
                percentile,
                location
            );
        } else {
            // Fixed percentile selection functions (P90SELECT, P95SELECT, etc.)
            if (arguments.size() < 3) {
                throw new ParserException(functionName + " function requires 3 arguments: tableName, idField, valueExpression");
            }
            
            double percentile = getPercentileSelectionValue(functionName);
            
            // Arguments should be: tableName, idField, valueExpression
            ExpressionNode tableExpr = arguments.get(0);
            ExpressionNode idFieldExpr = arguments.get(1);
            ExpressionNode valueExpression = arguments.get(2);
            
            // Extract table name and id field (should be identifiers)
            String tableName = tableExpr instanceof IdentifierNode id ? id.name() : tableExpr.toString();
            String idField = idFieldExpr instanceof IdentifierNode id ? id.name() : idFieldExpr.toString();
            
            return new PercentileSelectionNode(
                functionName,
                tableName,
                idField,
                valueExpression,
                percentile,
                location
            );
        }
    }
    
    /**
     * Validate arguments for percentile functions
     */
    public static void validatePercentileArguments(String functionName, List<ExpressionNode> arguments) throws ParserException {
        if (isPercentileFunction(functionName)) {
            if ("PERCENTILE".equalsIgnoreCase(functionName)) {
                if (arguments.isEmpty()) {
                    throw new ParserException("PERCENTILE function requires at least 1 argument (percentile value)");
                }
                if (arguments.size() > 2) {
                    throw new ParserException("PERCENTILE function accepts at most 2 arguments (percentile value, field expression)");
                }
            } else {
                // Fixed percentile functions (P90, P95, etc.) can have 0 or 1 argument
                if (arguments.size() > 1) {
                    throw new ParserException(functionName + " function accepts at most 1 argument (field expression)");
                }
            }
        } else if (isPercentileSelectionFunction(functionName)) {
            if ("PERCENTILE_SELECT".equalsIgnoreCase(functionName)) {
                if (arguments.size() < 4) {
                    throw new ParserException("PERCENTILE_SELECT function requires 4 arguments: percentile, tableName, idField, valueExpression");
                }
                if (arguments.size() > 4) {
                    throw new ParserException("PERCENTILE_SELECT function accepts exactly 4 arguments");
                }
            } else {
                // Fixed percentile selection functions (P90SELECT, etc.)
                if (arguments.size() < 3) {
                    throw new ParserException(functionName + " function requires 3 arguments: tableName, idField, valueExpression");
                }
                if (arguments.size() > 3) {
                    throw new ParserException(functionName + " function accepts exactly 3 arguments");
                }
            }
        }
    }
}
