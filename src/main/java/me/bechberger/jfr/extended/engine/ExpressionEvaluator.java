package me.bechberger.jfr.extended.engine;

import me.bechberger.jfr.extended.ast.ASTNodes.*;
import me.bechberger.jfr.extended.table.JfrTable;
import me.bechberger.jfr.extended.table.CellValue;

import me.bechberger.jfr.extended.evaluator.FunctionRegistry;
import me.bechberger.jfr.extended.evaluator.AggregateFunctions;

import java.util.List;
import java.util.ArrayList;

/**
 * Handles evaluation of expressions and conditions in the context of specific rows.
 * This class is responsible for:
 * - Evaluating expressions in row context
 * - Evaluating conditions for filtering
 * - Function call evaluation
 * - CASE expression evaluation
 * - Binary and comparison operations
 */
public class ExpressionEvaluator {
    
    private final FunctionRegistry functionRegistry;
    private final AggregateFunctions.EvaluationContext evaluationContext;
    
    public ExpressionEvaluator(FunctionRegistry functionRegistry, AggregateFunctions.EvaluationContext evaluationContext) {
        this.functionRegistry = functionRegistry;
        this.evaluationContext = evaluationContext;
    }
    
    public ExpressionEvaluator(FunctionRegistry functionRegistry) {
        this.functionRegistry = functionRegistry;
        this.evaluationContext = null; // Will need to be passed to methods that need it
    }
    
    /**
     * Evaluates an expression in the context of a specific row
     */
    public Object evaluateExpressionInRowContext(ExpressionNode expression, JfrTable.Row row, List<JfrTable.Column> columns) {
        if (expression == null) {
            return null;
        }
        
        // Handle different expression types
        if (expression instanceof LiteralNode) {
            return ((LiteralNode) expression).value().getValue();
        } else if (expression instanceof IdentifierNode) {
            String columnName = ((IdentifierNode) expression).name();
            // Find the column by name
            for (int i = 0; i < columns.size(); i++) {
                if (columns.get(i).name().equals(columnName)) {
                    CellValue cellValue = row.getCell(i);
                    return cellValue.getValue();
                }
            }
            throw new RuntimeException("Column not found: " + columnName);
        } else if (expression instanceof FunctionCallNode) {
            FunctionCallNode funcCall = (FunctionCallNode) expression;
            return evaluateFunctionCall(funcCall, row, columns);
        } else if (expression instanceof CaseExpressionNode) {
            return evaluateCaseExpression((CaseExpressionNode) expression, row, columns);
        } else if (expression instanceof BinaryExpressionNode) {
            BinaryExpressionNode binOp = (BinaryExpressionNode) expression;
            Object left = evaluateExpressionInRowContext(binOp.left(), row, columns);
            Object right = evaluateExpressionInRowContext(binOp.right(), row, columns);
            return evaluateBinaryOperation(binOp.operator(), left, right);
        }
        
        throw new RuntimeException("Unsupported expression type: " + expression.getClass().getName());
    }

    /**
     * Evaluates a condition in the context of a specific row
     */
    public boolean evaluateConditionForRow(JfrTable.Row row, ConditionNode condition, List<JfrTable.Column> columns) {
        if (condition == null) {
            return true;
        }
        
        if (condition instanceof ExpressionConditionNode) {
            ExpressionConditionNode exprCondition = (ExpressionConditionNode) condition;
            Object result = evaluateExpressionInRowContext(exprCondition.expression(), row, columns);
            
            // Convert result to boolean
            if (result instanceof Boolean) {
                return (Boolean) result;
            } else if (result instanceof Number) {
                return ((Number) result).doubleValue() != 0.0;
            } else if (result == null) {
                return false;
            } else {
                return true; // Non-null, non-zero values are truthy
            }
        }
        
        throw new RuntimeException("Unsupported condition type: " + condition.getClass().getName());
    }

    /**
     * Evaluates a function call in the context of a specific row
     */
    private Object evaluateFunctionCall(FunctionCallNode funcCall, JfrTable.Row row, List<JfrTable.Column> columns) {
        String funcName = funcCall.functionName().toUpperCase();
        List<ExpressionNode> args = funcCall.arguments();
        
        // For aggregate functions in row context, we need special handling
        // For now, just handle basic functions
        switch (funcName) {
            case "COLLECT":
                // COLLECT is an aggregate function and should not be evaluated in row context
                // This should only happen if aggregate detection failed
                throw new RuntimeException("COLLECT is an aggregate function and cannot be evaluated in row context. This suggests an issue with aggregate function detection.");
            case "COUNT":
                return 1; // Each row contributes 1 to count
            case "SUM":
                if (args.size() != 1) {
                    throw new RuntimeException("SUM function requires exactly one argument");
                }
                Object value = evaluateExpressionInRowContext(args.get(0), row, columns);
                return convertToNumber(value);
            case "AVG":
                if (args.size() != 1) {
                    throw new RuntimeException("AVG function requires exactly one argument");
                }
                Object avgValue = evaluateExpressionInRowContext(args.get(0), row, columns);
                return convertToNumber(avgValue);
            case "MIN":
            case "MAX":
                if (args.size() != 1) {
                    throw new RuntimeException(funcName + " function requires exactly one argument");
                }
                return evaluateExpressionInRowContext(args.get(0), row, columns);
            default:
                // Try to evaluate using function registry
                List<CellValue> argValues = new ArrayList<>();
                for (ExpressionNode arg : args) {
                    Object argValue = evaluateExpressionInRowContext(arg, row, columns);
                    argValues.add(CellValue.of(argValue));
                }
                
                try {
                    CellValue result = functionRegistry.evaluateFunction(funcName, argValues, evaluationContext);
                    return result.getValue();
                } catch (Exception e) {
                    throw new RuntimeException("Error evaluating function " + funcName + ": " + e.getMessage(), e);
                }
        }
    }

    /**
     * Evaluates a CASE expression in the context of a specific row
     */
    private Object evaluateCaseExpression(CaseExpressionNode caseExpr, JfrTable.Row row, List<JfrTable.Column> columns) {
        Object expressionValue = null;
        if (caseExpr.expression() != null) {
            // Simple CASE: CASE expression WHEN value THEN result ...
            expressionValue = evaluateExpressionInRowContext(caseExpr.expression(), row, columns);
        }
        
        // Check each WHEN clause
        for (WhenClauseNode whenClause : caseExpr.whenClauses()) {
            Object conditionValue = evaluateExpressionInRowContext(whenClause.condition(), row, columns);
            
            boolean matches;
            if (expressionValue != null) {
                // Simple CASE: compare expression value with WHEN value
                matches = compareValues(expressionValue, conditionValue) == 0;
            } else {
                // Searched CASE: evaluate WHEN condition as boolean
                matches = convertToBoolean(conditionValue);
            }
            
            if (matches) {
                return evaluateExpressionInRowContext(whenClause.result(), row, columns);
            }
        }
        
        // No WHEN clause matched, use ELSE clause or null
        if (caseExpr.elseExpression() != null) {
            return evaluateExpressionInRowContext(caseExpr.elseExpression(), row, columns);
        }
        
        return null;
    }

    /**
     * Evaluates a binary operation
     */
    Object evaluateBinaryOperation(BinaryOperator operator, Object left, Object right) {
        // Handle special cases for comparison operators with null values
        if (operator == BinaryOperator.EQUALS) {
            if (left == null && right == null) return true;
            if (left == null || right == null) return false;
            return compareValues(left, right) == 0;
        } else if (operator == BinaryOperator.NOT_EQUALS) {
            if (left == null && right == null) return false;
            if (left == null || right == null) return true;
            return compareValues(left, right) != 0;
        }
        
        // For arithmetic and other operations, return null if either operand is null
        if (left == null || right == null) {
            return null;
        }
        
        // Handle binary operations based on operator type
        // Use if-else chain to avoid potential switch statement issues
        if (operator == BinaryOperator.ADD) {
            return convertToNumber(left).doubleValue() + convertToNumber(right).doubleValue();
        } else if (operator == BinaryOperator.SUBTRACT) {
            return convertToNumber(left).doubleValue() - convertToNumber(right).doubleValue();
        } else if (operator == BinaryOperator.MULTIPLY) {
            return convertToNumber(left).doubleValue() * convertToNumber(right).doubleValue();
        } else if (operator == BinaryOperator.DIVIDE) {
            Number rightNum = convertToNumber(right);
            if (rightNum.doubleValue() == 0) {
                throw new RuntimeException("Division by zero");
            }
            return convertToNumber(left).doubleValue() / rightNum.doubleValue();
        } else if (operator == BinaryOperator.MODULO) {
            return convertToNumber(left).doubleValue() % convertToNumber(right).doubleValue();
        } else if (operator == BinaryOperator.LESS_THAN) {
            return compareValues(left, right) < 0;
        } else if (operator == BinaryOperator.LESS_EQUAL) {
            return compareValues(left, right) <= 0;
        } else if (operator == BinaryOperator.GREATER_THAN) {
            return compareValues(left, right) > 0;
        } else if (operator == BinaryOperator.GREATER_EQUAL) {
            return compareValues(left, right) >= 0;
        } else if (operator == BinaryOperator.AND) {
            return convertToBoolean(left) && convertToBoolean(right);
        } else if (operator == BinaryOperator.OR) {
            return convertToBoolean(left) || convertToBoolean(right);
        } else if (operator == BinaryOperator.LIKE) {
            // Handle LIKE operator for string pattern matching
            String leftStr = (left != null) ? left.toString() : "";
            String rightStr = (right != null) ? right.toString() : "";
            return leftStr.matches(rightStr.replace("%", ".*").replace("_", "."));
        } else if (operator == BinaryOperator.NOT_LIKE) {
            // Handle NOT LIKE operator
            String leftStr = (left != null) ? left.toString() : "";
            String rightStr = (right != null) ? right.toString() : "";
            return !leftStr.matches(rightStr.replace("%", ".*").replace("_", "."));
        } else if (operator == BinaryOperator.IN) {
            // Handle IN operator - for now, basic implementation
            if (right instanceof java.util.Collection) {
                return ((java.util.Collection<?>) right).contains(left);
            }
            return false;
        } else if (operator == BinaryOperator.BETWEEN) {
            // Handle BETWEEN operator - basic implementation
            // This should be handled by the parser to convert to AND expression
            throw new RuntimeException("BETWEEN operator should be handled by parser");
        } else if (operator == BinaryOperator.WITHIN) {
            // Handle WITHIN operator for temporal queries
            throw new RuntimeException("WITHIN operator not yet implemented");
        } else if (operator == BinaryOperator.OF) {
            // Handle OF operator for temporal queries  
            throw new RuntimeException("OF operator not yet implemented");
        } else {
            throw new RuntimeException("Unsupported binary operator: " + operator + " (class: " + operator.getClass().getName() + ")");
        }
    }

    /**
     * Compares two values for ordering
     */
    private int compareValues(Object left, Object right) {
        if (left == null && right == null) {
            return 0;
        }
        if (left == null) {
            return -1;
        }
        if (right == null) {
            return 1;
        }
        
        // Try numeric comparison first
        if (left instanceof Number && right instanceof Number) {
            double leftVal = ((Number) left).doubleValue();
            double rightVal = ((Number) right).doubleValue();
            return Double.compare(leftVal, rightVal);
        }
        
        // String comparison
        String leftStr = left.toString();
        String rightStr = right.toString();
        return leftStr.compareTo(rightStr);
    }

    /**
     * Converts a value to a Number
     */
    private Number convertToNumber(Object value) {
        if (value instanceof Number) {
            return (Number) value;
        }
        if (value instanceof String) {
            try {
                return Double.parseDouble((String) value);
            } catch (NumberFormatException e) {
                throw new RuntimeException("Cannot convert to number: " + value);
            }
        }
        if (value instanceof CellValue) {
            return convertToNumber(((CellValue) value).getValue());
        }
        throw new RuntimeException("Cannot convert to number: " + value);
    }

    /**
     * Converts a value to a Boolean
     */
    private boolean convertToBoolean(Object value) {
        if (value instanceof Boolean) {
            return (Boolean) value;
        }
        if (value instanceof Number) {
            return ((Number) value).doubleValue() != 0.0;
        }
        if (value == null) {
            return false;
        }
        return true; // Non-null, non-zero values are truthy
    }
}
