package me.bechberger.jfr.extended.engine;

import me.bechberger.jfr.extended.ast.ASTNodes.*;
import me.bechberger.jfr.extended.table.JfrTable;
import me.bechberger.jfr.extended.table.CellValue;
import me.bechberger.jfr.extended.table.CellType;
import java.util.*;

/**
 * Utility methods for query processing: aggregate detection, key extraction, row/column helpers, type helpers.
 */
public class QueryEvaluatorUtils {
    public static boolean isAggregateFunction(String functionName) {
        String upperName = functionName.toUpperCase();
        return switch (upperName) {
            case "COUNT", "SUM", "AVG", "AVERAGE", "MIN", "MAX", 
                 "STDDEV", "VARIANCE", "PERCENTILE", "MEDIAN", "COLLECT" -> true;
            default -> upperName.startsWith("P9") || upperName.startsWith("PERCENTILE");
        };
    }

    public static boolean containsAggregateFunction(ExpressionNode expr) {
        if (expr instanceof FunctionCallNode functionCall) {
            if (isAggregateFunction(functionCall.functionName())) {
                return true;
            }
            for (ExpressionNode arg : functionCall.arguments()) {
                if (containsAggregateFunction(arg)) {
                    return true;
                }
            }
        } else if (expr instanceof BinaryExpressionNode binaryExpr) {
            return containsAggregateFunction(binaryExpr.left()) || 
                   containsAggregateFunction(binaryExpr.right());
        } else if (expr instanceof UnaryExpressionNode unaryExpr) {
            return containsAggregateFunction(unaryExpr.operand());
        } else if (expr instanceof CaseExpressionNode caseExpr) {
            // Check if any part of the CASE expression contains aggregates
            if (caseExpr.expression() != null && containsAggregateFunction(caseExpr.expression())) {
                return true;
            }
            for (WhenClauseNode whenClause : caseExpr.whenClauses()) {
                if (containsAggregateFunction(whenClause.condition()) || 
                    containsAggregateFunction(whenClause.result())) {
                    return true;
                }
            }
            if (caseExpr.elseExpression() != null && containsAggregateFunction(caseExpr.elseExpression())) {
                return true;
            }
        }
        return false;
    }

    public static String createAggregateKey(FunctionCallNode functionCall) {
        StringBuilder key = new StringBuilder(functionCall.functionName().toUpperCase());
        key.append("(");
        for (int i = 0; i < functionCall.arguments().size(); i++) {
            if (i > 0) key.append(",");
            key.append(extractExpressionKey(functionCall.arguments().get(i)));
        }
        key.append(")");
        return key.toString();
    }

    public static String extractExpressionKey(ExpressionNode expr) {
        if (expr instanceof IdentifierNode id) {
            return id.name();
        } else if (expr instanceof FieldAccessNode field) {
            return field.field();
        } else if (expr instanceof LiteralNode literal) {
            return literal.value().toString();
        } else if (expr instanceof FunctionCallNode fn) {
            return createAggregateKey(fn);
        } else if (expr instanceof BinaryExpressionNode bin) {
            return extractExpressionKey(bin.left()) + bin.operator() + extractExpressionKey(bin.right());
        } else if (expr instanceof UnaryExpressionNode unary) {
            return unary.operator() + extractExpressionKey(unary.operand());
        }
        return expr.getClass().getSimpleName();
    }

    public static CellValue getRowValueByColumnName(JfrTable.Row row, List<JfrTable.Column> columns, String columnName) {
        for (int i = 0; i < columns.size() && i < row.getCells().size(); i++) {
            if (columns.get(i).name().equals(columnName)) {
                return row.getCells().get(i);
            }
        }
        return new CellValue.NullValue();
    }

    public static String getRowValueAsString(JfrTable.Row row, String fieldName) {
        if (!row.getCells().isEmpty()) {
            return row.getCells().get(0).toString();
        }
        return "";
    }

    public static String buildGroupKey(JfrTable.Row row, List<JfrTable.Column> columns, List<ExpressionNode> fields) {
        StringBuilder keyBuilder = new StringBuilder();
        for (ExpressionNode field : fields) {
            if (keyBuilder.length() > 0) {
                keyBuilder.append("|");
            }
            
            // Handle qualified field references like "o.category"
            CellValue value;
            String fieldKey;
            if (field instanceof FieldAccessNode fieldAccess) {
                // For qualified field access like "o.category", find the correct column in joined table
                String tableAlias = fieldAccess.qualifier();
                String fieldName = fieldAccess.field();
                
                // Try to find the column in the joined table
                // Check for columns with alias prefixes like "left_category" or "right_category"
                String leftPrefixedName = "left_" + fieldName;
                String rightPrefixedName = "right_" + fieldName;
                
                if (hasColumn(columns, leftPrefixedName)) {
                    value = getRowValueByColumnName(row, columns, leftPrefixedName);
                    fieldKey = tableAlias + "." + fieldName;
                } else if (hasColumn(columns, rightPrefixedName)) {
                    value = getRowValueByColumnName(row, columns, rightPrefixedName);
                    fieldKey = tableAlias + "." + fieldName;
                } else {
                    // Fallback to original field name if prefixed versions not found
                    value = getRowValueByColumnName(row, columns, fieldName);
                    fieldKey = fieldName;
                }
            } else {
                // For simple identifiers, use the original logic
                String fieldName = extractColumnName(field);
                value = getRowValueByColumnName(row, columns, fieldName);
                fieldKey = fieldName;
            }
            
            keyBuilder.append(fieldKey).append("=").append(value.toString());
        }
        return keyBuilder.toString();
    }
    
    private static boolean hasColumn(List<JfrTable.Column> columns, String columnName) {
        return columns.stream().anyMatch(c -> c.name().equals(columnName));
    }

    public static String extractColumnName(ExpressionNode expr) {
        if (expr instanceof FieldAccessNode fieldAccess) {
            return fieldAccess.field();
        } else if (expr instanceof IdentifierNode id) {
            return id.name();
        } else if (expr instanceof FunctionCallNode functionCall) {
            // For function calls, generate a meaningful name based on the function
            // This will only be used when no alias is provided
            String funcName = functionCall.functionName().toLowerCase();
            if (functionCall.arguments().isEmpty()) {
                return funcName;
            } else {
                // For functions with arguments, create a descriptive name
                // e.g., "count_value", "sum_duration", etc.
                ExpressionNode firstArg = functionCall.arguments().get(0);
                String argName = extractColumnName(firstArg);
                return funcName + "_" + argName;
            }
        }
        return "unknown";
    }


    public static boolean isNumericValue(Object value) {
        if (value instanceof Number) {
            return true;
        }
        if (value instanceof CellValue cellValue) {
            return switch (cellValue.getType()) {
                case NUMBER, DURATION, TIMESTAMP, MEMORY_SIZE, RATE -> true;
                default -> false;
            };
        }
        return false;
    }

    public static boolean isIntegerValue(Object value) {
        if (value instanceof Number number) {
            return number instanceof Byte || number instanceof Short || 
                   number instanceof Integer || number instanceof Long;
        }
        if (value instanceof CellValue cellValue) {
            return cellValue instanceof CellValue.NumberValue || 
                   cellValue instanceof CellValue.MemorySizeValue ||
                   cellValue instanceof CellValue.DurationValue ||
                   cellValue instanceof CellValue.TimestampValue ||
                   cellValue instanceof CellValue.RateValue;
        }
        return false;
    }

    public static double extractNumericValue(CellValue cellValue) {
        return switch (cellValue.getType()) {
            case DURATION -> ((CellValue.DurationValue) cellValue).value().toNanos() / 1_000_000.0;
            case TIMESTAMP -> ((CellValue.TimestampValue) cellValue).value().toEpochMilli();
            case MEMORY_SIZE -> ((CellValue.MemorySizeValue) cellValue).value();
            case RATE -> ((CellValue.RateValue) cellValue).count();
            default -> throw new IllegalArgumentException("Cannot extract numeric value from " + cellValue.getType());
        };
    }

    public static boolean isNumericColumn(CellType type) {
        return switch (type) {
            case NUMBER, DURATION, TIMESTAMP, MEMORY_SIZE, RATE -> true;
            default -> false;
        };
    }

    public static CellValue computeSum(List<JfrTable.Row> groupRows, int columnIndex) {
        double sum = 0.0;
        CellType resultType = CellType.NUMBER;
        
        for (JfrTable.Row row : groupRows) {
            if (columnIndex < row.getCells().size()) {
                CellValue cell = row.getCells().get(columnIndex);
                if (isNumericValue(cell)) {
                    sum += extractNumericValue(cell);
                    if (cell.getType() == CellType.NUMBER) {
                        resultType = CellType.NUMBER;
                    }
                }
            }
        }
        
        return resultType == CellType.NUMBER ? 
            new CellValue.NumberValue(sum) : 
            new CellValue.NumberValue((long) sum);
    }
    
    /**
     * Convert a literal node to its corresponding CellValue
     */
    public static CellValue convertLiteralToValue(LiteralNode literal) {
        Object value = literal.value();
        if (value instanceof Number) {
            if (value instanceof Double || value instanceof Float) {
                return new CellValue.NumberValue(((Number) value).doubleValue());
            } else {
                return new CellValue.NumberValue(((Number) value).longValue());
            }
        } else if (value instanceof String) {
            return new CellValue.StringValue((String) value);
        } else if (value instanceof Boolean) {
            return new CellValue.BooleanValue((Boolean) value);
        } else if (value == null) {
            return new CellValue.NullValue();
        } else {
            return new CellValue.StringValue(value.toString());
        }
    }
    
    /**
     * Convert an object to its corresponding CellValue
     */
    public static CellValue convertObjectToCellValue(Object value) {
        if (value == null) {
            return new CellValue.NullValue();
        } else if (value instanceof CellValue) {
            return (CellValue) value;
        } else if (value instanceof Number) {
            if (value instanceof Double || value instanceof Float) {
                return new CellValue.NumberValue(((Number) value).doubleValue());
            } else {
                return new CellValue.NumberValue(((Number) value).longValue());
            }
        } else if (value instanceof String) {
            return new CellValue.StringValue((String) value);
        } else if (value instanceof Boolean) {
            return new CellValue.BooleanValue((Boolean) value);
        } else if (value instanceof java.time.Instant) {
            return new CellValue.TimestampValue((java.time.Instant) value);
        } else if (value instanceof java.time.Duration) {
            return new CellValue.DurationValue((java.time.Duration) value);
        } else if (value instanceof List<?> list) {
            List<CellValue> elements = list.stream()
                .map(QueryEvaluatorUtils::convertObjectToCellValue)
                .collect(java.util.stream.Collectors.toList());
            return new CellValue.ArrayValue(elements);
        } else if (value instanceof Object[] array) {
            List<CellValue> elements = new ArrayList<>();
            for (Object element : array) {
                elements.add(convertObjectToCellValue(element));
            }
            return new CellValue.ArrayValue(elements);
        } else {
            return new CellValue.StringValue(value.toString());
        }
    }
    
    /**
     * Collect all aggregate expressions from ORDER BY and HAVING clauses.
     * This is used to ensure all aggregates needed by subsequent plans are computed during GROUP BY.
     */
    public static Set<FunctionCallNode> collectAdditionalAggregates(OrderByNode orderBy, HavingNode having) {
        Set<FunctionCallNode> aggregates = new HashSet<>();
        
        if (orderBy != null) {
            for (OrderFieldNode field : orderBy.fields()) {
                collectAggregatesFromExpression(field.field(), aggregates);
            }
        }
        
        if (having != null) {
            collectAggregatesFromCondition(having.condition(), aggregates);
        }
        
        return aggregates;
    }
    
    /**
     * Collect aggregates from a condition node.
     */
    private static void collectAggregatesFromCondition(ConditionNode condition, Set<FunctionCallNode> aggregates) {
        if (condition instanceof ExpressionConditionNode exprCondition) {
            collectAggregatesFromExpression(exprCondition.expression(), aggregates);
        }
        // Add other condition types as needed
    }
    
    /**
     * Recursively collect all aggregate function calls from an expression.
     */
    private static void collectAggregatesFromExpression(ExpressionNode expr, Set<FunctionCallNode> aggregates) {
        if (expr instanceof FunctionCallNode functionCall) {
            if (isAggregateFunction(functionCall.functionName())) {
                aggregates.add(functionCall);
            }
            // Also check arguments for nested aggregates
            for (ExpressionNode arg : functionCall.arguments()) {
                collectAggregatesFromExpression(arg, aggregates);
            }
        } else if (expr instanceof BinaryExpressionNode binaryExpr) {
            collectAggregatesFromExpression(binaryExpr.left(), aggregates);
            collectAggregatesFromExpression(binaryExpr.right(), aggregates);
        } else if (expr instanceof UnaryExpressionNode unaryExpr) {
            collectAggregatesFromExpression(unaryExpr.operand(), aggregates);
        } else if (expr instanceof CaseExpressionNode caseExpr) {
            if (caseExpr.expression() != null) {
                collectAggregatesFromExpression(caseExpr.expression(), aggregates);
            }
            for (WhenClauseNode whenClause : caseExpr.whenClauses()) {
                collectAggregatesFromExpression(whenClause.condition(), aggregates);
                collectAggregatesFromExpression(whenClause.result(), aggregates);
            }
            if (caseExpr.elseExpression() != null) {
                collectAggregatesFromExpression(caseExpr.elseExpression(), aggregates);
            }
        } else if (expr instanceof FieldAccessNode) {
            // Field access doesn't contain aggregates
        } else if (expr instanceof IdentifierNode) {
            // Identifiers don't contain aggregates
        } else if (expr instanceof LiteralNode) {
            // Literals don't contain aggregates
        } else if (expr instanceof StarNode) {
            // Star expressions don't contain aggregates
        } else if (expr instanceof PercentileFunctionNode percentileFunc) {
            // Percentile functions are aggregates themselves
            // Convert to equivalent FunctionCallNode for processing
            List<ExpressionNode> args = List.of(percentileFunc.valueExpression());
            aggregates.add(new FunctionCallNode(percentileFunc.functionName(), args, false, percentileFunc.location()));
        }
        // Add other expression types as needed
    }
}
