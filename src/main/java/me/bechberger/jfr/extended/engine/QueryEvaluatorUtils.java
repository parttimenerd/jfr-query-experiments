package me.bechberger.jfr.extended.engine;

import me.bechberger.jfr.extended.ast.ASTNodes.*;
import me.bechberger.jfr.extended.table.JfrTable;
import me.bechberger.jfr.extended.table.CellValue;
import me.bechberger.jfr.extended.table.CellType;
import java.util.*;

/**
 * Utility methods for QueryEvaluator: aggregate detection, key extraction, row/column helpers, type helpers.
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

    public static Map<String, CellValue> precomputeAggregatesInSelect(List<ExpressionNode> selectExpressions, JfrTable sourceData, QueryEvaluator evaluator) {
        Map<String, CellValue> aggregates = new HashMap<>();
        for (ExpressionNode expr : selectExpressions) {
            collectAggregateExpressions(expr, sourceData, aggregates, evaluator);
        }
        return aggregates;
    }

    public static void collectAggregateExpressions(ExpressionNode expr, JfrTable sourceData, Map<String, CellValue> aggregates, QueryEvaluator evaluator) {
        if (expr instanceof FunctionCallNode functionCall) {
            String functionName = functionCall.functionName();
            if (isAggregateFunction(functionName)) {
                String aggregateKey = createAggregateKey(functionCall);
                if (!aggregates.containsKey(aggregateKey)) {
                    CellValue aggregateValue = evaluator.computeAggregateValue(functionCall, sourceData);
                    aggregates.put(aggregateKey, aggregateValue);
                }
            }
            for (ExpressionNode arg : functionCall.arguments()) {
                collectAggregateExpressions(arg, sourceData, aggregates, evaluator);
            }
        } else if (expr instanceof BinaryExpressionNode binaryExpr) {
            collectAggregateExpressions(binaryExpr.left(), sourceData, aggregates, evaluator);
            collectAggregateExpressions(binaryExpr.right(), sourceData, aggregates, evaluator);
        } else if (expr instanceof UnaryExpressionNode unaryExpr) {
            collectAggregateExpressions(unaryExpr.operand(), sourceData, aggregates, evaluator);
        }
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

    public static CellType determineColumnType(ExpressionNode expression, JfrTable sourceData, QueryEvaluator evaluator) {
        try {
            if (!sourceData.getRows().isEmpty()) {
                CellValue sampleValue = CellValue.of(evaluator.evaluateExpressionInRowContext(expression, sourceData.getRows().get(0), sourceData.getColumns()));
                return sampleValue.getType();
            }
        } catch (Exception e) {
        }
        return CellType.STRING;
    }

    public static CellValue evaluateExpressionForRow(JfrTable.Row row, ExpressionNode expression, List<JfrTable.Column> columns, QueryEvaluator evaluator) {
        try {
            return CellValue.of(evaluator.evaluateExpressionInRowContext(expression, row, columns));
        } catch (Exception e) {
            return new CellValue.StringValue("Error: " + e.getMessage());
        }
    }

    public static boolean isNumericValue(Object value) {
        if (value instanceof Number) {
            return true;
        }
        if (value instanceof CellValue cellValue) {
            return switch (cellValue.getType()) {
                case NUMBER, FLOAT, DURATION, TIMESTAMP, MEMORY_SIZE, RATE -> true;
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
            case NUMBER -> ((CellValue.NumberValue) cellValue).value();
            case FLOAT -> ((CellValue.FloatValue) cellValue).value();
            case DURATION -> ((CellValue.DurationValue) cellValue).value().toNanos() / 1_000_000.0;
            case TIMESTAMP -> ((CellValue.TimestampValue) cellValue).value().toEpochMilli();
            case MEMORY_SIZE -> ((CellValue.MemorySizeValue) cellValue).value();
            case RATE -> ((CellValue.RateValue) cellValue).count();
            default -> throw new IllegalArgumentException("Cannot extract numeric value from " + cellValue.getType());
        };
    }

    public static boolean isNumericColumn(CellType type) {
        return switch (type) {
            case NUMBER, FLOAT, DURATION, TIMESTAMP, MEMORY_SIZE, RATE -> true;
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
                    if (cell.getType() == CellType.FLOAT) {
                        resultType = CellType.FLOAT;
                    }
                }
            }
        }
        
        return resultType == CellType.FLOAT ? 
            new CellValue.FloatValue(sum) : 
            new CellValue.NumberValue((long) sum);
    }

    public static boolean isNumericValue(CellValue cellValue) {
        return switch (cellValue.getType()) {
            case NUMBER, FLOAT, DURATION, TIMESTAMP, MEMORY_SIZE, RATE -> true;
            default -> false;
        };
    }
    
    /**
     * Convert a literal node to its corresponding CellValue
     */
    public static CellValue convertLiteralToValue(LiteralNode literal) {
        Object value = literal.value();
        if (value instanceof Number) {
            if (value instanceof Double || value instanceof Float) {
                return new CellValue.FloatValue(((Number) value).doubleValue());
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
                return new CellValue.FloatValue(((Number) value).doubleValue());
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
}
