package me.bechberger.jfr.extended.plan.evaluator;

import me.bechberger.jfr.extended.ast.ASTNodes.*;
import me.bechberger.jfr.extended.plan.QueryExecutionContext;
import me.bechberger.jfr.extended.table.JfrTable;
import me.bechberger.jfr.extended.table.CellValue;
import me.bechberger.jfr.extended.table.CellType;
import me.bechberger.jfr.extended.evaluator.FunctionRegistry;
import me.bechberger.jfr.extended.evaluator.AggregateFunctions;
import me.bechberger.jfr.extended.engine.QueryEvaluatorUtils;
import me.bechberger.jfr.extended.plan.evaluator.ExpressionEvaluationException.*;
import me.bechberger.jfr.extended.engine.RawJfrQueryExecutor;

import java.time.Instant;
import java.time.Duration;
import java.util.List;
import java.util.ArrayList;

/**
 * Comprehensive expression evaluator for query plans with type preservation.
 * Handles all AST expression types with proper variable resolution and arithmetic operations.
 * Supports type-preserving operations: timestamp + duration -> timestamp, timestamp - timestamp -> duration.
 * Used across ProjectionPlan, FilterPlan, GroupByPlan, and HavingPlan for consistent expression evaluation.
 */
public class PlanExpressionEvaluator {

    private final QueryExecutionContext executionContext;
    private final FunctionRegistry functionRegistry;
    private ExpressionHandler customHandler;

    public PlanExpressionEvaluator(QueryExecutionContext executionContext) {
        this.executionContext = executionContext;
        this.functionRegistry = FunctionRegistry.getInstance();
        this.customHandler = null;
    }

    public PlanExpressionEvaluator(QueryExecutionContext executionContext, ExpressionHandler customHandler) {
        this.executionContext = executionContext;
        this.functionRegistry = FunctionRegistry.getInstance();
        this.customHandler = customHandler;
    }

    /**
     * Set a custom expression handler for specialized evaluation contexts.
     * @param handler The custom handler, or null to remove custom handling
     */
    public void setCustomHandler(ExpressionHandler handler) {
        this.customHandler = handler;
    }

    /**
     * Evaluates an expression within the context of a table row.
     * 
     * @param expression The AST expression to evaluate
     * @param table The table providing row context
     * @param rowIndex The current row index
     * @return The evaluated result as CellValue
     */
    public CellValue evaluateExpression(ExpressionNode expression, JfrTable table, int rowIndex) {
        try {
            return evaluateExpressionInternal(expression, table, rowIndex);
        } catch (Exception e) {
            // Use specific ExpressionEvaluationException instead of RuntimeException
            throw new ExpressionEvaluationException(
                String.format("Failed to evaluate expression: %s at row %d - %s", 
                    expression.getClass().getSimpleName(), rowIndex, e.getMessage()),
                e
            );
        }
    }

    /**
     * Evaluates an expression without table row context (for standalone expressions).
     * 
     * @param expression The AST expression to evaluate
     * @return The evaluated result as CellValue
     */
    public CellValue evaluateExpression(ExpressionNode expression) {
        try {
            return evaluateExpressionInternal(expression, null, -1);
        } catch (Exception e) {
            // Use specific ExpressionEvaluationException instead of RuntimeException
            throw new ExpressionEvaluationException(
                String.format("Failed to evaluate expression: %s - %s", 
                    expression.getClass().getSimpleName(), e.getMessage()),
                e
            );
        }
    }

    /**
     * Evaluates an expression within the context of a specific row with column metadata.
     * This is optimized for FilterPlan usage without requiring table creation.
     * 
     * @param expression The AST expression to evaluate
     * @param row The row providing data context
     * @param columns The column metadata for field resolution
     * @return The evaluated result as CellValue
     */
    public CellValue evaluateExpressionWithRowContext(ExpressionNode expression, 
                                                     JfrTable.Row row, 
                                                     List<JfrTable.Column> columns) {
        try {
            return evaluateExpressionWithRowContextInternal(expression, row, columns);
        } catch (Exception e) {
            throw new ExpressionEvaluationException(
                String.format("Failed to evaluate expression: %s with row context - %s", 
                    expression.getClass().getSimpleName(), e.getMessage()),
                e
            );
        }
    }

    private CellValue evaluateExpressionInternal(ExpressionNode expression, JfrTable table, int rowIndex) {
        // First try custom handler if available
        if (customHandler != null) {
            CellValue customResult = customHandler.handleExpression(expression, table, rowIndex, this);
            if (customResult != null) {
                return customResult;
            }
        }
        
        // Fall back to standard evaluation
        return switch (expression) {
            case LiteralNode literal -> literal.value();
            case FieldAccessNode field -> evaluateFieldAccess(field, table, rowIndex);
            case IdentifierNode identifier -> evaluateIdentifier(identifier, table, rowIndex);
            case BinaryExpressionNode binaryOp -> evaluateBinaryOperation(binaryOp, table, rowIndex);
            case UnaryExpressionNode unaryOp -> evaluateUnaryOperation(unaryOp, table, rowIndex);
            case FunctionCallNode functionCall -> evaluateFunctionCall(functionCall, table, rowIndex);
            case ArrayLiteralNode arrayLiteral -> evaluateArrayLiteral(arrayLiteral, table, rowIndex);
            case CaseExpressionNode caseExpr -> evaluateCaseExpression(caseExpr, table, rowIndex);
            case StarNode star -> evaluateStarNode(star, table, rowIndex);
            case NestedQueryNode nestedQuery -> evaluateNestedQuery(nestedQuery, table, rowIndex);
            default -> throw new UnsupportedExpressionTypeException(expression.getClass().getSimpleName());
        };
    }

    /**
     * Internal method for evaluating expressions with direct row context.
     * This provides efficient evaluation without table creation overhead.
     */
    private CellValue evaluateExpressionWithRowContextInternal(ExpressionNode expression, 
                                                              JfrTable.Row row, 
                                                              List<JfrTable.Column> columns) {
        return switch (expression) {
            case LiteralNode literal -> literal.value();
            case FieldAccessNode field -> evaluateFieldAccessWithRowContext(field, row, columns);
            case IdentifierNode identifier -> evaluateIdentifierWithRowContext(identifier, row, columns);
            case BinaryExpressionNode binaryOp -> evaluateBinaryOperationWithRowContext(binaryOp, row, columns);
            case UnaryExpressionNode unaryOp -> evaluateUnaryOperationWithRowContext(unaryOp, row, columns);
            case FunctionCallNode functionCall -> evaluateFunctionCallWithRowContext(functionCall, row, columns);
            case ArrayLiteralNode arrayLiteral -> evaluateArrayLiteralWithRowContext(arrayLiteral, row, columns);
            case CaseExpressionNode caseExpr -> evaluateCaseExpressionWithRowContext(caseExpr, row, columns);
            case VariableAssignmentExpressionNode varAssign -> evaluateVariableAssignmentWithRowContext(varAssign, row, columns);
            case StarNode star -> CellValue.of("*"); // For aggregate functions like COUNT(*)
            case NestedQueryNode nestedQuery -> evaluateNestedQueryWithRowContext(nestedQuery, row, columns);
            default -> throw new UnsupportedExpressionTypeException(expression.getClass().getSimpleName());
        };
    }

    private CellValue evaluateFieldAccess(FieldAccessNode field, JfrTable table, int rowIndex) {
        if (table == null || rowIndex < 0) {
            throw new ExpressionEvaluationException("Field access requires table context");
        }
        
        // Handle qualified field access (table.field) vs simple field access
        String fieldName = field.qualifier() != null ? field.field() : field.field();
        return table.getCell(rowIndex, fieldName);
    }

    private CellValue evaluateIdentifier(IdentifierNode identifier, JfrTable table, int rowIndex) {
        String name = identifier.name();
        
        // If we have table context, try to resolve as column first
        if (table != null && rowIndex >= 0) {
            try {
                return table.getCell(rowIndex, name);
            } catch (Exception e) {
                // Fall through to variable resolution if column lookup fails
            }
        }
        
        // Check global variables
        Object variable = executionContext.getVariable(name);
        if (variable != null) {
            return CellValue.of(variable);
        }
        
        // Check lazy variables
        if (executionContext.hasLazyVariable(name)) {
            // For now, we'll evaluate lazy variables immediately
            // In a full implementation, this would trigger lazy evaluation
            throw new ExpressionEvaluationException("Lazy variable evaluation not yet implemented: " + name);
        }
        
        throw new ExpressionEvaluationException.UndefinedIdentifierException(
            name, 
            table != null ? 
                table.getColumns().stream().map(col -> col.name()).toArray(String[]::new) :
                new String[0]
        );
    }

    private CellValue evaluateBinaryOperation(BinaryExpressionNode binaryOp, JfrTable table, int rowIndex) {
        CellValue left = evaluateExpressionInternal(binaryOp.left(), table, rowIndex);
        CellValue right = evaluateExpressionInternal(binaryOp.right(), table, rowIndex);
        
        return switch (binaryOp.operator()) {
            case ADD -> evaluateAddition(left, right, binaryOp);
            case SUBTRACT -> evaluateSubtraction(left, right, binaryOp);
            case MULTIPLY -> evaluateMultiplication(left, right, binaryOp);
            case DIVIDE -> evaluateDivision(left, right, binaryOp);
            case MODULO -> evaluateModulo(left, right, binaryOp);
            case EQUALS -> CellValue.of(areEqual(left, right));
            case NOT_EQUALS -> CellValue.of(!areEqual(left, right));
            case LESS_THAN -> CellValue.of(isLessThan(left, right));
            case LESS_EQUAL -> CellValue.of(isLessThan(left, right) || areEqual(left, right));
            case GREATER_THAN -> CellValue.of(isGreaterThan(left, right));
            case GREATER_EQUAL -> CellValue.of(isGreaterThan(left, right) || areEqual(left, right));
            case AND -> CellValue.of(getBooleanValue(left) && getBooleanValue(right));
            case OR -> CellValue.of(getBooleanValue(left) || getBooleanValue(right));
            case LIKE -> evaluateLike(left, right);
            case IN -> evaluateIn(left, right);
            default -> throw new ExpressionEvaluationException.UnsupportedOperationException("binary operation " + binaryOp.operator(), left.getType().toString(), right.getType().toString(), binaryOp);
        };
    }

    private CellValue evaluateUnaryOperation(UnaryExpressionNode unaryOp, JfrTable table, int rowIndex) {
        CellValue operand = evaluateExpressionInternal(unaryOp.operand(), table, rowIndex);
        
        return switch (unaryOp.operator()) {
            case NOT -> CellValue.of(!getBooleanValue(operand));
            case MINUS -> {
                if (operand.getType() == CellType.NUMBER) {
                    yield CellValue.of(-((CellValue.NumberValue) operand).value());
                } else if (operand.getType() == CellType.NUMBER) {
                    yield CellValue.of(-((CellValue.NumberValue) operand).value());
                } else {
                    throw new ExpressionEvaluationException.TypeMismatchException(
                        "Cannot negate non-numeric value: " + operand.getType(), unaryOp);
                }
            }
            default -> throw new ExpressionEvaluationException.UnsupportedOperationException(
                "unary " + unaryOp.operator(), operand.getType().toString());
        };
    }

    private CellValue evaluateFunctionCall(FunctionCallNode functionCall, JfrTable table, int rowIndex) {
        String functionName = functionCall.functionName();
        
        // Check if this is a computed aggregate from a previous plan phase
        if (QueryEvaluatorUtils.isAggregateFunction(functionName)) {
            Object computedValue = executionContext.getComputedAggregate(functionCall);
            if (computedValue != null) {
                // Return the pre-computed aggregate value
                return (CellValue) computedValue;
            }
        }
        
        // Regular function call - evaluate all arguments
        // Note: StarNode will be handled by evaluateStarNode() which returns appropriate representation
        List<CellValue> arguments = new ArrayList<>();
        for (ExpressionNode arg : functionCall.arguments()) {
            arguments.add(evaluateExpressionInternal(arg, table, rowIndex));
        }
        
        if (functionRegistry.isFunction(functionName)) {
            // Use the evaluation context from the execution context
            AggregateFunctions.EvaluationContext evalContext = executionContext.getEvaluationContext();
            return functionRegistry.evaluateFunction(functionName, arguments, evalContext);
        } else {
            throw new ExpressionEvaluationException.UnknownFunctionException(functionName);
        }
    }

    private CellValue evaluateArrayLiteral(ArrayLiteralNode arrayLiteral, JfrTable table, int rowIndex) {
        List<Object> values = new ArrayList<>();
        
        for (ExpressionNode element : arrayLiteral.elements()) {
            CellValue value = evaluateExpressionInternal(element, table, rowIndex);
            values.add(value.getValue());
        }
        
        return CellValue.of(values);
    }

    private CellValue evaluateCaseExpression(CaseExpressionNode caseExpr, JfrTable table, int rowIndex) {
        for (WhenClauseNode whenClause : caseExpr.whenClauses()) {
            CellValue condition = evaluateExpressionInternal(whenClause.condition(), table, rowIndex);
            if (getBooleanValue(condition)) {
                return evaluateExpressionInternal(whenClause.result(), table, rowIndex);
            }
        }
        
        if (caseExpr.elseExpression() != null) {
            return evaluateExpressionInternal(caseExpr.elseExpression(), table, rowIndex);
        }
        
        return CellValue.of(null);
    }

    private CellValue evaluateStarNode(StarNode star, JfrTable table, int rowIndex) {
        // StarNode (*) represents "all rows in current context" 
        // This enables general aggregate functions like COUNT(*), HEAD(*), TAIL(*), COLLECT(*), etc.
        
        if (table != null) {
            // Create an array with one element per row (representing each row)
            // For COUNT(*), this gives us the row count when COUNT counts the array elements
            // For HEAD(*)/TAIL(*), this gives us access to first/last row
            // For other functions, they can interpret this as "operate on all rows"
            List<Object> rowMarkers = new ArrayList<>();
            
            for (int r = 0; r < table.getRowCount(); r++) {
                // Each row is represented by its index (could be more sophisticated)
                // The key insight is that COUNT(*) counts array elements, not their values
                rowMarkers.add(r); // Non-null value representing this row
            }
            
            return CellValue.of(rowMarkers);
        } else {
            // In contexts without table (like HAVING evaluation), return proper StarValue
            // This provides semantic clarity that this represents the SQL "*" symbol
            return new CellValue.StarValue();
        }
    }

    // ===== TYPE-PRESERVING ARITHMETIC OPERATIONS =====

    private CellValue evaluateAddition(CellValue left, CellValue right, BinaryExpressionNode node) {
        // Handle null values
        if (left.getType() == CellType.NULL || right.getType() == CellType.NULL) {
            return CellValue.of(null);
        }

        // timestamp + duration -> timestamp
        if (left.getType() == CellType.TIMESTAMP && right.getType() == CellType.DURATION) {
            Instant timestamp = ((CellValue.TimestampValue) left).value();
            Duration duration = ((CellValue.DurationValue) right).value();
            return CellValue.of(timestamp.plus(duration));
        }
        
        // duration + timestamp -> timestamp
        if (left.getType() == CellType.DURATION && right.getType() == CellType.TIMESTAMP) {
            Duration duration = ((CellValue.DurationValue) left).value();
            Instant timestamp = ((CellValue.TimestampValue) right).value();
            return CellValue.of(timestamp.plus(duration));
        }
        
        // duration + duration -> duration
        if (left.getType() == CellType.DURATION && right.getType() == CellType.DURATION) {
            Duration leftDur = ((CellValue.DurationValue) left).value();
            Duration rightDur = ((CellValue.DurationValue) right).value();
            return CellValue.of(leftDur.plus(rightDur));
        }
        
        // Numeric addition
        if (isNumeric(left) && isNumeric(right)) {
            double leftVal = getNumericValue(left);
            double rightVal = getNumericValue(right);
            return CellValue.of(leftVal + rightVal);
        }
        
        // String concatenation
        if (left.getType() == CellType.STRING || right.getType() == CellType.STRING) {
            return CellValue.of(left.toString() + right.toString());
        }
        
        throw new ExpressionEvaluationException.TypeMismatchException("addition", left.getType().toString(), right.getType().toString(), node);
    }

    private CellValue evaluateSubtraction(CellValue left, CellValue right, BinaryExpressionNode node) {
        // Handle null values
        if (left.getType() == CellType.NULL || right.getType() == CellType.NULL) {
            return CellValue.of(null);
        }

        // timestamp - timestamp -> duration
        if (left.getType() == CellType.TIMESTAMP && right.getType() == CellType.TIMESTAMP) {
            Instant leftTime = ((CellValue.TimestampValue) left).value();
            Instant rightTime = ((CellValue.TimestampValue) right).value();
            return CellValue.of(Duration.between(rightTime, leftTime));
        }
        
        // timestamp - duration -> timestamp
        if (left.getType() == CellType.TIMESTAMP && right.getType() == CellType.DURATION) {
            Instant timestamp = ((CellValue.TimestampValue) left).value();
            Duration duration = ((CellValue.DurationValue) right).value();
            return CellValue.of(timestamp.minus(duration));
        }
        
        // duration - duration -> duration
        if (left.getType() == CellType.DURATION && right.getType() == CellType.DURATION) {
            Duration leftDur = ((CellValue.DurationValue) left).value();
            Duration rightDur = ((CellValue.DurationValue) right).value();
            return CellValue.of(leftDur.minus(rightDur));
        }
        
        // Numeric subtraction
        if (isNumeric(left) && isNumeric(right)) {
            double leftVal = getNumericValue(left);
            double rightVal = getNumericValue(right);
            return CellValue.of(leftVal - rightVal);
        }
        
        throw new ExpressionEvaluationException.TypeMismatchException("subtraction", left.getType().toString(), right.getType().toString(), node);
    }

    private CellValue evaluateMultiplication(CellValue left, CellValue right, BinaryExpressionNode node) {
        if (left.getType() == CellType.NULL || right.getType() == CellType.NULL) {
            return CellValue.of(null);
        }

        if (isNumeric(left) && isNumeric(right)) {
            double leftVal = getNumericValue(left);
            double rightVal = getNumericValue(right);
            return CellValue.of(leftVal * rightVal);
        }
        
        throw new ExpressionEvaluationException.TypeMismatchException("multiplication", left.getType().toString(), right.getType().toString(), node);
    }

    private CellValue evaluateDivision(CellValue left, CellValue right, BinaryExpressionNode node) {
        if (left.getType() == CellType.NULL || right.getType() == CellType.NULL) {
            return CellValue.of(null);
        }

        if (isNumeric(left) && isNumeric(right)) {
            double leftVal = getNumericValue(left);
            double rightVal = getNumericValue(right);
            if (rightVal == 0.0) {
                throw new ArithmeticException("Division by zero");
            }
            
            // Always return float for division to preserve precision
            return CellValue.of(leftVal / rightVal);
        }
        
        throw new ExpressionEvaluationException.TypeMismatchException("division", left.getType().toString(), right.getType().toString(), node);
    }

    private CellValue evaluateModulo(CellValue left, CellValue right, BinaryExpressionNode node) {
        if (left.getType() == CellType.NULL || right.getType() == CellType.NULL) {
            return CellValue.of(null);
        }

        if (isNumeric(left) && isNumeric(right)) {
            double leftVal = getNumericValue(left);
            double rightVal = getNumericValue(right);
            if (rightVal == 0.0) {
                throw new ArithmeticException("Modulo by zero");
            }
            return CellValue.of(leftVal % rightVal);
        }
        
        throw new ExpressionEvaluationException.TypeMismatchException("modulo", left.getType().toString(), right.getType().toString(), node);
    }

    // ===== COMPARISON OPERATIONS =====

    private boolean areEqual(CellValue left, CellValue right) {
        if (left.getType() == CellType.NULL && right.getType() == CellType.NULL) return true;
        if (left.getType() == CellType.NULL || right.getType() == CellType.NULL) return false;
        
        if (left.getType() != right.getType()) {
            // Allow numeric comparison between NUMBER and NUMBER
            if (isNumeric(left) && isNumeric(right)) {
                return Double.compare(getNumericValue(left), getNumericValue(right)) == 0;
            }
            return false;
        }
        
        return left.getValue().equals(right.getValue());
    }

    private boolean isLessThan(CellValue left, CellValue right) {
        if (left.getType() == CellType.NULL || right.getType() == CellType.NULL) return false;
        
        if (isNumeric(left) && isNumeric(right)) {
            return Double.compare(getNumericValue(left), getNumericValue(right)) < 0;
        }
        
        if (left.getType() == CellType.STRING && right.getType() == CellType.STRING) {
            String leftStr = ((CellValue.StringValue) left).value();
            String rightStr = ((CellValue.StringValue) right).value();
            return leftStr.compareTo(rightStr) < 0;
        }
        
        if (left.getType() == CellType.TIMESTAMP && right.getType() == CellType.TIMESTAMP) {
            Instant leftTime = ((CellValue.TimestampValue) left).value();
            Instant rightTime = ((CellValue.TimestampValue) right).value();
            return leftTime.isBefore(rightTime);
        }
        
        if (left.getType() == CellType.DURATION && right.getType() == CellType.DURATION) {
            Duration leftDur = ((CellValue.DurationValue) left).value();
            Duration rightDur = ((CellValue.DurationValue) right).value();
            return leftDur.compareTo(rightDur) < 0;
        }
        
        throw new ExpressionEvaluationException.TypeMismatchException(
            "comparison", left.getType().toString(), right.getType().toString(), null);
    }

    private boolean isGreaterThan(CellValue left, CellValue right) {
        return !areEqual(left, right) && !isLessThan(left, right);
    }

    private CellValue evaluateLike(CellValue left, CellValue right) {
        if (left.getType() == CellType.NULL || right.getType() == CellType.NULL) {
            return CellValue.of(false);
        }
        
        if (left.getType() != CellType.STRING || right.getType() != CellType.STRING) {
            throw new ExpressionEvaluationException.TypeMismatchException(
                "LIKE operator requires string operands, got " + left.getType() + " and " + right.getType(), null);
        }
        
        String text = ((CellValue.StringValue) left).value();
        String pattern = ((CellValue.StringValue) right).value();
        
        // Convert SQL LIKE pattern to regex
        String regex = pattern
            .replace(".", "\\.")
            .replace("%", ".*")
            .replace("_", ".");
            
        return CellValue.of(text.matches(regex));
    }

    private CellValue evaluateIn(CellValue left, CellValue right) {
        if (left.getType() == CellType.NULL) {
            return CellValue.of(false);
        }
        
        if (right.getType() != CellType.ARRAY) {
            throw new ExpressionEvaluationException.TypeMismatchException(
                "IN operator requires array on right side, got " + right.getType(), null);
        }
        
        CellValue.ArrayValue arrayValue = (CellValue.ArrayValue) right;
        for (CellValue item : arrayValue.elements()) {
            if (areEqual(left, item)) {
                return CellValue.of(true);
            }
        }
        
        return CellValue.of(false);
    }

    // ===== HELPER METHODS =====

    private boolean isNumeric(CellValue value) {
        return value.getType() == CellType.NUMBER || value.getType() == CellType.NUMBER;
    }

    private double getNumericValue(CellValue value) {
        return switch (value.getType()) {
            case NUMBER -> ((CellValue.NumberValue) value).value();
            default -> throw new ExpressionEvaluationException.TypeMismatchException(
                "Value is not numeric: " + value.getType(), null);
        };
    }

    private boolean getBooleanValue(CellValue value) {
        return switch (value.getType()) {
            case BOOLEAN -> ((CellValue.BooleanValue) value).value();
            default -> throw new ExpressionEvaluationException.TypeMismatchException(
                "Value is not boolean: " + value.getType(), null);
        };
    }

    // ===== ROW CONTEXT EVALUATION METHODS =====

    private CellValue evaluateFieldAccessWithRowContext(FieldAccessNode field, JfrTable.Row row, List<JfrTable.Column> columns) {
        String fieldName = field.qualifier() != null ? field.field() : field.field();
        
        // Find the column index
        for (int i = 0; i < columns.size(); i++) {
            if (columns.get(i).name().equals(fieldName)) {
                return row.getCell(i);
            }
        }
        
        throw new ExpressionEvaluationException.ColumnNotFoundException(fieldName);
    }

    private CellValue evaluateIdentifierWithRowContext(IdentifierNode identifier, JfrTable.Row row, List<JfrTable.Column> columns) {
        String name = identifier.name();
        System.err.println("DEBUG PlanExpressionEvaluator: evaluateIdentifierWithRowContext - name: " + name);
        
        // Try to resolve as column first
        for (int i = 0; i < columns.size(); i++) {
            if (columns.get(i).name().equals(name)) {
                CellValue value = row.getCell(i);
                System.err.println("DEBUG PlanExpressionEvaluator: Found column " + name + " at index " + i + " with value: " + value);
                return value;
            }
        }
        
        System.err.println("DEBUG PlanExpressionEvaluator: Column " + name + " not found, trying variable resolution");
        
        // Check row-attached variables first (lightweight, row-specific)
        Object rowVariable = row.getVariable(name);
        if (rowVariable != null) {
            System.err.println("DEBUG PlanExpressionEvaluator: Found row variable " + name + " with value: " + rowVariable);
            return CellValue.of(rowVariable);
        }
        
        // Fall back to global variable resolution
        System.err.println("DEBUG PlanExpressionEvaluator: Looking for global variable: " + name);
        System.err.println("DEBUG PlanExpressionEvaluator: executionContext is null: " + (executionContext == null));
        Object globalVariable = executionContext.getVariable(name);
        if (globalVariable != null) {
            System.err.println("DEBUG PlanExpressionEvaluator: Found global variable " + name + " with value: " + globalVariable);
            return CellValue.of(globalVariable);
        }
        System.err.println("DEBUG PlanExpressionEvaluator: Global variable " + name + " not found");
        
        // Check lazy variables
        if (executionContext.hasLazyVariable(name)) {
            throw new ExpressionEvaluationException("Lazy variable evaluation not yet implemented: " + name);
        }
        
        // Build helpful error message with available options
        List<String> availableOptions = new ArrayList<>();
        if (!columns.isEmpty()) {
            availableOptions.add("Columns: " + columns.stream()
                .map(JfrTable.Column::name)
                .reduce((a, b) -> a + ", " + b)
                .orElse(""));
        }
        
        if (row.getVariables().size() > 0) {
            availableOptions.add("Row variables: " + String.join(", ", row.getVariables().keySet()));
        }
        
        if (executionContext != null) {
            availableOptions.add("Global variables available");
        }
        
        throw new ExpressionEvaluationException.UndefinedIdentifierException(name, 
            availableOptions.toArray(new String[0]));
    }

    private CellValue evaluateBinaryOperationWithRowContext(BinaryExpressionNode binaryOp, JfrTable.Row row, List<JfrTable.Column> columns) {
        System.err.println("DEBUG PlanExpressionEvaluator: evaluateBinaryOperationWithRowContext - operator: " + binaryOp.operator());
        CellValue left = evaluateExpressionWithRowContextInternal(binaryOp.left(), row, columns);
        CellValue right = evaluateExpressionWithRowContextInternal(binaryOp.right(), row, columns);
        
        System.err.println("DEBUG PlanExpressionEvaluator: Binary operation - left: " + left + ", right: " + right);
        
        CellValue result = switch (binaryOp.operator()) {
            case ADD -> evaluateAddition(left, right, binaryOp);
            case SUBTRACT -> evaluateSubtraction(left, right, binaryOp);
            case MULTIPLY -> evaluateMultiplication(left, right, binaryOp);
            case DIVIDE -> evaluateDivision(left, right, binaryOp);
            case MODULO -> evaluateModulo(left, right, binaryOp);
            case EQUALS -> CellValue.of(areEqual(left, right));
            case NOT_EQUALS -> CellValue.of(!areEqual(left, right));
            case LESS_THAN -> CellValue.of(isLessThan(left, right));
            case LESS_EQUAL -> CellValue.of(isLessThan(left, right) || areEqual(left, right));
            case GREATER_THAN -> CellValue.of(isGreaterThan(left, right));
            case GREATER_EQUAL -> CellValue.of(isGreaterThan(left, right) || areEqual(left, right));
            case AND -> CellValue.of(getBooleanValue(left) && getBooleanValue(right));
            case OR -> CellValue.of(getBooleanValue(left) || getBooleanValue(right));
            case LIKE -> evaluateLike(left, right);
            case IN -> evaluateIn(left, right);
            default -> throw new ExpressionEvaluationException.UnsupportedOperationException(
                "binary " + binaryOp.operator(), left.getType().toString(), right.getType().toString());
        };
        
        System.err.println("DEBUG PlanExpressionEvaluator: Binary result: " + result);
        return result;
    }

    private CellValue evaluateUnaryOperationWithRowContext(UnaryExpressionNode unaryOp, JfrTable.Row row, List<JfrTable.Column> columns) {
        CellValue operand = evaluateExpressionWithRowContextInternal(unaryOp.operand(), row, columns);
        
        return switch (unaryOp.operator()) {
            case NOT -> CellValue.of(!getBooleanValue(operand));
            case MINUS -> {
                if (operand.getType() == CellType.NUMBER) {
                    yield CellValue.of(-((CellValue.NumberValue) operand).value());
                } else if (operand.getType() == CellType.NUMBER) {
                    yield CellValue.of(-((CellValue.NumberValue) operand).value());
                } else {
                    throw new ExpressionEvaluationException.TypeMismatchException(
                        "Cannot negate non-numeric value: " + operand.getType(), unaryOp);
                }
            }
            default -> throw new ExpressionEvaluationException.UnsupportedOperationException(
                "unary " + unaryOp.operator(), operand.getType().toString());
        };
    }

    private CellValue evaluateFunctionCallWithRowContext(FunctionCallNode functionCall, JfrTable.Row row, List<JfrTable.Column> columns) {
        String functionName = functionCall.functionName();
        
        // Check if this is a computed aggregate from a previous plan phase
        if (QueryEvaluatorUtils.isAggregateFunction(functionName)) {
            Object computedValue = executionContext.getComputedAggregate(functionCall);
            if (computedValue != null) {
                // Return the pre-computed aggregate value
                return (CellValue) computedValue;
            }
        }
        
        // Special handling for COUNT(*) - don't try to evaluate the StarNode argument
        if ("COUNT".equalsIgnoreCase(functionName) && 
            functionCall.arguments().size() == 1 && 
            functionCall.arguments().get(0) instanceof StarNode) {
            
            // For COUNT(*), pass a special value indicating "count all"
            List<CellValue> arguments = new ArrayList<>();
            arguments.add(new CellValue.NumberValue(1L)); // Will be handled by aggregate function
            
            if (functionRegistry.isFunction(functionName)) {
                AggregateFunctions.EvaluationContext evalContext = executionContext.getEvaluationContext();
                return functionRegistry.evaluateFunction(functionName, arguments, evalContext);
            } else {
                throw new ExpressionEvaluationException.UnknownFunctionException(functionName);
            }
        }
        
        // Regular function call - evaluate all arguments
        List<CellValue> arguments = new ArrayList<>();
        for (ExpressionNode arg : functionCall.arguments()) {
            arguments.add(evaluateExpressionWithRowContextInternal(arg, row, columns));
        }
        
        if (functionRegistry.isFunction(functionName)) {
            // Use the evaluation context from the execution context
            AggregateFunctions.EvaluationContext evalContext = executionContext.getEvaluationContext();
            return functionRegistry.evaluateFunction(functionName, arguments, evalContext);
        } else {
            throw new ExpressionEvaluationException.UnknownFunctionException(functionName);
        }
    }

    private CellValue evaluateArrayLiteralWithRowContext(ArrayLiteralNode arrayLiteral, JfrTable.Row row, List<JfrTable.Column> columns) {
        List<Object> values = new ArrayList<>();
        
        for (ExpressionNode element : arrayLiteral.elements()) {
            CellValue value = evaluateExpressionWithRowContextInternal(element, row, columns);
            values.add(value.getValue());
        }
        
        return CellValue.of(values);
    }

    private CellValue evaluateVariableAssignmentWithRowContext(VariableAssignmentExpressionNode varAssign, JfrTable.Row row, List<JfrTable.Column> columns) {
        // Evaluate the value expression
        CellValue value = evaluateExpressionWithRowContextInternal(varAssign.value(), row, columns);
        
        // Store the variable in the row itself (lightweight, attached to this specific row)
        row.setVariable(varAssign.variable(), value.getValue());
        System.err.println("DEBUG PlanExpressionEvaluator: Assigned variable " + varAssign.variable() + " = " + value.getValue() + " to row");
        
        // Return the assigned value
        return value;
    }

    private CellValue evaluateCaseExpressionWithRowContext(CaseExpressionNode caseExpr, JfrTable.Row row, List<JfrTable.Column> columns) {
        for (WhenClauseNode whenClause : caseExpr.whenClauses()) {
            CellValue condition = evaluateExpressionWithRowContextInternal(whenClause.condition(), row, columns);
            if (getBooleanValue(condition)) {
                return evaluateExpressionWithRowContextInternal(whenClause.result(), row, columns);
            }
        }
        
        if (caseExpr.elseExpression() != null) {
            return evaluateExpressionWithRowContextInternal(caseExpr.elseExpression(), row, columns);
        }
        
        return CellValue.of(null);
    }
    
    /**
     * Evaluates a nested query with table context.
     * Executes the nested JFR query and returns the first cell value from the result.
     */
    private CellValue evaluateNestedQuery(NestedQueryNode nestedQuery, JfrTable table, int rowIndex) {
        RawJfrQueryExecutor executor = executionContext.getRawJfrExecutor();
        if (executor == null) {
            throw new ExpressionEvaluationException(
                "No raw JFR executor available for nested query: " + nestedQuery.jfrQuery()
            );
        }
        
        try {
            // Create a RawJfrQueryNode from the nested query string
            RawJfrQueryNode rawQuery = new RawJfrQueryNode(nestedQuery.jfrQuery(), nestedQuery.location());
            JfrTable result = executor.execute(rawQuery);
            
            // Extract single value from the result table
            return extractSingleValue(result, nestedQuery.jfrQuery());
        } catch (Exception e) {
            throw new ExpressionEvaluationException(
                "Failed to execute nested query: " + nestedQuery.jfrQuery() + " - " + e.getMessage(),
                e
            );
        }
    }
    
    /**
     * Evaluates a nested query with row context.
     * Handles correlated subqueries by substituting outer query references with actual values.
     */
    private CellValue evaluateNestedQueryWithRowContext(NestedQueryNode nestedQuery, JfrTable.Row row, List<JfrTable.Column> columns) {
        RawJfrQueryExecutor executor = executionContext.getRawJfrExecutor();
        if (executor == null) {
            throw new ExpressionEvaluationException(
                "No raw JFR executor available for nested query: " + nestedQuery.jfrQuery()
            );
        }
        
        try {
            // Handle correlated subquery by substituting outer query references
            String processedQuery = processCorrelatedSubquery(nestedQuery.jfrQuery(), row, columns);
            
            // Create a RawJfrQueryNode from the processed query string
            RawJfrQueryNode rawQuery = new RawJfrQueryNode(processedQuery, nestedQuery.location());
            JfrTable result = executor.execute(rawQuery);
            
            // Extract single value from the result table
            return extractSingleValue(result, processedQuery);
        } catch (Exception e) {
            throw new ExpressionEvaluationException(
                "Failed to execute nested query: " + nestedQuery.jfrQuery() + " - " + e.getMessage(),
                e
            );
        }
    }
    
    /**
     * Extracts a single value from a query result table.
     * For subqueries in SELECT clauses, we expect a single value result.
     */
    private CellValue extractSingleValue(JfrTable result, String query) {
        if (result.getRowCount() == 0) {
            return CellValue.of(null); // No rows returned
        }
        
        if (result.getColumnCount() == 0) {
            throw new ExpressionEvaluationException(
                "Nested query returned no columns: " + query
            );
        }
        
        // For subqueries like SELECT AVG(salary), we expect exactly one column and one row
        if (result.getRowCount() > 1) {
            throw new ExpressionEvaluationException(
                "Nested query returned multiple rows (" + result.getRowCount() + "). " +
                "Subqueries in SELECT clause must return a single value: " + query
            );
        }
        
        if (result.getColumnCount() > 1) {
            throw new ExpressionEvaluationException(
                "Nested query returned multiple columns (" + result.getColumnCount() + "). " +
                "Subqueries in SELECT clause must return a single value: " + query
            );
        }
        
        // Get the single value from row 0, column 0
        String columnName = result.getColumns().get(0).name();
        return result.getCell(0, columnName);
    }
    
    /**
     * Processes a correlated subquery by substituting outer query references with actual values.
     * This handles cases like "e1.department" where e1 refers to the outer query's table alias.
     */
    private String processCorrelatedSubquery(String query, JfrTable.Row row, List<JfrTable.Column> columns) {
        String processedQuery = query;
        
        // Simple pattern matching for table.column references
        // Look for patterns like "e1.columnName" and replace with the actual value
        for (int i = 0; i < columns.size(); i++) {
            String columnName = columns.get(i).name();
            CellValue cellValue = row.getCell(i);
            
            // Try various common table alias patterns
            String[] possibleAliases = {"e1", "e", "t1", "t", "outer"};
            
            for (String alias : possibleAliases) {
                // Handle both "e1.column" and "e1 . column" patterns
                String pattern1 = alias + "." + columnName;
                String pattern2 = alias + " . " + columnName;
                
                String replacement = formatValueForQuery(cellValue);
                
                if (processedQuery.contains(pattern1)) {
                    processedQuery = processedQuery.replace(pattern1, replacement);
                }
                if (processedQuery.contains(pattern2)) {
                    processedQuery = processedQuery.replace(pattern2, replacement);
                }
            }
        }
        
        return processedQuery;
    }
    
    /**
     * Formats a cell value for use in a SQL query string.
     */
    private String formatValueForQuery(CellValue value) {
        if (value.getType() == CellType.NULL) {
            return "NULL";
        } else if (value.getType() == CellType.STRING) {
            // Escape single quotes and wrap in quotes
            String stringValue = ((CellValue.StringValue) value).value();
            return "'" + stringValue.replace("'", "''") + "'";
        } else if (value.getType() == CellType.NUMBER) {
            return String.valueOf(((CellValue.NumberValue) value).value());
        } else if (value.getType() == CellType.BOOLEAN) {
            return String.valueOf(((CellValue.BooleanValue) value).value());
        } else {
            // For other types, convert to string and quote
            return "'" + value.toString().replace("'", "''") + "'";
        }
    }
}
