package me.bechberger.jfr.extended.plan.evaluator;

import me.bechberger.jfr.extended.ast.ASTNodes.BinaryOperator;
import me.bechberger.jfr.extended.table.CellType;
import me.bechberger.jfr.extended.table.CellValue;

import java.util.*;

/**
 * Declarative DSL for specifying type operations and their result types.
 * Provides a fluent API for defining which types can be used with which operations
 * and what the result type should be.
 * 
 * Example usage:
 * specify(ADD).withSelf(NUMBER, MEMORY_SIZE, DURATION, NUMBER).apply(TIMESTAMP, DURATION, TIMESTAMP)
 */
public class TypeOperationSpec {
    
    private final BinaryOperator operator;
    private final Map<TypePair, CellType> typeRules = new HashMap<>();
    private final Set<CellType> selfApplicableTypes = new HashSet<>();
    
    private TypeOperationSpec(BinaryOperator operator) {
        this.operator = operator;
    }
    
    /**
     * Create a new type operation specification for the given operator
     */
    public static TypeOperationSpec specify(BinaryOperator operator) {
        return new TypeOperationSpec(operator);
    }
    
    /**
     * Specify types that can be applied to themselves (e.g., NUMBER + NUMBER -> NUMBER)
     */
    public TypeOperationSpec withSelf(CellType... types) {
        for (CellType type : types) {
            selfApplicableTypes.add(type);
            typeRules.put(new TypePair(type, type), type);
        }
        return this;
    }
    
    /**
     * Specify specific type combinations and their result type
     * apply(leftType, rightType, resultType)
     */
    public TypeOperationSpec apply(CellType leftType, CellType rightType, CellType resultType) {
        typeRules.put(new TypePair(leftType, rightType), resultType);
        return this;
    }
    
    /**
     * Check if the operation is supported for the given types
     */
    public boolean isSupported(CellType leftType, CellType rightType) {
        return typeRules.containsKey(new TypePair(leftType, rightType));
    }
    
    /**
     * Get the result type for the operation between the given types
     */
    public CellType getResultType(CellType leftType, CellType rightType) {
        TypePair pair = new TypePair(leftType, rightType);
        CellType result = typeRules.get(pair);
        if (result == null) {
            throw new ExpressionEvaluationException.TypeMismatchException(
                String.format("Operation %s not supported between %s and %s", 
                    operator, leftType, rightType), null);
        }
        return result;
    }
    
    /**
     * Apply the operation between two cell values using the type rules
     */
    public CellValue applyOperation(CellValue left, CellValue right) {
        CellType leftType = left.getType();
        CellType rightType = right.getType();
        
        // Handle null values
        if (leftType == CellType.NULL || rightType == CellType.NULL) {
            return CellValue.of(null);
        }
        
        // Check if operation is supported
        if (!isSupported(leftType, rightType)) {
            throw new ExpressionEvaluationException.TypeMismatchException(
                String.format("Operation %s not supported between %s and %s", 
                    operator, leftType, rightType), null);
        }
        
        // Use CellValue.mapBinary for simple numeric operations
        if (isSimpleNumericOperation(leftType, rightType)) {
            return left.mapBinary(right, operator);
        }
        
        // Handle special type-preserving operations manually
        return handleSpecialOperation(left, right, leftType, rightType);
    }
    
    private boolean isSimpleNumericOperation(CellType leftType, CellType rightType) {
        // Operations between NUMBER, NUMBER are simple
        return (leftType == CellType.NUMBER || leftType == CellType.NUMBER) &&
               (rightType == CellType.NUMBER || rightType == CellType.NUMBER);
    }
    
    private CellValue handleSpecialOperation(CellValue left, CellValue right, 
                                           CellType leftType, CellType rightType) {
        
        return switch (operator) {
            case ADD -> handleAddition(left, right, leftType, rightType);
            case SUBTRACT -> handleSubtraction(left, right, leftType, rightType);
            case MULTIPLY, DIVIDE, MODULO -> left.mapBinary(right, operator);
            default -> throw new ExpressionEvaluationException.UnsupportedOperationException(
                "operator " + operator, null, null);
        };
    }
    
    private CellValue handleAddition(CellValue left, CellValue right, CellType leftType, CellType rightType) {
        // timestamp + duration -> timestamp
        if (leftType == CellType.TIMESTAMP && rightType == CellType.DURATION) {
            var timestamp = ((CellValue.TimestampValue) left).value();
            var duration = ((CellValue.DurationValue) right).value();
            return CellValue.of(timestamp.plus(duration));
        }
        
        // duration + timestamp -> timestamp
        if (leftType == CellType.DURATION && rightType == CellType.TIMESTAMP) {
            var duration = ((CellValue.DurationValue) left).value();
            var timestamp = ((CellValue.TimestampValue) right).value();
            return CellValue.of(timestamp.plus(duration));
        }
        
        // duration + duration -> duration
        if (leftType == CellType.DURATION && rightType == CellType.DURATION) {
            var leftDur = ((CellValue.DurationValue) left).value();
            var rightDur = ((CellValue.DurationValue) right).value();
            return CellValue.of(leftDur.plus(rightDur));
        }
        
        // memory_size + memory_size -> memory_size
        if (leftType == CellType.MEMORY_SIZE && rightType == CellType.MEMORY_SIZE) {
            var leftMem = ((CellValue.MemorySizeValue) left).value();
            var rightMem = ((CellValue.MemorySizeValue) right).value();
            return CellValue.of(leftMem + rightMem);
        }
        
        // String concatenation
        if (leftType == CellType.STRING || rightType == CellType.STRING) {
            return CellValue.of(left.toString() + right.toString());
        }
        
        // Fall back to numeric addition
        return left.mapBinary(right, operator);
    }
    
    private CellValue handleSubtraction(CellValue left, CellValue right, CellType leftType, CellType rightType) {
        // timestamp - timestamp -> duration
        if (leftType == CellType.TIMESTAMP && rightType == CellType.TIMESTAMP) {
            var leftTime = ((CellValue.TimestampValue) left).value();
            var rightTime = ((CellValue.TimestampValue) right).value();
            return CellValue.of(java.time.Duration.between(rightTime, leftTime));
        }
        
        // timestamp - duration -> timestamp
        if (leftType == CellType.TIMESTAMP && rightType == CellType.DURATION) {
            var timestamp = ((CellValue.TimestampValue) left).value();
            var duration = ((CellValue.DurationValue) right).value();
            return CellValue.of(timestamp.minus(duration));
        }
        
        // duration - duration -> duration
        if (leftType == CellType.DURATION && rightType == CellType.DURATION) {
            var leftDur = ((CellValue.DurationValue) left).value();
            var rightDur = ((CellValue.DurationValue) right).value();
            return CellValue.of(leftDur.minus(rightDur));
        }
        
        // memory_size - memory_size -> memory_size
        if (leftType == CellType.MEMORY_SIZE && rightType == CellType.MEMORY_SIZE) {
            var leftMem = ((CellValue.MemorySizeValue) left).value();
            var rightMem = ((CellValue.MemorySizeValue) right).value();
            return CellValue.of(leftMem - rightMem);
        }
        
        // Fall back to numeric subtraction
        return left.mapBinary(right, operator);
    }
    
    /**
     * Represents a pair of types for operation lookup
     */
    private record TypePair(CellType left, CellType right) {
        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null || getClass() != obj.getClass()) return false;
            TypePair typePair = (TypePair) obj;
            return left == typePair.left && right == typePair.right;
        }
        
        @Override
        public int hashCode() {
            return Objects.hash(left, right);
        }
    }
    
    /**
     * Pre-configured type operation specifications for common operations
     */
    public static class Operations {
        
        public static final TypeOperationSpec ADDITION = specify(BinaryOperator.ADD)
            .withSelf(CellType.NUMBER, CellType.NUMBER, CellType.DURATION, CellType.MEMORY_SIZE)
            .apply(CellType.TIMESTAMP, CellType.DURATION, CellType.TIMESTAMP)
            .apply(CellType.DURATION, CellType.TIMESTAMP, CellType.TIMESTAMP)
            .apply(CellType.STRING, CellType.STRING, CellType.STRING);
            
        public static final TypeOperationSpec SUBTRACTION = specify(BinaryOperator.SUBTRACT)
            .withSelf(CellType.NUMBER, CellType.NUMBER, CellType.DURATION, CellType.MEMORY_SIZE)
            .apply(CellType.TIMESTAMP, CellType.TIMESTAMP, CellType.DURATION)
            .apply(CellType.TIMESTAMP, CellType.DURATION, CellType.TIMESTAMP);
            
        public static final TypeOperationSpec MULTIPLICATION = specify(BinaryOperator.MULTIPLY)
            .withSelf(CellType.NUMBER, CellType.NUMBER);
            
        public static final TypeOperationSpec DIVISION = specify(BinaryOperator.DIVIDE)
            .withSelf(CellType.NUMBER, CellType.NUMBER);
            
        public static final TypeOperationSpec MODULO = specify(BinaryOperator.MODULO)
            .withSelf(CellType.NUMBER, CellType.NUMBER);
            
        /**
         * Get the type operation spec for a given operator
         */
        public static TypeOperationSpec forOperator(BinaryOperator operator) {
            return switch (operator) {
                case ADD -> ADDITION;
                case SUBTRACT -> SUBTRACTION;
                case MULTIPLY -> MULTIPLICATION;
                case DIVIDE -> DIVISION;
                case MODULO -> MODULO;
                default -> throw new ExpressionEvaluationException.UnsupportedOperationException(
                    "type spec for operator " + operator, null, null);
            };
        }
    }
}
