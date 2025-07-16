package me.bechberger.jfr.extended.evaluator;

import java.util.List;
import me.bechberger.jfr.extended.table.CellValue;
import me.bechberger.jfr.extended.engine.exception.QueryEvaluationException;

/**
 * Static methods for evaluating mathematical functions.
 * Package-private methods for use within the evaluator package.
 */
public class MathematicalFunctions {
    
    static CellValue evaluateAbs(List<CellValue> arguments) {
        FunctionUtils.assertArgumentCount("ABS", arguments, 1);
        CellValue.NumberValue numberValue = FunctionUtils.getNumberArgument("ABS", arguments, 0);
        
        return numberValue.mapNumeric(Math::abs);
    }
    
    static CellValue evaluateCeil(List<CellValue> arguments) {
        FunctionUtils.assertArgumentCount("CEIL", arguments, 1);
        CellValue.NumberValue numberValue = FunctionUtils.getNumberArgument("CEIL", arguments, 0);
        
        return numberValue.mapNumeric(Math::ceil);
    }
    
    static CellValue evaluateFloor(List<CellValue> arguments) {
        FunctionUtils.assertArgumentCount("FLOOR", arguments, 1);
        CellValue.NumberValue numberValue = FunctionUtils.getNumberArgument("FLOOR", arguments, 0);
        
        return numberValue.mapNumeric(Math::floor);
    }
    
    static CellValue evaluateRound(List<CellValue> arguments) {
        FunctionUtils.assertArgumentRange("ROUND", arguments, 1, 2);
        CellValue.NumberValue numberValue = FunctionUtils.getNumberArgument("ROUND", arguments, 0);
        
        if (arguments.size() == 1) {
            // Single argument: round to nearest integer
            return numberValue.mapNumeric(value -> (double) Math.round(value));
        } else {
            // Two arguments: round to specified decimal places
            CellValue.NumberValue placesValue = FunctionUtils.getNumberArgument("ROUND", arguments, 1);
            
            int decimalPlaces = (int) placesValue.extractNumericValue();
            double multiplier = Math.pow(10, decimalPlaces);
            return numberValue.mapNumeric(value -> Math.round(value * multiplier) / multiplier);
        }
    }
    
    static CellValue evaluateSqrt(List<CellValue> arguments) {
        FunctionUtils.assertArgumentCount("SQRT", arguments, 1);
        CellValue.NumberValue numberValue = FunctionUtils.getNumberArgument("SQRT", arguments, 0);
        
        return numberValue.mapNumeric(Math::sqrt);
    }
    
    static CellValue evaluatePow(List<CellValue> arguments) {
        FunctionUtils.assertArgumentCount("POW", arguments, 2);
        CellValue.NumberValue baseValue = FunctionUtils.getNumberArgument("POW", arguments, 0);
        CellValue.NumberValue exponentValue = FunctionUtils.getNumberArgument("POW", arguments, 1);
        
        return baseValue.mapNumeric(base -> Math.pow(base, exponentValue.extractNumericValue()));
    }
    
    static CellValue evaluateMod(List<CellValue> arguments) {
        FunctionUtils.assertArgumentCount("MOD", arguments, 2);
        CellValue.NumberValue dividendValue = FunctionUtils.getNumberArgument("MOD", arguments, 0);
        CellValue.NumberValue divisorValue = FunctionUtils.getNumberArgument("MOD", arguments, 1);
        
        return dividendValue.mapNumeric(dividend -> dividend % divisorValue.extractNumericValue());
    }
    
    static CellValue evaluateLog(List<CellValue> arguments) {
        FunctionUtils.assertArgumentCount("LOG", arguments, 1);
        CellValue.NumberValue numberValue = FunctionUtils.getNumberArgument("LOG", arguments, 0);
        
        return numberValue.mapNumeric(Math::log);
    }
    
    static CellValue evaluateLog10(List<CellValue> arguments) {
        FunctionUtils.assertArgumentCount("LOG10", arguments, 1);
        CellValue.NumberValue numberValue = FunctionUtils.getNumberArgument("LOG10", arguments, 0);
        
        return numberValue.mapNumeric(Math::log10);
    }
    
    static CellValue evaluateExp(List<CellValue> arguments) {
        FunctionUtils.assertArgumentCount("EXP", arguments, 1);
        CellValue.NumberValue numberValue = FunctionUtils.getNumberArgument("EXP", arguments, 0);
        
        return numberValue.mapNumeric(Math::exp);
    }
    
    static CellValue evaluateSin(List<CellValue> arguments) {
        FunctionUtils.assertArgumentCount("SIN", arguments, 1);
        CellValue.NumberValue numberValue = FunctionUtils.getNumberArgument("SIN", arguments, 0);
        
        return numberValue.mapNumeric(Math::sin);
    }
    
    static CellValue evaluateCos(List<CellValue> arguments) {
        FunctionUtils.assertArgumentCount("COS", arguments, 1);
        CellValue.NumberValue numberValue = FunctionUtils.getNumberArgument("COS", arguments, 0);
        
        return numberValue.mapNumeric(Math::cos);
    }
    
    static CellValue evaluateTan(List<CellValue> arguments) {
        FunctionUtils.assertArgumentCount("TAN", arguments, 1);
        CellValue.NumberValue numberValue = FunctionUtils.getNumberArgument("TAN", arguments, 0);
        
        return numberValue.mapNumeric(Math::tan);
    }
    
    // Mathematical functions for multiple values/arrays
    static CellValue evaluateMinMultiple(List<CellValue> arguments) {
        FunctionUtils.assertAtLeastArguments("MIN", arguments, 1);
        
        return CellValue.mapDouble(arguments, doubles -> 
            doubles.stream().mapToDouble(Double::doubleValue).min().orElse(0.0));
    }
    
    static CellValue evaluateMaxMultiple(List<CellValue> arguments) {
        FunctionUtils.assertAtLeastArguments("MAX", arguments, 1);
        
        return CellValue.mapDouble(arguments, doubles -> 
            doubles.stream().mapToDouble(Double::doubleValue).max().orElse(0.0));
    }
    
    /**
     * CLAMP function constrains a value between a minimum and maximum.
     * Usage: CLAMP(min, max, value)
     * Returns: min if value < min, max if value > max, otherwise value
     */
    static CellValue evaluateClamp(List<CellValue> arguments) {
        FunctionUtils.assertArgumentCount("CLAMP", arguments, 3);
        CellValue.NumberValue minValue = FunctionUtils.getNumberArgument("CLAMP", arguments, 0);
        CellValue.NumberValue maxValue = FunctionUtils.getNumberArgument("CLAMP", arguments, 1);
        CellValue.NumberValue valueValue = FunctionUtils.getNumberArgument("CLAMP", arguments, 2);
        
        double min = minValue.extractNumericValue();
        double max = maxValue.extractNumericValue();
        double value = valueValue.extractNumericValue();
        
        if (min > max) {
            throw new QueryEvaluationException("CLAMP function validation", 
                String.format("min=%f, max=%f", min, max), 
                QueryEvaluationException.EvaluationErrorType.INVALID_STATE, null);
        }
        
        double clampedValue;
        if (value < min) {
            clampedValue = min;
        } else if (value > max) {
            clampedValue = max;
        } else {
            clampedValue = value;
        }
        
        // Preserve the original value's type while applying the clamping
        return valueValue.mapNumeric(x -> clampedValue);
    }
}
