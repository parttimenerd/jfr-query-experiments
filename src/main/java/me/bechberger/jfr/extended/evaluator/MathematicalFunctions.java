package me.bechberger.jfr.extended.evaluator;

import java.util.List;
import me.bechberger.jfr.extended.table.CellValue;

/**
 * Static methods for evaluating mathematical functions.
 * Package-private methods for use within the evaluator package.
 */
public class MathematicalFunctions {
    
    static CellValue evaluateAbs(List<CellValue> arguments) {
        if (arguments.size() != 1) {
            throw new IllegalArgumentException("ABS function requires exactly 1 argument");
        }
        
        CellValue arg = arguments.get(0);
        if (!arg.isNumeric()) {
            throw new IllegalArgumentException("ABS function requires a numeric argument");
        }
        
        return arg.mapNumeric(Math::abs);
    }
    
    static CellValue evaluateCeil(List<CellValue> arguments) {
        if (arguments.size() != 1) {
            throw new IllegalArgumentException("CEIL function requires exactly 1 argument");
        }
        
        CellValue arg = arguments.get(0);
        if (!arg.isNumeric()) {
            throw new IllegalArgumentException("CEIL function requires a numeric argument");
        }
        
        return new CellValue.NumberValue(Math.ceil(arg.extractNumericValue()));
    }
    
    static CellValue evaluateFloor(List<CellValue> arguments) {
        if (arguments.size() != 1) {
            throw new IllegalArgumentException("FLOOR function requires exactly 1 argument");
        }
        
        CellValue arg = arguments.get(0);
        if (!arg.isNumeric()) {
            throw new IllegalArgumentException("FLOOR function requires a numeric argument");
        }
        
        return new CellValue.NumberValue(Math.floor(arg.extractNumericValue()));
    }
    
    static CellValue evaluateRound(List<CellValue> arguments) {
        if (arguments.size() != 1) {
            throw new IllegalArgumentException("ROUND function requires exactly 1 argument");
        }
        
        CellValue arg = arguments.get(0);
        if (!arg.isNumeric()) {
            throw new IllegalArgumentException("ROUND function requires a numeric argument");
        }
        
        return new CellValue.NumberValue(Math.round(arg.extractNumericValue()));
    }
    
    static CellValue evaluateSqrt(List<CellValue> arguments) {
        if (arguments.size() != 1) {
            throw new IllegalArgumentException("SQRT function requires exactly 1 argument");
        }
        
        CellValue arg = arguments.get(0);
        if (!arg.isNumeric()) {
            throw new IllegalArgumentException("SQRT function requires a numeric argument");
        }
        
        return new CellValue.NumberValue(Math.sqrt(arg.extractNumericValue()));
    }
    
    static CellValue evaluatePow(List<CellValue> arguments) {
        if (arguments.size() != 2) {
            throw new IllegalArgumentException("POW function requires exactly 2 arguments");
        }
        
        CellValue base = arguments.get(0);
        CellValue exponent = arguments.get(1);
        
        if (!base.isNumeric() || !exponent.isNumeric()) {
            throw new IllegalArgumentException("POW function requires numeric arguments");
        }
        
        return new CellValue.NumberValue(Math.pow(base.extractNumericValue(), exponent.extractNumericValue()));
    }
    
    static CellValue evaluateMod(List<CellValue> arguments) {
        if (arguments.size() != 2) {
            throw new IllegalArgumentException("MOD function requires exactly 2 arguments");
        }
        
        CellValue dividend = arguments.get(0);
        CellValue divisor = arguments.get(1);
        
        if (!dividend.isNumeric() || !divisor.isNumeric()) {
            throw new IllegalArgumentException("MOD function requires numeric arguments");
        }
        
        return new CellValue.NumberValue(dividend.extractNumericValue() % divisor.extractNumericValue());
    }
    
    static CellValue evaluateLog(List<CellValue> arguments) {
        if (arguments.size() != 1) {
            throw new IllegalArgumentException("LOG function requires exactly 1 argument");
        }
        
        CellValue arg = arguments.get(0);
        if (!arg.isNumeric()) {
            throw new IllegalArgumentException("LOG function requires a numeric argument");
        }
        
        return new CellValue.NumberValue(Math.log(arg.extractNumericValue()));
    }
    
    static CellValue evaluateLog10(List<CellValue> arguments) {
        if (arguments.size() != 1) {
            throw new IllegalArgumentException("LOG10 function requires exactly 1 argument");
        }
        
        CellValue arg = arguments.get(0);
        if (!arg.isNumeric()) {
            throw new IllegalArgumentException("LOG10 function requires a numeric argument");
        }
        
        return new CellValue.NumberValue(Math.log10(arg.extractNumericValue()));
    }
    
    static CellValue evaluateExp(List<CellValue> arguments) {
        if (arguments.size() != 1) {
            throw new IllegalArgumentException("EXP function requires exactly 1 argument");
        }
        
        CellValue arg = arguments.get(0);
        if (!arg.isNumeric()) {
            throw new IllegalArgumentException("EXP function requires a numeric argument");
        }
        
        return new CellValue.NumberValue(Math.exp(arg.extractNumericValue()));
    }
    
    static CellValue evaluateSin(List<CellValue> arguments) {
        if (arguments.size() != 1) {
            throw new IllegalArgumentException("SIN function requires exactly 1 argument");
        }
        
        CellValue arg = arguments.get(0);
        if (!arg.isNumeric()) {
            throw new IllegalArgumentException("SIN function requires a numeric argument");
        }
        
        return new CellValue.NumberValue(Math.sin(arg.extractNumericValue()));
    }
    
    static CellValue evaluateCos(List<CellValue> arguments) {
        if (arguments.size() != 1) {
            throw new IllegalArgumentException("COS function requires exactly 1 argument");
        }
        
        CellValue arg = arguments.get(0);
        if (!arg.isNumeric()) {
            throw new IllegalArgumentException("COS function requires a numeric argument");
        }
        
        return new CellValue.NumberValue(Math.cos(arg.extractNumericValue()));
    }
    
    static CellValue evaluateTan(List<CellValue> arguments) {
        if (arguments.size() != 1) {
            throw new IllegalArgumentException("TAN function requires exactly 1 argument");
        }
        
        CellValue arg = arguments.get(0);
        if (!arg.isNumeric()) {
            throw new IllegalArgumentException("TAN function requires a numeric argument");
        }
        
        return new CellValue.NumberValue(Math.tan(arg.extractNumericValue()));
    }
    
    // Mathematical functions for multiple values/arrays
    static CellValue evaluateMinMultiple(List<CellValue> arguments) {
        if (arguments.isEmpty()) throw new IllegalArgumentException("MIN requires at least one argument");
        
        return CellValue.mapDouble(arguments, doubles -> 
            doubles.stream().mapToDouble(Double::doubleValue).min().orElse(0.0));
    }
    
    static CellValue evaluateMaxMultiple(List<CellValue> arguments) {
        if (arguments.isEmpty()) throw new IllegalArgumentException("MAX requires at least one argument");
        
        return CellValue.mapDouble(arguments, doubles -> 
            doubles.stream().mapToDouble(Double::doubleValue).max().orElse(0.0));
    }
}
