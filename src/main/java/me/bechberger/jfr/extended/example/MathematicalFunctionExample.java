package me.bechberger.jfr.extended.example;

import me.bechberger.jfr.extended.evaluator.FunctionRegistry;

/**
 * Example showing how to work with the function registry.
 * Demonstrates listing all available function types including mathematical,
 * aggregate, string, date/time, and conditional functions.
 */
public class MathematicalFunctionExample {
    
    public static void main(String[] args) {
        FunctionRegistry registry = FunctionRegistry.getInstance();
        
        // Example: Check if a function is available
        boolean hasAbsFunction = registry.isFunction("ABS");
        System.out.println("ABS function available: " + hasAbsFunction);
        
        // Example: Get function definition
        FunctionRegistry.FunctionDefinition absDef = registry.getFunction("ABS");
        if (absDef != null) {
            System.out.println("Function: " + absDef.name());
            System.out.println("Type: " + absDef.type());
            System.out.println("Description: " + absDef.description());
        }
        
        // Example: List all mathematical functions
        System.out.println("Mathematical functions:");
        registry.getFunctionsByType(FunctionRegistry.FunctionType.MATHEMATICAL)
               .forEach(func -> System.out.println("  " + func.name() + ": " + func.description()));
        
        // Example: List all aggregate functions
        System.out.println("Aggregate functions:");
        registry.getFunctionsByType(FunctionRegistry.FunctionType.AGGREGATE)
               .forEach(func -> System.out.println("  " + func.name() + ": " + func.description()));
        
        // Example: List all string functions
        System.out.println("String functions:");
        registry.getFunctionsByType(FunctionRegistry.FunctionType.STRING)
               .forEach(func -> System.out.println("  " + func.name() + ": " + func.description()));
        
        // Example: List all date/time functions
        System.out.println("Date/time functions:");
        registry.getFunctionsByType(FunctionRegistry.FunctionType.DATE_TIME)
               .forEach(func -> System.out.println("  " + func.name() + ": " + func.description()));
        
        // Example: List all conditional functions
        System.out.println("Conditional functions:");
        registry.getFunctionsByType(FunctionRegistry.FunctionType.CONDITIONAL)
               .forEach(func -> System.out.println("  " + func.name() + ": " + func.description()));
        
        // Example: Get all supported function names
        System.out.println("All supported functions:");
        registry.getFunctionNames().stream().sorted()
               .forEach(name -> System.out.println("  " + name));
        
        // Example: Demonstrate conditional function usage
        System.out.println("\nConditional function examples:");
        System.out.println("  IF(condition, trueValue, falseValue) - Basic conditional");
        System.out.println("  CASE(cond1, val1, cond2, val2, default) - Multi-way conditional");
        System.out.println("  COALESCE(val1, val2, val3) - First non-null value");
        System.out.println("  NULLIF(val1, val2) - Return null if values are equal");
        System.out.println("  ISNULL(value) - Check if value is null");
        System.out.println("  ISNOTNULL(value) - Check if value is not null");
        System.out.println("  GREATEST(val1, val2, val3) - Return greatest value");
        System.out.println("  LEAST(val1, val2, val3) - Return smallest value");
    }
}
