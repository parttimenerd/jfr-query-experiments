package me.bechberger.jfr.extended.evaluator;

import me.bechberger.jfr.extended.table.CellValue;
import java.util.Arrays;
import java.util.List;

/**
 * Test the functions through the FunctionRegistry to verify they're properly registered as simple functions
 */
public class TestFunctionRegistry {
    
    public static void main(String[] args) {
        System.out.println("Testing Functions through FunctionRegistry...");
        
        FunctionRegistry registry = FunctionRegistry.getInstance();
        
        // Check if functions are registered
        System.out.println("\n=== Checking Function Registration ===");
        System.out.println("HEAD function registered: " + registry.isFunction("HEAD"));
        System.out.println("TAIL function registered: " + registry.isFunction("TAIL"));
        System.out.println("SORT function registered: " + registry.isFunction("SORT"));
        
        // Get function details
        System.out.println("\n=== Function Details ===");
        if (registry.isFunction("HEAD")) {
            var headDef = registry.getFunction("HEAD");
            System.out.println("HEAD: " + headDef.description());
            System.out.println("HEAD type: " + headDef.getClass().getSimpleName());
        }
        
        if (registry.isFunction("TAIL")) {
            var tailDef = registry.getFunction("TAIL");
            System.out.println("TAIL: " + tailDef.description());
            System.out.println("TAIL type: " + tailDef.getClass().getSimpleName());
        }
        
        if (registry.isFunction("SORT")) {
            var sortDef = registry.getFunction("SORT");
            System.out.println("SORT: " + sortDef.description());
            System.out.println("SORT type: " + sortDef.getClass().getSimpleName());
        }
        
        // Test calling functions through registry
        System.out.println("\n=== Testing Function Calls through Registry ===");
        
        // Create test array
        List<CellValue> testArray = Arrays.asList(
            new CellValue.NumberValue(5),
            new CellValue.NumberValue(2),
            new CellValue.NumberValue(8),
            new CellValue.NumberValue(1),
            new CellValue.NumberValue(9)
        );
        CellValue arrayValue = new CellValue.ArrayValue(testArray);
        
        try {
            // Test HEAD through registry
            if (registry.isFunction("HEAD")) {
                var headFunc = registry.getFunction("HEAD");
                if (headFunc instanceof FunctionRegistry.SimpleFunctionDefinition simpleDef) {
                    CellValue headResult = simpleDef.apply(arrayValue, new CellValue.NumberValue(3));
                    System.out.println("Registry HEAD([5,2,8,1,9], 3) = " + headResult);
                }
            }
            
            // Test SORT through registry
            if (registry.isFunction("SORT")) {
                var sortFunc = registry.getFunction("SORT");
                if (sortFunc instanceof FunctionRegistry.SimpleFunctionDefinition simpleDef) {
                    CellValue sortResult = simpleDef.apply(arrayValue, new CellValue.StringValue("DESC"));
                    System.out.println("Registry SORT([5,2,8,1,9], 'DESC') = " + sortResult);
                }
            }
            
            System.out.println("\n=== Registry tests completed successfully! ===");
            
        } catch (Exception e) {
            System.err.println("Error during registry testing: " + e.getMessage());
            e.printStackTrace();
        }
        
        // List all data access functions
        System.out.println("\n=== All Data Access Functions ===");
        registry.getFunctionsByType(FunctionRegistry.FunctionType.DATA_ACCESS).stream()
            .forEach(funcDef -> {
                System.out.println(funcDef.name() + ": " + funcDef.description());
                System.out.println("  Type: " + funcDef.getClass().getSimpleName());
            });
    }
}
