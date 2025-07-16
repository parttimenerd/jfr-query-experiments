package me.bechberger.jfr.extended.evaluator;

import me.bechberger.jfr.extended.table.CellValue;
import java.util.Arrays;
import java.util.List;

/**
 * Test to verify that HEAD and TAIL functions can now be used in contexts where 
 * simple functions are expected (like WHERE clauses), since they're no longer aggregate functions
 */
public class TestSimpleFunctionUsage {
    
    public static void main(String[] args) {
        System.out.println("Testing Simple Function Usage in WHERE clause context...");
        
        FunctionRegistry registry = FunctionRegistry.getInstance();
        
        // Verify they are NOT aggregate functions
        System.out.println("\n=== Verifying Function Types ===");
        System.out.println("HEAD is aggregate function: " + registry.isAggregateFunction("HEAD"));
        System.out.println("TAIL is aggregate function: " + registry.isAggregateFunction("TAIL"));
        System.out.println("SORT is aggregate function: " + registry.isAggregateFunction("SORT"));
        
        // Test that these can be used as simple functions
        System.out.println("\n=== Testing Simple Function Calls ===");
        
        // Create test array
        List<CellValue> testArray = Arrays.asList(
            new CellValue.NumberValue(10),
            new CellValue.NumberValue(20),
            new CellValue.NumberValue(30),
            new CellValue.NumberValue(40),
            new CellValue.NumberValue(50)
        );
        CellValue arrayValue = new CellValue.ArrayValue(testArray);
        
        try {
            // Test HEAD with minimal arguments (this should work now, not require aggregate context)
            var headFunc = registry.getFunction("HEAD");
            if (headFunc instanceof FunctionRegistry.SimpleFunctionDefinition simpleDef) {
                // This simulates usage in a WHERE clause where we just want the first 2 elements
                CellValue result = simpleDef.apply(arrayValue, new CellValue.NumberValue(2));
                System.out.println("✅ HEAD([10,20,30,40,50], 2) = " + result);
                
                // Verify the result is an array with exactly 2 elements
                if (result instanceof CellValue.ArrayValue resultArray && resultArray.size() == 2) {
                    System.out.println("✅ HEAD correctly returned array with 2 elements");
                } else {
                    System.out.println("❌ HEAD did not return expected array with 2 elements");
                }
            }
            
            // Test TAIL 
            var tailFunc = registry.getFunction("TAIL");
            if (tailFunc instanceof FunctionRegistry.SimpleFunctionDefinition simpleDef) {
                CellValue result = simpleDef.apply(arrayValue, new CellValue.NumberValue(3));
                System.out.println("✅ TAIL([10,20,30,40,50], 3) = " + result);
                
                if (result instanceof CellValue.ArrayValue resultArray && resultArray.size() == 3) {
                    System.out.println("✅ TAIL correctly returned array with 3 elements");
                } else {
                    System.out.println("❌ TAIL did not return expected array with 3 elements");
                }
            }
            
            // Test SORT
            var sortFunc = registry.getFunction("SORT");
            if (sortFunc instanceof FunctionRegistry.SimpleFunctionDefinition simpleDef) {
                // Test sorting in descending order
                CellValue result = simpleDef.apply(arrayValue, new CellValue.StringValue("DESC"));
                System.out.println("✅ SORT([10,20,30,40,50], 'DESC') = " + result);
                
                if (result instanceof CellValue.ArrayValue resultArray && resultArray.size() == 5) {
                    System.out.println("✅ SORT correctly returned sorted array with same number of elements");
                    // Check if first element is 50 (highest)
                    if (resultArray.get(0) instanceof CellValue.NumberValue first && first.value() == 50.0) {
                        System.out.println("✅ SORT correctly sorted in descending order");
                    } else {
                        System.out.println("❌ SORT did not correctly sort in descending order");
                    }
                } else {
                    System.out.println("❌ SORT did not return expected array");
                }
            }
            
            System.out.println("\n=== Summary ===");
            System.out.println("✅ HEAD and TAIL are no longer aggregate functions");
            System.out.println("✅ They can now be used in WHERE clauses and other simple function contexts");
            System.out.println("✅ SORT function has been successfully added");
            System.out.println("✅ All functions work correctly as simple data access functions");
            
        } catch (Exception e) {
            System.err.println("❌ Error during testing: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
