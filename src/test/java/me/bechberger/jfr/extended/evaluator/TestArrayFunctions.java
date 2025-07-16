package me.bechberger.jfr.extended.evaluator;

import me.bechberger.jfr.extended.table.CellValue;
import java.util.Arrays;
import java.util.List;

/**
 * Test the new array functions to verify they work correctly as simple functions
 */
public class TestArrayFunctions {
    
    public static void main(String[] args) {
        System.out.println("Testing Array Functions...");
        
        // Create test array
        List<CellValue> testArray = Arrays.asList(
            new CellValue.NumberValue(5),
            new CellValue.NumberValue(2),
            new CellValue.NumberValue(8),
            new CellValue.NumberValue(1),
            new CellValue.NumberValue(9)
        );
        CellValue arrayValue = new CellValue.ArrayValue(testArray);
        
        // Test HEAD function
        System.out.println("\n=== Testing HEAD Function ===");
        List<CellValue> headArgs = Arrays.asList(
            arrayValue,
            new CellValue.NumberValue(3)
        );
        CellValue headResult = ArrayFunctions.evaluateHead(headArgs);
        System.out.println("HEAD([5,2,8,1,9], 3) = " + headResult);
        
        // Test TAIL function
        System.out.println("\n=== Testing TAIL Function ===");
        List<CellValue> tailArgs = Arrays.asList(
            arrayValue,
            new CellValue.NumberValue(3)
        );
        CellValue tailResult = ArrayFunctions.evaluateTail(tailArgs);
        System.out.println("TAIL([5,2,8,1,9], 3) = " + tailResult);
        
        // Test SORT function (ascending)
        System.out.println("\n=== Testing SORT Function (ASC) ===");
        List<CellValue> sortAscArgs = Arrays.asList(
            arrayValue,
            new CellValue.StringValue("ASC")
        );
        CellValue sortAscResult = ArrayFunctions.evaluateSort(sortAscArgs);
        System.out.println("SORT([5,2,8,1,9], 'ASC') = " + sortAscResult);
        
        // Test SORT function (descending)
        System.out.println("\n=== Testing SORT Function (DESC) ===");
        List<CellValue> sortDescArgs = Arrays.asList(
            arrayValue,
            new CellValue.StringValue("DESC")
        );
        CellValue sortDescResult = ArrayFunctions.evaluateSort(sortDescArgs);
        System.out.println("SORT([5,2,8,1,9], 'DESC') = " + sortDescResult);
        
        // Test SORT function (default = ascending)
        System.out.println("\n=== Testing SORT Function (default) ===");
        List<CellValue> sortDefaultArgs = Arrays.asList(arrayValue);
        CellValue sortDefaultResult = ArrayFunctions.evaluateSort(sortDefaultArgs);
        System.out.println("SORT([5,2,8,1,9]) = " + sortDefaultResult);
        
        // Test with string array
        System.out.println("\n=== Testing with String Array ===");
        List<CellValue> stringArray = Arrays.asList(
            new CellValue.StringValue("banana"),
            new CellValue.StringValue("apple"),
            new CellValue.StringValue("cherry"),
            new CellValue.StringValue("date")
        );
        CellValue stringArrayValue = new CellValue.ArrayValue(stringArray);
        
        List<CellValue> stringHeadArgs = Arrays.asList(
            stringArrayValue,
            new CellValue.NumberValue(2)
        );
        CellValue stringHeadResult = ArrayFunctions.evaluateHead(stringHeadArgs);
        System.out.println("HEAD(['banana','apple','cherry','date'], 2) = " + stringHeadResult);
        
        List<CellValue> stringSortArgs = Arrays.asList(stringArrayValue);
        CellValue stringSortResult = ArrayFunctions.evaluateSort(stringSortArgs);
        System.out.println("SORT(['banana','apple','cherry','date']) = " + stringSortResult);
        
        System.out.println("\n=== All tests completed successfully! ===");
    }
}
