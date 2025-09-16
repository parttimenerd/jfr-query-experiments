package me.bechberger.jfr.extended.plan.exception;

import me.bechberger.jfr.extended.table.CellType;

/**
 * Demonstration of the new plan exception system.
 * 
 * This class shows how to properly wrap and create exceptions with plan context,
 * replacing plan-specific exception classes with a unified wrapper approach.
 * 
 * @author JFR Query Plan Architecture
 * @since 2.0
 */
public class PlanExceptionDemo {
    
    public static void main(String[] args) {
        System.out.println("=== PLAN EXCEPTION SYSTEM DEMO ===\n");
        
        // Demo 1: DataAccessException with suggestions
        demonstrateDataAccessException();
        
        // Demo 2: TypeException with conversion suggestions
        demonstrateTypeException();
        
        // Demo 3: Exception wrapping concept
        demonstrateExceptionWrapping();
    }
    
    private static void demonstrateDataAccessException() {
        System.out.println("1. DataAccessException with suggestions:");
        
        // NOTE: These are hardcoded examples for demonstration purposes only
        // In production code, available tables should be queried from the actual JFR metadata
        String[] availableTables = {"Users", "GarbageCollection", "ExecutionSample"};
        DataAccessException dataAccessError = new DataAccessException(
            "Table 'User' not found", 
            "User", 
            DataAccessException.AccessType.TABLE_LOOKUP, 
            availableTables);
        
        System.out.println("Message: " + dataAccessError.getMessage());
        System.out.println("Resource: " + dataAccessError.getResource());
        System.out.println("Access type: " + dataAccessError.getAccessType().getDescription());
        System.out.println("Has alternatives: " + dataAccessError.hasAlternatives());
        if (dataAccessError.hasAlternatives()) {
            System.out.println("Alternatives: " + String.join(", ", dataAccessError.getAlternatives()));
        }
        System.out.println("\nError Report:");
        System.out.println(dataAccessError.getDetailedMessage());
        System.out.println();
    }
    
    private static void demonstrateTypeException() {
        System.out.println("2. TypeException with conversion suggestions:");
        
        TypeException typeError = new TypeException(
            "Cannot compare string with number", 
            CellType.NUMBER, 
            CellType.STRING, 
            "test value");
        
        System.out.println("Message: " + typeError.getMessage());
        System.out.println("Expected: " + typeError.getExpectedType());
        System.out.println("Actual: " + typeError.getActualType());
        System.out.println("Problematic value: " + typeError.getProblematicValue());
        System.out.println("Conversion possible: " + typeError.isConversionPossible());
        String suggestion = typeError.getConversionSuggestion();
        if (suggestion != null) {
            System.out.println("Suggestion: " + suggestion);
        }
        System.out.println("\nError Report:");
        System.out.println(typeError.createErrorReport());
        System.out.println();
    }
    
    private static void demonstrateExceptionWrapping() {
        System.out.println("3. Exception wrapping concept:");
        
        System.out.println("The PlanException system provides:");
        System.out.println("- Universal wrapper for any exception with plan context");
        System.out.println("- Automatic plan association through PlanException.wrap() methods");
        System.out.println("- PlanExceptionFactory for common exception patterns");
        System.out.println("- Enhanced error reporting with execution phase tracking");
        System.out.println("- Support for wrapping QueryEvaluationException and QueryExecutionException");
        System.out.println("- Specialized exceptions (DataAccessException, TypeException) for specific scenarios");
        System.out.println();
        
        System.out.println("Usage in plan execute() methods:");
        System.out.println("try {");
        System.out.println("    // Plan execution logic...");
        System.out.println("} catch (Exception e) {");
        System.out.println("    PlanException wrapped = PlanExceptionFactory.wrapException(");
        System.out.println("        e, this, ExecutionPhase.DATA_PROCESSING, \"context\");");
        System.out.println("    return QueryResult.failure(wrapped);");
        System.out.println("}");
        System.out.println();
        
        System.out.println("Benefits:");
        System.out.println("- No more plan-specific exception classes needed");
        System.out.println("- Consistent exception handling across all plans");
        System.out.println("- Enhanced debugging with plan context");
        System.out.println("- Better error messages with suggestions");
        System.out.println("- Preservation of original exception details");
    }
}
