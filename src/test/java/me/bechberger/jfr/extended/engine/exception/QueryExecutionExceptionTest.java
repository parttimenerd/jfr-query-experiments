package me.bechberger.jfr.extended.engine.exception;

import me.bechberger.jfr.extended.ast.ASTNode;
import me.bechberger.jfr.extended.ast.ASTNodes;
import me.bechberger.jfr.extended.ast.Location;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive test suite for the enhanced exception model.
 * 
 * This test class demonstrates the improved exception hierarchy with specific
 * exception types for different kinds of query execution errors, following
 * the coding guidelines for comprehensive testing with JUnit5.
 * 
 * @author Query Engine Team
 * @since 1.0
 */
@DisplayName("Enhanced Exception Model Tests")
class QueryExecutionExceptionTest {
    
    // ===== BASE EXCEPTION TESTS =====
    
    @Test
    @DisplayName("QueryExecutionException should provide detailed error messages")
    void testBaseQueryExecutionException() {
        // Arrange
        Location location = new Location(5, 10);
        ASTNode errorNode = new ASTNodes.IdentifierNode("testColumn", location);
        String context = "Evaluating SELECT clause";
        String userHint = "Check column name spelling";
        
        // Act
        QueryExecutionException exception = new QueryExecutionException(
            "Test error message", errorNode, context, userHint, null
        );
        
        // Assert
        assertNotNull(exception.getMessage());
        assertTrue(exception.getMessage().contains("Test error message"));
        assertTrue(exception.getMessage().contains("Line 5, Column 10"));
        assertTrue(exception.getMessage().contains("IdentifierNode"));
        assertTrue(exception.getMessage().contains(context));
        assertTrue(exception.getMessage().contains(userHint));
        
        assertEquals(errorNode, exception.getErrorNode());
        assertEquals(location.line(), exception.getLocation().line());
        assertEquals(location.column(), exception.getLocation().column());
        assertEquals(context, exception.getContext());
        assertEquals(userHint, exception.getUserHint());
        assertTrue(exception.hasLocation());
        assertTrue(exception.hasErrorNode());
    }
    
    @Test
    @DisplayName("QueryExecutionException should handle null values gracefully")
    void testBaseExceptionWithNullValues() {
        // Act
        QueryExecutionException exception = new QueryExecutionException(
            "Error without context", null, null, null, null
        );
        
        // Assert
        assertNotNull(exception.getMessage());
        assertNull(exception.getErrorNode());
        assertNull(exception.getLocation());
        assertNull(exception.getContext());
        assertNull(exception.getUserHint());
        assertFalse(exception.hasLocation());
        assertFalse(exception.hasErrorNode());
    }
    
    // ===== COLUMN NOT FOUND EXCEPTION TESTS =====
    
    @Test
    @DisplayName("ColumnNotFoundException should provide helpful column suggestions")
    void testColumnNotFoundException() {
        // Arrange
        String columnName = "ColumnNam"; // Typo in "ColumnName"
        String[] availableColumns = {"ColumnName", "duration", "timestamp", "eventType"};
        String tableName = "GarbageCollection";
        Location location = new Location(3, 15);
        ASTNode errorNode = new ASTNodes.IdentifierNode(columnName, location);
        
        // Act
        ColumnNotFoundException exception = new ColumnNotFoundException(
            columnName, availableColumns, tableName, errorNode
        );
        
        // Assert
        assertTrue(exception.getMessage().contains("Column 'ColumnNam' not found"));
        assertTrue(exception.getMessage().contains("Did you mean 'ColumnName'?"));
        assertTrue(exception.getMessage().contains("Available columns:"));
        assertTrue(exception.getMessage().contains("'duration'"));
        
        assertEquals(columnName, exception.getColumnName());
        assertArrayEquals(availableColumns, exception.getAvailableColumns());
        assertEquals(tableName, exception.getTableName());
        assertTrue(exception.hasAvailableColumns());
    }
    
    @Test
    @DisplayName("ColumnNotFoundException should handle empty available columns")
    void testColumnNotFoundWithEmptyColumns() {
        // Arrange
        String columnName = "nonexistent";
        String[] availableColumns = {};
        
        // Act
        ColumnNotFoundException exception = new ColumnNotFoundException(
            columnName, availableColumns, null
        );
        
        // Assert
        assertTrue(exception.getMessage().contains("Check if the table has any columns"));
        assertFalse(exception.hasAvailableColumns());
    }
    
    // ===== UNKNOWN FUNCTION EXCEPTION TESTS =====
    
    @Test
    @DisplayName("UnknownFunctionException should suggest similar function names")
    void testUnknownFunctionException() {
        // Arrange
        String functionName = "COUT"; // Typo in "COUNT"
        Set<String> availableFunctions = Set.of("COUNT", "SUM", "AVG", "MAX", "MIN", "COLLECT");
        Location location = new Location(2, 8);
        ASTNode errorNode = new ASTNodes.FunctionCallNode(functionName, null, false, location);
        
        // Act
        UnknownFunctionException exception = new UnknownFunctionException(
            functionName, availableFunctions, false, errorNode
        );
        
        // Assert
        assertTrue(exception.getMessage().contains("Unknown function 'COUT'"));
        assertTrue(exception.getMessage().contains("Did you mean 'COUNT'?"));
        assertTrue(exception.getMessage().contains("Available functions:"));
        
        assertEquals(functionName, exception.getFunctionName());
        assertEquals(availableFunctions, exception.getAvailableFunctions());
        assertFalse(exception.isAggregateContext());
        assertTrue(exception.hasAvailableFunctions());
    }
    
    @Test
    @DisplayName("UnknownFunctionException should handle aggregate context")
    void testUnknownFunctionInAggregateContext() {
        // Arrange
        String functionName = "INVALID_AGG";
        Set<String> availableFunctions = Set.of("COUNT", "SUM", "AVG");
        
        // Act
        UnknownFunctionException exception = new UnknownFunctionException(
            functionName, availableFunctions, true, null
        );
        
        // Assert
        assertTrue(exception.getMessage().contains("aggregate context"));
        assertTrue(exception.isAggregateContext());
    }
    
    // ===== TABLE NOT FOUND EXCEPTION TESTS =====
    
    @Test
    @DisplayName("TableNotFoundException should suggest similar table names")
    void testTableNotFoundException() {
        // Arrange
        String tableName = "GarbageColection"; // Typo in "GarbageCollection"
        String[] availableTables = {"GarbageCollection", "ExecutionSample", "AllocationSample"};
        
        // Act
        TableNotFoundException exception = new TableNotFoundException(
            tableName, availableTables, "event type", null
        );
        
        // Assert
        assertTrue(exception.getMessage().contains("Event type 'GarbageColection' not found"));
        assertTrue(exception.getMessage().contains("Did you mean 'GarbageCollection'?"));
        assertTrue(exception.getMessage().contains("Available event types:"));
        
        assertEquals(tableName, exception.getTableName());
        assertArrayEquals(availableTables, exception.getAvailableTables());
        assertEquals("event type", exception.getSourceType());
        assertTrue(exception.hasAvailableTables());
    }
    
    @Test
    @DisplayName("TableNotFoundException should provide factory methods for specific types")
    void testTableNotFoundFactoryMethods() {
        // Test event type factory
        TableNotFoundException eventException = TableNotFoundException.forEventType(
            "InvalidEvent", new String[]{"ValidEvent"}, null
        );
        assertTrue(eventException.getMessage().contains("Event type"));
        assertEquals("event type", eventException.getSourceType());
        
        // Test view factory
        TableNotFoundException viewException = TableNotFoundException.forView(
            "InvalidView", new String[]{"ValidView"}, null
        );
        assertTrue(viewException.getMessage().contains("View"));
        assertEquals("view", viewException.getSourceType());
    }
    
    // ===== VARIABLE NOT FOUND EXCEPTION TESTS =====
    
    @Test
    @DisplayName("VariableNotFoundException should suggest similar variable names")
    void testVariableNotFoundException() {
        // Arrange
        String variableName = "gcEvenst"; // Typo in "gcEvents"
        String[] availableVariables = {"gcEvents", "samples", "allocations"};
        String scopeContext = "current query scope";
        
        // Act
        VariableNotFoundException exception = new VariableNotFoundException(
            variableName, availableVariables, scopeContext, null
        );
        
        // Assert
        assertTrue(exception.getMessage().contains("Variable 'gcEvenst' not found"));
        assertTrue(exception.getMessage().contains("Did you mean 'gcEvents'?"));
        assertTrue(exception.getMessage().contains("Available variables:"));
        
        assertEquals(variableName, exception.getVariableName());
        assertArrayEquals(availableVariables, exception.getAvailableVariables());
        assertEquals(scopeContext, exception.getScopeContext());
        assertTrue(exception.hasAvailableVariables());
    }
    
    @Test
    @DisplayName("VariableNotFoundException should handle empty scope")
    void testVariableNotFoundEmptyScope() {
        // Act
        VariableNotFoundException exception = new VariableNotFoundException(
            "someVar", new String[]{}, null
        );
        
        // Assert
        assertTrue(exception.getMessage().contains("No variables are defined"));
        assertTrue(exception.getMessage().contains("var := expression"));
        assertFalse(exception.hasAvailableVariables());
    }
    
    // ===== TYPE MISMATCH EXCEPTION TESTS =====
    
    @Test
    @DisplayName("TypeMismatchException should provide detailed type information")
    void testTypeMismatchException() {
        // Arrange
        String expectedType = "number";
        String actualType = "string";
        Object actualValue = "hello";
        String operationContext = "arithmetic operation";
        
        // Act
        TypeMismatchException exception = new TypeMismatchException(
            expectedType, actualType, actualValue, operationContext, null
        );
        
        // Assert
        assertTrue(exception.getMessage().contains("expected number but got string"));
        assertTrue(exception.getMessage().contains("arithmetic operation"));
        assertTrue(exception.getMessage().contains("Current value: hello"));
        
        assertEquals(expectedType, exception.getExpectedType());
        assertEquals(actualType, exception.getActualType());
        assertEquals(actualValue, exception.getActualValue());
        assertEquals(operationContext, exception.getOperationContext());
        assertTrue(exception.hasActualValue());
    }
    
    @Test
    @DisplayName("TypeMismatchException should provide specialized factory methods")
    void testTypeMismatchFactoryMethods() {
        // Test function argument factory
        TypeMismatchException funcException = TypeMismatchException.forFunctionArgument(
            "COUNT", 0, "number", "string", "abc", null
        );
        assertTrue(funcException.getMessage().contains("function 'COUNT' argument 1"));
        
        // Test binary operation factory
        TypeMismatchException binException = TypeMismatchException.forBinaryOperation(
            "+", "string", "number", "hello", 42, null
        );
        assertTrue(binException.getMessage().contains("binary operation '+'"));
        assertTrue(binException.getMessage().contains("string and number"));
        
        // Test assignment factory
        TypeMismatchException assignException = TypeMismatchException.forAssignment(
            "myVar", "boolean", "string", "true", null
        );
        assertTrue(assignException.getMessage().contains("assignment to variable 'myVar'"));
    }
    
    // ===== QUERY SYNTAX EXCEPTION TESTS =====
    
    @Test
    @DisplayName("QuerySyntaxException should categorize syntax errors")
    void testQuerySyntaxException() {
        // Arrange
        String syntaxRule = "SELECT clause is required";
        String queryFragment = "FROM GarbageCollection";
        QuerySyntaxException.SyntaxErrorType errorType = QuerySyntaxException.SyntaxErrorType.MISSING_CLAUSE;
        
        // Act
        QuerySyntaxException exception = new QuerySyntaxException(
            syntaxRule, queryFragment, errorType, null
        );
        
        // Assert
        assertTrue(exception.getMessage().contains("Missing required clause"));
        assertTrue(exception.getMessage().contains("SELECT clause is required"));
        assertTrue(exception.getMessage().contains("FROM GarbageCollection"));
        
        assertEquals(syntaxRule, exception.getSyntaxRule());
        assertEquals(queryFragment, exception.getQueryFragment());
        assertEquals(errorType, exception.getErrorType());
        assertTrue(exception.hasQueryFragment());
    }
    
    @Test
    @DisplayName("QuerySyntaxException should provide specialized factory methods")
    void testQuerySyntaxFactoryMethods() {
        // Test aggregate without group by factory
        QuerySyntaxException aggException = QuerySyntaxException.forAggregateWithoutGroupBy("COUNT", null);
        assertTrue(aggException.getMessage().contains("requires GROUP BY clause"));
        assertEquals(QuerySyntaxException.SyntaxErrorType.AGGREGATE_WITHOUT_GROUP_BY, aggException.getErrorType());
        
        // Test missing clause factory
        QuerySyntaxException missingException = QuerySyntaxException.forMissingClause("WHERE", "SELECT * FROM Users", null);
        assertTrue(missingException.getMessage().contains("Missing required WHERE clause"));
        assertEquals(QuerySyntaxException.SyntaxErrorType.MISSING_CLAUSE, missingException.getErrorType());
        
        // Test invalid operator factory
        QuerySyntaxException opException = QuerySyntaxException.forInvalidOperator("+", "string", "boolean", null);
        assertTrue(opException.getMessage().contains("cannot be applied to string and boolean"));
        assertEquals(QuerySyntaxException.SyntaxErrorType.INVALID_OPERATOR, opException.getErrorType());
    }
    
    // ===== QUERY EVALUATION EXCEPTION TESTS =====
    
    @Test
    @DisplayName("QueryEvaluationException should categorize evaluation errors")
    void testQueryEvaluationException() {
        // Arrange
        String evaluationContext = "division operation";
        Object problematicValue = 0;
        QueryEvaluationException.EvaluationErrorType errorType = QueryEvaluationException.EvaluationErrorType.DIVISION_BY_ZERO;
        
        // Act
        QueryEvaluationException exception = new QueryEvaluationException(
            evaluationContext, problematicValue, errorType, null
        );
        
        // Assert
        assertTrue(exception.getMessage().contains("Division by zero"));
        assertTrue(exception.getMessage().contains("division operation"));
        assertTrue(exception.getMessage().contains("Problematic value: 0"));
        
        assertEquals(evaluationContext, exception.getEvaluationContext());
        assertEquals(problematicValue, exception.getProblematicValue());
        assertEquals(errorType, exception.getErrorType());
        assertTrue(exception.hasProblematicValue());
    }
    
    @Test
    @DisplayName("QueryEvaluationException should provide specialized factory methods")
    void testQueryEvaluationFactoryMethods() {
        // Test division by zero factory
        QueryEvaluationException divException = QueryEvaluationException.forDivisionByZero("x / y", null);
        assertTrue(divException.getMessage().contains("Division by zero"));
        assertEquals(QueryEvaluationException.EvaluationErrorType.DIVISION_BY_ZERO, divException.getErrorType());
        
        // Test null pointer factory
        QueryEvaluationException nullException = QueryEvaluationException.forNullPointer("field access", null);
        assertTrue(nullException.getMessage().contains("Null value access"));
        assertEquals(QueryEvaluationException.EvaluationErrorType.NULL_POINTER, nullException.getErrorType());
        
        // Test invalid aggregation factory
        QueryEvaluationException aggException = QueryEvaluationException.forInvalidAggregation("SUM", "empty dataset", null);
        assertTrue(aggException.getMessage().contains("aggregate function 'SUM'"));
        assertEquals(QueryEvaluationException.EvaluationErrorType.INVALID_AGGREGATION, aggException.getErrorType());
        
        // Test invalid conversion factory
        QueryEvaluationException convException = QueryEvaluationException.forInvalidConversion("string", "number", "abc", null);
        assertTrue(convException.getMessage().contains("type conversion from string to number"));
        assertEquals(QueryEvaluationException.EvaluationErrorType.INVALID_CONVERSION, convException.getErrorType());
        
        // Test out of bounds factory
        QueryEvaluationException boundsException = QueryEvaluationException.forOutOfBounds("array", 5, 3, null);
        assertTrue(boundsException.getMessage().contains("index 5, size 3"));
        assertEquals(QueryEvaluationException.EvaluationErrorType.OUT_OF_BOUNDS, boundsException.getErrorType());
    }
    
    // ===== EXCEPTION HIERARCHY TESTS =====
    
    @Test
    @DisplayName("All specialized exceptions should inherit from QueryExecutionException")
    void testExceptionHierarchy() {
        // Assert that all specialized exceptions are QueryExecutionExceptions
        assertTrue(new ColumnNotFoundException("col", new String[]{}, null) instanceof QueryExecutionException);
        assertTrue(new UnknownFunctionException("func", Set.of(), null) instanceof QueryExecutionException);
        assertTrue(new TableNotFoundException("table", new String[]{}, null) instanceof QueryExecutionException);
        assertTrue(new VariableNotFoundException("var", new String[]{}, null) instanceof QueryExecutionException);
        assertTrue(new TypeMismatchException("expected", "actual", null, null) instanceof QueryExecutionException);
        assertTrue(new QuerySyntaxException("rule", "fragment", null) instanceof QueryExecutionException);
        assertTrue(new QueryEvaluationException("context", null, QueryEvaluationException.EvaluationErrorType.DIVISION_BY_ZERO, null) instanceof QueryExecutionException);
    }
    
    @Test
    @DisplayName("Exception messages should be informative and user-friendly")
    void testExceptionMessageQuality() {
        // Test that exception messages are detailed and helpful
        ColumnNotFoundException colEx = new ColumnNotFoundException("badCol", new String[]{"goodCol"}, null);
        assertTrue(colEx.getMessage().length() > 50); // Detailed message
        assertTrue(colEx.getMessage().contains("Column 'badCol' not found"));
        assertTrue(colEx.getMessage().contains("Available columns"));
        
        UnknownFunctionException funcEx = new UnknownFunctionException("badFunc", Set.of("goodFunc"), null);
        assertTrue(funcEx.getMessage().length() > 50);
        assertTrue(funcEx.getMessage().contains("Unknown function 'badFunc'"));
        assertTrue(funcEx.getMessage().contains("Available functions"));
        
        TypeMismatchException typeEx = new TypeMismatchException("number", "string", "abc", null);
        assertTrue(typeEx.getMessage().length() > 50);
        assertTrue(typeEx.getMessage().contains("expected number but got string"));
    }
}
