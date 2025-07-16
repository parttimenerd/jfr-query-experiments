package me.bechberger.jfr.extended.evaluator;

import me.bechberger.jfr.extended.engine.framework.QueryTestFramework;
import me.bechberger.jfr.extended.table.CellValue;
import me.bechberger.jfr.extended.table.JfrTable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.jupiter.params.provider.CsvSource;
import static org.junit.jupiter.api.Assertions.*;

import java.util.concurrent.TimeUnit;

/**
 * End-to-end tests for the COLLECT function with expressions using the full
 * query parsing and execution mechanism on complete tables.
 * 
 * This comprehensive test suite follows enhanced testing guidelines and validates:
 * - Full query parsing of COLLECT with expressions using QueryTestFramework
 * - Complete table processing with GROUP BY aggregation
 * - Integration with ORDER BY, WHERE clauses, and complex expressions
 * - Complex nested function expressions and chaining
 * - Real-world JFR event simulation scenarios
 * - Performance testing with larger datasets
 * - Edge cases and error conditions in end-to-end context
 * 
 * @author Enhanced Testing Framework
 * @since 3.0
 */
public class CollectFunctionEndToEndTest {
    
    private QueryTestFramework framework;
    
    @BeforeEach
    void setUp() {
        framework = new QueryTestFramework();
        setupComprehensiveTestTables();
    }
    
    /**
     * Setup comprehensive test tables for end-to-end testing using enhanced framework patterns
     */
    private void setupComprehensiveTestTables() {
        // Users table with comprehensive employee data - using fluent API
        framework.customTable("Users")
            .withStringColumn("name")
            .withNumberColumn("age") 
            .withStringColumn("department")
            .withNumberColumn("salary")
            .withNumberColumn("experience")
            .withRow("Alice", 25L, "Engineering", 75000L, 3L)
            .withRow("Bob", 35L, "Marketing", 65000L, 8L)
            .withRow("Charlie", 28L, "Engineering", 80000L, 5L)
            .withRow("Diana", 22L, "Sales", 45000L, 1L)
            .withRow("Eve", 30L, "Engineering", 85000L, 7L)
            .withRow("Frank", 27L, "Marketing", 60000L, 4L)
            .build();
            
        // Events table simulating JFR event data - realistic event patterns
        framework.customTable("Events")
            .withStringColumn("eventType")
            .withNumberColumn("duration")
            .withStringColumn("thread")
            .withNumberColumn("timestamp")
            .withStringColumn("stackFrame")
            .withRow("GarbageCollection", 150L, "GC-Thread-1", 1000L, "java.lang.System.gc")
            .withRow("AllocationSample", 50L, "Main-Thread", 1100L, "com.example.App.allocate")
            .withRow("GarbageCollection", 200L, "GC-Thread-1", 1200L, "java.lang.System.gc")
            .withRow("ExecutionSample", 75L, "Worker-Thread-1", 1300L, "com.example.Worker.process")
            .withRow("AllocationSample", 30L, "Main-Thread", 1400L, "com.example.App.allocate")
            .withRow("GarbageCollection", 180L, "GC-Thread-1", 1500L, "java.lang.System.gc")
            .withRow("ExecutionSample", 120L, "Worker-Thread-2", 1600L, "com.example.Worker.process")
            .build();
            
        // Performance table with metric data - using createTable with multi-line format
        framework.createTable("Performance", """
            metric | value | unit | timestamp | host
            CPU_Usage | 65 | percent | 2000 | server-1
            Memory_Usage | 4096 | MB | 2000 | server-1
            CPU_Usage | 72 | percent | 2100 | server-1
            Disk_IO | 1024 | KB/s | 2100 | server-1
            Memory_Usage | 4200 | MB | 2200 | server-1
            CPU_Usage | 58 | percent | 2200 | server-1
            Network_IO | 512 | KB/s | 2300 | server-1
            """);
    }
    
    // ===== BASIC COLLECT WITH EXPRESSIONS =====
    
    @Test
    @DisplayName("COLLECT should work with simple arithmetic expressions")
    void testCollectWithSimpleExpressions() {
        // Test COLLECT with simple arithmetic expressions using executeAndExpectTable
        framework.executeAndExpectTable(
            "@SELECT COLLECT(age + 5) as aged_up FROM Users", """
            aged_up
            [30.0, 40.0, 33.0, 27.0, 35.0, 32.0]
            """);
    }

    @Test
    @DisplayName("COLLECT should handle string concatenation expressions")
    void testCollectWithStringExpressions() {
        // Test COLLECT with string concatenation - using traditional approach
        var result = framework.executeAndAssertSuccess(
            "@SELECT COLLECT(name + ' (' + department + ')') as formatted_names FROM Users"
        );
        
        JfrTable table = result.getTable();
        assertEquals(1, table.getRowCount());
        
        // Use traditional CellValue approach
        CellValue collectedValue = table.getCell(0, "formatted_names");
        assertInstanceOf(CellValue.ArrayValue.class, collectedValue);
        
        CellValue.ArrayValue arrayValue = (CellValue.ArrayValue) collectedValue;
        assertEquals(6, arrayValue.elements().size());
        
        // Check some formatted names
        assertTrue(arrayValue.elements().contains(new CellValue.StringValue("Alice (Engineering)")));
        assertTrue(arrayValue.elements().contains(new CellValue.StringValue("Bob (Marketing)")));
        assertTrue(arrayValue.elements().contains(new CellValue.StringValue("Diana (Sales)")));
    }
    
    @Test
    @DisplayName("COLLECT should handle conditional expressions using CASE statements")
    void testCollectWithConditionalExpressions() {
        // Test COLLECT with conditional expressions using CASE
        var result = framework.executeAndAssertSuccess(
            "@SELECT COLLECT(CASE WHEN age >= 30 THEN 'Senior' ELSE 'Junior' END) as seniority FROM Users"
        );
        
        JfrTable table = result.getTable();
        CellValue collectedValue = table.getCell(0, "seniority");
        CellValue.ArrayValue arrayValue = (CellValue.ArrayValue) collectedValue;
        
        assertEquals(6, arrayValue.elements().size());
        
        // Should have both Senior and Junior values
        assertTrue(arrayValue.elements().contains(new CellValue.StringValue("Senior")));
        assertTrue(arrayValue.elements().contains(new CellValue.StringValue("Junior")));
        
        // Count seniors vs juniors
        long seniorCount = arrayValue.elements().stream()
            .filter(v -> v.equals(new CellValue.StringValue("Senior")))
            .count();
        
        // Bob (35), Eve (30) should be Senior, others Junior
        assertEquals(2, seniorCount, "Should have 2 senior employees");
    }
    
    // ===== ENHANCED TESTING PATTERNS =====
    
    @Test
    @DisplayName("COLLECT should work with executeAndExpectTable pattern for GROUP BY")
    void testCollectWithGroupByUsingExpectPattern() {
        // Using the enhanced executeAndExpectTable pattern for GROUP BY testing
        framework.executeAndExpectTable("""
            @SELECT 
                department,
                COLLECT(name) as team_members
            FROM Users 
            GROUP BY department 
            ORDER BY department
            """, """
            department | team_members
            Engineering | ["Alice", "Charlie", "Eve"]
            Marketing | ["Bob", "Frank"]
            Sales | ["Diana"]
            """);
    }
    
    @ParameterizedTest
    @ValueSource(strings = {
        "@SELECT COLLECT(age) FROM Users", 
        "@SELECT COLLECT(name) FROM Users",
        "@SELECT COLLECT(salary) FROM Users"
    })
    @DisplayName("COLLECT should work with different field types")
    void testCollectWithDifferentFieldTypes(String query) {
        // Arrange & Act
        var result = framework.executeAndAssertSuccess(query);
        
        // Assert
        JfrTable table = result.getTable();
        assertEquals(1, table.getRowCount());
        
        CellValue collectedValue = table.getCell(0, 0);
        assertInstanceOf(CellValue.ArrayValue.class, collectedValue);
        
        CellValue.ArrayValue arrayValue = (CellValue.ArrayValue) collectedValue;
        assertEquals(6, arrayValue.elements().size(), "Should collect all 6 user records");
    }
    
    @ParameterizedTest
    @CsvSource({
        "@SELECT COLLECT(age * 2) FROM Users, 6",
        "@SELECT COLLECT(salary / 1000) FROM Users, 6", 
        "@SELECT COLLECT(experience + 1) FROM Users, 6"
    })
    @DisplayName("COLLECT should handle arithmetic expressions correctly")
    void testCollectWithArithmeticExpressions(String query, int expectedSize) {
        // Arrange & Act
        var result = framework.executeAndAssertSuccess(query);
        
        // Assert
        JfrTable table = result.getTable();
        CellValue collectedValue = table.getCell(0, 0);
        assertInstanceOf(CellValue.ArrayValue.class, collectedValue);
        
        CellValue.ArrayValue arrayValue = (CellValue.ArrayValue) collectedValue;
        assertEquals(expectedSize, arrayValue.elements().size());
    }
    
    @Test
    @DisplayName("COLLECT should handle complex GROUP BY scenarios with realistic JFR data")
    void testCollectWithJfrEventGrouping() {
        // Test realistic JFR event grouping scenario
        framework.executeAndExpectTable("""
            @SELECT 
                eventType,
                COLLECT(duration) as durations,
                COUNT(*) as event_count
            FROM Events 
            GROUP BY eventType 
            ORDER BY event_count DESC
            """, """
            eventType | durations | event_count
            GarbageCollection | [150, 200, 180] | 3
            AllocationSample | [50, 30] | 2
            ExecutionSample | [75, 120] | 2
            """);
    }
    
    @Test
    @DisplayName("COLLECT should work with WHERE clause filtering")
    void testCollectWithWhereClause() {
        // Test COLLECT with WHERE clause filtering using executeAndExpectTable
        framework.executeAndExpectTable("""
            @SELECT COLLECT(name) as senior_engineers 
            FROM Users 
            WHERE department = 'Engineering' AND age >= 28
            """, """
            senior_engineers
            ["Charlie", "Eve"]
            """);
    }
    
    @Test
    @DisplayName("COLLECT should integrate with array functions in complex expressions")
    void testCollectWithArrayFunctionIntegration() {
        // Test COLLECT integration with array functions using createTableAndExpect
        framework.createTableAndExpect("SimpleData", """
            value
            10
            20
            30
            40
            50
            """, 
            "@SELECT HEAD(COLLECT(value), 3) as first_three FROM SimpleData", """
            first_three
            [10, 20, 30]
            """);
    }
    
    @ParameterizedTest
    @ValueSource(strings = {"COLLECT", "collect", "Collect", "CoLlEcT"})
    @DisplayName("COLLECT function should be case-insensitive in queries")
    void testCollectCaseInsensitivityInQueries(String functionName) {
        // Test that COLLECT is case-insensitive in full queries
        String query = "@SELECT " + functionName + "(name) as names FROM Users";
        var result = framework.executeAndAssertSuccess(query);
        
        JfrTable table = result.getTable();
        CellValue names = table.getCell(0, "names");
        assertInstanceOf(CellValue.ArrayValue.class, names);
        
        CellValue.ArrayValue namesArray = (CellValue.ArrayValue) names;
        assertEquals(6, namesArray.elements().size());
    }
    
    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    @DisplayName("COLLECT should perform adequately with moderately large datasets")
    void testCollectPerformanceWithMediumDataset() {
        // Create a medium-sized test dataset for performance testing
        var builder = framework.customTable("LargeEvents")
            .withStringColumn("name")
            .withNumberColumn("value")
            .withStringColumn("category");
            
        // Add 1000 rows of test data
        for (int i = 0; i < 1000; i++) {
            builder.withRow("Event" + i, (long) i, "Category" + (i % 10));
        }
        builder.build();
        
        // Test COLLECT performance with moderate dataset
        var result = framework.executeAndAssertSuccess("@SELECT COLLECT(value) FROM LargeEvents");
        
        JfrTable table = result.getTable();
        CellValue collectedValue = table.getCell(0, 0);
        assertInstanceOf(CellValue.ArrayValue.class, collectedValue);
        
        CellValue.ArrayValue arrayValue = (CellValue.ArrayValue) collectedValue;
        assertEquals(1000, arrayValue.elements().size(), "Should collect all 1000 values");
    }
    
    // ===== ERROR HANDLING TESTS =====
    
    @Test
    @DisplayName("COLLECT should handle non-existent fields gracefully")
    void testCollectWithInvalidField() {
        // Test COLLECT with non-existent field
        var result = framework.executeQuery(
            "@SELECT COLLECT(nonexistent_field) as invalid FROM Users"
        );
        
        // Should either succeed with empty array or fail gracefully
        if (result.isSuccess()) {
            JfrTable table = result.getTable();
            CellValue invalid = table.getCell(0, "invalid");
            assertInstanceOf(CellValue.ArrayValue.class, invalid);
            CellValue.ArrayValue invalidArray = (CellValue.ArrayValue) invalid;
            assertEquals(0, invalidArray.elements().size(), "Unknown field should return empty array");
        } else {
            // Or it should fail with a meaningful error message
            assertNotNull(result.getError());
            String errorMsg = result.getError().getMessage();
            assertTrue(errorMsg.contains("nonexistent_field") ||
                      errorMsg.contains("Unknown") ||
                      errorMsg.contains("not found"));
        }
    }
    
    @Test
    @DisplayName("COLLECT should fail gracefully with syntax errors")
    void testCollectWithSyntaxError() {
        // Test COLLECT with invalid syntax
        var result = framework.executeQuery(
            "@SELECT COLLECT( FROM Users"
        );
        
        assertFalse(result.isSuccess());
        assertNotNull(result.getError());
    }
    
    @Test 
    @DisplayName("COLLECT should handle multiple expressions in same query")
    void testCollectWithMultipleExpressions() {
        // Test multiple COLLECT expressions in same query
        var result = framework.executeAndAssertSuccess(
            "@SELECT " +
            "COLLECT(name) as all_names, " +
            "COLLECT(age * 12) as age_in_months, " +
            "COLLECT(UPPER(department)) as departments_upper " +
            "FROM Users"
        );
        
        JfrTable table = result.getTable();
        assertEquals(1, table.getRowCount());
        
        // Check all three collections using traditional approach
        CellValue allNames = table.getCell(0, "all_names");
        CellValue ageInMonths = table.getCell(0, "age_in_months");
        CellValue deptsUpper = table.getCell(0, "departments_upper");
        
        CellValue.ArrayValue namesArray = (CellValue.ArrayValue) allNames;
        CellValue.ArrayValue monthsArray = (CellValue.ArrayValue) ageInMonths;
        CellValue.ArrayValue deptsArray = (CellValue.ArrayValue) deptsUpper;
        
        assertEquals(6, namesArray.elements().size());
        assertEquals(6, monthsArray.elements().size());
        assertEquals(6, deptsArray.elements().size());
        
        // Check some calculated values
        assertTrue(monthsArray.elements().contains(new CellValue.NumberValue(300L))); // Alice: 25*12
        assertTrue(monthsArray.elements().contains(new CellValue.NumberValue(420L))); // Bob: 35*12
        
        // Check uppercase departments
        assertTrue(deptsArray.elements().contains(new CellValue.StringValue("ENGINEERING")));
        assertTrue(deptsArray.elements().contains(new CellValue.StringValue("MARKETING")));
        assertTrue(deptsArray.elements().contains(new CellValue.StringValue("SALES")));
    }
}
