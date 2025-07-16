package me.bechberger.jfr.extended.engine;

import me.bechberger.jfr.extended.engine.framework.QueryTestFramework;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Timeout;
import static org.junit.jupiter.api.Assertions.*;

import java.util.concurrent.TimeUnit;

/**
 * Comprehensive tests for the SHOW commands and HELP commands in QueryEvaluator.
 * These tests verify that SHOW EVENTS, SHOW FIELDS, HELP, HELP FUNCTION, and HELP GRAMMAR
 * commands work correctly and return meaningful content.
 */
class ShowCommandsTest {
    
    private QueryTestFramework framework;
    
    @BeforeEach
    void setUp() {
        framework = new QueryTestFramework();
    }
    
    // ===== SHOW EVENTS TESTS =====
    
    @Test
    @DisplayName("SHOW EVENTS should list available event types")
    void testShowEventsCommand() {
        var result = framework.executeQuery("SHOW EVENTS");
        
        assertTrue(result.isSuccess(), "SHOW EVENTS command should succeed");
        assertNotNull(result.getTable(), "Result should contain a table");
        assertTrue(result.getTable().getRowCount() >= 0, "Should have zero or more event types");
        
        // Check that we have the expected column
        assertEquals(1, result.getTable().getColumnCount(), "Should have exactly one column");
        assertEquals("Events", result.getTable().getColumnName(0), "Column should be named 'Events'");
        
        // If we have events, verify they are strings
        if (result.getTable().getRowCount() > 0) {
            var eventTypes = result.getTable().getColumnValues(0);
            assertNotNull(eventTypes.get(0).toString(), "First event type should not be null");
        }
    }
    
    @Test
    @DisplayName("SHOW EVENTS should return sorted results")
    void testShowEventsIsSorted() {
        var result = framework.executeQuery("SHOW EVENTS");
        
        assertTrue(result.isSuccess());
        var eventTypes = result.getTable().getColumnValues(0);
        
        // Verify the list is sorted
        for (int i = 1; i < eventTypes.size(); i++) {
            String current = eventTypes.get(i).toString();
            String previous = eventTypes.get(i - 1).toString();
            assertTrue(current.compareTo(previous) >= 0, 
                "Event types should be sorted alphabetically");
        }
    }
    
    // ===== SHOW FIELDS TESTS =====
    
    @Test
    @DisplayName("SHOW FIELDS should show field information for existing event type")
    void testShowFieldsWithValidEventType() {
        // First get available event types
        var eventsResult = framework.executeQuery("SHOW EVENTS");
        assertTrue(eventsResult.isSuccess());
        
        // If no events are available in test environment, skip the detailed check
        if (eventsResult.getTable().getRowCount() == 0) {
            // Test with a mock event type name - this should return an error message
            var fieldsResult = framework.executeQuery("SHOW FIELDS TestEvent");
            assertTrue(fieldsResult.isSuccess(), "SHOW FIELDS should handle unknown event gracefully");
            return;
        }
        
        // Use the first available event type
        String firstEventType = eventsResult.getTable().getString(0, 0);
        
        // Test SHOW FIELDS with this event type
        var fieldsResult = framework.executeQuery("SHOW FIELDS " + firstEventType);
        
        assertTrue(fieldsResult.isSuccess(), "SHOW FIELDS should succeed for valid event type");
        assertNotNull(fieldsResult.getTable(), "Result should contain a table");
    }
    
    @Test
    @DisplayName("SHOW FIELDS should handle non-existent event type gracefully")
    void testShowFieldsWithInvalidEventType() {
        var result = framework.executeQuery("SHOW FIELDS NonExistentEventType");
        
        // The implementation returns an error message, so it succeeds with 1 row containing the error
        assertTrue(result.isSuccess(), "SHOW FIELDS should succeed even for non-existent event type");
        assertEquals(1, result.getTable().getRowCount(), "Should have one row with error message");
        
        String errorMessage = result.getTable().getString(0, 0);
        assertTrue(errorMessage.contains("NonExistentEventType") || 
                  errorMessage.contains("not found") ||
                  errorMessage.contains("Unknown"),
                "Should indicate that event type is unknown: " + errorMessage);
    }
    
    // ===== HELP COMMAND TESTS =====
    
    @Test
    @DisplayName("HELP command should provide general help content")
    void testGeneralHelpCommand() {
        var result = framework.executeQuery("HELP");
        
        assertTrue(result.isSuccess(), "HELP command should succeed");
        assertNotNull(result.getTable(), "Result should contain a table");
        assertEquals(1, result.getTable().getRowCount(), "Should have exactly one row");
        assertEquals(1, result.getTable().getColumnCount(), "Should have exactly one column");
        assertEquals("Help", result.getTable().getColumnName(0), "Column should be named 'Help'");
        
        String helpContent = result.getTable().getString(0, 0);
        assertNotNull(helpContent, "Help content should not be null");
        assertFalse(helpContent.trim().isEmpty(), "Help content should not be empty");
        
        // Check for expected content sections
        assertTrue(helpContent.contains("JFR Query Language"), "Should contain title");
        assertTrue(helpContent.contains("BASIC USAGE"), "Should contain usage section");
        assertTrue(helpContent.contains("SELECT"), "Should mention SELECT syntax");
        assertTrue(helpContent.contains("SHOW EVENTS"), "Should mention SHOW EVENTS");
        assertTrue(helpContent.contains("HELP FUNCTION"), "Should mention HELP FUNCTION");
    }
    
    // ===== HELP FUNCTION TESTS =====
    
    @Test
    @DisplayName("HELP FUNCTION should provide function-specific help")
    void testHelpFunctionCommand() {
        var result = framework.executeQuery("HELP FUNCTION COUNT");
        
        assertTrue(result.isSuccess(), "HELP FUNCTION COUNT should succeed");
        assertNotNull(result.getTable(), "Result should contain a table");
        assertEquals(1, result.getTable().getRowCount(), "Should have exactly one row");
        assertEquals(1, result.getTable().getColumnCount(), "Should have exactly one column");
        assertEquals("Function Help", result.getTable().getColumnName(0), "Column should be named 'Function Help'");
        
        String functionHelp = result.getTable().getString(0, 0);
        assertNotNull(functionHelp, "Function help should not be null");
        assertFalse(functionHelp.trim().isEmpty(), "Function help should not be empty");
        
        // Should contain information about the COUNT function
        assertTrue(functionHelp.toLowerCase().contains("count"), "Should contain COUNT function info");
    }
    
    @Test
    @DisplayName("HELP FUNCTION with non-existent function should handle gracefully")
    void testHelpFunctionWithInvalidFunction() {
        var result = framework.executeQuery("HELP FUNCTION NONEXISTENT");
        
        assertTrue(result.isSuccess(), "HELP FUNCTION should succeed even for non-existent functions");
        String functionHelp = result.getTable().getString(0, 0);
        
        // Should provide helpful message about unknown function
        assertTrue(functionHelp.contains("NONEXISTENT") || 
                  functionHelp.contains("not found") ||
                  functionHelp.contains("unknown"),
                "Should indicate that function is unknown");
    }
    
    // ===== HELP GRAMMAR TESTS =====
    
    @Test
    @DisplayName("HELP GRAMMAR should provide grammar documentation")
    void testHelpGrammarCommand() {
        var result = framework.executeQuery("HELP GRAMMAR");
        
        assertTrue(result.isSuccess(), "HELP GRAMMAR should succeed");
        assertNotNull(result.getTable(), "Result should contain a table");
        assertEquals(1, result.getTable().getRowCount(), "Should have exactly one row");
        assertEquals(1, result.getTable().getColumnCount(), "Should have exactly one column");
        assertEquals("Grammar", result.getTable().getColumnName(0), "Column should be named 'Grammar'");
        
        String grammarContent = result.getTable().getString(0, 0);
        assertNotNull(grammarContent, "Grammar content should not be null");
        assertFalse(grammarContent.trim().isEmpty(), "Grammar content should not be empty");
        
        // Should contain grammar-related information
        assertTrue(grammarContent.contains("Grammar") || grammarContent.contains("Syntax"), 
                  "Should contain grammar or syntax information");
    }
    
    // ===== PERFORMANCE TESTS =====
    
    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    @DisplayName("All visitor methods should complete within reasonable time")
    void testVisitorMethodsPerformance() {
        // Test that all commands complete quickly
        assertTrue(framework.executeQuery("SHOW EVENTS").isSuccess());
        assertTrue(framework.executeQuery("HELP").isSuccess());
        assertTrue(framework.executeQuery("HELP FUNCTION COUNT").isSuccess());
        assertTrue(framework.executeQuery("HELP GRAMMAR").isSuccess());
    }
    
    // ===== INTEGRATION TESTS =====
    
    @Test
    @DisplayName("Visitor methods should work in combination with regular queries")
    void testVisitorMethodsWithRegularQueries() {
        // Create a simple table for testing
        framework.mockTable("TestEvents")
            .withStringColumn("eventName")
            .withNumberColumn("count")
            .withRow("Event1", 10L)
            .withRow("Event2", 20L)
            .build();
        
        // Test that we can still execute regular queries after using visitor methods
        assertTrue(framework.executeQuery("HELP").isSuccess());
        
        var queryResult = framework.executeQuery("@SELECT COUNT(*) FROM TestEvents");
        assertTrue(queryResult.isSuccess());
        assertEquals(2L, queryResult.getTable().getNumber(0, 0));
        
        // Test SHOW EVENTS still works after regular query
        assertTrue(framework.executeQuery("SHOW EVENTS").isSuccess());
    }
    
    // ===== ERROR HANDLING TESTS =====
    
    @Test
    @DisplayName("Visitor methods should handle malformed syntax gracefully")
    void testVisitorMethodsErrorHandling() {
        // These should fail gracefully with proper error messages
        var result1 = framework.executeQuery("SHOW");  // Incomplete command
        assertFalse(result1.isSuccess());
        
        var result2 = framework.executeQuery("HELP FUNCTION");  // Missing function name
        assertFalse(result2.isSuccess());
    }
}
