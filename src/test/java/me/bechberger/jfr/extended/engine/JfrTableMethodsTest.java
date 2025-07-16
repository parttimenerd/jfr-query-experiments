package me.bechberger.jfr.extended.engine;

import me.bechberger.jfr.extended.engine.framework.QueryTestFramework;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests to demonstrate that the new JfrTable methods getColumnName() and getColumnType() work correctly.
 */
class JfrTableMethodsTest {
    
    private QueryTestFramework framework;
    
    @BeforeEach
    void setUp() {
        framework = new QueryTestFramework();
    }
    
    @Test
    @DisplayName("JfrTable.getColumnName() should work correctly")
    void testGetColumnName() {
        // Create a test table
        framework.mockTable("TestTable")
            .withStringColumn("name")
            .withNumberColumn("count")
            .withRow("Alice", 10L)
            .withRow("Bob", 20L)
            .build();
        
        var result = framework.executeQuery("@SELECT * FROM TestTable");
        assertTrue(result.isSuccess());
        
        var table = result.getTable();
        assertEquals(2, table.getColumnCount());
        
        // Test getColumnName method
        assertEquals("name", table.getColumnName(0));
        assertEquals("count", table.getColumnName(1));
    }
    
    @Test
    @DisplayName("JfrTable.getColumnType() should work correctly")
    void testGetColumnType() {
        // Create a test table with different column types
        framework.mockTable("TypedTable")
            .withStringColumn("text")
            .withNumberColumn("number")
            .withBooleanColumn("flag")
            .withRow("hello", 42L, true)
            .build();
        
        var result = framework.executeQuery("@SELECT * FROM TypedTable");
        assertTrue(result.isSuccess());
        
        var table = result.getTable();
        assertEquals(3, table.getColumnCount());
        
        // Test getColumnType method
        assertNotNull(table.getColumnType(0));
        assertNotNull(table.getColumnType(1)); 
        assertNotNull(table.getColumnType(2));
    }
    
    @Test
    @DisplayName("JfrTable.getColumnValues(int) should work correctly")
    void testGetColumnValuesByIndex() {
        // Create a test table
        framework.mockTable("ValueTable")
            .withStringColumn("items")
            .withRow("apple")
            .withRow("banana")
            .withRow("cherry")
            .build();
        
        var result = framework.executeQuery("@SELECT * FROM ValueTable");
        assertTrue(result.isSuccess());
        
        var table = result.getTable();
        
        // Test getColumnValues by index
        var values = table.getColumnValues(0);
        assertEquals(3, values.size());
        assertEquals("apple", values.get(0).toString());
        assertEquals("banana", values.get(1).toString());
        assertEquals("cherry", values.get(2).toString());
    }
    
    @Test
    @DisplayName("SHOW commands should use new JfrTable methods correctly")
    void testShowCommandsUseNewMethods() {
        // Test SHOW EVENTS
        var eventsResult = framework.executeQuery("SHOW EVENTS");
        assertTrue(eventsResult.isSuccess());
        
        var eventsTable = eventsResult.getTable();
        assertEquals(1, eventsTable.getColumnCount());
        assertEquals("Events", eventsTable.getColumnName(0));
        
        // Test HELP  
        var helpResult = framework.executeQuery("HELP");
        assertTrue(helpResult.isSuccess());
        
        var helpTable = helpResult.getTable();
        assertEquals(1, helpTable.getColumnCount());
        assertEquals("Help", helpTable.getColumnName(0));
        
        // Test HELP FUNCTION
        var funcHelpResult = framework.executeQuery("HELP FUNCTION COUNT");
        assertTrue(funcHelpResult.isSuccess());
        
        var funcHelpTable = funcHelpResult.getTable();
        assertEquals(1, funcHelpTable.getColumnCount());
        assertEquals("Function Help", funcHelpTable.getColumnName(0));
        
        // Test HELP GRAMMAR
        var grammarResult = framework.executeQuery("HELP GRAMMAR");
        assertTrue(grammarResult.isSuccess());
        
        var grammarTable = grammarResult.getTable();
        assertEquals(1, grammarTable.getColumnCount());
        assertEquals("Grammar", grammarTable.getColumnName(0));
    }
}
