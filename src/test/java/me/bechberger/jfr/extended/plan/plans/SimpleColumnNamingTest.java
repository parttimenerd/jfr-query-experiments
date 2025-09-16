package me.bechberger.jfr.extended.plan.plans;

import me.bechberger.jfr.extended.engine.framework.QueryTestFramework;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Simple test to verify column naming works for expressions like "duration + 2"
 */
public class SimpleColumnNamingTest {
    
    private QueryTestFramework framework;
    
    @BeforeEach
    void setUp() {
        framework = new QueryTestFramework();
        
        // Simple test data
        framework.mockTable("Events", """
            name | duration
            EventA | 100
            EventB | 200
            """);
    }
    
    @Test
    void testDurationPlusTwo() {
        var result = framework.executeQuery("@SELECT duration + 2 FROM Events");
        assertTrue(result.isSuccess());
        
        var table = result.getTable();
        var columns = table.getColumns();
        
        // Check column name
        assertEquals(1, columns.size());
        assertEquals("$0", columns.get(0).name());
        
        // Check values
        var rows = table.getRows();
        assertEquals(2, rows.size());
        assertEquals(102.0, ((Number) rows.get(0).getCells().get(0).getValue()).doubleValue());
        assertEquals(202.0, ((Number) rows.get(1).getCells().get(0).getValue()).doubleValue());
    }
    
    @Test
    void testAggregateFunction() {
        var result = framework.executeQuery("@SELECT COUNT(*) FROM Events");
        assertTrue(result.isSuccess());
        
        var table = result.getTable();
        var columns = table.getColumns();
        
        // Check column name - should be COUNT(*) not $0
        assertEquals(1, columns.size());
        assertEquals("COUNT(*)", columns.get(0).name());
        
        // Check value
        var rows = table.getRows();
        assertEquals(1, rows.size());
        assertEquals(2L, ((Number) rows.get(0).getCells().get(0).getValue()).longValue());
    }
    
    @Test
    void testMultipleExpressions() {
        var result = framework.executeQuery("@SELECT duration + 2, duration * 2, name FROM Events");
        assertTrue(result.isSuccess());
        
        var table = result.getTable();
        var columns = table.getColumns();
        
        // Check column names
        assertEquals(3, columns.size());
        assertEquals("$0", columns.get(0).name());      // duration + 2
        assertEquals("$1", columns.get(1).name());      // duration * 2
        assertEquals("name", columns.get(2).name());    // field name
    }
    
    @Test
    void testWithAlias() {
        var result = framework.executeQuery("@SELECT duration + 2 as increased, duration * 2 as doubled FROM Events");
        assertTrue(result.isSuccess());
        
        var table = result.getTable();
        var columns = table.getColumns();
        
        // Check column names use aliases
        assertEquals(2, columns.size());
        assertEquals("increased", columns.get(0).name());
        assertEquals("doubled", columns.get(1).name());
    }
}
