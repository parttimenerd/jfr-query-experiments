package me.bechberger.jfr.extended;

import me.bechberger.jfr.extended.engine.framework.QueryTestFramework;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Simple test for double-quoted string support.
 */
class SimpleDoubleQuotedStringTest {
    
    private QueryTestFramework framework;
    
    @BeforeEach
    void setUp() {
        framework = new QueryTestFramework();
    }
    
    @Test
    void testSingleQuotedString() {
        framework.createSingleCellTable("Test", "value", "dummy");
        
        var result = framework.executeQuery("@SELECT 'Hello World' as result FROM Test");
        if (!result.isSuccess()) {
            System.out.println("Single quoted string error: " + result.getError().getMessage());
            result.getError().printStackTrace();
        }
        assertTrue(result.isSuccess(), "Single quoted string should work: " + (result.isSuccess() ? "" : result.getError().getMessage()));
        assertEquals("Hello World", result.getTable().getString(0, "result"));
    }
    
    @Test
    void testDoubleQuotedString() {
        framework.createSingleCellTable("Test", "value", "dummy");
        
        var result = framework.executeQuery("@SELECT \"Hello World\" as result FROM Test");
        assertTrue(result.isSuccess(), "Double quoted string should work: " + (result.isSuccess() ? "" : result.getError().getMessage()));
        assertEquals("Hello World", result.getTable().getString(0, "result"));
    }
    
    @Test
    void testEmptyDoubleQuotedString() {
        framework.createSingleCellTable("Test", "value", "dummy");
        
        var result = framework.executeQuery("@SELECT \"\" as empty FROM Test");
        assertTrue(result.isSuccess(), "Empty double quoted string should work: " + (result.isSuccess() ? "" : result.getError().getMessage()));
        assertEquals("", result.getTable().getString(0, "empty"));
    }
}
