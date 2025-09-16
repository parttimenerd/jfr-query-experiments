package me.bechberger.jfr.extended;

import me.bechberger.jfr.extended.engine.framework.QueryTestFramework;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class StringLiteralDebugTest {
    
    private QueryTestFramework framework;
    
    @BeforeEach
    void setUp() {
        framework = new QueryTestFramework();
    }
    
    @Test
    void debugSingleQuotes() {
        framework.createSingleCellTable("Test", "value", "dummy");
        
        var result = framework.executeAndAssertSuccess(
            "@SELECT 'Hello World' as result FROM Test"
        );
        
        String actualValue = result.getTable().getString(0, "result");
        System.out.println("Single quote result: [" + actualValue + "]");
        assertEquals("Hello World", actualValue);
    }
    
    @Test 
    void debugDoubleQuotes() {
        framework.createSingleCellTable("Test", "value", "dummy");
        
        var result = framework.executeAndAssertSuccess(
            "@SELECT \"Hello World\" as result FROM Test"
        );
        
        String actualValue = result.getTable().getString(0, "result");
        System.out.println("Double quote result: [" + actualValue + "]");
        assertEquals("Hello World", actualValue);
    }
    
    @Test 
    void debugEscapes() {
        framework.createSingleCellTable("Test", "value", "dummy");
        
        var result = framework.executeAndAssertSuccess(
            "@SELECT \"line1\\nline2\\ttab\" as result FROM Test"
        );
        
        String actualValue = result.getTable().getString(0, "result");
        System.out.println("Escape result: [" + actualValue + "]");
        assertEquals("line1\nline2\ttab", actualValue);
    }
}
