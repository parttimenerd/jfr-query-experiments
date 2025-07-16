import me.bechberger.jfr.extended.engine.framework.QueryTestFramework;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import static org.junit.jupiter.api.Assertions.*;

public class TestCollectAllContexts {
    
    private QueryTestFramework framework;
    
    @BeforeEach
    void setUp() {
        framework = new QueryTestFramework();
        
        // Create test data for all contexts
        framework.mockTable("Events")
            .withStringColumn("type")
            .withStringColumn("value") 
            .withNumberColumn("score")
            .withRow("A", "v1", 10L)
            .withRow("A", "v2", 20L)
            .withRow("B", "v3", 30L)
            .withRow("B", "v4", 40L)
            .build();
    }
    
    @Test
    @DisplayName("COLLECT in simple SELECT context")
    void testCollectSimpleSelect() {
        framework.executeAndExpectTable("@SELECT COLLECT(value) FROM Events", """
            COLLECT(value)
            ["v1","v2","v3","v4"]
            """);
    }
    
    @Test 
    @DisplayName("COLLECT with WHERE clause")
    void testCollectWithWhere() {
        framework.executeAndExpectTable("@SELECT COLLECT(value) FROM Events WHERE type = 'A'", """
            COLLECT(value)
            ["v1","v2"]
            """);
    }
    
    @Test
    @DisplayName("COLLECT in GROUP BY context")
    void testCollectGroupBy() {
        framework.executeAndExpectTable("@SELECT type, COLLECT(value) FROM Events GROUP BY type ORDER BY type", """
            type | COLLECT(value)
            A | ["v1","v2"]
            B | ["v3","v4"]
            """);
    }
    
    @Test
    @DisplayName("COLLECT with nested functions - HEAD")
    void testCollectWithHead() {
        framework.executeAndExpectTable("@SELECT HEAD(COLLECT(value)) FROM Events", """
            HEAD(COLLECT(value))
            v1
            """);
    }
    
    @Test
    @DisplayName("COLLECT with nested functions - TAIL")
    void testCollectWithTail() {
        framework.executeAndExpectTable("@SELECT TAIL(COLLECT(value)) FROM Events", """
            TAIL(COLLECT(value))
            v4
            """);
    }
    
    @Test
    @DisplayName("COLLECT with nested functions in GROUP BY")
    void testCollectNestedInGroupBy() {
        framework.executeAndExpectTable("@SELECT type, HEAD(COLLECT(value)) FROM Events GROUP BY type ORDER BY type", """
            type | HEAD(COLLECT(value))
            A | v1
            B | v3
            """);
    }
    
    @Test
    @DisplayName("COLLECT of numeric values")
    void testCollectNumericValues() {
        framework.executeAndExpectTable("@SELECT COLLECT(score) FROM Events WHERE type = 'A'", """
            COLLECT(score)
            [10,20]
            """);
    }
    
    @Test
    @DisplayName("COLLECT with complex WHERE and GROUP BY")
    void testCollectComplexQuery() {
        framework.executeAndExpectTable("""
            @SELECT type, COLLECT(value) 
            FROM Events 
            WHERE score > 15 
            GROUP BY type 
            ORDER BY type
            """, """
            type | COLLECT(value)
            A | ["v2"]
            B | ["v3","v4"]
            """);
    }
}
