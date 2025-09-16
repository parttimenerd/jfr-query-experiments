import me.bechberger.jfr.extended.engine.framework.QueryTestFramework;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Test the GROUP BY fixes for CellValue equality and group key uniqueness
 */
public class TestGroupByFix {
    private QueryTestFramework framework;
    
    @BeforeEach
    void setUp() {
        framework = new QueryTestFramework();
    }
    
    @Test
    void testGroupByWithComplexKeys() {
        // Create a table with various data types
        framework.createTable("TestData", """
            name | count | rate_per_min | timestamp
            Alice | 10 | 60.0 | 2024-01-01T10:00:00Z
            Bob | 20 | 120.0 | 2024-01-01T10:01:00Z
            Alice | 10 | 60.0 | 2024-01-01T10:02:00Z
            Charlie | 15 | 90.0 | 2024-01-01T10:03:00Z
            """);
        
        // Test GROUP BY with string keys (should work)
        framework.executeAndExpectTable("@SELECT name, SUM(count) as total FROM TestData GROUP BY name", """
            name | total
            Alice | 20
            Bob | 20
            Charlie | 15
            """);
    }
    
    @Test
    void testRateValueEquality() {
        // Create a table with rate values that should be equal when normalized
        framework.createTable("RateData", """
            service | requests_per_min | requests_per_sec
            API | 60.0 | 1.0
            Web | 120.0 | 2.0
            """);
        
        // Test basic functionality first
        var result = framework.executeQuery("@SELECT COUNT(*) FROM RateData");
        System.out.println("Rate data count: " + result.getTable().getNumber(0, 0));
        
        // Test simple GROUP BY
        framework.executeAndExpectTable("@SELECT service, COUNT(*) as count FROM RateData GROUP BY service", """
            service | count
            API | 1
            Web | 1
            """);
    }
    
    @Test
    void testNumericGroupBy() {
        // Test GROUP BY with numeric values
        framework.createTable("NumericData", """
            category | value | score
            A | 100 | 95.5
            B | 200 | 87.2
            A | 150 | 92.1
            C | 300 | 78.9
            """);
        
        framework.executeAndExpectTable("@SELECT category, COUNT(*) as count, AVG(value) as avg_value FROM NumericData GROUP BY category", """
            category | count | avg_value
            A | 2 | 125.0
            B | 1 | 200.0
            C | 1 | 300.0
            """);
    }
}
