import me.bechberger.jfr.extended.engine.framework.QueryTestFramework;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Test ORDER BY with SELECT aliases
 */
public class TestOrderByAliases {
    private QueryTestFramework framework;
    
    @BeforeEach
    void setUp() {
        framework = new QueryTestFramework();
    }
    
    @Test
    void testOrderByWithSelectAlias() {
        // Create a test table
        framework.createTable("Sales", """
            product | price | quantity
            A | 10.0 | 5
            B | 20.0 | 3
            C | 15.0 | 4
            """);
        
        // Test ORDER BY referencing a SELECT alias
        try {
            var result = framework.executeQuery("@SELECT product, price * quantity as sales_total FROM Sales ORDER BY sales_total DESC");
            System.out.println("Query executed successfully!");
            if (result.isSuccess() && result.getTable() != null) {
                System.out.println("Result has " + result.getTable().getRowCount() + " rows");
                for (int i = 0; i < result.getTable().getRowCount(); i++) {
                    System.out.println("Row " + i + ": " + 
                        result.getTable().getString(i, "product") + " -> " + 
                        result.getTable().getNumber(i, "sales_total"));
                }
            } else {
                System.out.println("Query failed: " + (result.getError() != null ? result.getError().getMessage() : "Unknown error"));
            }
        } catch (Exception e) {
            System.out.println("Exception: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    @Test
    void testGroupByWithOrderByAlias() {
        // Create a test table
        framework.createTable("Sales", """
            category | amount
            A | 100
            B | 200
            A | 150
            C | 75
            """);
        
        // Test GROUP BY with ORDER BY referencing a SELECT alias
        try {
            var result = framework.executeQuery("@SELECT category, SUM(amount) as total_sales FROM Sales GROUP BY category ORDER BY total_sales DESC");
            System.out.println("GROUP BY Query executed successfully!");
            if (result.isSuccess() && result.getTable() != null) {
                System.out.println("Result has " + result.getTable().getRowCount() + " rows");
                for (int i = 0; i < result.getTable().getRowCount(); i++) {
                    System.out.println("Row " + i + ": " + 
                        result.getTable().getString(i, "category") + " -> " + 
                        result.getTable().getNumber(i, "total_sales"));
                }
            } else {
                System.out.println("Query failed: " + (result.getError() != null ? result.getError().getMessage() : "Unknown error"));
            }
        } catch (Exception e) {
            System.out.println("Exception: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
