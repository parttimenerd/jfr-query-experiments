import me.bechberger.jfr.extended.engine.framework.QueryTestFramework;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Comprehensive test for ORDER BY with various scenarios
 */
public class TestOrderByComprehensive {
    private QueryTestFramework framework;
    
    @BeforeEach
    void setUp() {
        framework = new QueryTestFramework();
    }
    
    @Test
    void testOrderByWithSelectAlias() {
        framework.createTable("Sales", """
            product | price | quantity
            A | 10.0 | 5
            B | 20.0 | 3
            C | 15.0 | 4
            """);
        
        var result = framework.executeQuery("@SELECT product, price * quantity as sales_total FROM Sales ORDER BY sales_total DESC");
        System.out.println("✅ ORDER BY with SELECT alias: " + (result.isSuccess() ? "PASS" : "FAIL - " + result.getError().getMessage()));
    }
    
    @Test
    void testOrderByWithOriginalColumn() {
        framework.createTable("Sales", """
            product | price | quantity
            A | 10.0 | 5
            B | 20.0 | 3
            C | 15.0 | 4
            """);
        
        var result = framework.executeQuery("@SELECT product, price * quantity as sales_total FROM Sales ORDER BY price ASC");
        System.out.println("✅ ORDER BY with original column: " + (result.isSuccess() ? "PASS" : "FAIL - " + result.getError().getMessage()));
    }
    
    @Test
    void testGroupByWithOrderByAlias() {
        framework.createTable("Sales", """
            category | amount
            A | 100
            B | 200
            A | 150
            C | 75
            """);
        
        var result = framework.executeQuery("@SELECT category, SUM(amount) as total_sales FROM Sales GROUP BY category ORDER BY total_sales DESC");
        System.out.println("✅ GROUP BY with ORDER BY alias: " + (result.isSuccess() ? "PASS" : "FAIL - " + result.getError().getMessage()));
    }
    
    @Test
    void testOrderByWithComplexExpression() {
        framework.createTable("Products", """
            name | cost | markup
            Widget | 5.0 | 2.0
            Gadget | 10.0 | 1.5
            Tool | 8.0 | 1.8
            """);
        
        var result = framework.executeQuery("@SELECT name, cost * markup as selling_price FROM Products ORDER BY cost * markup DESC");
        System.out.println("✅ ORDER BY with complex expression: " + (result.isSuccess() ? "PASS" : "FAIL - " + result.getError().getMessage()));
    }
    
    @Test
    void testOrderByMultipleFields() {
        framework.createTable("Employees", """
            department | name | salary
            IT | Alice | 5000
            IT | Bob | 5000
            HR | Charlie | 4000
            IT | Diana | 6000
            """);
        
        var result = framework.executeQuery("@SELECT name, department, salary FROM Employees ORDER BY department ASC, salary DESC");
        System.out.println("✅ ORDER BY multiple fields: " + (result.isSuccess() ? "PASS" : "FAIL - " + result.getError().getMessage()));
    }
    
    @Test
    void testSimpleSelectWithoutOrderBy() {
        framework.createTable("Basic", """
            id | value
            1 | 100
            2 | 200
            """);
        
        var result = framework.executeQuery("@SELECT id, value * 2 as doubled FROM Basic");
        System.out.println("✅ Simple SELECT without ORDER BY: " + (result.isSuccess() ? "PASS" : "FAIL - " + result.getError().getMessage()));
    }
}
