import me.bechberger.jfr.extended.engine.framework.QueryTestFramework;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class QuickCollectTest {
    
    private QueryTestFramework framework;
    
    @BeforeEach
    void setUp() {
        framework = new QueryTestFramework();
        framework.mockTable("Events")
            .withStringColumn("type")
            .withStringColumn("value") 
            .withRow("A", "v1")
            .withRow("A", "v2")
            .withRow("B", "v3")
            .build();
    }
    
    @Test
    void testCollectSimple() {
        System.out.println("Testing simple COLLECT...");
        var result = framework.executeQuery("@SELECT COLLECT(value) FROM Events");
        System.out.println("Success: " + result.isSuccess());
        if (result.isSuccess()) {
            System.out.println("Result table:");
            System.out.println(result.getTable().toString());
        } else {
            System.out.println("Error: " + result.getError());
        }
    }
    
    @Test
    void testCollectWithHead() {
        System.out.println("Testing COLLECT with HEAD...");
        var result = framework.executeQuery("@SELECT HEAD(COLLECT(value)) FROM Events");
        System.out.println("Success: " + result.isSuccess());
        if (result.isSuccess()) {
            System.out.println("Result table:");
            System.out.println(result.getTable().toString());
        } else {
            System.out.println("Error: " + result.getError());
        }
    }
    
    @Test
    void testCollectGroupBy() {
        System.out.println("Testing COLLECT with GROUP BY...");
        var result = framework.executeQuery("@SELECT type, COLLECT(value) FROM Events GROUP BY type ORDER BY type");
        System.out.println("Success: " + result.isSuccess());
        if (result.isSuccess()) {
            System.out.println("Result table:");
            System.out.println(result.getTable().toString());
        } else {
            System.out.println("Error: " + result.getError());
        }
    }
}
