package me.bechberger.jfr.extended;

import me.bechberger.jfr.extended.engine.framework.QueryResult;
import me.bechberger.jfr.extended.engine.framework.QueryTestFramework;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class DebugComplexHaving {
    
    private QueryTestFramework framework;
    
    @BeforeEach
    void setUp() {
        framework = new QueryTestFramework();
    }
    
    @Test
    void testJustGroupBy() {
        framework.createTable("ProjectData", """
            team | budget | spent | hours_worked | bonus_pool
            Alpha | 500000 | 320000 | 2400 | 25000
            Alpha | 500000 | 180000 | 1800 | 15000
            Beta | 300000 | 280000 | 2200 | 12000
            Beta | 300000 | 250000 | 2000 | 18000
            Gamma | 200000 | 150000 | 1200 | 8000
            Delta | 400000 | 390000 | 3000 | 20000
            """);
        
        // First, let's see the GROUP BY results without HAVING
        QueryResult result = framework.executeQuery("""
            @SELECT team, 
                   SUM(budget) as total_budget,
                   SUM(spent) as total_spent,
                   SUM(budget) - SUM(spent) as savings,
                   (SUM(budget) - SUM(spent)) / SUM(budget) as efficiency_ratio,
                   SUM(bonus_pool) as total_bonus,
                   COUNT(*) as project_count
            FROM ProjectData 
            GROUP BY team 
            """);
        
        if (!result.isSuccess()) {
            fail("Query failed: " + result.getError().getMessage());
        }
        
        System.out.println("GROUP BY results:");
        System.out.println("Rows: " + result.getTable().getRowCount());
        for (int i = 0; i < result.getTable().getRowCount(); i++) {
            System.out.printf("Team: %s, Budget: %s, Spent: %s, Savings: %s, Efficiency: %s, Bonus: %s%n",
                result.getTable().getString(i, "team"),
                result.getTable().getNumber(i, "total_budget"),
                result.getTable().getNumber(i, "total_spent"),
                result.getTable().getNumber(i, "savings"),
                result.getTable().getNumber(i, "efficiency_ratio"),
                result.getTable().getNumber(i, "total_bonus")
            );
        }
        
        assertTrue(result.getTable().getRowCount() > 0);
    }
    
    @Test 
    void testSimpleHaving() {
        framework.createTable("ProjectData", """
            team | budget | spent | hours_worked | bonus_pool
            Alpha | 500000 | 320000 | 2400 | 25000
            Alpha | 500000 | 180000 | 1800 | 15000
            Beta | 300000 | 280000 | 2200 | 12000
            Beta | 300000 | 250000 | 2000 | 18000
            Gamma | 200000 | 150000 | 1200 | 8000
            Delta | 400000 | 390000 | 3000 | 20000
            """);
        
        // Test simple HAVING clause with just one aggregate
        QueryResult result = framework.executeQuery("""
            @SELECT team, SUM(bonus_pool) as total_bonus
            FROM ProjectData 
            GROUP BY team 
            HAVING SUM(bonus_pool) > 30000
            """);
        
        if (!result.isSuccess()) {
            fail("Query failed: " + result.getError().getMessage());
        }
        
        System.out.println("Simple HAVING results:");
        System.out.println("Rows: " + result.getTable().getRowCount());
        for (int i = 0; i < result.getTable().getRowCount(); i++) {
            System.out.printf("Team: %s, Total Bonus: %s%n",
                result.getTable().getString(i, "team"),
                result.getTable().getNumber(i, "total_bonus")
            );
        }
        
        // Alpha team should have 40000 bonus (25000 + 15000)
        assertEquals(1, result.getTable().getRowCount());
        assertEquals("Alpha", result.getTable().getString(0, "team"));
        assertEquals(40000.0, result.getTable().getNumber(0, "total_bonus"));
    }
}
