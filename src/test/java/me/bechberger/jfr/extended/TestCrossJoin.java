package me.bechberger.jfr.extended;

import me.bechberger.jfr.extended.engine.framework.QueryTestFramework;
import me.bechberger.jfr.extended.engine.framework.QueryResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test class for CROSS JOIN functionality
 */
public class TestCrossJoin {
    
    private QueryTestFramework framework;
    
    @BeforeEach
    void setUp() {
        framework = new QueryTestFramework();
    }
    
    @Test
    @DisplayName("Basic CROSS JOIN should create Cartesian product")
    void testBasicCrossJoin() {
        // Create two simple tables
        framework.createTable("Users", """
            name | age
            Alice | 25
            Bob | 30
            """);
            
        framework.createTable("Products", """
            product | price
            Laptop | 1000
            Mouse | 20
            """);
            
        // Test CROSS JOIN - should create 2x2 = 4 rows
        framework.executeAndExpectTable("@SELECT * FROM Users CROSS JOIN Products", """
            name | age | product | price
            Alice | 25 | Laptop | 1000
            Alice | 25 | Mouse | 20
            Bob | 30 | Laptop | 1000
            Bob | 30 | Mouse | 20
            """);
    }
    
    @Test
    @DisplayName("CROSS JOIN with aliases should work correctly")
    void testCrossJoinWithAliases() {
        framework.createTable("TableA", """
            id | value
            1 | A
            2 | B
            """);
            
        framework.createTable("TableB", """
            id | status
            100 | OK
            200 | ERROR
            """);
            
        // Test CROSS JOIN with table aliases
        framework.executeAndExpectTable("@SELECT a.id, a.value, b.id, b.status FROM TableA a CROSS JOIN TableB AS b", """
            id | value | id | status
            1 | A | 100 | OK
            1 | A | 200 | ERROR
            2 | B | 100 | OK
            2 | B | 200 | ERROR
            """);
    }
    
    @Test
    @DisplayName("CROSS JOIN with WHERE clause should filter results")
    void testCrossJoinWithWhere() {
        framework.createTable("Numbers", """
            num
            1
            2
            3
            """);
            
        framework.createTable("Letters", """
            letter
            A
            B
            """);
            
        // Test CROSS JOIN with filtering
        framework.executeAndExpectTable("@SELECT * FROM Numbers CROSS JOIN Letters WHERE num > 1", """
            num | letter
            2 | A
            2 | B
            3 | A
            3 | B
            """);
    }
    
    @Test
    @DisplayName("CROSS JOIN should work with empty tables")
    void testCrossJoinWithEmptyTable() {
        framework.createTable("NonEmpty", """
            value
            test
            """);
            
        framework.createTable("Empty", """
            data
            """);
            
        // CROSS JOIN with empty table should return empty result
        QueryResult result = framework.executeQuery("@SELECT * FROM NonEmpty CROSS JOIN Empty");
        assertTrue(result.isSuccess());
        assertEquals(0, result.getTable().getRowCount());
    }
    
    @Test
    @DisplayName("CROSS JOIN should work with single row tables")
    void testCrossJoinSingleRows() {
        framework.createTable("Single1", """
            col1
            X
            """);
            
        framework.createTable("Single2", """
            col2
            Y
            """);
            
        framework.executeAndExpectTable("@SELECT * FROM Single1 CROSS JOIN Single2", """
            col1 | col2
            X | Y
            """);
    }
    
    @Test
    @DisplayName("Multiple CROSS JOINs should work correctly")
    void testMultipleCrossJoins() {
        framework.createTable("A", """
            a
            1
            2
            """);
            
        framework.createTable("B", """
            b
            X
            """);
            
        framework.createTable("C", """
            c
            Z
            """);
            
        // Test chained CROSS JOINs: 2 x 1 x 1 = 2 rows
        framework.executeAndExpectTable("@SELECT * FROM A CROSS JOIN B CROSS JOIN C", """
            a | b | c
            1 | X | Z
            2 | X | Z
            """);
    }
    
    @Test
    @DisplayName("CROSS JOIN parsing should not require ON clause")
    void testCrossJoinNoOnClause() {
        framework.createTable("Test1", """
            id
            1
            """);
            
        framework.createTable("Test2", """
            name
            Alice
            """);
            
        // This should parse successfully without ON clause
        QueryResult result = framework.executeQuery("@SELECT * FROM Test1 CROSS JOIN Test2");
        assertTrue(result.isSuccess());
        assertEquals(1, result.getTable().getRowCount());
    }
    
    @Test
    @DisplayName("CROSS JOIN should work in mixed join scenarios")
    void testCrossJoinWithOtherJoins() {
        framework.createTable("Main", """
            id | name
            1 | Alice
            2 | Bob
            """);
            
        framework.createTable("Cross", """
            value
            X
            Y
            """);
            
        framework.createTable("Inner", """
            id | status
            1 | Active
            2 | Inactive
            """);
            
        // Test CROSS JOIN followed by INNER JOIN
        framework.executeAndExpectTable("""
            @SELECT m.name, c.value, i.status 
            FROM Main m 
            CROSS JOIN Cross c 
            INNER JOIN Inner i ON m.id = i.id
            """, """
            name | value | status
            Alice | X | Active
            Alice | Y | Active
            Bob | X | Inactive
            Bob | Y | Inactive
            """);
    }
}
