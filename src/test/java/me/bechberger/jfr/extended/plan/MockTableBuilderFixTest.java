package me.bechberger.jfr.extended.plan;

import me.bechberger.jfr.extended.engine.framework.QueryTestFramework;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

/**
 * Test to verify the MockTableBuilder fix is working correctly with the preferred createTable method
 */
public class MockTableBuilderFixTest {

    private QueryTestFramework framework;

    @BeforeEach
    void setUp() {
        framework = new QueryTestFramework();
    }

    @Test
    @DisplayName("Test createTable with timestamp parsing")
    void testCreateTableWithTimestamps() {
        // Test createTable method with timestamp columns
        framework.createTable("TestEvents", """
            eventType | duration | startTime
            GarbageCollection | 200 | 2023-01-01T10:00:00Z
            ExecutionSample | 300 | 2023-01-01T10:00:05Z
            """);

        // Verify the table was created successfully using executeAndExpectTable
        framework.executeAndExpectTable("@SELECT eventType, duration FROM TestEvents", """
            eventType | duration
            GarbageCollection | 200
            ExecutionSample | 300
            """);
    }

    @Test
    @DisplayName("Test createTable with array columns")
    void testCreateTableWithArrays() {
        // Test createTable method with array columns
        framework.createTable("TestArrayData", """
            name | tags
            Item1 | ["tag1", "tag2"]
            Item2 | ["tag3", "tag4", "tag5"]
            """);

        // Verify the table was created successfully using executeAndExpectTable
        framework.executeAndExpectTable("@SELECT name, tags FROM TestArrayData", """
            name | tags
            Item1 | ["tag1", "tag2"]
            Item2 | ["tag3", "tag4", "tag5"]
            """);
    }

    @Test
    @DisplayName("Test createTable with all column types")
    void testCreateTableWithAllTypes() {
        // Test createTable method with all column types
        framework.createTable("TestAllTypes", """
            name | count | ratio | active | created | elapsed | memoryUsed | items
            Test1 | 42 | 3.14 | true | 2023-01-01T10:00:00Z | 5000 | 1024 | ["a", "b"]
            """);

        // Verify the table was created successfully using executeAndExpectTable
        framework.executeAndExpectTable("@SELECT name, count, ratio, active, items FROM TestAllTypes", """
            name | count | ratio | active | items
            Test1 | 42 | 3.14 | true | ["a", "b"]
            """);
    }

    @Test
    @DisplayName("Test basic table creation and simple queries")
    void testBasicTableCreationAndQueries() {
        // Create test data using the preferred createTable method
        framework.createTable("EventData", """
            eventType | duration
            GarbageCollection | 100
            GarbageCollection | 200
            GarbageCollection | 300
            ExecutionSample | 400
            ExecutionSample | 500
            """);

        // Test basic SELECT without GROUP BY
        framework.executeAndExpectTable(
            "@SELECT eventType, duration FROM EventData",
            """
            eventType | duration
            GarbageCollection | 100
            GarbageCollection | 200
            GarbageCollection | 300
            ExecutionSample | 400
            ExecutionSample | 500
            """
        );
    }

    @Test
    @DisplayName("Test basic COUNT query with preferred createTable method")
    void testBasicCountWithCreateTable() {
        // Create test data using the preferred createTable method
        framework.createTable("SimpleData", """
            name | value
            Alice | 10
            Bob | 20
            Charlie | 30
            """);

        // Test simple COUNT query
        framework.executeAndExpectTable(
            "@SELECT COUNT(*) AS total FROM SimpleData",
            """
            total
            3
            """
        );
    }
}
