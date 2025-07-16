package me.bechberger.jfr.extended.engine;

import me.bechberger.jfr.extended.table.JfrTable;
import me.bechberger.jfr.extended.table.StandardJfrTable;import me.bechberger.jfr.extended.table.CellValue;
import me.bechberger.jfr.extended.table.CellType;
import me.bechberger.jfr.extended.ast.ASTNodes.RawJfrQueryNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for QueryEvaluator multi-line query functionality and state management.
 */
class QueryEvaluatorMultiLineTest {

    private QueryEvaluator evaluator;
    private MockRawJfrQueryExecutor mockExecutor;

    @BeforeEach
    void setUp() {
        mockExecutor = new MockRawJfrQueryExecutor();
        
        // Set up mock tables
        setupMockTables();
        
        evaluator = new QueryEvaluator(mockExecutor);
    }

    private void setupMockTables() {
        // Create mock Users table
        List<JfrTable.Column> userColumns = List.of(
            new JfrTable.Column("name", CellType.STRING),
            new JfrTable.Column("age", CellType.NUMBER),
            new JfrTable.Column("id", CellType.NUMBER)
        );
        JfrTable usersTable = new StandardJfrTable(userColumns);
        usersTable.addRow(new JfrTable.Row(List.of(
            new CellValue.StringValue("Alice"),
            new CellValue.NumberValue(25),
            new CellValue.NumberValue(1)
        )));
        usersTable.addRow(new JfrTable.Row(List.of(
            new CellValue.StringValue("Bob"),
            new CellValue.NumberValue(30),
            new CellValue.NumberValue(2)
        )));
        usersTable.addRow(new JfrTable.Row(List.of(
            new CellValue.StringValue("Charlie"),
            new CellValue.NumberValue(35),
            new CellValue.NumberValue(3)
        )));
        
        mockExecutor.addMockTable("Users", usersTable);
        
        // Create mock Events table
        List<JfrTable.Column> eventColumns = List.of(
            new JfrTable.Column("eventType", CellType.STRING),
            new JfrTable.Column("timestamp", CellType.TIMESTAMP),
            new JfrTable.Column("duration", CellType.DURATION)
        );
        JfrTable eventsTable = new StandardJfrTable(eventColumns);
        eventsTable.addRow(new JfrTable.Row(List.of(
            new CellValue.StringValue("GCEvent"),
            new CellValue.TimestampValue(java.time.Instant.now()),
            new CellValue.DurationValue(java.time.Duration.ofMillis(10))
        )));
        
        mockExecutor.addMockTable("Events", eventsTable);
    }

    @Test
    @DisplayName("Should execute single statement query")
    void testSingleStatement() throws Exception {
        String query = "@SELECT * FROM Users";
        
        JfrTable result = evaluator.query(query);
        
        assertNotNull(result);
        assertEquals(3, result.getRows().size());
        assertEquals("Alice", result.getRows().get(0).getCells().get(0).toString());
    }

    @Test
    @DisplayName("Should execute multi-statement query with semicolon separators")
    void testMultiStatementWithSemicolons() throws Exception {
        String multiQuery = """
            @SELECT * FROM Users WHERE age > 25;
            @SELECT COUNT(*) FROM Users;
            """;
        
        JfrTable result = evaluator.query(multiQuery);
        
        // Should return the result of the last statement (COUNT)
        assertNotNull(result);
        // The exact result depends on how COUNT is implemented
    }

    @Test
    @DisplayName("Should handle variable assignments and maintain state")
    void testVariableAssignments() throws Exception {
        String multiQuery = """
            youngUsers := @SELECT * FROM Users WHERE age <= 30;
            oldUsers := @SELECT * FROM Users WHERE age > 30;
            @SELECT * FROM youngUsers;
            """;
        
        JfrTable result = evaluator.query(multiQuery);
        
        assertNotNull(result);
        // Should return youngUsers (Alice, Bob)
        assertEquals(2, result.getRows().size());
    }

    @Test
    @DisplayName("Should handle view definitions")
    void testViewDefinitions() throws Exception {
        String multiQuery = """
            VIEW young_view AS @SELECT * FROM Users WHERE age <= 30;
            @SELECT * FROM young_view;
            """;
        
        JfrTable result = evaluator.query(multiQuery);
        
        assertNotNull(result);
        // Should return users from the view (Alice, Bob)
        assertEquals(2, result.getRows().size());
    }

    @Test
    @DisplayName("Should maintain state between query executions")
    void testStateBetweenExecutions() throws Exception {
        // First execution: set up variables
        String setupQuery = "users_backup := @SELECT * FROM Users";
        evaluator.query(setupQuery);
        
        // Second execution: use the variable
        String useQuery = "@SELECT * FROM users_backup WHERE age > 25";
        JfrTable result = evaluator.query(useQuery);
        
        assertNotNull(result);
        // Should return Bob and Charlie
        assertEquals(2, result.getRows().size());
    }

    @Test
    @DisplayName("Should handle complex multi-line queries with all statement types")
    void testComplexMultiLineQuery() throws Exception {
        String complexQuery = """
            // Set up some variables
            adult_users := @SELECT * FROM Users WHERE age >= 25;
            
            // Create a view
            VIEW adult_view AS @SELECT name, age FROM adult_users;
            
            // Use both in a final query  
            @SELECT name FROM adult_view WHERE age >= 30;
            """;
        
        JfrTable result = evaluator.query(complexQuery);
        
        assertNotNull(result);
        // Should return Bob and Charlie (age >= 30)
        assertEquals(2, result.getRows().size());
    }

    @Test
    @DisplayName("Should handle syntax errors with clear error messages")
    void testSyntaxErrorHandling() {
        String invalidQuery = """
            invalid_var := @SELECT * FROM;
            @SELECT * FROM Users;
            """;
        
        Exception exception = assertThrows(Exception.class, () -> {
            evaluator.query(invalidQuery);
        });
        
        assertTrue(exception.getMessage().contains("parse") || 
                  exception.getMessage().contains("syntax") ||
                  exception.getMessage().contains("semantic"));
    }

    @Test
    @DisplayName("Should handle semantic validation errors")
    void testSemanticValidationErrors() {
        String invalidQuery = """
            @SELECT * FROM Users ORDER BY COUNT(*);
            """;
        
        Exception exception = assertThrows(Exception.class, () -> {
            evaluator.query(invalidQuery);
        });
        
        assertTrue(exception.getMessage().contains("semantic") ||
                  exception.getMessage().contains("aggregate"));
    }

    @Test
    @DisplayName("Should handle empty and whitespace-only queries")
    void testEmptyQueries() throws Exception {
        // Empty query
        assertThrows(Exception.class, () -> evaluator.query(""));
        
        // Whitespace only
        assertThrows(Exception.class, () -> evaluator.query("   \n\t  "));
        
        // Just semicolons
        assertThrows(Exception.class, () -> evaluator.query(";;;"));
    }

    @Test
    @DisplayName("Should handle queries with comments")
    void testQueriesWithComments() throws Exception {
        String queryWithComments = """
            // This is a comment
            @SELECT * FROM Users WHERE age > 25  // Filter adults
            """;
        
        JfrTable result = evaluator.query(queryWithComments);
        
        assertNotNull(result);
        assertEquals(2, result.getRows().size()); // Bob and Charlie
    }

    /**
     * Mock implementation of RawJfrQueryExecutor for testing
     */
    private static class MockRawJfrQueryExecutor implements RawJfrQueryExecutor {
        private final java.util.Map<String, JfrTable> mockTables = new java.util.HashMap<>();
        
        public void addMockTable(String tableName, JfrTable table) {
            mockTables.put(tableName, table);
        }
        
        @Override
        public JfrTable execute(RawJfrQueryNode queryNode) throws Exception {
            String query = queryNode.rawQuery().trim();
            
            // Simple parsing to extract table name from SELECT * FROM TableName
            if (query.toLowerCase().startsWith("select * from ")) {
                String tableName = query.substring(14).trim();
                if (mockTables.containsKey(tableName)) {
                    return mockTables.get(tableName);
                }
            }
            
            // Return empty table for unrecognized queries
            return new StandardJfrTable(List.of(new JfrTable.Column("result", CellType.STRING)));
        }
    }
}
