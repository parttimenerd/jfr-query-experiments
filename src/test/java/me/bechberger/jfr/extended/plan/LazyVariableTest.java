package me.bechberger.jfr.extended.plan;

import me.bechberger.jfr.extended.Parser;
import me.bechberger.jfr.extended.ast.ASTNodes.*;
import me.bechberger.jfr.extended.engine.framework.QueryTestFramework;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for lazy variable evaluation using AST matching and parameterized tests.
 * 
 * This test suite validates lazy variable creation, storage, and lifecycle management
 * with comprehensive coverage of different query types and operations.
 */
public class LazyVariableTest {
    
    private QueryTestFramework framework;
    
    @BeforeEach
    void setUp() {
        framework = new QueryTestFramework();
        framework.createTable("TestEvent", """
                a | b
                1 | 2
                3 | 4
                """);
        framework.createTable("GarbageCollection", """
                collector | duration
                G1GC      | 50
                ParallelGC| 75
                """);
        framework.createTable("ExecutionSample", """
                duration
                1000
                2000
                1500
                """);
        framework.createTable("DummyTable", """
                dummy
                1
                """);
    }
    
    // ===== PARAMETERIZED AST PARSING TESTS =====
    
    static Stream<Arguments> lazyVariableTestCases() {
        return Stream.of(
            Arguments.of("myVar := @SELECT * FROM TestEvent", "myVar", "QueryNode", "TestEvent"),
            Arguments.of("count := @SELECT COUNT(*) FROM GarbageCollection", "count", "QueryNode", "GarbageCollection"), 
            Arguments.of("samples := @SELECT duration FROM ExecutionSample", "samples", "QueryNode", "ExecutionSample"),
            Arguments.of("filtered := @SELECT * FROM TestEvent WHERE duration > 1000", "filtered", "QueryNode", "TestEvent")
        );
    }
    
    @ParameterizedTest
    @MethodSource("lazyVariableTestCases")
    @DisplayName("Lazy variable assignments should parse correctly with different query types")
    void testLazyVariableASTParsing(String input, String expectedVariable, String expectedNodeType, String expectedTableName) throws Exception {
        // Arrange & Act
        AssignmentNode assignment = parseAssignment(input);
        
        // Assert - Check variable name
        assertEquals(expectedVariable, assignment.variable());
        
        // Assert - Check query structure  
        QueryNode queryNode = assignment.query();
        assertNotNull(queryNode);
        
        assertThat(queryNode.select())
            .isInstanceOf(SelectNode.class);
        
        assertThat(queryNode.from())
            .isInstanceOf(FromNode.class);
        
        FromNode fromNode = queryNode.from();
        assertEquals(1, fromNode.sources().size());
        
        SourceNodeBase source = fromNode.sources().get(0);
        assertThat(source).isInstanceOf(SourceNode.class);
        
        SourceNode sourceNode = (SourceNode) source;
        assertEquals(expectedTableName, sourceNode.source());
    }
    
    @ParameterizedTest
    @ValueSource(strings = {
        "result := @SELECT COUNT(*) FROM TestEvent",
        "data := @SELECT * FROM GarbageCollection WHERE duration > 500",
        "aggregates := @SELECT AVG(duration), MAX(duration) FROM ExecutionSample",
        "joined := @SELECT t1.name, t2.value FROM Table1 AS t1 JOIN Table2 AS t2 ON t1.id = t2.ref_id"
    })
    @DisplayName("Complex lazy variable queries should parse without errors")
    void testComplexLazyVariableQueries(String input) throws Exception {
        // Arrange & Act
        AssignmentNode assignment = parseAssignment(input);
        
        // Assert
        assertNotNull(assignment.variable());
        assertNotNull(assignment.query());
        
        QueryNode queryNode = assignment.query();
        assertNotNull(queryNode.select());
    }
    
    // ===== PARAMETERIZED EXECUTION TESTS =====
    
    static Stream<Arguments> lazyVariableExecutionTestCases() {
        return Stream.of(
            Arguments.of("count := @SELECT COUNT(*) FROM TestEvents", "count", 2L),
            Arguments.of("total := @SELECT COUNT(*) FROM GarbageCollection", "total", 2L),
            Arguments.of("filtered := @SELECT COUNT(*) FROM TestEvents WHERE id > 1", "filtered", 1L)
        );
    }
    
    @ParameterizedTest
    @MethodSource("lazyVariableExecutionTestCases")
    @DisplayName("Lazy variables should execute and return correct results")
    void testLazyVariableExecution(String assignmentQuery, String variableName, Long expectedCount) {
        // Arrange - Create test data
        framework.mockTable("TestEvents")
            .withNumberColumn("id")
            .withStringColumn("name")
            .withRow(1L, "Event1")
            .withRow(2L, "Event2")
            .build();
            
        framework.mockTable("GarbageCollection")
            .withStringColumn("collector")
            .withNumberColumn("duration")
            .withRow("G1GC", 50L)
            .withRow("ParallelGC", 75L)
            .build();
        
        // Act - Execute assignment and then use variable
        var results = framework.executeMultiStatementQuery(
            assignmentQuery + ";\n" +
            "@SELECT " + variableName + " as result FROM DummyTable"
        );
        
        // Assert
        assertTrue(results.get(0).isSuccess(), "Assignment should succeed");
        if (!results.get(1).isSuccess()) {
            System.err.println("Variable usage failed: " + results.get(1).getError().getMessage());
            if (results.get(1).getError().getCause() != null) {
                System.err.println("Caused by: " + results.get(1).getError().getCause().getMessage());
                results.get(1).getError().getCause().printStackTrace();
            }
        }
        assertTrue(results.get(1).isSuccess(), "Variable usage should succeed");
        
        var resultTable = results.get(1).getTable();
        assertEquals(expectedCount, resultTable.getNumber(0, "result"));
    }
    
    @Test
    @DisplayName("Lazy variable with string result should execute correctly")
    void testLazyVariableStringResult() {
        // Arrange
        framework.mockTable("Users")
            .withStringColumn("name")
            .withStringColumn("role")
            .withRow("Alice", "admin")
            .withRow("Bob", "user")
            .build();
        
        // Act
        var results = framework.executeMultiStatementQuery(
            "adminName := @SELECT name FROM Users WHERE role = 'admin';\n" +
            "@SELECT adminName as admin_user FROM DummyTable"
        );
        
        // Assert
        assertTrue(results.get(0).isSuccess());
        if (!results.get(1).isSuccess()) {
            System.err.println("String result query failed: " + results.get(1).getError().getMessage());
        }
        assertTrue(results.get(1).isSuccess());
        
        var resultTable = results.get(1).getTable();
        assertEquals("Alice", resultTable.getString(0, "admin_user"));
    }
    
    @Test 
    @DisplayName("Multiple lazy variables should work independently")
    void testMultipleLazyVariables() {
        // Arrange
        framework.mockTable("Events")
            .withNumberColumn("id")
            .withStringColumn("type")
            .withRow(1L, "start")
            .withRow(2L, "end")
            .withRow(3L, "start")
            .build();
        
        // Act
        var results = framework.executeMultiStatementQuery(
            "totalCount := @SELECT COUNT(*) FROM Events;\n" +
            "startCount := @SELECT COUNT(*) FROM Events WHERE type = 'start';\n" +
            "@SELECT totalCount as total, startCount as starts FROM DummyTable"
        );
        
        // Assert
        assertTrue(results.get(0).isSuccess()); // totalCount assignment
        assertTrue(results.get(1).isSuccess()); // startCount assignment  
        if (!results.get(2).isSuccess()) {
            System.err.println("Multiple variables query failed: " + results.get(2).getError().getMessage());
        }
        assertTrue(results.get(2).isSuccess()); // final query
        
        var resultTable = results.get(2).getTable();
        assertEquals(3L, resultTable.getNumber(0, "total"));
        assertEquals(2L, resultTable.getNumber(0, "starts"));
    }
    
    @Test
    @DisplayName("Lazy variable with aggregation should work correctly")
    void testLazyVariableAggregation() {
        // Arrange
        framework.mockTable("Measurements")
            .withNumberColumn("value")
            .withRow(10L)
            .withRow(20L)
            .withRow(30L)
            .build();
        
        // Act
        var results = framework.executeMultiStatementQuery(
            "avgValue := @SELECT AVG(value) FROM Measurements;\n" +
            "maxValue := @SELECT MAX(value) FROM Measurements;\n" +
            "@SELECT avgValue as average, maxValue as maximum FROM DummyTable"
        );
        
        // Assert
        assertTrue(results.get(0).isSuccess());
        assertTrue(results.get(1).isSuccess());
        if (!results.get(2).isSuccess()) {
            System.err.println("Aggregation query failed: " + results.get(2).getError().getMessage());
        }
        assertTrue(results.get(2).isSuccess());
        
        var resultTable = results.get(2).getTable();
        assertEquals(20.0, resultTable.getNumber(0, "average"), 0.001);
        assertEquals(30L, resultTable.getNumber(0, "maximum"));
    }
    
    // ===== HELPER METHODS =====
    
    /**
     * Helper method to parse assignment statements
     */
    private AssignmentNode parseAssignment(String input) throws Exception {
        Parser parser = new Parser(input);
        ProgramNode program = parser.parse();
        
        assertEquals(1, program.statements().size(), 
            "Expected exactly one statement in: " + input);
        
        StatementNode statement = program.statements().get(0);
        assertInstanceOf(AssignmentNode.class, statement,
            "Expected AssignmentNode for: " + input);
        
        return (AssignmentNode) statement;
    }
    
    /**
     * Custom assertion helper for better AST validation
     */
    private static ASTAssertion assertThat(Object actual) {
        return new ASTAssertion(actual);
    }
    
    private static class ASTAssertion {
        private final Object actual;
        
        public ASTAssertion(Object actual) {
            this.actual = actual;
        }
        
        public ASTAssertion isInstanceOf(Class<?> expectedType) {
            assertInstanceOf(expectedType, actual);
            return this;
        }
    }
}
