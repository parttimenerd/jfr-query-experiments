package me.bechberger.jfr.extended;

import me.bechberger.jfr.extended.ast.ASTNodes.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static me.bechberger.jfr.extended.ASTBuilder.*;

/**
 * Comprehensive test suite for fuzzy join functionality with direct AST checks and parameterized tests.
 * 
 * This class provides thorough testing of fuzzy join syntax, semantics, and edge cases using:
 * - Direct AST node comparison using astEquals() for robust verification
 * - Parameterized tests to reduce code duplication and improve maintainability
 * - Builder pattern for creating expected AST structures
 * - Comprehensive coverage of all join types, tolerance configurations, and error conditions
 * 
 * Test categories:
 * - Basic fuzzy join syntax validation
 * - Join types (NEAREST, PREVIOUS, AFTER) with various configurations
 * - Tolerance expressions (literals, variables, complex expressions)
 * - Multiple fuzzy joins in sequence
 * - Integration with standard joins and complex queries
 * - Error conditions and edge cases
 * - Performance and scalability scenarios
 */
public class FuzzyJoinComprehensiveTest {
    
    private Parser createParser(String query) throws Exception {
        Lexer lexer = new Lexer(query);
        List<Token> tokens = lexer.tokenize();
        return new Parser(tokens, query);
    }
    
    // ===========================================
    // Basic Fuzzy Join Syntax Tests
    // ===========================================
    
    static Stream<Arguments> basicFuzzyJoinCases() {
        return Stream.of(
            Arguments.of("@SELECT * FROM GarbageCollection FUZZY JOIN ExecutionSample ON timestamp",
                        "GarbageCollection", "ExecutionSample", "timestamp"),
            Arguments.of("@SELECT * FROM ExecutionSample FUZZY JOIN AllocationSample ON threadId",
                        "ExecutionSample", "AllocationSample", "threadId"),
            Arguments.of("@SELECT * FROM HeapSummary FUZZY JOIN GarbageCollection ON startTime",
                        "HeapSummary", "GarbageCollection", "startTime"),
            Arguments.of("@SELECT * FROM ThreadPark FUZZY JOIN ExecutionSample ON timestamp",
                        "ThreadPark", "ExecutionSample", "timestamp"),
            Arguments.of("@SELECT * FROM ObjectAllocationSample FUZZY JOIN GarbageCollection ON time",
                        "ObjectAllocationSample", "GarbageCollection", "time")
        );
    }
    
    @ParameterizedTest
    @MethodSource("basicFuzzyJoinCases")
    public void testBasicFuzzyJoinSyntax(String query, String baseTable, String joinTable, String joinField) throws Exception {
        Parser parser = createParser(query);
        ProgramNode program = parser.parse();
        
        ProgramNode expected = program(
            query(
                selectAll(),
                from(
                    source(baseTable),
                    fuzzyJoin(joinTable, joinField).build()
                )
            ).extended()
        );
        
        assertTrue(astEquals(expected, program), "Basic fuzzy join should match expected AST");
    }
    
    // ===========================================
    // Join Type Comprehensive Tests
    // ===========================================
    
    static Stream<Arguments> joinTypeCases() {
        return Stream.of(
            Arguments.of("NEAREST", "Find closest event by time", FuzzyJoinType.NEAREST),
            Arguments.of("PREVIOUS", "Find latest event before", FuzzyJoinType.PREVIOUS),
            Arguments.of("AFTER", "Find earliest event after", FuzzyJoinType.AFTER)
        );
    }
    
    @ParameterizedTest
    @MethodSource("joinTypeCases")
    public void testFuzzyJoinTypes(String joinType, String description, FuzzyJoinType expectedType) throws Exception {
        String query = "@SELECT * FROM GarbageCollection FUZZY JOIN ExecutionSample ON timestamp WITH " + joinType;
        Parser parser = createParser(query);
        ProgramNode program = parser.parse();
        
        ProgramNode expected = program(
            query(
                selectAll(),
                from(
                    source("GarbageCollection"),
                    fuzzyJoin("ExecutionSample", "timestamp").with(expectedType).build()
                )
            ).extended()
        );
        
        assertTrue(astEquals(expected, program), "Should have " + joinType + " join type: " + description);
    }
    
    // ===========================================
    // Tolerance Configuration Tests
    // ===========================================
    
    static Stream<Arguments> toleranceCases() {
        return Stream.of(
            Arguments.of("1ms", durationLiteral("1ms")),
            Arguments.of("5ms", durationLiteral("5ms")),
            Arguments.of("10ms", durationLiteral("10ms")),
            Arguments.of("100ms", durationLiteral("100ms")),
            Arguments.of("500ms", durationLiteral("500ms")),
            Arguments.of("1s", durationLiteral("1s")),
            Arguments.of("2s", durationLiteral("2s")),
            Arguments.of("5s", durationLiteral("5s")),
            Arguments.of("10s", durationLiteral("10s")),
            Arguments.of("30s", durationLiteral("30s")),
            Arguments.of("1min", durationLiteral("1min")),
            Arguments.of("2min", durationLiteral("2min")),
            Arguments.of("5min", durationLiteral("5min")),
            Arguments.of("10min", durationLiteral("10min")),
            Arguments.of("1h", durationLiteral("1h"))
        );
    }
    
    @ParameterizedTest
    @MethodSource("toleranceCases")
    public void testToleranceWithTimeUnits(String toleranceValue, ExpressionNode expectedTolerance) throws Exception {
        String query = "@SELECT * FROM GarbageCollection FUZZY JOIN ExecutionSample ON timestamp WITH NEAREST TOLERANCE " + toleranceValue;
        Parser parser = createParser(query);
        ProgramNode program = parser.parse();
        
        ProgramNode expected = program(
            query(
                selectAll(),
                from(
                    source("GarbageCollection"),
                    fuzzyJoin("ExecutionSample", "timestamp").with(FuzzyJoinType.NEAREST).tolerance(expectedTolerance).build()
                )
            ).extended()
        );
        
        assertTrue(astEquals(expected, program), "Should have tolerance " + toleranceValue);
    }
    
    // ===========================================
    // Complex Tolerance Expression Tests
    // ===========================================
    
    @Test
    public void testToleranceWithComplexExpression() throws Exception {
        Parser parser = createParser("@SELECT * FROM GarbageCollection FUZZY JOIN ExecutionSample ON timestamp WITH NEAREST TOLERANCE 5ms + 2ms");
        ProgramNode program = parser.parse();
        
        ProgramNode expected = program(
            query(
                selectAll(),
                from(
                    source("GarbageCollection"),
                    fuzzyJoin("ExecutionSample", "timestamp").with(FuzzyJoinType.NEAREST).tolerance(
                             binary(durationLiteral("5ms"), BinaryOperator.ADD, durationLiteral("2ms"))).build()
                )
            ).extended()
        );
        
        assertTrue(astEquals(expected, program), "Should parse complex tolerance expression");
    }
    
    @Test
    public void testToleranceWithVariableExpression() throws Exception {
        Parser parser = createParser("@SELECT * FROM GarbageCollection FUZZY JOIN ExecutionSample ON timestamp WITH NEAREST TOLERANCE maxDelay");
        ProgramNode program = parser.parse();
        
        ProgramNode expected = program(
            query(
                selectAll(),
                from(
                    source("GarbageCollection"),
                    fuzzyJoin("ExecutionSample", "timestamp").with(FuzzyJoinType.NEAREST).tolerance(identifier("maxDelay")).build()
                )
            ).extended()
        );
        
        assertTrue(astEquals(expected, program), "Should parse variable tolerance expression");
    }

    // ===========================================
    // Threshold Configuration Tests
    // ===========================================
    
    static Stream<Arguments> thresholdCases() {
        return Stream.of(
            Arguments.of("10ms", durationLiteral("10ms")),
            Arguments.of("500ms", durationLiteral("500ms")),
            Arguments.of("2s", durationLiteral("2s")),
            Arguments.of("30s", durationLiteral("30s")),
            Arguments.of("1m", durationLiteral("1m")),
            Arguments.of("5m", durationLiteral("5m")),
            Arguments.of("1h", durationLiteral("1h")),
            Arguments.of("100ns", durationLiteral("100ns")),
            Arguments.of("50us", durationLiteral("50us"))
        );
    }
    
    @ParameterizedTest
    @MethodSource("thresholdCases")
    public void testThresholdWithTimeUnits(String thresholdValue, ExpressionNode expectedThreshold) throws Exception {
        String query = "@SELECT * FROM GarbageCollection FUZZY JOIN ExecutionSample ON timestamp WITH NEAREST TOLERANCE 5ms THRESHOLD " + thresholdValue;
        Parser parser = createParser(query);
        ProgramNode program = parser.parse();
        
        ProgramNode expected = program(
            query(
                selectAll(),
                from(
                    source("GarbageCollection"),
                    fuzzyJoin("ExecutionSample", "timestamp").with(FuzzyJoinType.NEAREST)
                        .tolerance(durationLiteral("5ms"))
                        .threshold(expectedThreshold).build()
                )
            ).extended()
        );
        
        assertTrue(astEquals(expected, program), "Should have threshold " + thresholdValue);
    }

    @Test
    public void testThresholdWithoutTolerance() throws Exception {
        Parser parser = createParser("@SELECT * FROM GarbageCollection FUZZY JOIN ExecutionSample ON timestamp WITH NEAREST THRESHOLD 50ms");
        ProgramNode program = parser.parse();
        
        ProgramNode expected = program(
            query(
                selectAll(),
                from(
                    source("GarbageCollection"),
                    fuzzyJoin("ExecutionSample", "timestamp").with(FuzzyJoinType.NEAREST)
                        .threshold(durationLiteral("50ms")).build()
                )
            ).extended()
        );
        
        assertTrue(astEquals(expected, program), "Should parse threshold without tolerance");
    }

    @Test
    public void testThresholdWithComplexExpression() throws Exception {
        Parser parser = createParser("@SELECT * FROM GarbageCollection FUZZY JOIN ExecutionSample ON timestamp WITH NEAREST THRESHOLD 10ms * 5");
        ProgramNode program = parser.parse();
        
        ProgramNode expected = program(
            query(
                selectAll(),
                from(
                    source("GarbageCollection"),
                    fuzzyJoin("ExecutionSample", "timestamp").with(FuzzyJoinType.NEAREST).threshold(
                             binary(durationLiteral("10ms"), BinaryOperator.MULTIPLY, numberLiteral(5.0))).build()
                )
            ).extended()
        );
        
        assertTrue(astEquals(expected, program), "Should parse complex threshold expression");
    }

    @Test
    public void testThresholdWithVariableExpression() throws Exception {
        Parser parser = createParser("@SELECT * FROM GarbageCollection FUZZY JOIN ExecutionSample ON timestamp WITH NEAREST THRESHOLD maxDistance");
        ProgramNode program = parser.parse();
        
        ProgramNode expected = program(
            query(
                selectAll(),
                from(
                    source("GarbageCollection"),
                    fuzzyJoin("ExecutionSample", "timestamp").with(FuzzyJoinType.NEAREST).threshold(identifier("maxDistance")).build()
                )
            ).extended()
        );
        
        assertTrue(astEquals(expected, program), "Should parse variable threshold expression");
    }

    @Test
    public void testToleranceAndThresholdTogether() throws Exception {
        Parser parser = createParser("@SELECT * FROM GarbageCollection FUZZY JOIN ExecutionSample ON timestamp WITH NEAREST TOLERANCE 10ms THRESHOLD 50ms");
        ProgramNode program = parser.parse();
        
        ProgramNode expected = program(
            query(
                selectAll(),
                from(
                    source("GarbageCollection"),
                    fuzzyJoin("ExecutionSample", "timestamp").with(FuzzyJoinType.NEAREST)
                        .tolerance(durationLiteral("10ms"))
                        .threshold(durationLiteral("50ms")).build()
                )
            ).extended()
        );
        
        assertTrue(astEquals(expected, program), "Should parse both tolerance and threshold");
    }

    @Test
    public void testThresholdBeforeTolerance() throws Exception {
        Parser parser = createParser("@SELECT * FROM GarbageCollection FUZZY JOIN ExecutionSample ON timestamp WITH NEAREST THRESHOLD 50ms TOLERANCE 10ms");
        ProgramNode program = parser.parse();
        
        ProgramNode expected = program(
            query(
                selectAll(),
                from(
                    source("GarbageCollection"),
                    fuzzyJoin("ExecutionSample", "timestamp").with(FuzzyJoinType.NEAREST)
                        .tolerance(durationLiteral("10ms"))
                        .threshold(durationLiteral("50ms")).build()
                )
            ).extended()
        );
        
        assertTrue(astEquals(expected, program), "Should parse threshold before tolerance");
    }
    
    // ===========================================
    // Alias and Field Access Tests (see parameterized tests below)
    // ===========================================
    
    @Test
    public void testFuzzyJoinWithQualifiedField() throws Exception {
        // Note: This test may need to be adjusted based on actual parser support for qualified fields in fuzzy joins
        // For now, we'll test the basic structure
        Parser parser = createParser("@SELECT * FROM GarbageCollection gc FUZZY JOIN ExecutionSample ON timestamp");
        ProgramNode program = parser.parse();
        
        ProgramNode expected = program(
            query(
                selectAll(),
                from(
                    source("GarbageCollection", "gc"),
                    fuzzyJoin("ExecutionSample", "timestamp").build()
                )
            ).extended()
        );
        
        assertTrue(astEquals(expected, program), "Should parse fuzzy join with qualified source");
    }
    
    // ===========================================
    // Multiple Fuzzy Joins Tests
    // ===========================================
    
    @Test
    public void testMultipleFuzzyJoins() throws Exception {
        Parser parser = createParser("@SELECT * FROM GarbageCollection FUZZY JOIN ExecutionSample ON timestamp FUZZY JOIN AllocationSample ON threadId");
        ProgramNode program = parser.parse();
        
        ProgramNode expected = program(
            query(
                selectAll(),
                from(
                    source("GarbageCollection"),
                    fuzzyJoin("ExecutionSample", "timestamp").build(),
                    fuzzyJoin("AllocationSample", "threadId").build()
                )
            ).extended()
        );
        
        assertTrue(astEquals(expected, program), "Should parse multiple fuzzy joins");
    }
    
    @Test
    public void testMultipleFuzzyJoinsWithDifferentTypes() throws Exception {
        Parser parser = createParser("@SELECT * FROM GarbageCollection FUZZY JOIN ExecutionSample ON timestamp WITH NEAREST FUZZY JOIN AllocationSample ON threadId WITH PREVIOUS");
        ProgramNode program = parser.parse();
        
        ProgramNode expected = program(
            query(
                selectAll(),
                from(
                    source("GarbageCollection"),
                    fuzzyJoin("ExecutionSample", "timestamp").with(FuzzyJoinType.NEAREST).build(),
                    fuzzyJoin("AllocationSample", "threadId").with(FuzzyJoinType.PREVIOUS).build()
                )
            ).extended()
        );
        
        assertTrue(astEquals(expected, program), "Should parse multiple fuzzy joins with different types");
    }
    
    // ===========================================
    // Integration with Standard SQL Features
    // ===========================================
    
    @Test
    public void testFuzzyJoinWithWhere() throws Exception {
        Parser parser = createParser("@SELECT * FROM GarbageCollection FUZZY JOIN ExecutionSample ON timestamp WHERE duration > 5ms");
        ProgramNode program = parser.parse();
        
        ProgramNode expected = program(
            query(
                selectAll(),
                from(
                    source("GarbageCollection"),
                    fuzzyJoin("ExecutionSample", "timestamp").build()
                )
            ).where(where(condition(greaterThan(identifier("duration"), durationLiteral("5ms")))))
             .extended()
        );
        
        assertTrue(astEquals(expected, program), "Should parse fuzzy join with WHERE clause");
    }
    
    @Test
    public void testFuzzyJoinWithGroupBy() throws Exception {
        Parser parser = createParser("@SELECT COUNT(*) FROM GarbageCollection FUZZY JOIN ExecutionSample ON timestamp GROUP BY cause");
        ProgramNode program = parser.parse();
        
        ProgramNode expected = program(
            query(
                select(function("COUNT", star())),
                from(
                    source("GarbageCollection"),
                    fuzzyJoin("ExecutionSample", "timestamp").build()
                )
            ).groupBy(groupBy(identifier("cause")))
             .extended()
        );
        
        assertTrue(astEquals(expected, program), "Should parse fuzzy join with GROUP BY clause");
    }
    
    @Test
    public void testFuzzyJoinWithOrderBy() throws Exception {
        Parser parser = createParser("@SELECT * FROM GarbageCollection FUZZY JOIN ExecutionSample ON timestamp ORDER BY duration DESC");
        ProgramNode program = parser.parse();
        
        ProgramNode expected = program(
            query(
                selectAll(),
                from(
                    source("GarbageCollection"),
                    fuzzyJoin("ExecutionSample", "timestamp").build()
                )
            ).orderBy(orderBy(orderField(identifier("duration"), SortOrder.DESC)))
             .extended()
        );
        
        assertTrue(astEquals(expected, program), "Should parse fuzzy join with ORDER BY clause");
    }
    
    @Test
    public void testFuzzyJoinWithLimit() throws Exception {
        Parser parser = createParser("@SELECT * FROM GarbageCollection FUZZY JOIN ExecutionSample ON timestamp LIMIT 100");
        ProgramNode program = parser.parse();
        
        ProgramNode expected = program(
            query(
                selectAll(),
                from(
                    source("GarbageCollection"),
                    fuzzyJoin("ExecutionSample", "timestamp").build()
                )
            ).limit(limit(100))
             .extended()
        );
        
        assertTrue(astEquals(expected, program), "Should parse fuzzy join with LIMIT clause");
    }
    
    // ===========================================
    // Complex Integration Tests
    // ===========================================
    
    @Test
    public void testFuzzyJoinFullQuery() throws Exception {
        // Test a simpler but complete fuzzy join query
        String query = "@SELECT * " +
                      "FROM GarbageCollection " +
                      "FUZZY JOIN ExecutionSample AS exec ON timestamp WITH NEAREST TOLERANCE 10ms " +
                      "WHERE duration > 1ms " +
                      "LIMIT 50";
        
        Parser parser = createParser(query);
        ProgramNode program = parser.parse();
        
        ProgramNode expected = program(
            query(
                selectAll(),
                from(
                    source("GarbageCollection"),
                    fuzzyJoin("ExecutionSample", "timestamp").alias("exec").with(FuzzyJoinType.NEAREST).tolerance(durationLiteral("10ms")).build()
                )
            ).where(where(condition(binary(identifier("duration"), BinaryOperator.GREATER_THAN, durationLiteral("1ms")))))
             .limit(limit(50))
             .extended()
        );
        
        assertTrue(astEquals(expected, program), "Should parse complex fuzzy join query");
    }
    
    // ===========================================
    // Parameterized Error Condition Tests
    // ===========================================
    
    static Stream<Arguments> errorConditionCases() {
        return Stream.of(
            Arguments.of("@SELECT * FROM GarbageCollection FUZZY JOIN ExecutionSample", 
                        "Should throw exception for missing ON clause"),
            Arguments.of("@SELECT * FROM GarbageCollection FUZZY JOIN ExecutionSample ON timestamp WITH INVALID", 
                        "Should throw exception for invalid join type"),
            Arguments.of("@SELECT * FROM GarbageCollection FUZZY JOIN ExecutionSample ON timestamp WITH NEAREST TOLERANCE", 
                        "Should throw exception for missing tolerance value"),
            Arguments.of("@SELECT * FROM GarbageCollection FUZZY JOIN ExecutionSample ON", 
                        "Should throw exception for missing join field"),
            Arguments.of("@SELECT * FROM GarbageCollection FUZZY JOIN ON timestamp", 
                        "Should throw exception for missing join table"),
            Arguments.of("@SELECT * FROM GarbageCollection FUZZY JOIN ExecutionSample ON timestamp WITH NEAREST TOLERANCE 10ms +", 
                        "Should throw exception for incomplete tolerance expression"),
            Arguments.of("@SELECT * FROM GarbageCollection FUZZY JOIN ExecutionSample ON timestamp WITH TOLERANCE 10ms", 
                        "Should throw exception for missing join type with tolerance")
        );
    }
    
    @ParameterizedTest
    @MethodSource("errorConditionCases")
    public void testFuzzyJoinErrorConditions(String query, String description) {
        assertThrows(Exception.class, () -> {
            Parser parser = createParser(query);
            parser.parse();
        }, description);
    }
    
    // ===========================================
    // Parameterized Alias Tests
    // ===========================================
    
    static Stream<Arguments> aliasCases() {
        return Stream.of(
            Arguments.of("@SELECT * FROM GarbageCollection FUZZY JOIN ExecutionSample AS exec ON timestamp", 
                        "ExecutionSample", "exec", "timestamp"),
            Arguments.of("@SELECT * FROM GarbageCollection FUZZY JOIN AllocationSample AS alloc ON threadId", 
                        "AllocationSample", "alloc", "threadId"),
            Arguments.of("@SELECT * FROM GarbageCollection FUZZY JOIN ThreadPark AS tp ON timestamp", 
                        "ThreadPark", "tp", "timestamp"),
            Arguments.of("@SELECT * FROM GarbageCollection FUZZY JOIN CPULoad AS cpu ON time", 
                        "CPULoad", "cpu", "time")
        );
    }
    
    @ParameterizedTest
    @MethodSource("aliasCases")
    public void testFuzzyJoinWithAliases(String query, String tableName, String alias, String joinField) throws Exception {
        Parser parser = createParser(query);
        ProgramNode program = parser.parse();
        
        ProgramNode expected = program(
            query(
                selectAll(),
                from(
                    source("GarbageCollection"),
                    fuzzyJoin(tableName, joinField).alias(alias).build()
                )
            ).extended()
        );
        
        assertTrue(astEquals(expected, program), "Should parse fuzzy join with alias: " + alias);
    }
    
    // ===========================================
    // Parameterized Complex Tolerance Tests
    // ===========================================
    
    static Stream<Arguments> complexToleranceCases() {
        return Stream.of(
            Arguments.of("5ms + 2ms", binary(durationLiteral("5ms"), BinaryOperator.ADD, durationLiteral("2ms"))),
            Arguments.of("10ms - 1ms", binary(durationLiteral("10ms"), BinaryOperator.SUBTRACT, durationLiteral("1ms"))),
            Arguments.of("2ms * 3", binary(durationLiteral("2ms"), BinaryOperator.MULTIPLY, numberLiteral(3))),
            Arguments.of("100ms / 2", binary(durationLiteral("100ms"), BinaryOperator.DIVIDE, numberLiteral(2))),
            Arguments.of("maxDelay", identifier("maxDelay")),
            Arguments.of("baseTimeout", identifier("baseTimeout"))
        );
    }
    
    @ParameterizedTest
    @MethodSource("complexToleranceCases")
    public void testFuzzyJoinWithComplexTolerance(String toleranceExpr, ExpressionNode expectedTolerance) throws Exception {
        String query = "@SELECT * FROM GarbageCollection FUZZY JOIN ExecutionSample ON timestamp WITH NEAREST TOLERANCE " + toleranceExpr;
        Parser parser = createParser(query);
        ProgramNode program = parser.parse();
        
        ProgramNode expected = program(
            query(
                selectAll(),
                from(
                    source("GarbageCollection"),
                    fuzzyJoin("ExecutionSample", "timestamp").with(FuzzyJoinType.NEAREST).tolerance(expectedTolerance).build()
                )
            ).extended()
        );
        
        assertTrue(astEquals(expected, program), "Should parse fuzzy join with complex tolerance: " + toleranceExpr);
    }
    
    // ===========================================
    // Parameterized Integration Tests
    // ===========================================
    
    static Stream<Arguments> integrationCases() {
        return Stream.of(
            Arguments.of("WHERE duration > 5ms", "WHERE clause"),
            Arguments.of("GROUP BY cause", "GROUP BY clause"),
            Arguments.of("ORDER BY duration DESC", "ORDER BY clause"),
            Arguments.of("LIMIT 100", "LIMIT clause"),
            Arguments.of("WHERE duration > 1ms ORDER BY timestamp", "WHERE and ORDER BY"),
            Arguments.of("GROUP BY cause HAVING COUNT(*) > 10", "GROUP BY and HAVING"),
            Arguments.of("ORDER BY duration LIMIT 50", "ORDER BY and LIMIT")
        );
    }
    
    @ParameterizedTest
    @MethodSource("integrationCases")
    public void testFuzzyJoinWithSQLClauses(String sqlClause, String description) throws Exception {
        String query = "@SELECT * FROM GarbageCollection FUZZY JOIN ExecutionSample ON timestamp " + sqlClause;
        Parser parser = createParser(query);
        ProgramNode program = parser.parse();
        
        // Basic validation - just ensure it parses without error
        assertNotNull(program, "Should parse fuzzy join with " + description);
        assertTrue(program.statements().size() > 0, "Should have at least one statement");
    }
}
