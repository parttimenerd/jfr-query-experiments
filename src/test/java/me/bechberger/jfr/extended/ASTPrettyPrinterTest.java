package me.bechberger.jfr.extended;

import me.bechberger.jfr.extended.ast.ASTNodes.*;
import me.bechberger.jfr.extended.ast.ASTNode;
import me.bechberger.jfr.extended.ast.Location;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static me.bechberger.jfr.extended.ASTBuilder.*;
import static me.bechberger.jfr.extended.table.CellValue.*;

/**
 * Comprehensive test suite for the AST pretty printer and AST round-trip functionality.
 * <p>
 * This class covers:
 * <ul>
 *   <li>Pretty printing of all supported AST node types</li>
 *   <li>Round-trip parsing and formatting</li>
 *   <li>Edge cases, error handling, and advanced query structures</li>
 *   <li>Structural AST equality checks for round-trip and parser tests</li>
 * </ul>
 * <p>
 * Uses the builder pattern for AST node construction and JUnit5 parameterized tests for coverage.
 */
public class ASTPrettyPrinterTest {

    /**
     * Helper method to create a boolean literal
     */
    private static LiteralNode booleanLiteral(boolean value) {
        return new LiteralNode(new BooleanValue(value), new Location(1, 1));
    }

    /**
     * Compare two AST nodes for structural equality using the ASTBuilder's deep equality method
     */
    private boolean astEquals(ASTNode node1, ASTNode node2) {
        return ASTBuilder.astEquals(node1, node2);
    }

    private Parser createParser(String query) throws Exception {
        Lexer lexer = new Lexer(query);
        List<Token> tokens = lexer.tokenize();
        return new Parser(tokens, query);
    }

    /**
     * Helper method to normalize query strings by removing newlines and excess whitespace
     */
    private String normalizeQuery(String query) {
        return query.replaceAll("\\s+", " ").trim();
    }

    // ===========================================
    // Basic Query Formatting Tests
    // ===========================================

    static Stream<Arguments> basicQueryCases() {
        return Stream.of(
            Arguments.of("Simple SELECT",
                        program(query(selectAll(), from(source("GarbageCollection"))).extended().build()),
                        "@SELECT *\nFROM GarbageCollection"),

            Arguments.of("SELECT with specific fields",
                        program(query(select(selectItem(identifier("timestamp")), selectItem(identifier("duration"))),
                              from(source("GarbageCollection"))).extended().build()),
                        "@SELECT timestamp, duration\nFROM GarbageCollection"),

            Arguments.of("SELECT with alias",
                        program(query(select(identifierWithAlias("timestamp", "ts")),
                              from(source("GarbageCollection"))).extended().build()),
                        "@SELECT timestamp AS ts\nFROM GarbageCollection"),

            Arguments.of("SELECT with WHERE",
                        program(query(selectAll(), from(source("GarbageCollection")))
                            .where(where(condition(binary(identifier("duration"), BinaryOperator.GREATER_THAN, durationLiteral("5ms")))))
                            .extended().build()),
                        "@SELECT *\nFROM GarbageCollection\nWHERE (duration > 5ms)"),

            Arguments.of("SELECT with ORDER BY",
                        program(query(selectAll(), from(source("GarbageCollection")))
                            .orderBy(orderBy(orderField(identifier("duration"), SortOrder.DESC)))
                            .extended().build()),
                        "@SELECT *\nFROM GarbageCollection\nORDER BY duration DESC"),

            Arguments.of("SELECT with LIMIT",
                        program(query(selectAll(), from(source("GarbageCollection")))
                            .limit(limit(100))
                            .extended().build()),
                        "@SELECT *\nFROM GarbageCollection\nLIMIT 100")
        );
    }

    /**
     * Test that just uses the built-in format() methods in AST nodes.
     * We don't need a separate ASTPrettyPrinter class anymore.
     */
    @ParameterizedTest
    @MethodSource("basicQueryCases")
    public void testBasicQueryFormatting(String description, ProgramNode program, String expected) {
        String result = program.format();
        assertEquals(expected, result, description + " should format correctly");
    }

    // ===========================================
    // Fuzzy Join Formatting Tests
    // ===========================================

    static Stream<Arguments> fuzzyJoinCases() {
        return Stream.of(
            Arguments.of("Basic fuzzy join",
                        program(query(selectAll(), from(source("GarbageCollection"),
                                              fuzzyJoin("ExecutionSample", "timestamp").build())).extended().build()),
                        "@SELECT *\nFROM GarbageCollection\n  FUZZY JOIN ExecutionSample ON timestamp"),

            Arguments.of("Fuzzy join with type",
                        program(query(selectAll(), from(source("GarbageCollection"),
                                              fuzzyJoin("ExecutionSample", "timestamp").with(FuzzyJoinType.PREVIOUS).build())).extended().build()),
                        "@SELECT *\nFROM GarbageCollection\n  FUZZY JOIN ExecutionSample ON timestamp WITH PREVIOUS"),

            Arguments.of("Fuzzy join with tolerance",
                        program(query(selectAll(), from(source("GarbageCollection"),
                                              fuzzyJoin("ExecutionSample", "timestamp").with(FuzzyJoinType.NEAREST).tolerance(durationLiteral("10ms")).build())).extended().build()),
                        "@SELECT *\nFROM GarbageCollection\n  FUZZY JOIN ExecutionSample ON timestamp TOLERANCE 10ms"),

            Arguments.of("Fuzzy join with alias",
                        program(query(selectAll(), from(source("GarbageCollection"),
                                              fuzzyJoin("ExecutionSample", "timestamp").alias("exec").build())).extended().build()),
                        "@SELECT *\nFROM GarbageCollection\n  FUZZY JOIN ExecutionSample AS exec ON timestamp")
        );
    }

    @ParameterizedTest
    @MethodSource("fuzzyJoinCases")
    public void testFuzzyJoinFormatting(String description, ProgramNode program, String expected) {
        String result = program.format();
        assertEquals(expected, result, description + " should format correctly");
    }

    // ===========================================
    // Expression Formatting Tests
    // ===========================================

    static Stream<Arguments> expressionCases() {
        return Stream.of(
            Arguments.of("Binary expression",
                        binary(identifier("duration"), BinaryOperator.GREATER_THAN, durationLiteral("5ms")),
                        "(duration > 5ms)"),

            Arguments.of("Complex binary expression",
                        binary(binary(identifier("a"), BinaryOperator.ADD, identifier("b")),
                               BinaryOperator.MULTIPLY, numberLiteral(2)),
                        "((a + b) * 2)"),

            Arguments.of("Function call",
                        function("COUNT", star()),
                        "COUNT(*)"),

            Arguments.of("Function call with alias",
                        function("AVG", identifier("duration")),
                        "AVG(duration)"),

            Arguments.of("Field access",
                        fieldAccess("gc", "cause"),
                        "gc.cause"),

            Arguments.of("Unary expression",
                        unary(UnaryOperator.NOT, binary(identifier("active"), BinaryOperator.EQUALS, booleanLiteral(true))),
                        "NOT (active = TRUE)")
        );
    }

    @ParameterizedTest
    @MethodSource("expressionCases")
    public void testExpressionFormatting(String description, ExpressionNode expression, String expected) {
        String result = expression.format();
        assertEquals(expected, result, description + " should format correctly");
    }

    // ===========================================
    // Literal Formatting Tests
    // ===========================================

    static Stream<Arguments> literalCases() {
        return Stream.of(
            Arguments.of("String literal",
                        stringLiteral("test"),
                        "'test'"),

            Arguments.of("String literal with quotes",
                        stringLiteral("test's value"),
                        "'test''s value'"),

            Arguments.of("Number literal",
                        numberLiteral(42),
                        "42"),

            Arguments.of("Duration literal",
                        durationLiteral("5ms"),
                        "5ms"),

            Arguments.of("Boolean literal true",
                        booleanLiteral(true),
                        "TRUE"),

            Arguments.of("Boolean literal false",
                        booleanLiteral(false),
                        "FALSE")
        );
    }

    @ParameterizedTest
    @MethodSource("literalCases")
    public void testLiteralFormatting(String description, LiteralNode literal, String expected) {
        String result = literal.format();
        assertEquals(expected, result, description + " should format correctly");
    }

    // ===========================================
    // Complex Query Formatting Tests
    // ===========================================

    @Test
    public void testComplexQueryFormatting() {
        ProgramNode program = program(
            query(
                select(
                    selectItem(identifier("timestamp")),
                    selectItem(identifier("duration")),
                    selectItem(function("COUNT", star()))
                ),
                from(
                    source("GarbageCollection", "gc"),
                    fuzzyJoin("ExecutionSample", "timestamp").alias("exec").with(FuzzyJoinType.NEAREST).tolerance(durationLiteral("10ms")).build()
                )
            ).where(where(condition(binary(identifier("duration"), BinaryOperator.GREATER_THAN, durationLiteral("1ms")))))
             .groupBy(groupBy(identifier("cause")))
             .orderBy(orderBy(orderField(identifier("duration"), SortOrder.DESC)))
             .limit(limit(50))
             .extended().build()
        );

        String result = program.format();

        // Verify the essential parts are present
        assertTrue(result.contains("@SELECT timestamp, duration, COUNT(*)"));
        assertTrue(result.contains("FROM GarbageCollection AS gc"));
        assertTrue(result.contains("FUZZY JOIN ExecutionSample AS exec ON timestamp"));
        assertTrue(result.contains("WHERE (duration > 1ms)"));
        assertTrue(result.contains("GROUP BY cause"));
        assertTrue(result.contains("ORDER BY duration DESC"));
        assertTrue(result.contains("LIMIT 50"));
    }

    // ===========================================
    // Round-trip Tests
    // ===========================================

    static Stream<Arguments> roundTripCases() {
        return Stream.of(
            Arguments.of("@SELECT * FROM GarbageCollection"),
            Arguments.of("@SELECT timestamp, duration FROM GarbageCollection WHERE duration > 5ms"),
            Arguments.of("@SELECT * FROM GarbageCollection FUZZY JOIN ExecutionSample ON timestamp"),
            Arguments.of("@SELECT * FROM GarbageCollection ORDER BY duration DESC LIMIT 10"),
            Arguments.of("@SELECT COUNT(*) FROM GarbageCollection GROUP BY cause")
        );
    }

    @ParameterizedTest
    @MethodSource("roundTripCases")
    public void testRoundTripFormatting(String originalQuery) throws Exception {
        // Parse the original query
        Parser parser = createParser(originalQuery);
        ProgramNode program = parser.parse();

        // Format it back to string
        String formatted = program.format();

        // Parse the formatted string again
        Parser parser2 = createParser(formatted);
        ProgramNode program2 = parser2.parse();

        // Compare normalized strings for output purposes
        String normalizedOriginal = normalizeQuery(originalQuery);
        String normalizedFormatted = normalizeQuery(formatted);

        // Check if the ASTs are structurally equivalent
        boolean isEqual = astEquals(program, program2);

        assertTrue(isEqual,
                  "Round-trip formatting should preserve AST structure.\nOriginal: " + normalizedOriginal +
                  "\nFormatted: " + normalizedFormatted);
    }

    // ===========================================
    // Special Node Tests
    // ===========================================

    @Test
    public void testRawJfrQueryFormatting() {
        RawJfrQueryNode rawQuery = new RawJfrQueryNode("SELECT * FROM jdk.GarbageCollection", new Location(1, 1));
        String result = rawQuery.format();
        assertEquals("SELECT * FROM jdk.GarbageCollection", result);
    }

    @Test
    public void testViewDefinitionFormatting() {
        QueryNode queryNode = query(selectAll(), from(source("GarbageCollection"))).build();
        ViewDefinitionNode view = new ViewDefinitionNode("MyView", queryNode, new Location(1, 1));
        String result = view.format();
        assertEquals("VIEW MyView AS @SELECT *\nFROM GarbageCollection", result);
    }

    @Test
    public void testVariableDeclarationFormatting() {
        VariableDeclarationNode varDecl = new VariableDeclarationNode("maxDuration", durationLiteral("10ms"), new Location(1, 1));
        String result = varDecl.format();
        assertEquals("maxDuration := 10ms", result);
    }

    @Test
    public void testAssignmentFormatting() {
        QueryNode queryNode = query(selectAll(), from(source("GarbageCollection"))).build();
        AssignmentNode assignment = new AssignmentNode("result", queryNode, new Location(1, 1));
        String result = assignment.format();
        assertEquals("result := @SELECT *\nFROM GarbageCollection", result);
    }

    // ===========================================
    // Edge Cases and Error Handling
    // ===========================================

    @Test
    public void testEmptyProgramFormatting() {
        ProgramNode emptyProgram = program();
        String result = emptyProgram.format();
        assertEquals("", result);
    }

    @Test
    public void testNullHandling() {
        // Test nodes with null optional fields
        SourceNode sourceWithoutAlias = new SourceNode("TestTable", null, new Location(1, 1));
        String result = sourceWithoutAlias.format();
        assertEquals("TestTable", result);
    }

    @Test
    public void testNestedQueryFormatting() {
        NestedQueryNode nestedQuery = new NestedQueryNode("SELECT * FROM jdk.GarbageCollection", new Location(1, 1));
        String result = nestedQuery.format();
        assertEquals("SELECT * FROM jdk.GarbageCollection", result);
    }

    // ===========================================
    // Nested Queries and Subqueries Tests
    // ===========================================

    static Stream<Arguments> nestedQueryCases() {
        return Stream.of(
            Arguments.of("Simple nested JFR query",
                        nestedQuery("SELECT * FROM jdk.GarbageCollection"),
                        "SELECT * FROM jdk.GarbageCollection"),

            Arguments.of("Complex nested JFR query",
                        nestedQuery("SELECT timestamp, duration FROM jdk.GarbageCollection WHERE cause = 'System.gc()'"),
                        "SELECT timestamp, duration FROM jdk.GarbageCollection WHERE cause = 'System.gc()'"),

            Arguments.of("Subquery in FROM clause",
                        program(query(selectAll(),
                              from(subquery(query(select(selectItem(identifier("timestamp")), selectItem(identifier("duration"))),
                                                  from(source("GarbageCollection")))
                                                  .where(where(condition(binary(identifier("duration"), BinaryOperator.GREATER_THAN, durationLiteral("10ms")))))
                                                  .build(), "filtered_gc"))).extended().build()),
                        "@SELECT *\nFROM (@SELECT timestamp, duration\nFROM GarbageCollection\nWHERE (duration > 10ms)) AS filtered_gc"),

            Arguments.of("Query with nested subquery and joins",
                        program(query(select(selectItem(identifier("gc.timestamp")), selectItem(identifier("exec.threadName"))),
                              from(subquery(query(selectAll(), from(source("GarbageCollection")))
                                           .where(where(condition(binary(identifier("duration"), BinaryOperator.GREATER_THAN, durationLiteral("5ms")))))
                                           .build(), "gc"),
                                   fuzzyJoin("ExecutionSample", "timestamp").alias("exec").build())).extended().build()),
                        "@SELECT gc.timestamp, exec.threadName\nFROM (@SELECT *\nFROM GarbageCollection\nWHERE (duration > 5ms)) AS gc\n  FUZZY JOIN ExecutionSample AS exec ON timestamp")
        );
    }

    @ParameterizedTest
    @MethodSource("nestedQueryCases")
    public void testNestedQueryFormatting(String description, ASTNode queryOrExpression, String expected) {
        String result = queryOrExpression.format();
        assertEquals(expected, result, description + " should format correctly");
    }

    // ===========================================
    // Standard Join Tests
    // ===========================================

    static Stream<Arguments> standardJoinCases() {
        return Stream.of(
            Arguments.of("INNER JOIN",
                        program(query(selectAll(),
                              from(source("GarbageCollection"),
                                   standardJoin("ExecutionSample", "id", "sampleId").inner().build())).extended().build()),
                        "@SELECT *\nFROM GarbageCollection\n  INNER JOIN ExecutionSample ON id = sampleId"),

            Arguments.of("LEFT JOIN with alias",
                        program(query(select(selectItem(identifier("gc.timestamp")), selectItem(identifier("exec.threadName"))),
                              from(source("GarbageCollection", "gc"),
                                   standardJoin("ExecutionSample", "gcId", "id").alias("exec").left().build())).extended().build()),
                        "@SELECT gc.timestamp, exec.threadName\nFROM GarbageCollection AS gc\n  LEFT JOIN ExecutionSample AS exec ON gcId = id"),

            Arguments.of("RIGHT JOIN",
                        program(query(selectAll(),
                              from(source("GarbageCollection"),
                                   standardJoin("ExecutionSample", "id", "sampleId").right().build())).extended().build()),
                        "@SELECT *\nFROM GarbageCollection\n  RIGHT JOIN ExecutionSample ON id = sampleId"),

            Arguments.of("FULL JOIN",
                        program(query(selectAll(),
                              from(source("GarbageCollection"),
                                   standardJoin("ExecutionSample", "id", "sampleId").full().build())).extended().build()),
                        "@SELECT *\nFROM GarbageCollection\n  FULL JOIN ExecutionSample ON id = sampleId")
        );
    }

    @ParameterizedTest
    @MethodSource("standardJoinCases")
    public void testStandardJoinFormatting(String description, ProgramNode program, String expected) {
        String result = program.format();
        assertEquals(expected, result, description + " should format correctly");
    }

    // ===========================================
    // Complex Expression Tests
    // ===========================================

    static Stream<Arguments> complexExpressionCases() {
        return Stream.of(
            Arguments.of("Arithmetic with memory sizes",
                        binary(identifier("heapUsed"), BinaryOperator.ADD, memorySizeLiteral("100MB")),
                        "(heapUsed + 100MB)"),

            Arguments.of("Complex arithmetic expression",
                        binary(
                            binary(identifier("duration"), BinaryOperator.ADD, durationLiteral("5ms")),
                            BinaryOperator.MULTIPLY,
                            binary(numberLiteral(2), BinaryOperator.DIVIDE, identifier("count"))
                        ),
                        "((duration + 5ms) * (2 / count))"),

            Arguments.of("Nested function calls",
                        function("MAX", function("AVG", identifier("duration"))),
                        "MAX(AVG(duration))"),

            Arguments.of("Complex condition with NOT",
                        unary(UnaryOperator.NOT,
                              binary(
                                  binary(identifier("active"), BinaryOperator.EQUALS, booleanLiteral(true)),
                                  BinaryOperator.AND,
                                  binary(identifier("duration"), BinaryOperator.GREATER_THAN, durationLiteral("100ms"))
                              )),
                        "NOT ((active = TRUE) AND (duration > 100ms))"),

            Arguments.of("String literal with escape",
                        stringLiteral("test's \"quoted\" value"),
                        "'test''s \"quoted\" value'")
        );
    }

    @ParameterizedTest
    @MethodSource("complexExpressionCases")
    public void testComplexExpressionFormatting(String description, ExpressionNode expression, String expected) {
        String result = expression.format();
        assertEquals(expected, result, description + " should format correctly");
    }

    // ===========================================
    // Advanced Query Structure Tests
    // ===========================================

    static Stream<Arguments> advancedQueryCases() {
        return Stream.of(
            Arguments.of("Query with all clauses",
                        program(query(select(selectItem(identifier("gc.timestamp")), functionWithAlias("COUNT", "count", star())),
                              from(source("GarbageCollection", "gc")))
                            .where(where(condition(binary(identifier("duration"), BinaryOperator.GREATER_THAN, durationLiteral("10ms")))))
                            .groupBy(groupBy(identifier("cause")))
                            .having(having(condition(binary(function("COUNT", star()), BinaryOperator.GREATER_THAN, numberLiteral(5)))))
                            .orderBy(orderBy(orderField(identifier("timestamp"), SortOrder.ASC), orderField(identifier("count"), SortOrder.DESC)))
                            .limit(limit(100))
                            .extended().build()),
                        "@SELECT gc.timestamp, COUNT(*) AS count\nFROM GarbageCollection AS gc\nWHERE (duration > 10ms)\nGROUP BY cause\nHAVING (COUNT(*) > 5)\nORDER BY timestamp ASC, count DESC\nLIMIT 100"),

            Arguments.of("Query with multiple fuzzy joins",
                        program(query(select(selectItem(identifier("gc.timestamp")), selectItem(identifier("exec.threadName")), selectItem(identifier("alloc.size"))),
                              from(source("GarbageCollection", "gc"),
                                   fuzzyJoin("ExecutionSample", "timestamp").alias("exec").with(FuzzyJoinType.NEAREST).tolerance(durationLiteral("1ms")).build(),
                                   fuzzyJoin("AllocationSample", "timestamp").alias("alloc").with(FuzzyJoinType.PREVIOUS).build())).extended().build()),
                        "@SELECT gc.timestamp, exec.threadName, alloc.size\nFROM GarbageCollection AS gc\n  FUZZY JOIN ExecutionSample AS exec ON timestamp TOLERANCE 1ms\n  FUZZY JOIN AllocationSample AS alloc ON timestamp WITH PREVIOUS"),

            Arguments.of("Deeply nested subquery",
                        program(query(selectAll(),
                              from(subquery(
                                      query(select(selectItem(identifier("outer.timestamp"))),
                                            from(subquery(
                                                    query(selectAll(), from(source("GarbageCollection")))
                                                        .where(where(condition(binary(identifier("duration"), BinaryOperator.GREATER_THAN, durationLiteral("50ms")))))
                                                        .build(), "inner")))
                                          .build(), "outer"))).extended().build()),
                        "@SELECT *\nFROM (@SELECT outer.timestamp\nFROM (@SELECT *\nFROM GarbageCollection\nWHERE (duration > 50ms)) AS inner) AS outer")
        );
    }

    @ParameterizedTest
    @MethodSource("advancedQueryCases")
    public void testAdvancedQueryFormatting(String description, ProgramNode program, String expected) {
        String result = program.format();
        assertEquals(expected, result, description + " should format correctly");
    }

    // ===========================================
    // Statement and Program Tests
    // ===========================================

    static Stream<Arguments> statementCases() {
        return Stream.of(
            Arguments.of("Assignment with literal value",
                        program(new AssignmentNode("maxDuration",
                                                  query(select(selectItem(durationLiteral("100ms"))),
                                                       from(source("DUMMY"))).build(),
                                                  new Location(1, 1))),
                        "maxDuration := @SELECT 100ms\nFROM DUMMY"),

            Arguments.of("Assignment statement",
                        program(new AssignmentNode("slowGCs",
                                                  query(selectAll(), from(source("GarbageCollection")))
                                                      .where(where(condition(binary(identifier("duration"), BinaryOperator.GREATER_THAN, durationLiteral("50ms")))))
                                                      .build(),
                                                  new Location(1, 1))),
                        "slowGCs := @SELECT *\nFROM GarbageCollection\nWHERE (duration > 50ms)"),

            Arguments.of("SHOW EVENTS statement",
                        program(new ShowEventsNode(new Location(1, 1))),
                        "SHOW EVENTS"),

            Arguments.of("SHOW FIELDS statement",
                        program(new ShowFieldsNode("GarbageCollection", new Location(1, 1))),
                        "SHOW FIELDS GarbageCollection"),

            Arguments.of("Multiple statements",
                        program(
                            new AssignmentNode("threshold",
                                              query(select(selectItem(durationLiteral("10ms"))),
                                                   from(source("DUMMY"))).build(),
                                              new Location(1, 1)),
                            new AssignmentNode("filtered",
                                              query(selectAll(), from(source("GarbageCollection")))
                                                  .where(where(condition(binary(identifier("duration"), BinaryOperator.GREATER_THAN, identifier("threshold")))))
                                                  .build(),
                                              new Location(1, 1))
                        ),
                        "threshold := @SELECT 10ms\nFROM DUMMY\nfiltered := @SELECT *\nFROM GarbageCollection\nWHERE (duration > threshold)")
        );
    }

    @ParameterizedTest
    @MethodSource("statementCases")
    public void testStatementFormatting(String description, ProgramNode program, String expected) {
        String result = program.format();
        assertEquals(expected, result, description + " should format correctly");
    }
}