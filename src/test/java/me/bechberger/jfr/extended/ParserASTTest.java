package me.bechberger.jfr.extended;

import me.bechberger.jfr.extended.ast.ASTNodes.*;
import me.bechberger.jfr.extended.table.CellValue;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.Arguments;
import static org.junit.jupiter.api.Assertions.*;
import static me.bechberger.jfr.extended.ASTBuilder.*;

import java.util.List;
import java.util.stream.Stream;
import java.util.stream.Stream;

/**
 * Parser tests using the DSL-based approach
 */
public class ParserASTTest {
    
    private Parser createParser(String query) throws Exception {
        Lexer lexer = new Lexer(query);
        List<Token> tokens = lexer.tokenize();
        return new Parser(tokens);
    }
    
    @Test
    public void testSimpleSelectQuery() throws Exception {
        Parser parser = createParser("SELECT * FROM GarbageCollection");
        ProgramNode program = parser.parse();
        
        ProgramNode expected = program(
            rawJfrQuery("SELECT * FROM GarbageCollection")
        );
        
        assertTrue(astEquals(expected, program), "AST should match expected structure");
    }
    
    @Test
    public void testExtendedQuery() throws Exception {
        Parser parser = createParser("@SELECT * FROM GarbageCollection");
        ProgramNode program = parser.parse();
        
        ProgramNode expected = program(
            query(selectAll(), from(source("GarbageCollection")))
                .extended()
        );
        
        assertTrue(astEquals(expected, program), "AST should match expected structure");
    }
    
    @Test
    public void testSelectWithFields() throws Exception {
        Parser parser = createParser("@SELECT duration, stackTrace FROM ExecutionSample");
        ProgramNode program = parser.parse();
        
        ProgramNode expected = program(
            query(select(identifier("duration"), identifier("stackTrace")), 
                  from(source("ExecutionSample")))
                .extended()
        );
        
        assertTrue(astEquals(expected, program), "AST should match expected structure");
    }
    
    @Test
    public void testSelectWithAlias() throws Exception {
        Parser parser = createParser("@SELECT duration AS d FROM ExecutionSample AS es");
        ProgramNode program = parser.parse();
        
        ProgramNode expected = program(
            query(select(selectItem(identifier("duration"), "d")), 
                  from(source("ExecutionSample", "es")))
                .extended()
        );
        
        assertTrue(astEquals(expected, program), "AST should match expected structure");
    }
    
    @Test
    public void testFunctionCall() throws Exception {
        Parser parser = createParser("@SELECT COUNT(*) FROM GarbageCollection");
        ProgramNode program = parser.parse();
        
        ProgramNode expected = program(
            query(
                select(
                    function("COUNT", star())
                ),
                from(source("GarbageCollection"))
            )
            .extended()
        );
        
        assertTrue(astEquals(expected, program), "AST should match expected structure");
    }
    
    @Test
    public void testWhereClause() throws Exception {
        Parser parser = createParser("@SELECT * FROM GarbageCollection WHERE duration > 10");
        ProgramNode program = parser.parse();
        
        ProgramNode expected = program(
            query(selectAll(), from(source("GarbageCollection")))
                .where(where(condition(
                    greaterThan(identifier("duration"), numberLiteral(10.0))
                )))
                .extended()
        );
        
        assertTrue(astEquals(expected, program), "AST should match expected structure");
    }
    
    @Test
    public void testArithmeticWithTime() throws Exception {
        Parser parser = createParser("@SELECT duration + 5ms FROM GarbageCollection");
        ProgramNode program = parser.parse();
        
        ProgramNode expected = program(
            query(select(add(identifier("duration"), durationLiteral("5ms"))), 
                  from(source("GarbageCollection")))
                .extended()
        );
        
        assertTrue(astEquals(expected, program), "AST should match expected structure");
    }
    
    @Test
    public void testArithmeticWithMemory() throws Exception {
        Parser parser = createParser("@SELECT heapUsed - 100MB FROM HeapSummary");
        ProgramNode program = parser.parse();
        
        ProgramNode expected = program(
            query(select(subtract(identifier("heapUsed"), memorySizeLiteral("100MB"))), 
                  from(source("HeapSummary")))
                .extended()
        );
        
        assertTrue(astEquals(expected, program), "AST should match expected structure");
    }
    
    @Test
    public void testComplexArithmeticExpression() throws Exception {
        Parser parser = createParser("@SELECT (duration + 5ms) * 2 / heapSize FROM GarbageCollection");
        ProgramNode program = parser.parse();
        
        ProgramNode expected = program(
            query(select(divide(
                    multiply(
                        add(identifier("duration"), durationLiteral("5ms")),
                        numberLiteral(2.0)
                    ),
                    identifier("heapSize")
                )), 
                from(source("GarbageCollection")))
                .extended()
        );
        
        assertTrue(astEquals(expected, program), "AST should match expected structure");
    }
    
    @Test
    public void testTimeArithmeticInWhere() throws Exception {
        Parser parser = createParser("@SELECT * FROM GarbageCollection WHERE duration > 10ms + 5ms");
        ProgramNode program = parser.parse();
        
        ProgramNode expected = program(
            query(selectAll(), from(source("GarbageCollection")))
                .where(where(condition(
                    greaterThan(
                        identifier("duration"),
                        add(durationLiteral("10ms"), durationLiteral("5ms"))
                    )
                )))
                .extended()
        );
        
        assertTrue(astEquals(expected, program), "AST should match expected structure");
    }
    
    @Test
    public void testGroupByClause() throws Exception {
        Parser parser = createParser("@SELECT thread, COUNT(*) FROM ExecutionSample GROUP BY thread");
        ProgramNode program = parser.parse();
        
        ProgramNode expected = program(
            query(select(identifier("thread"), function("COUNT", star())), 
                 from(source("ExecutionSample")))
                .groupBy(groupBy(identifier("thread")))
                .extended()
        );
        
        assertTrue(astEquals(expected, program), "AST should match expected structure");
    }
    
    @Test
    public void testHavingClause() throws Exception {
        Parser parser = createParser("@SELECT thread, COUNT(*) FROM ExecutionSample GROUP BY thread HAVING COUNT(*) > 10");
        ProgramNode program = parser.parse();
        
        ProgramNode expected = program(
            query(select(identifier("thread"), function("COUNT", star())), 
                 from(source("ExecutionSample")))
                .groupBy(groupBy(identifier("thread")))
                .having(having(condition(
                    greaterThan(
                        function("COUNT", star()),
                        numberLiteral(10.0)
                    )
                )))
                .extended()
        );
        
        assertTrue(astEquals(expected, program), "AST should match expected structure");
    }
    
    @Test
    public void testOrderByClause() throws Exception {
        Parser parser = createParser("@SELECT * FROM GarbageCollection ORDER BY duration DESC");
        ProgramNode program = parser.parse();
        
        ProgramNode expected = program(
            query(selectAll(), from(source("GarbageCollection")))
                .orderBy(orderBy(orderField(identifier("duration"), SortOrder.DESC)))
                .extended()
        );
        
        assertTrue(astEquals(expected, program), "AST should match expected structure");
    }
    
    @Test
    public void testLimitClause() throws Exception {
        Parser parser = createParser("@SELECT * FROM GarbageCollection LIMIT 100");
        ProgramNode program = parser.parse();
        
        ProgramNode expected = program(
            query(selectAll(), from(source("GarbageCollection")))
                .limit(limit(100))
                .extended()
        );
        
        assertTrue(astEquals(expected, program), "AST should match expected structure");
    }
    
    @Test
    public void testAssignmentStatement() throws Exception {
        Parser parser = createParser("slowGCs := @SELECT * FROM GarbageCollection WHERE duration > 100");
        ProgramNode program = parser.parse();
        
        ProgramNode expected = program(
            assignment("slowGCs",
                query(selectAll(), from(source("GarbageCollection")))
                    .where(where(condition(
                        greaterThan(identifier("duration"), numberLiteral(100.0))
                    )))
                    .build()
            )
        );
        
        assertTrue(astEquals(expected, program), "AST should match expected structure");
    }
    
    @Test
    public void testViewDefinition() throws Exception {
        Parser parser = createParser("VIEW FastGC AS @SELECT * FROM GarbageCollection WHERE duration < 10");
        ProgramNode program = parser.parse();
        
        ProgramNode expected = program(
            view("FastGC",
                query(selectAll(), from(source("GarbageCollection")))
                    .where(where(condition(
                        lessThan(identifier("duration"), numberLiteral(10.0))
                    )))
                    .build()
            )
        );
        
        assertTrue(astEquals(expected, program), "AST should match expected structure");
    }
    
    @Test
    public void testShowEventsQuery() throws Exception {
        Parser parser = createParser("SHOW EVENTS");
        ProgramNode program = parser.parse();
        
        ProgramNode expected = program(showEvents());
        
        assertTrue(astEquals(expected, program), "AST should match expected structure");
    }
    
    @Test
    public void testShowFieldsQuery() throws Exception {
        Parser parser = createParser("SHOW FIELDS GarbageCollection");
        ProgramNode program = parser.parse();
        
        ProgramNode expected = program(showFields("GarbageCollection"));
        
        assertTrue(astEquals(expected, program), "AST should match expected structure");
    }
    
    @Test
    public void testNestedQuery() throws Exception {
        Parser parser = createParser("@SELECT * FROM (SELECT * FROM GarbageCollection)");
        ProgramNode program = parser.parse();
        
        ProgramNode expected = program(
            query(
                selectAll(),
                from(subquery(rawJfrQuery("SELECT * FROM GarbageCollection")))
            ).extended()
        );
        
        assertTrue(astEquals(expected, program), "AST should match expected structure");
    }
    
    @Test
    public void testComplexQueryWithAllClauses() throws Exception {
        Parser parser = createParser("""
            @SELECT 
                thread, 
                COUNT(*) AS count, 
                AVG(duration) AS avg_duration
            FROM ExecutionSample AS es
            WHERE 
                duration > 10 AND 
                stackTrace LIKE '%MyClass%'
            GROUP BY thread
            HAVING COUNT(*) > 5
            ORDER BY avg_duration DESC
            LIMIT 50
            """);
        ProgramNode program = parser.parse();
        
        ProgramNode expected = program(
            query(select(
                    selectItem(identifier("thread")),
                    selectItem(function("COUNT", star()), "count"),
                    selectItem(function("AVG", identifier("duration")), "avg_duration")
                ), 
                from(source("ExecutionSample", "es")))
                .where(where(condition(
                    and(
                        greaterThan(identifier("duration"), numberLiteral(10.0)),
                        like(identifier("stackTrace"), stringLiteral("%MyClass%"))
                    )
                )))
                .groupBy(groupBy(identifier("thread")))
                .having(having(condition(
                    greaterThan(
                        function("COUNT", star()),
                        numberLiteral(5.0)
                    )
                )))
                .orderBy(orderBy(orderField(identifier("avg_duration"), SortOrder.DESC)))
                .limit(limit(50))
                .extended()
        );
        
        assertTrue(astEquals(expected, program), "AST should match expected structure");
    }
    
    @Test
    public void testSubqueryInFrom() throws Exception {
        Parser parser = createParser("@SELECT * FROM (SELECT * FROM GarbageCollection WHERE duration > 100) AS slowGCs");
        ProgramNode program = parser.parse();
        
        ProgramNode expected = program(
            query(selectAll(),
                from(subquery(
                    rawJfrQuery("SELECT * FROM GarbageCollection WHERE duration > 100"),
                    "slowGCs"
                )))
                .extended()
        );
        
        assertTrue(astEquals(expected, program), "AST should match expected structure");
    }
    
    @Test
    public void testVariableDeclarationInWhere() throws Exception {
        String query = "@SELECT * FROM GarbageCollection WHERE threshold := 50 AND duration > threshold";
        Parser parser = createParser(query);
        ProgramNode program = parser.parse();
        
        assertNotNull(program, "Should parse without errors");
        assertTrue(!program.statements().isEmpty(), "Should have statements");
        
        QueryNode queryNode = (QueryNode) program.statements().get(0);
        assertNotNull(queryNode.where(), "Should have WHERE clause");
        
        // The WHERE clause should contain a complex condition with variable declaration
        WhereNode where = queryNode.where();
        assertNotNull(where.condition(), "Should have a condition");
        
        // For now, just verify the structure is parsed correctly
        // More detailed AST structure testing would require complex builder patterns
    }
    
    @Test
    public void testFuzzyJoinBasic() throws Exception {
        Parser parser = createParser("@SELECT * FROM GarbageCollection FUZZY JOIN ExecutionSample ON timestamp");
        ProgramNode program = parser.parse();
        
        ProgramNode expected = program(
            query(selectAll(), from(source("GarbageCollection"), fuzzyJoin("ExecutionSample", "timestamp").build())).extended()
        );
        
        assertTrue(astEquals(expected, program), "Should parse fuzzy join correctly");
        
        QueryNode query = (QueryNode) program.statements().get(0);
        FromNode fromNode = query.from();
        assertEquals(2, fromNode.sources().size(), "Should have 2 sources");
        
        SourceNodeBase fuzzySource = fromNode.sources().get(1);
        assertTrue(fuzzySource instanceof FuzzyJoinSourceNode, "Second source should be fuzzy join");
        
        FuzzyJoinSourceNode fuzzyJoin = (FuzzyJoinSourceNode) fuzzySource;
        assertEquals("ExecutionSample", fuzzyJoin.source(), "Should join ExecutionSample");
        assertEquals("timestamp", fuzzyJoin.joinField(), "Should join on timestamp");
        assertEquals(FuzzyJoinType.NEAREST, fuzzyJoin.joinType(), "Should default to NEAREST");
        assertNull(fuzzyJoin.tolerance(), "Should have no tolerance by default");
    }
    
    @Test
    public void testFuzzyJoinWithAlias() throws Exception {
        Parser parser = createParser("@SELECT * FROM GarbageCollection FUZZY JOIN ExecutionSample AS exec ON timestamp");
        ProgramNode program = parser.parse();
        
        ProgramNode expected = program(
            query(selectAll(), from(source("GarbageCollection"), fuzzyJoin("ExecutionSample", "timestamp").alias("exec").build())).extended()
        );
        
        assertTrue(astEquals(expected, program), "Should parse fuzzy join with alias correctly");
        
        QueryNode query = (QueryNode) program.statements().get(0);
        FuzzyJoinSourceNode fuzzyJoin = (FuzzyJoinSourceNode) query.from().sources().get(1);
        assertEquals("exec", fuzzyJoin.alias(), "Should have alias");
    }
    
    @Test
    public void testFuzzyJoinWithJoinType() throws Exception {
        Parser parser = createParser("@SELECT * FROM GarbageCollection FUZZY JOIN ExecutionSample ON timestamp WITH PREVIOUS");
        ProgramNode program = parser.parse();
        
        ProgramNode expected = program(
            query(
                selectAll(),
                from(
                    source("GarbageCollection"), 
                    fuzzyJoin("ExecutionSample", "timestamp").withPrevious().build()
                )
            ).extended()
        );
        
        assertTrue(astEquals(expected, program), "Should parse fuzzy join with PREVIOUS correctly");
        
        QueryNode query = (QueryNode) program.statements().get(0);
        FuzzyJoinSourceNode fuzzyJoin = (FuzzyJoinSourceNode) query.from().sources().get(1);
        assertEquals(FuzzyJoinType.PREVIOUS, fuzzyJoin.joinType(), "Should have PREVIOUS join type");
    }
    
    @Test
    public void testFuzzyJoinWithAfterType() throws Exception {
        Parser parser = createParser("@SELECT * FROM GarbageCollection FUZZY JOIN ExecutionSample ON timestamp WITH AFTER");
        ProgramNode program = parser.parse();
        
        ProgramNode expected = program(
            query(selectAll(), from(source("GarbageCollection"), fuzzyJoin("ExecutionSample", "timestamp").withAfter().build())).extended()
        );
        
        assertTrue(astEquals(expected, program), "Should parse fuzzy join with AFTER correctly");
        
        QueryNode query = (QueryNode) program.statements().get(0);
        FuzzyJoinSourceNode fuzzyJoin = (FuzzyJoinSourceNode) query.from().sources().get(1);
        assertEquals(FuzzyJoinType.AFTER, fuzzyJoin.joinType(), "Should have AFTER join type");
    }
    
    @Test
    public void testFuzzyJoinWithTolerance() throws Exception {
        Parser parser = createParser("@SELECT * FROM GarbageCollection FUZZY JOIN ExecutionSample ON timestamp WITH NEAREST TOLERANCE 10ms");
        ProgramNode program = parser.parse();
        
        ProgramNode expected = program(
            query(selectAll(), from(source("GarbageCollection"), 
                fuzzyJoin("ExecutionSample", "timestamp").withNearest().tolerance("10ms").build())).extended()
        );
        
        assertTrue(astEquals(expected, program), "Should parse fuzzy join with tolerance correctly");
        
        QueryNode query = (QueryNode) program.statements().get(0);
        FuzzyJoinSourceNode fuzzyJoin = (FuzzyJoinSourceNode) query.from().sources().get(1);
        assertNotNull(fuzzyJoin.tolerance(), "Should have tolerance");
        assertTrue(fuzzyJoin.tolerance() instanceof LiteralNode, "Tolerance should be literal");
        
        LiteralNode toleranceLiteral = (LiteralNode) fuzzyJoin.tolerance();
        assertTrue(toleranceLiteral.value() instanceof CellValue.DurationValue, "Should have duration literal");
        assertEquals("10ms", toleranceLiteral.value().toString(), "Should have correct value");
    }
    
    @Test
    public void testFuzzyJoinComplexExample() throws Exception {
        Parser parser = createParser("@SELECT gc.type, exec.threadName FROM GarbageCollection AS gc FUZZY JOIN ExecutionSample AS exec ON timestamp WITH PREVIOUS TOLERANCE 5ms");
        ProgramNode program = parser.parse();
        
        ProgramNode expected = program(
            query(
                select(
                    fieldAccess("gc", "type"),
                    fieldAccess("exec", "threadName")
                ),
                from(
                    source("GarbageCollection", "gc"), 
                    fuzzyJoin("ExecutionSample", "timestamp").alias("exec").withPrevious().tolerance("5ms").build()
                )
            ).extended()
        );
        
        assertTrue(astEquals(expected, program), "Should parse complex fuzzy join correctly");
    }
    
    @Test
    public void testPercentileFunctionBasic() throws Exception {
        Parser parser = createParser("@SELECT P90(duration) FROM ExecutionSample");
        ProgramNode program = parser.parse();
        
        ProgramNode expected = program(
            query(
                select(p90(identifier("duration")).build()),
                from(source("ExecutionSample"))
            ).extended()
        );
        
        assertTrue(astEquals(expected, program), "Should parse basic P90 function correctly");
    }
    
    @Test
    public void testPercentileFunctionWithTimeSliceFilter() throws Exception {
        Parser parser = createParser("@SELECT P95(latency) FROM ExecutionSample");
        ProgramNode program = parser.parse();
        
        ProgramNode expected = program(
            query(
                select(p95(identifier("latency")).build()),
                from(source("ExecutionSample"))
            ).extended()
        );
        
        assertTrue(astEquals(expected, program), "Should parse P95 correctly");
    }
    
    @Test
    public void testPercentileFunctionWithInlineTiming() throws Exception {
        Parser parser = createParser("@SELECT P99(response_time) FROM WebRequests");
        ProgramNode program = parser.parse();
        
        ProgramNode expected = program(
            query(
                select(p99(identifier("response_time")).build()),
                from(source("WebRequests"))
            ).extended()
        );
        
        assertTrue(astEquals(expected, program), "Should parse P99 correctly");
    }
    
    @Test
    public void testPercentileFunctionP999() throws Exception {
        Parser parser = createParser("@SELECT P999(cpu_usage) AS high_cpu FROM SystemMetrics");
        ProgramNode program = parser.parse();
        
        ProgramNode expected = program(
            query(
                select(selectItem(p999(identifier("cpu_usage")).build(), "high_cpu")),
                from(source("SystemMetrics"))
            ).extended()
        );
        
        assertTrue(astEquals(expected, program), "Should parse P999 with alias correctly");
        
        QueryNode query = (QueryNode) program.statements().get(0);
        SelectItemNode selectItem = query.select().items().get(0);
        PercentileFunctionNode p999Func = (PercentileFunctionNode) selectItem.expression();
        assertEquals("P999", p999Func.functionName(), "Should be P999");
        assertEquals(99.9, p999Func.percentile(), 0.001, "Should be 99.9th percentile");
        assertEquals("high_cpu", selectItem.alias(), "Should have alias");
    }
    
    @Test
    public void testMultiplePercentileFunctions() throws Exception {
        Parser parser = createParser("@SELECT P90(latency), P95(latency), P99(latency) FROM RequestLog");
        ProgramNode program = parser.parse();
        
        ProgramNode expected = program(
            query(
                select(
                    p90(identifier("latency")).build(),
                    p95(identifier("latency")).build(),
                    p99(identifier("latency")).build()
                ),
                from(source("RequestLog"))
            ).extended()
        );
        
        assertTrue(astEquals(expected, program), "Should parse multiple percentile functions correctly");
        
        QueryNode query = (QueryNode) program.statements().get(0);
        assertEquals(3, query.select().items().size(), "Should have 3 expressions");
        
        // Check that all are percentile functions
        for (SelectItemNode selectItem : query.select().items()) {
            ExpressionNode expr = selectItem.expression();
            assertTrue(expr instanceof PercentileFunctionNode, "All should be PercentileFunctionNode");
        }
    }
    
    @ParameterizedTest
    @ValueSource(strings = {
        "SELECT * FROM GarbageCollection",
        "SELECT * FROM ExecutionSample",
        "SELECT * FROM ThreadSleep",
        "SELECT * FROM CPULoad",
        "SELECT * FROM AllocationSample"
    })
    public void testRawJfrQueries(String query) throws Exception {
        Parser parser = createParser(query);
        ProgramNode program = parser.parse();
        
        ProgramNode expected = program(
            rawJfrQuery(query)
        );
        
        assertTrue(astEquals(expected, program), "AST should match expected structure for: " + query);
    }
    
    @ParameterizedTest
    @ValueSource(strings = {
        "@SELECT * FROM GarbageCollection",
        "@SELECT * FROM ExecutionSample",
        "@SELECT * FROM ThreadSleep",
        "@SELECT * FROM CPULoad",
        "@SELECT * FROM AllocationSample"
    })
    public void testExtendedQueries(String query) throws Exception {
        Parser parser = createParser(query);
        ProgramNode program = parser.parse();
        
        String tableName = query.split(" FROM ")[1];
        
        ProgramNode expected = program(
            query(selectAll(), from(source(tableName)))
                .extended()
        );
        
        assertTrue(astEquals(expected, program), "AST should match expected structure for: " + query);
    }
    
    @ParameterizedTest
    @CsvSource({
        "COUNT(*), COUNT",
        "MAX(duration), MAX",
        "MIN(duration), MIN",
        "AVG(duration), AVG",
        "SUM(duration), SUM"
    })
    public void testFunctionParsing(String expression, String expectedFunction) throws Exception {
        Parser parser = createParser("@SELECT " + expression + " FROM GarbageCollection");
        ProgramNode program = parser.parse();
        
        assertNotNull(program, "Program should not be null");
        assertTrue(program.statements().size() > 0, "Should have statements");
        
        QueryNode queryNode = (QueryNode) program.statements().get(0);
        assertTrue(queryNode.select().items().size() > 0, "Should have expressions");
        
        ExpressionNode expr = queryNode.select().items().get(0).expression();
        if (expr instanceof FunctionCallNode) {
            FunctionCallNode funcCall = (FunctionCallNode) expr;
            assertEquals(expectedFunction, funcCall.functionName(), "Function name should match");
        }
    }

    @Test
    public void testPercentileTokenization() throws Exception {
        // Test that percentile functions tokenize correctly
        Lexer lexer = new Lexer("P90(value)");
        List<Token> tokens = lexer.tokenize();

        assertTrue(tokens.size() >= 3, "Should have at least 3 tokens");

        Token token = tokens.get(0);
        assertEquals(TokenType.IDENTIFIER, token.type(), "P90 should tokenize as IDENTIFIER token");
        assertEquals("P90", token.value(), "P90 token value should be 'P90'");

        token = tokens.get(1);
        assertEquals(TokenType.LPAREN, token.type(), "Should get LPAREN after P90");
    }

    @Test
    public void testSimplePercentileParsing() throws Exception {
        // Test simple percentile parsing
        Lexer lexer = new Lexer("@SELECT P90(duration) FROM GarbageCollection");
        List<Token> tokens = lexer.tokenize();

        Parser parser = new Parser(tokens);
        var result = parser.parse();

        assertNotNull(result, "Parse result should not be null");
        assertFalse(result.statements().isEmpty(), "Should have parsed without error");
        
        // Verify it's a percentile function
        QueryNode query = (QueryNode) result.statements().get(0);
        ExpressionNode expr = query.select().items().get(0).expression();
        assertTrue(expr instanceof PercentileFunctionNode, "Should be a percentile function");
        
        PercentileFunctionNode percentileFunc = (PercentileFunctionNode) expr;
        assertEquals("P90", percentileFunc.functionName(), "Should be P90");
        assertEquals(90.0, percentileFunc.percentile(), 0.001, "Should be 90th percentile");
    }
    
    @Test
    public void testP99SelectFunction() throws Exception {
        Parser parser = createParser("@SELECT * FROM ExecutionSample WHERE gcId IN P99SELECT(GarbageCollection, id, duration)");
        ProgramNode program = parser.parse();
        
        ProgramNode expected = program(
            query(
                selectAll(),
                from(source("ExecutionSample"))
            )
            .where(where(condition(
                in(
                    identifier("gcId"),
                    p99select("GarbageCollection", "id", identifier("duration"))
                )
            )))
            .extended()
        );
        
        assertTrue(astEquals(expected, program), "Should parse P99SELECT function correctly");
    }
    
    @Test
    public void testPercentileSelectWithExplicitPercentile() throws Exception {
        Parser parser = createParser("@SELECT * FROM ExecutionSample WHERE gcId IN PERCENTILE_SELECT(95, GarbageCollection, id, duration)");
        ProgramNode program = parser.parse();
        
        ProgramNode expected = program(
            query(
                selectAll(),
                from(source("ExecutionSample"))
            )
            .where(where(condition(
                in(
                    identifier("gcId"),
                    percentileSelect(95.0, "GarbageCollection", "id", identifier("duration"))
                )
            )))
            .extended()
        );
        
        assertTrue(astEquals(expected, program), "Should parse explicit PERCENTILE_SELECT function correctly");
    }
    
    // Tests from PercentileSelectionAliasTest.java - now consolidated
    @Test
    public void testPercentileSelectionWithoutAlias() throws Exception {
        String query = "@SELECT * FROM Events WHERE id IN P99SELECT(GarbageCollection, id, duration)";
        Parser parser = createParser(query);
        ProgramNode program = parser.parse();
        
        // Should parse successfully without any AS clause
        assertNotNull(program, "Should parse successfully");
        assertTrue(!program.statements().isEmpty(), "Should have statements");
        
        // Verify the structure
        QueryNode queryNode = (QueryNode) program.statements().get(0);
        assertNotNull(queryNode.where(), "Should have WHERE clause");
    }
    
    @Test 
    public void testPercentileSelectionIgnoresAlias() throws Exception {
        // This test verifies that if someone tries to add AS to a percentile selection,
        // it should be parsed as part of the larger expression, not as an alias to the percentile function
        String query = "@SELECT * FROM Events WHERE id IN P99SELECT(GarbageCollection, id, duration) AS alias";
        
        try {
            Parser parser = createParser(query);
            ProgramNode program = parser.parse();
            
            // If this parses, it means the AS is being interpreted in a different context,
            // which is expected behavior since we don't support aliases for percentile selection
            assertNotNull(program, "Should parse or fail gracefully");
        } catch (me.bechberger.jfr.extended.ParserException e) {
            // This is also acceptable - the AS clause might cause a parsing error
            // since it's not valid in this context
            assertTrue(e.getMessage().contains("AS") || e.getMessage().contains("alias"), 
                      "Error should relate to the misplaced AS clause");
        }
    }
    
    // Tests from ASTComparisonTest.java - now consolidated
    static Stream<Arguments> astComparisonCases() {
        return Stream.of(
            Arguments.of(
                program(query(selectAll(), from(source("GarbageCollection"))).extended()),
                program(query(selectAll(), from(source("GarbageCollection"))).extended()),
                true
            ),
            Arguments.of(
                program(query(selectAll(), from(source("GarbageCollection"))).extended()),
                program(query(selectAll(), from(source("ExecutionSample"))).extended()),
                false
            ),
            Arguments.of(
                program(query(select(identifier("duration")), from(source("GarbageCollection"))).extended()),
                program(query(select(identifier("duration")), from(source("GarbageCollection"))).extended()),
                true
            ),
            Arguments.of(
                program(query(select(identifier("duration")), from(source("GarbageCollection"))).extended()),
                program(query(select(identifier("startTime")), from(source("GarbageCollection"))).extended()),
                false
            )
        );
    }

    @ParameterizedTest
    @MethodSource("astComparisonCases")
    void testAstComparison(ProgramNode node1, ProgramNode node2, boolean shouldBeEqual) {
        assertEquals(shouldBeEqual, astEquals(node1, node2), "AST equality should match expectation");
    }
    
    // Tests from MultilineCommentTest.java - now consolidated
    static Stream<Arguments> multilineCommentCases() {
        return Stream.of(
            Arguments.of(
                "/* this is a comment */ SELECT * FROM test",
                new TokenType[] {TokenType.SELECT, TokenType.STAR, TokenType.FROM, TokenType.IDENTIFIER, TokenType.EOF},
                new String[] {"SELECT", "*", "FROM", "test", null}
            ),
            Arguments.of(
                "/* comment 1 */ SELECT /* comment 2 */ * FROM test",
                new TokenType[] {TokenType.SELECT, TokenType.STAR, TokenType.FROM, TokenType.IDENTIFIER, TokenType.EOF},
                new String[] {"SELECT", "*", "FROM", "test", null}
            ),
            Arguments.of(
                "/* multi\nline\ncomment */ SELECT name FROM mytable",
                new TokenType[] {TokenType.SELECT, TokenType.IDENTIFIER, TokenType.FROM, TokenType.IDENTIFIER, TokenType.EOF},
                new String[] {"SELECT", "name", "FROM", "mytable", null}
            ),
            Arguments.of(
                "SELECT * /* comment at end */",
                new TokenType[] {TokenType.SELECT, TokenType.STAR, TokenType.EOF},
                new String[] {"SELECT", "*", null}
            )
        );
    }

    @ParameterizedTest
    @MethodSource("multilineCommentCases")
    void testMultilineComments(String input, TokenType[] expectedTypes, String[] expectedValues) throws Exception {
        Lexer lexer = new Lexer(input);
        List<Token> tokens = lexer.tokenize();
        assertEquals(expectedTypes.length, tokens.size(), "Token count");
        for (int i = 0; i < expectedTypes.length; i++) {
            assertEquals(expectedTypes[i], tokens.get(i).type(), "Token type at " + i);
            if (expectedValues[i] != null) {
                assertEquals(expectedValues[i], tokens.get(i).value(), "Token value at " + i);
            }
        }
    }
    
    // Tests from ParserFunctionRegistryIntegrationTest.java - now consolidated  
    static Stream<Arguments> functionCallCases() {
        return Stream.of(
            Arguments.of(
                "@SELECT COUNT(*) FROM GarbageCollection",
                (FunctionCallASTSupplier) () -> program(
                    query(
                        select(function("COUNT", star())),
                        from(source("GarbageCollection"))
                    ).extended()
                )
            ),
            Arguments.of(
                "@SELECT MAX(duration) FROM GarbageCollection",
                (FunctionCallASTSupplier) () -> program(
                    query(
                        select(function("MAX", identifier("duration"))),
                        from(source("GarbageCollection"))
                    ).extended()
                )
            ),
            Arguments.of(
                "@SELECT SUM(heapUsed) FROM HeapSummary",
                (FunctionCallASTSupplier) () -> program(
                    query(
                        select(function("SUM", identifier("heapUsed"))),
                        from(source("HeapSummary"))
                    ).extended()
                )
            ),
            Arguments.of(
                "@SELECT AVG(duration), COUNT(*) FROM GarbageCollection",
                (FunctionCallASTSupplier) () -> program(
                    query(
                        select(function("AVG", identifier("duration")), function("COUNT", star())),
                        from(source("GarbageCollection"))
                    ).extended()
                )
            )
        );
    }

    @ParameterizedTest
    @MethodSource("functionCallCases")
    void testFunctionCallParsingParameterized(String query, FunctionCallASTSupplier expectedAstSupplier) throws Exception {
        Lexer lexer = new Lexer(query);
        Parser parser = new Parser(lexer.tokenize());
        ProgramNode program = parser.parse();
        ProgramNode expected = expectedAstSupplier.get();
        assertTrue(astEquals(expected, program), "AST should match for " + query);
    }

    @FunctionalInterface
    interface FunctionCallASTSupplier {
        ProgramNode get();
    }
    
    // Tests from FactoryPatternTest.java - now consolidated
    @ParameterizedTest
    @MethodSource("fuzzyJoinVariants")
    void testFuzzyJoinFactoryVariants(FuzzyJoinSourceNode actual, FuzzyJoinType expectedType, String expectedAlias, String expectedSource, String expectedJoinField, String expectedTolerance) {
        assertEquals(expectedSource, actual.source());
        assertEquals(expectedAlias, actual.alias());
        assertEquals(expectedJoinField, actual.joinField());
        assertEquals(expectedType, actual.joinType());
        if (expectedTolerance != null) {
            assertNotNull(actual.tolerance());
            assertEquals(durationLiteral(expectedTolerance), actual.tolerance());
        } else {
            assertNull(actual.tolerance());
        }
    }

    static Stream<Arguments> fuzzyJoinVariants() {
        return Stream.of(
            Arguments.of(fuzzyJoin("ExecutionSample", "timestamp").alias("exec").withPrevious().tolerance("5ms").build(), FuzzyJoinType.PREVIOUS, "exec", "ExecutionSample", "timestamp", "5ms"),
            Arguments.of(fuzzyJoin("Sample", "time").withNearest().build(), FuzzyJoinType.NEAREST, null, "Sample", "time", null),
            Arguments.of(fuzzyJoin("Sample", "time").withAfter().build(), FuzzyJoinType.AFTER, null, "Sample", "time", null),
            Arguments.of(fuzzyJoin("Sample", "time").tolerance(durationLiteral("10ms")).build(), FuzzyJoinType.NEAREST, null, "Sample", "time", "10ms")
        );
    }

    @Test
    public void testFuzzyJoinFactoryBackwardCompatibility() {
        FuzzyJoinSourceNode fuzzyJoin1 = fuzzyJoin("ExecutionSample", "timestamp").alias("exec").withPrevious().tolerance("5ms").build();
        FuzzyJoinSourceNode fuzzyJoin2 = fuzzyJoin("ExecutionSample", "exec", "timestamp", FuzzyJoinType.PREVIOUS, durationLiteral("5ms"));
        assertTrue(astEquals(fuzzyJoin1, fuzzyJoin2), "Should be equivalent");
    }

    @ParameterizedTest
    @MethodSource("percentileFunctionVariants")
    void testPercentileFunctionFactoryVariants(PercentileFunctionNode node, String expectedName, double expectedPercentile, String expectedTimeSlice) {
        assertEquals(expectedName, node.functionName());
        assertEquals(expectedPercentile, node.percentile(), 0.001);
        if (expectedTimeSlice != null) {
            assertNotNull(node.timeSliceFilter());
            assertEquals(durationLiteral(expectedTimeSlice), node.timeSliceFilter());
        }
    }

    static Stream<Arguments> percentileFunctionVariants() {
        return Stream.of(
            Arguments.of(p90("latency").build(), "P90", 90.0, null),
            Arguments.of(p999("cpu").build(), "P999", 99.9, null)
        );
    }

    @Test
    public void testFactoryPatternImmutability() {
        FuzzyJoinBuilder baseBuilder = fuzzyJoin("Sample", "timestamp").alias("s");
        FuzzyJoinSourceNode nearest = baseBuilder.withNearest().build();
        FuzzyJoinSourceNode previous = baseBuilder.withPrevious().build();
        assertEquals(FuzzyJoinType.NEAREST, nearest.joinType());
        assertEquals(FuzzyJoinType.PREVIOUS, previous.joinType());
        assertEquals("s", nearest.alias());
        assertEquals("s", previous.alias());
    }
}
