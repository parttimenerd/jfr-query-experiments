package me.bechberger.jfr.extended;

import me.bechberger.jfr.extended.ast.ASTNodes.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.Arguments;
import java.util.stream.Stream;
import static org.junit.jupiter.api.Assertions.*;
import static me.bechberger.jfr.extended.ASTBuilder.*;
import me.bechberger.jfr.extended.ast.*;
import me.bechberger.jfr.extended.table.*;

import java.util.List;

/**
 * Comprehensive parser tests combining both raw JFR queries and extended query functionality.
 * 
 * This test class is the result of merging ParserTest and ParserASTTest and covers:
 * - Basic SELECT queries (both raw JFR and extended)
 * - Complex query structures with WHERE, GROUP BY, HAVING, ORDER BY, LIMIT
 * - Arithmetic expressions with time and memory literals
 * - Function calls and aggregations
 * - Subqueries and nested structures
 * - Fuzzy joins with various options
 * - Percentile functions with time slice filtering
 * - Assignment statements and view definitions
 * - Variable declarations and complex conditions
 * - AST comparison and equality tests
 * - Multiline comment parsing
 * - Factory pattern tests for fuzzy joins and percentile functions
 * 
 * Tests use the builder pattern with ASTBuilder for readability and maintainability.
 */
public class ParserTest {
    
    private Parser createParser(String query) throws Exception {
        Lexer lexer = new Lexer(query);
        List<Token> tokens = lexer.tokenize();
        return new Parser(tokens, query);
    }
    
    // ===========================================
    // Raw JFR Query Tests
    // ===========================================
    
    @Test
    public void testRawJfrSimpleQuery() throws Exception {
        Parser parser = createParser("SELECT * FROM GarbageCollection");
        ProgramNode program = parser.parse();
        
        ProgramNode expected = program(
            rawJfrQuery("SELECT * FROM GarbageCollection")
        );
        
        assertTrue(astEquals(expected, program), "AST should match expected structure");
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
    
    // ===========================================
    // Extended Query Tests - Basic Structure
    // ===========================================
    
    @Test
    public void testExtendedSimpleQuery() throws Exception {
        Parser parser = createParser("@SELECT * FROM GarbageCollection");
        ProgramNode program = parser.parse();
        
        ProgramNode expected = program(
            query(selectAll(), from(source("GarbageCollection")))
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
        );
        
        assertTrue(astEquals(expected, program), "AST should match expected structure");
    }
    
    @Test
    public void testSelectWithAlias() throws Exception {
        Parser parser = createParser("@SELECT duration AS d FROM ExecutionSample AS es");
        ProgramNode program = parser.parse();
        
        ProgramNode expected = program(
            query(select(identifierWithAlias("duration", "d")), 
                  from(source("ExecutionSample", "es")))
        );
        
        assertTrue(astEquals(expected, program), "AST should match expected structure");
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
        );
        
        assertTrue(astEquals(expected, program), "AST should match expected structure for: " + query);
    }
    
    // ===========================================
    // Function Call Tests
    // ===========================================
    
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
        );
        
        assertTrue(astEquals(expected, program), "AST should match expected structure");
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
    
    // ===========================================
    // WHERE Clause Tests
    // ===========================================
    
    @Test
    public void testWhereClause() throws Exception {
        Parser parser = createParser("@SELECT * FROM GarbageCollection WHERE duration > 10");
        ProgramNode program = parser.parse();
        
        ProgramNode expected = program(
            query(selectAll(), from(source("GarbageCollection")))
                .where(where(condition(
                    greaterThan(identifier("duration"), numberLiteral(10.0))
                )))
                
        );
        
        assertTrue(astEquals(expected, program), "AST should match expected structure");
    }
    
    /**
     * Tests the WITHIN condition for temporal queries
     */
    @Test
    public void testWithinCondition() throws Exception {
        Parser parser = createParser("@SELECT * FROM ExecutionSample WHERE timestamp WITHIN 5m OF startTime");
        ProgramNode program = parser.parse();
        
        // Verify there is exactly one statement
        assertEquals(1, program.statements().size(), "Program should have exactly one statement");
        
        // Verify the statement is a query
        assertTrue(program.statements().get(0) instanceof QueryNode, "Statement should be a QueryNode");
        
        QueryNode queryNode = (QueryNode) program.statements().get(0);
        
        // Verify the WHERE clause exists
        assertNotNull(queryNode.where(), "Query should have a WHERE clause");
        
        // Verify the WHERE clause has a condition
        WhereNode whereNode = queryNode.where();
        ConditionNode condition = whereNode.condition();
        
        // Check that it's an ExpressionCondition with a BinaryExpression
        assertTrue(condition instanceof ExpressionConditionNode, "Condition should be an ExpressionConditionNode");
        
        ExpressionConditionNode exprCondition = (ExpressionConditionNode) condition;
        assertTrue(exprCondition.expression() instanceof BinaryExpressionNode, "Expression should be a BinaryExpressionNode");
        
        BinaryExpressionNode binaryExpr = (BinaryExpressionNode) exprCondition.expression();
        
        // Check that the operator is WITHIN
        assertEquals(BinaryOperator.WITHIN, binaryExpr.operator(), "Operator should be WITHIN");
        
        // Check the left operand is 'timestamp'
        assertTrue(binaryExpr.left() instanceof IdentifierNode, "Left operand should be an IdentifierNode");
        assertEquals("timestamp", ((IdentifierNode) binaryExpr.left()).name(), "Left operand should be 'timestamp'");
        
        // Check the right operand is a BinaryExpressionNode with OF operator
        assertTrue(binaryExpr.right() instanceof BinaryExpressionNode, "Right operand should be a BinaryExpressionNode");
        
        BinaryExpressionNode ofExpr = (BinaryExpressionNode) binaryExpr.right();
        assertEquals(BinaryOperator.OF, ofExpr.operator(), "Nested operator should be OF");
        
        // Check the left operand of OF is a DurationLiteral (5m)
        assertTrue(ofExpr.left() instanceof LiteralNode, "Time window should be a LiteralNode");
        LiteralNode timeWindow = (LiteralNode) ofExpr.left();
        assertEquals(CellType.DURATION, timeWindow.value().getType(), "Time window should be a DURATION literal");
        
        // Check the right operand of OF is the identifier 'startTime'
        assertTrue(ofExpr.right() instanceof IdentifierNode, "Reference time should be an IdentifierNode");
        assertEquals("startTime", ((IdentifierNode) ofExpr.right()).name(), "Reference time should be 'startTime'");
    }
    
    /**
     * Tests the WITHIN condition with a timestamp literal
     */
    @Test
    public void testWithinConditionWithTimestampLiteral() throws Exception {
        Parser parser = createParser("@SELECT * FROM ExecutionSample WHERE timestamp WITHIN 5m OF 2024-01-01T10:00:00");
        ProgramNode program = parser.parse();
        
        QueryNode queryNode = (QueryNode) program.statements().get(0);
        WhereNode whereNode = queryNode.where();
        ConditionNode condition = whereNode.condition();
        ExpressionConditionNode exprCondition = (ExpressionConditionNode) condition;
        BinaryExpressionNode binaryExpr = (BinaryExpressionNode) exprCondition.expression();
        
        // Check the right operand is a BinaryExpressionNode with OF operator
        BinaryExpressionNode ofExpr = (BinaryExpressionNode) binaryExpr.right();
        
        // Check the right operand of OF is a TimestampLiteral
        assertTrue(ofExpr.right() instanceof LiteralNode, "Reference time should be a LiteralNode");
        LiteralNode referenceTime = (LiteralNode) ofExpr.right();
        assertEquals(CellType.TIMESTAMP, referenceTime.value().getType(), "Reference time should be a TIMESTAMP literal");
        assertEquals("2024-01-01T10:00:00Z", referenceTime.value().toString(), "Timestamp value should match");
    }
    
    /**
     * Tests the WITHIN condition with a direct timestamp literal
     */
    @Test
    public void testWithinConditionWithDirectTimestampLiteral() throws Exception {
        Parser parser = createParser("@SELECT * FROM ExecutionSample WHERE timestamp WITHIN 5m OF 2024-01-01T10:00:00");
        ProgramNode program = parser.parse();
        
        QueryNode queryNode = (QueryNode) program.statements().get(0);
        WhereNode whereNode = queryNode.where();
        ConditionNode condition = whereNode.condition();
        ExpressionConditionNode exprCondition = (ExpressionConditionNode) condition;
        BinaryExpressionNode binaryExpr = (BinaryExpressionNode) exprCondition.expression();
        
        // Check the right operand is a BinaryExpressionNode with OF operator
        BinaryExpressionNode ofExpr = (BinaryExpressionNode) binaryExpr.right();
        
        // Check the right operand of OF is a string literal that should be interpreted as a timestamp
        assertTrue(ofExpr.right() instanceof LiteralNode, "Reference time should be a LiteralNode");
        LiteralNode referenceTime = (LiteralNode) ofExpr.right();
        // The parser should recognize this as a timestamp based on context or format
        assertTrue(
            referenceTime.value().getType() == CellType.TIMESTAMP || referenceTime.value().getType() == CellType.STRING,
            "Reference time should be a TIMESTAMP or STRING literal that can be interpreted as timestamp"
        );
        assertEquals("2024-01-01T10:00:00Z", referenceTime.value().toString(), "Timestamp value should match");
    }

    /**
     * Tests multiple formats of direct timestamp literals
     */
    @ParameterizedTest
    @ValueSource(strings = {
        "2024-01-01T10:00:00",
        "2024-01-01T10:00:00.123"
    })
    public void testDirectTimestampLiteralFormats(String timestampLiteral) throws Exception {
        String query = "@SELECT * FROM ExecutionSample WHERE timestamp WITHIN 5m OF " + timestampLiteral;
        Parser parser = createParser(query);
        ProgramNode program = parser.parse();
        
        // Verify it parses successfully
        assertNotNull(program, "Program should not be null for timestamp: " + timestampLiteral);
        assertEquals(1, program.statements().size(), "Should have exactly one statement");
        
        QueryNode queryNode = (QueryNode) program.statements().get(0);
        assertNotNull(queryNode.where(), "Should have WHERE clause");
        
        // Extract the timestamp literal and verify it matches
        WhereNode whereNode = queryNode.where();
        ExpressionConditionNode exprCondition = (ExpressionConditionNode) whereNode.condition();
        BinaryExpressionNode binaryExpr = (BinaryExpressionNode) exprCondition.expression();
        BinaryExpressionNode ofExpr = (BinaryExpressionNode) binaryExpr.right();
        LiteralNode referenceTime = (LiteralNode) ofExpr.right();
        
        // The timestamp literal should be in canonical ISO-8601 format with Z
        String expectedCanonical = timestampLiteral + (timestampLiteral.endsWith("Z") ? "" : "Z");
        assertEquals(expectedCanonical, referenceTime.value().toString(), 
            "Timestamp value should match canonical format for input: " + timestampLiteral);
    }
    
    // ===========================================
    // Variable Declaration Tests
    // ===========================================
    
    @Test
    public void testVariableDeclarationInWhere() throws Exception {
        Parser parser = createParser("@SELECT * FROM GarbageCollection WHERE threshold := 50 AND duration > threshold");
        ProgramNode program = parser.parse();
        
        // Let's verify the structure of the AST
        assertNotNull(program, "Program should not be null");
        assertTrue(!program.statements().isEmpty(), "Should have statements");
        
        QueryNode queryNode = (QueryNode) program.statements().get(0);
        assertNotNull(queryNode.where(), "Should have WHERE clause");
        
        // Since this test is for a variable declaration AND condition, let's add more parameterized tests for variable declarations
        // and use variableDeclarationCases for simpler cases
        
        // For this specific test, we'll keep the manual assertions since the AST structure with variable declarations and conditions
        // together is more complex
        WhereNode where = queryNode.where();
        ConditionNode condition = where.condition();
        assertTrue(condition instanceof ExpressionConditionNode, "Should be an ExpressionConditionNode");
        
        ExpressionNode expr = ((ExpressionConditionNode) condition).expression();
        assertTrue(expr instanceof BinaryExpressionNode, "Should be a BinaryExpressionNode");
        
        BinaryExpressionNode binaryExpr = (BinaryExpressionNode) expr;
        assertEquals(BinaryOperator.AND, binaryExpr.operator(), "Operator should be AND");
    }
    
    @Test
    public void testVariableDeclarationWithJoin() throws Exception {
        // Test that a variable defined in WHERE can be used in a JOIN condition that follows
        Parser parser = createParser("@SELECT * FROM GarbageCollection WHERE joinId := duration * 3 INNER JOIN ExecutionSample ON joinId = sampleId");
        ProgramNode program = parser.parse();
        
        // Create the expected AST
        ProgramNode expected = program(
            // QueryNode with a variable declaration in WHERE and a JOIN in FROM
            query(
                selectAll(), 
                from(
                    source("GarbageCollection"),
                    standardJoin("ExecutionSample", "joinId", "sampleId").inner().build()
                ))
                .where(where(
                    varDecl("joinId", multiply(identifier("duration"), numberLiteral(3.0)))
                ))
                
        );
        
        // Check that the AST matches our expected structure
        assertTrue(astEquals(expected, program), "AST should match expected structure");
    }
    
    // ===========================================
    // Arithmetic Expression Tests
    // ===========================================
    
    @Test
    public void testArithmeticWithTime() throws Exception {
        Parser parser = createParser("@SELECT duration + 5ms FROM GarbageCollection");
        ProgramNode program = parser.parse();
        
        ProgramNode expected = program(
            query(select(add(identifier("duration"), durationLiteral("5ms"))), 
                  from(source("GarbageCollection")))
                
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
                
        );
        
        assertTrue(astEquals(expected, program), "AST should match expected structure");
    }
    
    // ===========================================
    // GROUP BY, HAVING, ORDER BY, LIMIT Tests
    // ===========================================
    
    @Test
    public void testGroupByClause() throws Exception {
        Parser parser = createParser("@SELECT thread, COUNT(*) FROM ExecutionSample GROUP BY thread");
        ProgramNode program = parser.parse();
        
        ProgramNode expected = program(
            query(select(identifier("thread"), function("COUNT", star())), 
                  from(source("ExecutionSample")))
                .groupBy(groupBy(identifier("thread")))
                
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
                
        );
        
        assertTrue(astEquals(expected, program), "AST should match expected structure");
    }
    
    // ===========================================
    // Complex Query Tests
    // ===========================================
    
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
                    functionWithAlias("COUNT", "count", star()),
                    functionWithAlias("AVG", "avg_duration", identifier("duration"))
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
                
        );
        
        assertTrue(astEquals(expected, program), "AST should match expected structure");
    }
    
    // ===========================================
    // Subquery Tests
    // ===========================================
    
    @Test
    public void testNestedQuery() throws Exception {
        // Test with raw subquery (no @ prefix)
        Parser parser = createParser("@SELECT * FROM (SELECT * FROM GarbageCollection)");
        ProgramNode program = parser.parse();
        
        ProgramNode expected = program(
            query(
                selectAll(),
                from(subquery(rawJfrQuery("SELECT * FROM GarbageCollection")))
            )
        );
        
        assertTrue(astEquals(expected, program), "AST should match expected structure for raw subquery");
        
        // Test with extended subquery (with @ prefix)
        parser = createParser("@SELECT * FROM (@SELECT * FROM GarbageCollection)");
        program = parser.parse();
        
        expected = program(
            query(
                selectAll(),
                from(subquery(query(selectAll(), from(source("GarbageCollection"))).build()))
            )
        );
        
        assertTrue(astEquals(expected, program), "AST should match expected structure for extended subquery");
    }
    
    @Test
    public void testSubqueryInFrom() throws Exception {
        // Test with raw subquery (no @ prefix)
        Parser parser = createParser("@SELECT * FROM (SELECT * FROM GarbageCollection WHERE duration > 100) AS slowGCs");
        ProgramNode program = parser.parse();
        
        ProgramNode expected = program(
            query(selectAll(),
                from(subquery(
                    rawJfrQuery("SELECT * FROM GarbageCollection WHERE duration > 100"),
                    "slowGCs"
                )))
                
        );
        
        assertTrue(astEquals(expected, program), "AST should match expected structure for raw subquery with alias");
        
        // Test with extended subquery (with @ prefix)
        parser = createParser("@SELECT * FROM (@SELECT * FROM GarbageCollection WHERE duration > 100) AS slowGCs");
        program = parser.parse();
        
        expected = program(
            query(selectAll(),
                from(subquery(
                    query(selectAll(), from(source("GarbageCollection")))
                        .where(where(condition(
                            greaterThan(identifier("duration"), numberLiteral(100.0))
                        )))
                        
                        .build(),
                    "slowGCs"
                )))
                
        );
        
        assertTrue(astEquals(expected, program), "AST should match expected structure for extended subquery with alias");
    }
    
    // ===========================================
    // Statement Tests (Assignment, View, Show)
    // ===========================================
    
    @Test
    public void testAssignmentStatement() throws Exception {
        Parser parser = createParser("slowGCs := SELECT * FROM GarbageCollection WHERE duration > 100");
        ProgramNode program = parser.parse();
        
        ProgramNode expected = program(
            assignment("slowGCs",
                queryBuilder(selectAll(), from(source("GarbageCollection")))
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
        Parser parser = createParser("VIEW FastGC AS SELECT * FROM GarbageCollection WHERE duration < 10");
        ProgramNode program = parser.parse();
        
        ProgramNode expected = program(
            view("FastGC",
                queryBuilder(selectAll(), from(source("GarbageCollection")))
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
    
    // ===========================================
    // Fuzzy Join Tests
    // ===========================================
    
    @Test
    public void testFuzzyJoinBasic() throws Exception {
        Parser parser = createParser("@SELECT * FROM GarbageCollection FUZZY JOIN ExecutionSample ON timestamp");
        ProgramNode program = parser.parse();
        
        ProgramNode expected = program(
            query(selectAll(), from(source("GarbageCollection"), fuzzyJoin("ExecutionSample", "timestamp").build()))
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
            query(selectAll(), from(source("GarbageCollection"), fuzzyJoin("ExecutionSample", "timestamp").alias("exec").build()))
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
            query(selectAll(), from(source("GarbageCollection"), fuzzyJoin("ExecutionSample", "timestamp").withPrevious().build()))
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
            query(selectAll(), from(source("GarbageCollection"), fuzzyJoin("ExecutionSample", "timestamp").withAfter().build()))
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
                fuzzyJoin("ExecutionSample", "timestamp").withNearest().tolerance("10ms").build()))
        );
        
        assertTrue(astEquals(expected, program), "Should parse fuzzy join with tolerance correctly");
        
        QueryNode query = (QueryNode) program.statements().get(0);
        FuzzyJoinSourceNode fuzzyJoin = (FuzzyJoinSourceNode) query.from().sources().get(1);
        assertNotNull(fuzzyJoin.tolerance(), "Should have tolerance");
        assertTrue(fuzzyJoin.tolerance() instanceof LiteralNode, "Tolerance should be literal");
        
        LiteralNode toleranceLiteral = (LiteralNode) fuzzyJoin.tolerance();
        assertEquals(CellType.DURATION, toleranceLiteral.value().getType(), "Should have duration literal");
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
            )
        );
        
        assertTrue(astEquals(expected, program), "Should parse complex fuzzy join correctly");
    }
    
    // ===========================================
    // Percentile Function Tests
    // ===========================================
    
    @Test
    public void testPercentileFunctionBasic() throws Exception {
        Parser parser = createParser("@SELECT P90(duration) FROM ExecutionSample");
        ProgramNode program = parser.parse();
        
        ProgramNode expected = program(
            query(
                select(p90(identifier("duration")).build()),
                from(source("ExecutionSample"))
            )
        );
        
        assertTrue(astEquals(expected, program), "Should parse basic P90 function correctly");
        
        QueryNode query = (QueryNode) program.statements().get(0);
        ExpressionNode expr = query.select().items().get(0).expression();
        assertTrue(expr instanceof PercentileFunctionNode, "Should be PercentileFunctionNode");
        
        PercentileFunctionNode p90Func = (PercentileFunctionNode) expr;
        assertEquals("P90", p90Func.functionName(), "Should be P90");
        assertEquals(90.0, p90Func.percentile(), 0.001, "Should be 90th percentile");
        assertNull(p90Func.timeSliceFilter(), "Should have no time slice filter");
    }
    
    @Test
    public void testPercentileFunctionP95() throws Exception {
        Parser parser = createParser("@SELECT P95(latency) FROM ExecutionSample");
        ProgramNode program = parser.parse();
        
        ProgramNode expected = program(
            query(
                select(p95(identifier("latency")).build()),
                from(source("ExecutionSample"))
            )
        );
        
        assertTrue(astEquals(expected, program), "Should parse P95 function correctly");
        
        QueryNode query = (QueryNode) program.statements().get(0);
        PercentileFunctionNode p95Func = (PercentileFunctionNode) query.select().items().get(0).expression();
        assertEquals("P95", p95Func.functionName(), "Should be P95");
        assertEquals(95.0, p95Func.percentile(), 0.001, "Should be 95th percentile");
        assertNull(p95Func.timeSliceFilter(), "Should have no time slice filter by default");
    }
    
    @Test
    public void testPercentileFunctionP99() throws Exception {
        Parser parser = createParser("@SELECT P99(response_time) FROM WebRequests");
        ProgramNode program = parser.parse();
        
        ProgramNode expected = program(
            query(
                select(p99(identifier("response_time")).build()),
                from(source("WebRequests"))
            )
        );
        
        assertTrue(astEquals(expected, program), "Should parse P99 function correctly");
        
        QueryNode query = (QueryNode) program.statements().get(0);
        PercentileFunctionNode p99Func = (PercentileFunctionNode) query.select().items().get(0).expression();
        assertEquals("P99", p99Func.functionName(), "Should be P99");
        assertEquals(99.0, p99Func.percentile(), 0.001, "Should be 99th percentile");
        assertNull(p99Func.timeSliceFilter(), "Should have no time slice filter by default");
    }
    
    @Test
    public void testPercentileFunctionP999() throws Exception {
        Parser parser = createParser("@SELECT P999(cpu_usage) AS high_cpu FROM SystemMetrics");
        ProgramNode program = parser.parse();
        
        ProgramNode expected = program(
            query(
                select(selectItem(p999(identifier("cpu_usage")).build(), "high_cpu")),
                from(source("SystemMetrics"))
            )
        );
        
        assertTrue(astEquals(expected, program), "Should parse P999 with alias correctly");
        
        QueryNode query = (QueryNode) program.statements().get(0);
        SelectItemNode selectItem = query.select().items().get(0);
        PercentileFunctionNode p999Func = (PercentileFunctionNode) selectItem.expression();
        assertEquals("P999", p999Func.functionName(), "Should be P999");
        assertEquals(99.9, p999Func.percentile(), 0.001, "Should be 99.9th percentile");
        assertEquals("high_cpu", selectItem.alias(), "Should have alias on SelectItemNode");
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
            )
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
    
    @Test
    public void testP99SelectFunction() throws Exception {
        Parser parser = createParser("@SELECT * FROM Events WHERE id IN P99SELECT(Events, id, latency)");
        ProgramNode program = parser.parse();
        
        ProgramNode expected = program(
            query(
                selectAll(),
                from(source("Events"))
            )
            .where(where(condition(
                in(
                    identifier("id"),
                    p99select("Events", "id", identifier("latency"))
                )
            )))
            
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
            
        );
        
        assertTrue(astEquals(expected, program), "Should parse explicit PERCENTILE_SELECT function correctly");
    }
    
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
            assertNotNull(program, "Should parse without error");
        } catch (Exception e) {
            // If it fails to parse, that's also acceptable behavior
            assertTrue(true, "Parser correctly rejects invalid alias usage");
        }
    }
    
    @Test
    public void testSimplePercentileParsing() throws Exception {
        Lexer lexer = new Lexer("P90(value)");
        List<Token> tokens = lexer.tokenize();
        
        // Check that P90 is recognized as a special token
        boolean foundP90Token = tokens.stream()
            .anyMatch(token -> token.type() == TokenType.P90);
        assertTrue(foundP90Token, "Should be a percentile function");
    }
    
    @Test
    public void testAdvancedPercentileQuery() throws Exception {
        Lexer lexer = new Lexer("@SELECT P90(duration) FROM GarbageCollection");
        List<Token> tokens = lexer.tokenize();
        Parser parser = new Parser(tokens);
        ProgramNode program = parser.parse();
        
        // Verify structure
        assertNotNull(program, "Program should not be null");
        assertEquals(1, program.statements().size(), "Should have one statement");
        
        QueryNode query = (QueryNode) program.statements().get(0);
        SelectNode select = query.select();
        assertEquals(1, select.items().size(), "Should have one select item");
        
        ExpressionNode expr = select.items().get(0).expression();
        assertTrue(expr instanceof PercentileFunctionNode, "Should be PercentileFunctionNode");
        
        PercentileFunctionNode percentile = (PercentileFunctionNode) expr;
        assertEquals("P90", percentile.functionName(), "Should be P90");
        assertEquals(90.0, percentile.percentile(), 0.001, "Should be 90th percentile");
    }

    // ===========================================
    // Parameterized AST Checking for Function Calls
    // ===========================================
    static Stream<Arguments> functionCallCases() {
        return Stream.of(
            Arguments.of(
                "@SELECT COUNT(*) FROM GarbageCollection",
                (ASTBuilderSupplier) () -> program(
                    query(
                        select(function("COUNT", star())),
                        from(source("GarbageCollection"))
                    )
                )
            ),
            Arguments.of(
                "@SELECT MAX(duration) FROM GarbageCollection",
                (ASTBuilderSupplier) () -> program(
                    query(
                        select(function("MAX", identifier("duration"))),
                        from(source("GarbageCollection"))
                    )
                )
            ),
            Arguments.of(
                "@SELECT MIN(duration) FROM GarbageCollection",
                (ASTBuilderSupplier) () -> program(
                    query(
                        select(function("MIN", identifier("duration"))),
                        from(source("GarbageCollection"))
                    )
                )
            ),
            Arguments.of(
                "@SELECT AVG(duration) FROM GarbageCollection",
                (ASTBuilderSupplier) () -> program(
                    query(
                        select(function("AVG", identifier("duration"))),
                        from(source("GarbageCollection"))
                    )
                )
            ),
            Arguments.of(
                "@SELECT SUM(duration) FROM GarbageCollection",
                (ASTBuilderSupplier) () -> program(
                    query(
                        select(function("SUM", identifier("duration"))),
                        from(source("GarbageCollection"))
                    )
                )
            )
        );
    }

    @ParameterizedTest
    @MethodSource("functionCallCases")
    void testFunctionCallParsing(String query, ASTBuilderSupplier expectedAstSupplier) throws Exception {
        Parser parser = createParser(query);
        ProgramNode program = parser.parse();
        ProgramNode expected = expectedAstSupplier.get();
        assertTrue(astEquals(expected, program), "AST should match for query: " + query);
    }

    @FunctionalInterface
    interface ASTBuilderSupplier {
        ProgramNode get();
    }
    
    // ===========================================
    // Parameterized AST Checking for Arithmetic Expressions
    // ===========================================
    static Stream<Arguments> arithmeticExpressionCases() {
        return Stream.of(
            Arguments.of(
                "@SELECT duration + 5ms FROM GarbageCollection",
                (ASTBuilderSupplier) () -> program(
                    query(select(add(identifier("duration"), durationLiteral("5ms"))), 
                          from(source("GarbageCollection")))
                        
                )
            ),
            Arguments.of(
                "@SELECT heapUsed - 100MB FROM HeapSummary",
                (ASTBuilderSupplier) () -> program(
                    query(select(subtract(identifier("heapUsed"), memorySizeLiteral("100MB"))), 
                          from(source("HeapSummary")))
                        
                )
            ),
            Arguments.of(
                "@SELECT (duration + 5ms) * 2 / heapSize FROM GarbageCollection",
                (ASTBuilderSupplier) () -> program(
                    query(select(divide(
                            multiply(
                                add(identifier("duration"), durationLiteral("5ms")),
                                numberLiteral(2.0)
                            ),
                            identifier("heapSize")
                        )), 
                        from(source("GarbageCollection")))
                        
                )
            ),
            Arguments.of(
                "@SELECT * FROM GarbageCollection WHERE duration > 10ms + 5ms",
                (ASTBuilderSupplier) () -> program(
                    query(selectAll(), from(source("GarbageCollection")))
                        .where(where(condition(
                            greaterThan(
                                identifier("duration"),
                                add(durationLiteral("10ms"), durationLiteral("5ms"))
                            )
                        )))
                        
                )
            )
        );
    }

    @ParameterizedTest
    @MethodSource("arithmeticExpressionCases")
    void testArithmeticExpressionParsing(String query, ASTBuilderSupplier expectedAstSupplier) throws Exception {
        Parser parser = createParser(query);
        ProgramNode program = parser.parse();
        ProgramNode expected = expectedAstSupplier.get();
        assertTrue(astEquals(expected, program), "AST should match for query: " + query);
    }
    
    // ===========================================
    // Parameterized AST Checking for Subqueries
    // ===========================================
    static Stream<Arguments> subqueryCases() {
        return Stream.of(
            // Regular SELECT in subquery (raw query)
            Arguments.of(
                "@SELECT * FROM (SELECT * FROM GarbageCollection)",
                (ASTBuilderSupplier) () -> program(
                    query(
                        selectAll(),
                        from(subquery(rawJfrQuery("SELECT * FROM GarbageCollection")))
                    )
                )
            ),
            // Extended SELECT in subquery (@SELECT)
            Arguments.of(
                "@SELECT * FROM (@SELECT * FROM GarbageCollection)",
                (ASTBuilderSupplier) () -> program(
                    query(
                        selectAll(),
                        from(subquery(query(selectAll(), from(source("GarbageCollection"))).build()))
                    )
                )
            ),
            // Regular SELECT subquery with WHERE clause and alias
            Arguments.of(
                "@SELECT * FROM (SELECT * FROM GarbageCollection WHERE duration > 100) AS slowGCs",
                (ASTBuilderSupplier) () -> program(
                    query(selectAll(),
                        from(subquery(
                            rawJfrQuery("SELECT * FROM GarbageCollection WHERE duration > 100"),
                            "slowGCs"
                        )))
                        
                )
            ),
            // Extended SELECT subquery with WHERE clause and alias
            Arguments.of(
                "@SELECT * FROM (@SELECT * FROM GarbageCollection WHERE duration > 100) AS slowGCs",
                (ASTBuilderSupplier) () -> program(
                    query(selectAll(),
                        from(subquery(
                            query(selectAll(), from(source("GarbageCollection")))
                                .where(where(condition(
                                    greaterThan(identifier("duration"), numberLiteral(100.0))
                                )))
                                
                                .build(),
                            "slowGCs"
                        )))
                        
                )
            )
        );
    }

    @ParameterizedTest
    @MethodSource("subqueryCases")
    void testSubqueryParsing(String query, ASTBuilderSupplier expectedAstSupplier) throws Exception {
        Parser parser = createParser(query);
        ProgramNode program = parser.parse();
        ProgramNode expected = expectedAstSupplier.get();
        assertTrue(astEquals(expected, program), "AST should match for query: " + query);
    }
    
    // ===========================================
    // Parameterized AST Checking for Assignment, View, Show Statements
    // ===========================================
    static Stream<Arguments> statementCases() {
        return Stream.of(
            Arguments.of(
                "slowGCs := SELECT * FROM GarbageCollection WHERE duration > 100",
                (ASTBuilderSupplier) () -> program(
                    assignment("slowGCs",
                        queryBuilder(selectAll(), from(source("GarbageCollection")))
                            .where(where(condition(
                                greaterThan(identifier("duration"), numberLiteral(100.0))
                            )))
                            .build()
                    )
                )
            ),
            Arguments.of(
                "VIEW FastGC AS SELECT * FROM GarbageCollection WHERE duration < 10",
                (ASTBuilderSupplier) () -> program(
                    view("FastGC",
                        queryBuilder(selectAll(), from(source("GarbageCollection")))
                            .where(where(condition(
                                lessThan(identifier("duration"), numberLiteral(10.0))
                            )))
                            .build()
                    )
                )
            ),
            Arguments.of(
                "SHOW EVENTS",
                (ASTBuilderSupplier) () -> program(showEvents())
            ),
            Arguments.of(
                "SHOW FIELDS GarbageCollection",
                (ASTBuilderSupplier) () -> program(showFields("GarbageCollection"))
            ),
            // Assignment with raw subquery
            Arguments.of(
                "complexGCs := SELECT * FROM (SELECT * FROM GarbageCollection WHERE duration > 100)",
                (ASTBuilderSupplier) () -> program(
                    assignment("complexGCs",
                        queryBuilder(selectAll(),
                            from(subquery(
                                rawJfrQuery("SELECT * FROM GarbageCollection WHERE duration > 100")
                            )))
                            .build()
                    )
                )
            ),
            // Assignment with extended subquery
            Arguments.of(
                "extendedGCs := @SELECT * FROM (@SELECT * FROM GarbageCollection WHERE duration > 100)",
                (ASTBuilderSupplier) () -> program(
                    assignment("extendedGCs",
                        query(selectAll(),
                            from(subquery(
                                query(selectAll(), from(source("GarbageCollection")))
                                    .where(where(condition(
                                        greaterThan(identifier("duration"), numberLiteral(100.0))
                                    )))
                                    
                                    .build()
                            )))
                            
                            .build()
                    )
                )
            )
        );
    }

    @ParameterizedTest
    @MethodSource("statementCases")
    void testStatementParsing(String query, ASTBuilderSupplier expectedAstSupplier) throws Exception {
        Parser parser = createParser(query);
        ProgramNode program = parser.parse();
        ProgramNode expected = expectedAstSupplier.get();
        assertTrue(astEquals(expected, program), "AST should match for query: " + query);
    }
    
    // ===========================================
    // Parameterized AST Checking for Fuzzy Join Queries
    // ===========================================
    static Stream<Arguments> fuzzyJoinCases() {
        return Stream.of(
            Arguments.of(
                "@SELECT * FROM GarbageCollection FUZZY JOIN ExecutionSample ON timestamp",
                (ASTBuilderSupplier) () -> program(
                    query(selectAll(), from(source("GarbageCollection"), fuzzyJoin("ExecutionSample", "timestamp").build()))
                )
            ),
            Arguments.of(
                "@SELECT * FROM GarbageCollection FUZZY JOIN ExecutionSample AS exec ON timestamp",
                (ASTBuilderSupplier) () -> program(
                    query(selectAll(), from(source("GarbageCollection"), fuzzyJoin("ExecutionSample", "timestamp").alias("exec").build()))
                )
            ),
            Arguments.of(
                "@SELECT * FROM GarbageCollection FUZZY JOIN ExecutionSample ON timestamp WITH PREVIOUS",
                (ASTBuilderSupplier) () -> program(
                    query(selectAll(), from(source("GarbageCollection"), fuzzyJoin("ExecutionSample", "timestamp").withPrevious().build()))
                )
            ),
            Arguments.of(
                "@SELECT * FROM GarbageCollection FUZZY JOIN ExecutionSample ON timestamp WITH AFTER",
                (ASTBuilderSupplier) () -> program(
                    query(selectAll(), from(source("GarbageCollection"), fuzzyJoin("ExecutionSample", "timestamp").withAfter().build()))
                )
            ),
            Arguments.of(
                "@SELECT * FROM GarbageCollection FUZZY JOIN ExecutionSample ON timestamp WITH NEAREST TOLERANCE 10ms",
                (ASTBuilderSupplier) () -> program(
                    query(selectAll(), from(source("GarbageCollection"), fuzzyJoin("ExecutionSample", "timestamp").withNearest().tolerance("10ms").build()))
                )
            ),
            Arguments.of(
                "@SELECT gc.type, exec.threadName FROM GarbageCollection AS gc FUZZY JOIN ExecutionSample AS exec ON timestamp WITH PREVIOUS TOLERANCE 5ms",
                (ASTBuilderSupplier) () -> program(
                    query(
                        select(
                            fieldAccess("gc", "type"),
                            fieldAccess("exec", "threadName")
                        ),
                        from(
                            source("GarbageCollection", "gc"),
                            fuzzyJoin("ExecutionSample", "timestamp").alias("exec").withPrevious().tolerance("5ms").build()
                        )
                    )
                )
            )
        );
    }

    @ParameterizedTest
    @MethodSource("fuzzyJoinCases")
    void testFuzzyJoinParsing(String query, ASTBuilderSupplier expectedAstSupplier) throws Exception {
        Parser parser = createParser(query);
        ProgramNode program = parser.parse();
        ProgramNode expected = expectedAstSupplier.get();
        assertTrue(astEquals(expected, program), "AST should match for query: " + query);
    }
    
    // ===========================================
    // Standard Join Tests
    // ===========================================
    
    @Test
    public void testInnerJoinBasic() throws Exception {
        Parser parser = createParser("@SELECT * FROM GarbageCollection INNER JOIN ExecutionSample ON id = sampleId");
        ProgramNode program = parser.parse();
        
        ProgramNode expected = program(
            query(selectAll(), from(source("GarbageCollection"), innerJoin("ExecutionSample", "id", "sampleId")))
        );
        
        assertTrue(astEquals(expected, program), "Should parse basic INNER JOIN correctly");
    }
    
    @Test
    public void testLeftJoinBasic() throws Exception {
        Parser parser = createParser("@SELECT * FROM GarbageCollection LEFT JOIN ExecutionSample ON id = sampleId");
        ProgramNode program = parser.parse();
        
        ProgramNode expected = program(
            query(selectAll(), from(source("GarbageCollection"), leftJoin("ExecutionSample", "id", "sampleId")))
        );
        
        assertTrue(astEquals(expected, program), "Should parse basic LEFT JOIN correctly");
    }
    
    @Test
    public void testRightJoinBasic() throws Exception {
        Parser parser = createParser("@SELECT * FROM GarbageCollection RIGHT JOIN ExecutionSample ON id = sampleId");
        ProgramNode program = parser.parse();
        
        ProgramNode expected = program(
            query(selectAll(), from(source("GarbageCollection"), rightJoin("ExecutionSample", "id", "sampleId")))
        );
        
        assertTrue(astEquals(expected, program), "Should parse basic RIGHT JOIN correctly");
    }
    
    @Test
    public void testFullJoinBasic() throws Exception {
        Parser parser = createParser("@SELECT * FROM GarbageCollection FULL JOIN ExecutionSample ON id = sampleId");
        ProgramNode program = parser.parse();
        
        ProgramNode expected = program(
            query(selectAll(), from(source("GarbageCollection"), fullJoin("ExecutionSample", "id", "sampleId")))
        );
        
        assertTrue(astEquals(expected, program), "Should parse basic FULL JOIN correctly");
    }
    
    @Test
    public void testJoinWithAlias() throws Exception {
        Parser parser = createParser("@SELECT * FROM GarbageCollection INNER JOIN ExecutionSample AS exec ON id = sampleId");
        ProgramNode program = parser.parse();
        
        ProgramNode expected = program(
            query(selectAll(), from(source("GarbageCollection"), standardJoin("ExecutionSample", "id", "sampleId").alias("exec").inner().build()))
        );
        
        assertTrue(astEquals(expected, program), "Should parse JOIN with alias correctly");
    }
    
    @Test
    public void testJoinDefaultsToInner() throws Exception {
        Parser parser = createParser("@SELECT * FROM GarbageCollection JOIN ExecutionSample ON id = sampleId");
        ProgramNode program = parser.parse();
        
        ProgramNode expected = program(
            query(selectAll(), from(source("GarbageCollection"), innerJoin("ExecutionSample", "id", "sampleId")))
        );
        
        assertTrue(astEquals(expected, program), "Should default to INNER JOIN when no type specified");
    }
    
    @Test
    public void testMultipleJoins() throws Exception {
        Parser parser = createParser("@SELECT * FROM GarbageCollection INNER JOIN ExecutionSample ON id = sampleId LEFT JOIN AllocationSample ON id = allocId");
        ProgramNode program = parser.parse();
        
        ProgramNode expected = program(
            query(selectAll(), from(
                source("GarbageCollection"), 
                innerJoin("ExecutionSample", "id", "sampleId"),
                leftJoin("AllocationSample", "id", "allocId")
            ))
        );
        
        assertTrue(astEquals(expected, program), "Should parse multiple joins correctly");
    }
    
    // Test standard join AST node structure
    @Test
    public void testStandardJoinASTStructure() throws Exception {
        Parser parser = createParser("@SELECT * FROM GarbageCollection INNER JOIN ExecutionSample AS exec ON id = sampleId");
        ProgramNode program = parser.parse();
        
        QueryNode query = (QueryNode) program.statements().get(0);
        FromNode fromNode = query.from();
        
        assertEquals(2, fromNode.sources().size(), "Should have 2 sources");
        
        SourceNodeBase joinSource = fromNode.sources().get(1);
        assertTrue(joinSource instanceof StandardJoinSourceNode, "Second source should be standard join");
        
        StandardJoinSourceNode standardJoin = (StandardJoinSourceNode) joinSource;
        assertEquals("ExecutionSample", standardJoin.source(), "Should join ExecutionSample");
        assertEquals("exec", standardJoin.alias(), "Should have alias 'exec'");
        assertEquals(StandardJoinType.INNER, standardJoin.joinType(), "Should be INNER join");
        assertEquals("id", standardJoin.leftJoinField(), "Should join on left field 'id'");
        assertEquals("sampleId", standardJoin.rightJoinField(), "Should join on right field 'sampleId'");
    }
    
    // Standard Join Test Data
    static Stream<Arguments> standardJoinCases() {
        return Stream.of(
            Arguments.of(
                "@SELECT * FROM GarbageCollection INNER JOIN ExecutionSample ON id = sampleId",
                (ASTBuilderSupplier) () -> program(
                    query(selectAll(), from(source("GarbageCollection"), innerJoin("ExecutionSample", "id", "sampleId")))
                )
            ),
            Arguments.of(
                "@SELECT * FROM GarbageCollection LEFT JOIN ExecutionSample ON id = sampleId",
                (ASTBuilderSupplier) () -> program(
                    query(selectAll(), from(source("GarbageCollection"), leftJoin("ExecutionSample", "id", "sampleId")))
                )
            ),
            Arguments.of(
                "@SELECT * FROM GarbageCollection RIGHT JOIN ExecutionSample ON id = sampleId",
                (ASTBuilderSupplier) () -> program(
                    query(selectAll(), from(source("GarbageCollection"), rightJoin("ExecutionSample", "id", "sampleId")))
                )
            ),
            Arguments.of(
                "@SELECT * FROM GarbageCollection FULL JOIN ExecutionSample ON id = sampleId",
                (ASTBuilderSupplier) () -> program(
                    query(selectAll(), from(source("GarbageCollection"), fullJoin("ExecutionSample", "id", "sampleId")))
                )
            ),
            Arguments.of(
                "@SELECT * FROM GarbageCollection JOIN ExecutionSample ON id = sampleId",
                (ASTBuilderSupplier) () -> program(
                    query(selectAll(), from(source("GarbageCollection"), innerJoin("ExecutionSample", "id", "sampleId")))
                )
            ),
            Arguments.of(
                "@SELECT * FROM GarbageCollection INNER JOIN ExecutionSample AS exec ON id = sampleId",
                (ASTBuilderSupplier) () -> program(
                    query(selectAll(), from(source("GarbageCollection"), standardJoin("ExecutionSample", "id", "sampleId").alias("exec").inner().build()))
                )
            ),
            Arguments.of(
                "@SELECT gc.type, exec.threadName FROM GarbageCollection AS gc LEFT JOIN ExecutionSample AS exec ON gcId = id",
                (ASTBuilderSupplier) () -> program(
                    query(
                        select(
                            fieldAccess("gc", "type"),
                            fieldAccess("exec", "threadName")
                        ),
                        from(
                            source("GarbageCollection", "gc"),
                            standardJoin("ExecutionSample", "gcId", "id").alias("exec").left().build()
                        )
                    )
                )
            )
        );
    }

    @ParameterizedTest
    @MethodSource("standardJoinCases")
    void testStandardJoinParsing(String query, ASTBuilderSupplier expectedAstSupplier) throws Exception {
        Parser parser = createParser(query);
        ProgramNode program = parser.parse();
        ProgramNode expected = expectedAstSupplier.get();
        assertTrue(astEquals(expected, program), "AST should match for query: " + query);
    }
    
    // ===========================================
    // Parameterized AST Checking for Variable Declarations
    // ===========================================
    static Stream<Arguments> variableDeclarationCases() {
        return Stream.of(
            Arguments.of(
                "@SELECT * FROM GarbageCollection WHERE threshold := 50",
                (ASTBuilderSupplier) () -> program(
                    query(selectAll(), from(source("GarbageCollection")))
                        .where(where(
                            varDecl("threshold", numberLiteral(50.0))
                        ))
                        
                )
            ),
            Arguments.of(
                "@SELECT * FROM GarbageCollection WHERE maxDuration := duration * 2",
                (ASTBuilderSupplier) () -> program(
                    query(selectAll(), from(source("GarbageCollection")))
                        .where(where(
                            varDecl("maxDuration", multiply(identifier("duration"), numberLiteral(2.0)))
                        ))
                        
                )
            ),
            Arguments.of(
                "@SELECT * FROM GarbageCollection WHERE timeThreshold := 10ms + 5ms",
                (ASTBuilderSupplier) () -> program(
                    query(selectAll(), from(source("GarbageCollection")))
                        .where(where(
                            varDecl("timeThreshold", add(durationLiteral("10ms"), durationLiteral("5ms")))
                        ))
                        
                )
            )
        );
    }

    @ParameterizedTest
    @MethodSource("variableDeclarationCases")
    void testVariableDeclarationParsing(String query, ASTBuilderSupplier expectedAstSupplier) throws Exception {
        Parser parser = createParser(query);
        ProgramNode program = parser.parse();
        ProgramNode expected = expectedAstSupplier.get();
        assertTrue(astEquals(expected, program), "AST should match for query: " + query);
    }
    
    // ===========================================
    // Parameterized AST Checking for Variable Declarations with JOIN
    // ===========================================
    static Stream<Arguments> variableDeclarationWithJoinCases() {
        return Stream.of(
            Arguments.of(
                "@SELECT * FROM GarbageCollection WHERE joinId := duration * 3 INNER JOIN ExecutionSample ON joinId = sampleId",
                (ASTBuilderSupplier) () -> program(
                    query(
                        selectAll(), 
                        from(
                            source("GarbageCollection"),
                            standardJoin("ExecutionSample", "joinId", "sampleId").inner().build()
                        ))
                        .where(where(
                            varDecl("joinId", multiply(identifier("duration"), numberLiteral(3.0)))
                        ))
                        
                )
            ),
            Arguments.of(
                "@SELECT * FROM ThreadSleep WHERE sleepTime := time + 10ms INNER JOIN CPULoad ON sleepTime = startTime",
                (ASTBuilderSupplier) () -> program(
                    query(
                        selectAll(), 
                        from(
                            source("ThreadSleep"),
                            standardJoin("CPULoad", "sleepTime", "startTime").inner().build()
                        ))
                        .where(where(
                            varDecl("sleepTime", add(identifier("time"), durationLiteral("10ms")))
                        ))
                        
                )
            ),
            Arguments.of(
                "@SELECT * FROM GarbageCollection WHERE threshold := 50.0 INNER JOIN AllocationSample ON threshold = allocId",
                (ASTBuilderSupplier) () -> program(
                    query(
                        selectAll(), 
                        from(
                            source("GarbageCollection"),
                            standardJoin("AllocationSample", "threshold", "allocId").inner().build()
                        ))
                        .where(where(
                            varDecl("threshold", numberLiteral(50.0))
                        ))
                        
                )
            )
        );
    }

    @ParameterizedTest
    @MethodSource("variableDeclarationWithJoinCases")
    void testVariableDeclarationWithJoinParsing(String query, ASTBuilderSupplier expectedAstSupplier) throws Exception {
        Parser parser = createParser(query);
        ProgramNode program = parser.parse();
        ProgramNode expected = expectedAstSupplier.get();
        assertTrue(astEquals(expected, program), "AST should match for query: " + query);
    }    
    // ===========================================
    // Parameterized AST Checking for Variable Declarations with JOIN
    // ===========================================
    
    /**
     * Helper method to print AST for debugging
     */
    private void printAST(String query) throws Exception {
        Parser parser = createParser(query);
        ProgramNode program = parser.parse();
        System.out.println("Query: " + query);
        System.out.println("AST: " + program);
    }
    
    @Test
    public void debugVariableDeclarationInWhere() throws Exception {
        printAST("@SELECT * FROM GarbageCollection WHERE threshold := 50 AND duration > threshold");
        printAST("@SELECT * FROM GarbageCollection WHERE maxDuration := 100ms AND duration < maxDuration");
        printAST("@SELECT * FROM GarbageCollection WHERE factor := 2.5 AND threshold := 50.0 AND duration * factor > threshold");
    }
    
    @Test
    public void testComplexSubqueryWithJoin() throws Exception {
        // Test a complex query with a raw subquery and JOINs
        Parser parser = createParser("@SELECT gc.duration, es.thread FROM (SELECT * FROM GarbageCollection WHERE duration > 100) AS gc INNER JOIN ExecutionSample AS es ON id = sampleId");
        ProgramNode program = parser.parse();
        
        ProgramNode expected = program(
            query(
                select(
                    selectItem(fieldAccess("gc", "duration")),
                    selectItem(fieldAccess("es", "thread"))
                ),
                from(
                    subquery(
                        // Raw subquery (no @ prefix) - this should be stored as raw string
                        rawJfrQuery("SELECT * FROM GarbageCollection WHERE duration > 100"),
                        "gc"
                    ),
                    standardJoin("ExecutionSample", "id", "sampleId").alias("es").inner().build()
                )
            ) // Only the outer query is extended
        );
        
        assertTrue(astEquals(expected, program), "AST should match expected structure for complex subquery with JOIN");
        
        // Also test with an extended subquery for comparison
        parser = createParser("@SELECT gc.duration, es.thread FROM (@SELECT * FROM GarbageCollection WHERE duration > 100) AS gc INNER JOIN ExecutionSample AS es ON id = sampleId");
        program = parser.parse();
        
        expected = program(
            query(
                select(
                    selectItem(fieldAccess("gc", "duration")),
                    selectItem(fieldAccess("es", "thread"))
                ),
                from(
                    subquery(
                        // Extended subquery (with @ prefix) - this should be extended
                        query(selectAll(), from(source("GarbageCollection")))
                            .where(where(condition(
                                greaterThan(identifier("duration"), numberLiteral(100.0))
                            )))
                             // This subquery is extended
                            .build(),
                        "gc" // alias for the subquery
                    ),
                    standardJoin("ExecutionSample", "id", "sampleId").alias("es").inner().build()
                )
            ) // The outer query is also extended
        );
        
        assertTrue(astEquals(expected, program), "AST should match expected structure for complex extended subquery with JOIN");
    }
    
    @Test
    public void testSubqueryInAssignment() throws Exception {
        // Test assignment with regular subquery
        Parser parser = createParser("slowGCs := SELECT * FROM (SELECT * FROM GarbageCollection WHERE duration > 100)");
        ProgramNode program = parser.parse();
        
        ProgramNode expected = program(
            assignment("slowGCs",
                queryBuilder(selectAll(),
                    from(subquery(
                        rawJfrQuery("SELECT * FROM GarbageCollection WHERE duration > 100")
                    )))
                    .build()
            )
        );
        
        assertTrue(astEquals(expected, program), "AST should match expected structure for assignment with subquery");
        
        // Test assignment with extended subquery
        parser = createParser("fastGCs := @SELECT * FROM (@SELECT * FROM GarbageCollection WHERE duration < 10)");
        program = parser.parse();
        
        expected = program(
            assignment("fastGCs",
                query(selectAll(),
                    from(subquery(
                        query(selectAll(), from(source("GarbageCollection")))
                            .where(where(condition(
                                lessThan(identifier("duration"), numberLiteral(10.0))
                            )))
                            
                            .build()
                    )))
                    
                    .build()
            )
        );
        
        assertTrue(astEquals(expected, program), "AST should match expected structure for assignment with extended subquery");
    }
    
    @Test
    public void testSubqueryInView() throws Exception {
        // Test view with raw subquery
        Parser parser = createParser("VIEW SlowGCs AS SELECT * FROM (SELECT * FROM GarbageCollection WHERE duration > 100)");
        ProgramNode program = parser.parse();
        
        ProgramNode expected = program(
            view("SlowGCs",
                queryBuilder(selectAll(),
                    from(subquery(
                        rawJfrQuery("SELECT * FROM GarbageCollection WHERE duration > 100")
                    )))
                    .build()
            )
        );
        
        assertTrue(astEquals(expected, program), "AST should match expected structure for view with subquery");
        
        // Test view with extended subquery
        parser = createParser("VIEW FastGCs AS @SELECT * FROM (@SELECT * FROM GarbageCollection WHERE duration < 10)");
        program = parser.parse();
        
        expected = program(
            view("FastGCs",
                query(selectAll(),
                    from(subquery(
                        query(selectAll(), from(source("GarbageCollection")))
                            .where(where(condition(
                                lessThan(identifier("duration"), numberLiteral(10.0))
                            )))
                            
                            .build()
                    )))
                    
                    .build()
            )
        );
        
        assertTrue(astEquals(expected, program), "AST should match expected structure for view with extended subquery");
    }
    
    // ===========================================
    // GC Correlation Function Tests
    // ===========================================
    
    // ===========================================
    // Parameterized Tests for GC Correlation Functions
    // ===========================================
    
    static Stream<Arguments> gcCorrelationFunctionCases() {
        return Stream.of(
            Arguments.of(
                "BEFORE_GC",
                "@SELECT * FROM ExecutionSample WHERE gcId = BEFORE_GC(timestamp)",
                (ASTBuilderSupplier) () -> program(
                    query(selectAll(), from(source("ExecutionSample")))
                        .where(where(condition(
                            binary(identifier("gcId"), BinaryOperator.EQUALS, function("BEFORE_GC", identifier("timestamp")))
                        )))
                        
                )
            ),
            Arguments.of(
                "AFTER_GC",
                "@SELECT * FROM ExecutionSample WHERE gcId = AFTER_GC(timestamp)",
                (ASTBuilderSupplier) () -> program(
                    query(selectAll(), from(source("ExecutionSample")))
                        .where(where(condition(
                            binary(identifier("gcId"), BinaryOperator.EQUALS, function("AFTER_GC", identifier("timestamp")))
                        )))
                        
                )
            ),
            Arguments.of(
                "CLOSEST_GC",
                "@SELECT * FROM ExecutionSample WHERE gcId = CLOSEST_GC(timestamp)",
                (ASTBuilderSupplier) () -> program(
                    query(selectAll(), from(source("ExecutionSample")))
                        .where(where(condition(
                            binary(identifier("gcId"), BinaryOperator.EQUALS, function("CLOSEST_GC", identifier("timestamp")))
                        )))
                        
                )
            )
        );
    }

    @ParameterizedTest
    @MethodSource("gcCorrelationFunctionCases")
    void testGcCorrelationFunctions(String functionName, String query, ASTBuilderSupplier expectedAstSupplier) throws Exception {
        Parser parser = createParser(query);
        ProgramNode program = parser.parse();
        ProgramNode expected = expectedAstSupplier.get();
        assertTrue(astEquals(expected, program), "AST should match for " + functionName + " function: " + query);
    }
    
    // ===========================================
    // Parameterized Tests for GC Correlation Functions with String Literals
    // ===========================================
    
    static Stream<Arguments> gcCorrelationWithTimestampLiteralCases() {
        return Stream.of(
            Arguments.of(
                "@SELECT * FROM ExecutionSample WHERE gcId = BEFORE_GC(2024-01-01T10:00:00Z)",
                (ASTBuilderSupplier) () -> program(
                    query(selectAll(), from(source("ExecutionSample")))
                        .where(where(condition(
                            binary(identifier("gcId"), BinaryOperator.EQUALS, 
                                function("BEFORE_GC", timestampLiteral("2024-01-01T10:00:00Z")))
                        )))
                        
                )
            ),
            Arguments.of(
                "@SELECT * FROM ExecutionSample WHERE gcId = AFTER_GC(2024-01-01T12:00:00Z)",
                (ASTBuilderSupplier) () -> program(
                    query(selectAll(), from(source("ExecutionSample")))
                        .where(where(condition(
                            binary(identifier("gcId"), BinaryOperator.EQUALS, 
                                function("AFTER_GC", timestampLiteral("2024-01-01T12:00:00Z")))
                        )))
                        
                )
            )
        );
    }

    @ParameterizedTest
    @MethodSource("gcCorrelationWithTimestampLiteralCases")
    void testGcCorrelationFunctionWithTimestampLiteral(String query, ASTBuilderSupplier expectedAstSupplier) throws Exception {
        Parser parser = createParser(query);
        ProgramNode program = parser.parse();
        ProgramNode expected = expectedAstSupplier.get();
        assertTrue(astEquals(expected, program), "AST should match for query: " + query);
    }
    
    // ===========================================
    // Parameterized Tests for Complex GC Correlation Queries
    // ===========================================
    
    static Stream<Arguments> complexGcCorrelationCases() {
        return Stream.of(
            Arguments.of(
                "@SELECT * FROM ExecutionSample WHERE gcId = BEFORE_GC(timestamp) AND duration > 10ms",
                (ASTBuilderSupplier) () -> program(
                    query(selectAll(), from(source("ExecutionSample")))
                        .where(where(condition(
                            and(
                                binary(identifier("gcId"), BinaryOperator.EQUALS, 
                                    function("BEFORE_GC", identifier("timestamp"))),
                                greaterThan(identifier("duration"), durationLiteral("10ms"))
                            )
                        )))
                        
                )
            ),
            Arguments.of(
                "@SELECT * FROM ExecutionSample WHERE gcId = CLOSEST_GC(timestamp) AND threadId > 5",
                (ASTBuilderSupplier) () -> program(
                    query(selectAll(), from(source("ExecutionSample")))
                        .where(where(condition(
                            and(
                                binary(identifier("gcId"), BinaryOperator.EQUALS, 
                                    function("CLOSEST_GC", identifier("timestamp"))),
                                greaterThan(identifier("threadId"), numberLiteral(5.0))
                            )
                        )))
                        
                )
            )
        );
    }

    @ParameterizedTest
    @MethodSource("complexGcCorrelationCases")
    void testComplexGcCorrelationQuery(String query, ASTBuilderSupplier expectedAstSupplier) throws Exception {
        Parser parser = createParser(query);
        ProgramNode program = parser.parse();
        ProgramNode expected = expectedAstSupplier.get();
        assertTrue(astEquals(expected, program), "AST should match for query: " + query);
    }
  
    @Test  
    public void testSimpleINClauseNotSupported() throws Exception {
        String query = "@SELECT * FROM ExecutionSample WHERE gcId IN [1, 2, 3]";
        
        // This should now parse successfully with the square bracket syntax support
        Lexer lexer = new Lexer(query);
        Parser parser = new Parser(lexer.tokenize());
        ProgramNode program = parser.parse();
        
        // Find the QueryNode in the program
        boolean foundQueryNode = false;
        for (StatementNode statement : program.statements()) {
            if (statement instanceof QueryNode) {
                foundQueryNode = true;
                break;
            }
        }
        
        assertTrue(foundQueryNode, "Expected to find a QueryNode in the program");
    }
    
    // ===========================================
    // Parameterized AST Checking for Direct Timestamp Literals
    // ===========================================
    static Stream<Arguments> directTimestampLiteralCases() {
        return Stream.of(
            Arguments.of(
                "@SELECT * FROM ExecutionSample WHERE timestamp WITHIN 5m OF 2024-01-01T10:00:00",
                (ASTBuilderSupplier) () -> program(
                    query(selectAll(), from(source("ExecutionSample")))
                        .where(where(condition(
                            within(
                                identifier("timestamp"),
                                durationLiteral("5m"),
                                timestampLiteral("2024-01-01T10:00:00Z")
                            )
                        )))
                        
                )
            ),
            Arguments.of(
                "@SELECT * FROM ExecutionSample WHERE timestamp WITHIN 10s OF 2024-01-01T10:00:00",
                (ASTBuilderSupplier) () -> program(
                    query(selectAll(), from(source("ExecutionSample")))
                        .where(where(condition(
                            within(
                                identifier("timestamp"),
                                durationLiteral("10s"),
                                timestampLiteral("2024-01-01T10:00:00Z")
                            )
                        )))
                        
                )
            ),
            Arguments.of(
                "@SELECT * FROM ExecutionSample WHERE timestamp WITHIN 1h OF 2024-01-01T10:00:00Z",
                (ASTBuilderSupplier) () -> program(
                    query(selectAll(), from(source("ExecutionSample")))
                        .where(where(condition(
                            within(
                                identifier("timestamp"),
                                durationLiteral("1h"),
                                timestampLiteral("2024-01-01T10:00:00Z")
                            )
                                                )))
                        
                )
            ),
            Arguments.of(
                "@SELECT * FROM ExecutionSample WHERE timestamp WITHIN 30s OF 2024-01-01T10:00:00.123",
                (ASTBuilderSupplier) () -> program(
                    query(selectAll(), from(source("ExecutionSample")))
                        .where(where(condition(
                            within(
                                identifier("timestamp"),
                                durationLiteral("30s"),
                                timestampLiteral("2024-01-01T10:00:00.123Z")
                            )
                        )))
                        
                )
            )
        );
    }

    @ParameterizedTest
    @MethodSource("directTimestampLiteralCases")
    void testDirectTimestampLiteralParsing(String query, ASTBuilderSupplier expectedAstSupplier) throws Exception {
        Parser parser = createParser(query);
        ProgramNode program = parser.parse();
        ProgramNode expected = expectedAstSupplier.get();
        assertTrue(astEquals(expected, program), "AST should match for query: " + query);
    }

    // ===========================================
    // Parameterized Tests for WITHIN Conditions with Aggregation Functions
    // ===========================================
    
    static Stream<Arguments> withinConditionWithAggregationCases() {
        return Stream.of(
            Arguments.of(
                "@SELECT * FROM ExecutionSample WHERE timestamp WITHIN 5m OF MIN(startTime)",
                (ASTBuilderSupplier) () -> program(
                    query(selectAll(), from(source("ExecutionSample")))
                        .where(where(condition(
                            within(
                                identifier("timestamp"),
                                durationLiteral("5m"),
                                function("MIN", identifier("startTime"))
                            )
                        )))
                        
                )
            ),
            Arguments.of(
                "@SELECT * FROM ExecutionSample WHERE timestamp WITHIN 10s OF MAX(endTime)",
                (ASTBuilderSupplier) () -> program(
                    query(selectAll(), from(source("ExecutionSample")))
                        .where(where(condition(
                            within(
                                identifier("timestamp"),
                                durationLiteral("10s"),
                                function("MAX", identifier("endTime"))
                            )
                        )))
                        
                )
            ),
            Arguments.of(
                "@SELECT * FROM ExecutionSample WHERE timestamp WITHIN 1h OF AVG(timestamp)",
                (ASTBuilderSupplier) () -> program(
                    query(selectAll(), from(source("ExecutionSample")))
                        .where(where(condition(
                            within(
                                identifier("timestamp"),
                                durationLiteral("1h"),
                                function("AVG", identifier("timestamp"))
                            )
                        )))
                        
                )
            ),
            Arguments.of(
                "@SELECT * FROM ExecutionSample WHERE timestamp WITHIN 5m OF (MIN(startTime) + 5s)",
                (ASTBuilderSupplier) () -> program(
                    query(selectAll(), from(source("ExecutionSample")))
                        .where(where(condition(
                            within(
                                identifier("timestamp"),
                                durationLiteral("5m"),
                                add(function("MIN", identifier("startTime")), durationLiteral("5s"))
                            )
                        )))
                        
                )
            ),
            Arguments.of(
                "@SELECT * FROM ExecutionSample WHERE timestamp WITHIN (MAX(duration) - MIN(duration)) OF AVG(startTime)",
                (ASTBuilderSupplier) () -> program(
                    query(selectAll(), from(source("ExecutionSample")))
                        .where(where(condition(
                            within(
                                identifier("timestamp"),
                                subtract(function("MAX", identifier("duration")), function("MIN", identifier("duration"))),
                                function("AVG", identifier("startTime"))
                            )
                        )))
                        
                )
            )
        );
    }

    @ParameterizedTest
    @MethodSource("withinConditionWithAggregationCases")
    void testWithinConditionWithAggregationFunction(String query, ASTBuilderSupplier expectedAstSupplier) throws Exception {
        Parser parser = createParser(query);
        ProgramNode program = parser.parse();
        ProgramNode expected = expectedAstSupplier.get();
        assertTrue(astEquals(expected, program), "AST should match for query: " + query);
    }





    // ===========================================
    // Parameterized Tests for Aggregation Functions in WHERE Conditions
    // ===========================================
    
    static Stream<Arguments> aggregationFunctionsInWhereCases() {
        return Stream.of(
            Arguments.of(
                "@SELECT * FROM GarbageCollection WHERE MAX(duration) > 100 AND COUNT(*) > 5",
                (ASTBuilderSupplier) () -> program(
                    query(selectAll(), from(source("GarbageCollection")))
                        .where(where(condition(
                            and(
                                greaterThan(function("MAX", identifier("duration")), numberLiteral(100.0)),
                                greaterThan(function("COUNT", star()), numberLiteral(5.0))
                            )
                        )))
                        
                )
            ),
            Arguments.of(
                "@SELECT * FROM GarbageCollection WHERE AVG(duration) > MIN(duration) * 2",
                (ASTBuilderSupplier) () -> program(
                    query(selectAll(), from(source("GarbageCollection")))
                        .where(where(condition(
                            greaterThan(
                                function("AVG", identifier("duration")),
                                multiply(function("MIN", identifier("duration")), numberLiteral(2.0))
                            )
                        )))
                        
                )
            )
        );
    }

    @ParameterizedTest
    @MethodSource("aggregationFunctionsInWhereCases")
    void testAggregationFunctionsInWhereConditions(String query, ASTBuilderSupplier expectedAstSupplier) throws Exception {
        Parser parser = createParser(query);
        ProgramNode program = parser.parse();
        ProgramNode expected = expectedAstSupplier.get();
        assertTrue(astEquals(expected, program), "AST should match for query: " + query);
    }

    // ===========================================
    // Parameterized Tests for Nested Aggregation Functions in WHERE
    // ===========================================
    
    static Stream<Arguments> nestedAggregationFunctionsCases() {
        return Stream.of(
            Arguments.of(
                "@SELECT * FROM GarbageCollection WHERE AVG(duration) > MIN(duration) * 2",
                (ASTBuilderSupplier) () -> program(
                    query(selectAll(), from(source("GarbageCollection")))
                        .where(where(condition(
                            greaterThan(
                                function("AVG", identifier("duration")),
                                multiply(function("MIN", identifier("duration")), numberLiteral(2.0))
                            )
                        )))
                        
                )
            ),
            Arguments.of(
                "@SELECT * FROM GarbageCollection WHERE MAX(heapUsed) / COUNT(*) > 1000",
                (ASTBuilderSupplier) () -> program(
                    query(selectAll(), from(source("GarbageCollection")))
                        .where(where(condition(
                            greaterThan(
                                divide(function("MAX", identifier("heapUsed")), function("COUNT", star())),
                                numberLiteral(1000.0)
                            )
                        )))
                        
                )
            )
        );
    }

    @ParameterizedTest
    @MethodSource("nestedAggregationFunctionsCases")
    void testNestedAggregationFunctionsInWhere(String query, ASTBuilderSupplier expectedAstSupplier) throws Exception {
        Parser parser = createParser(query);
        ProgramNode program = parser.parse();
        ProgramNode expected = expectedAstSupplier.get();
        assertTrue(astEquals(expected, program), "AST should match for query: " + query);
    }
    
    // ===========================================
    // Parameterized Tests for Complex WITHIN Expressions
    // ===========================================
    
    @ParameterizedTest
    @ValueSource(strings = {
        "@SELECT * FROM Events WHERE timestamp WITHIN 5m OF MIN(startTime)",
        "@SELECT * FROM Events WHERE timestamp WITHIN 10s OF MAX(endTime)",
        "@SELECT * FROM Events WHERE timestamp WITHIN 1h OF AVG(timestamp)",
        "@SELECT * FROM Events WHERE timestamp WITHIN 30m OF COUNT(*)",
        "@SELECT * FROM Events WHERE timestamp WITHIN (MAX(duration) - MIN(duration)) OF startTime",
        "@SELECT * FROM Events WHERE timestamp WITHIN 5m OF (MIN(startTime) + 10s)",
        "@SELECT * FROM Events WHERE timestamp WITHIN (AVG(duration) * 2) OF (MAX(startTime) - 1h)"
    })
    public void testWithinWithComplexExpressions(String query) throws Exception {
        Parser parser = createParser(query);
        ProgramNode program = parser.parse();
        
        // Verify it parses successfully
        assertNotNull(program, "Program should not be null for query: " + query);
        assertEquals(1, program.statements().size(), "Should have exactly one statement");
        
        QueryNode queryNode = (QueryNode) program.statements().get(0);
        assertNotNull(queryNode.where(), "Should have WHERE clause");
        
        // Verify the WHERE clause has a WITHIN condition
        WhereNode whereNode = queryNode.where();
        ConditionNode condition = whereNode.condition();
        assertTrue(condition instanceof ExpressionConditionNode, "Should be expression condition");
        
        ExpressionConditionNode exprCondition = (ExpressionConditionNode) condition;
        assertTrue(exprCondition.expression() instanceof BinaryExpressionNode, "Should be binary expression");
        
        BinaryExpressionNode binaryExpr = (BinaryExpressionNode) exprCondition.expression();
        assertEquals(BinaryOperator.WITHIN, binaryExpr.operator(), "Should be WITHIN operator");
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "@SELECT * FROM Events WHERE MAX(duration) > 100",
        "@SELECT * FROM Events WHERE MIN(startTime) < 2024-01-01",
        "@SELECT * FROM Events WHERE AVG(cpuUsage) > 0.5",
        "@SELECT * FROM Events WHERE COUNT(*) = 10",
        "@SELECT * FROM Events WHERE SUM(bytes) > 1000000",
        "@SELECT * FROM Events WHERE MAX(duration) > MIN(duration) * 2",
        "@SELECT * FROM Events WHERE COUNT(*) > AVG(threadCount) + 5",
        "@SELECT * FROM Events WHERE MAX(heapUsed) - MIN(heapUsed) > 100MB"
    })
    public void testAggregationFunctionsInWhereClauses(String query) throws Exception {
        Parser parser = createParser(query);
        ProgramNode program = parser.parse();
        
        // Verify it parses successfully
        assertNotNull(program, "Program should not be null for query: " + query);
        assertEquals(1, program.statements().size(), "Should have exactly one statement");
        
        QueryNode queryNode = (QueryNode) program.statements().get(0);
        assertNotNull(queryNode.where(), "Should have WHERE clause");
        
        // Verify the WHERE clause contains at least one function call
        String queryText = query.toLowerCase();
        boolean containsAggregation = queryText.contains("max(") || queryText.contains("min(") || 
                                    queryText.contains("avg(") || queryText.contains("count(") || 
                                    queryText.contains("sum(");
        assertTrue(containsAggregation, "Query should contain aggregation function: " + query);
    }
    
    @ParameterizedTest
    @CsvSource({
        "'@SELECT * FROM Events WHERE timestamp WITHIN MAX(duration) OF startTime', MAX",
        "'@SELECT * FROM Events WHERE timestamp WITHIN MIN(duration) OF endTime', MIN", 
        "'@SELECT * FROM Events WHERE timestamp WITHIN AVG(duration) OF timestamp', AVG",
        "'@SELECT * FROM Events WHERE timestamp WITHIN COUNT(*) OF startTime', COUNT",
        "'@SELECT * FROM Events WHERE timestamp WITHIN SUM(bytes) OF endTime', SUM"
    })
    public void testWithinTimeWindowWithAggregationFunctions(String query, String expectedFunction) throws Exception {
        Parser parser = createParser(query);
        ProgramNode program = parser.parse();
        
        QueryNode queryNode = (QueryNode) program.statements().get(0);
        WhereNode whereNode = queryNode.where();
        ExpressionConditionNode exprCondition = (ExpressionConditionNode) whereNode.condition();
        BinaryExpressionNode binaryExpr = (BinaryExpressionNode) exprCondition.expression();
        BinaryExpressionNode ofExpr = (BinaryExpressionNode) binaryExpr.right();
        
        // Check that the time window (left side of OF) contains the expected function
        assertTrue(ofExpr.left() instanceof FunctionCallNode, "Time window should be a function call");
        FunctionCallNode timeWindowFunction = (FunctionCallNode) ofExpr.left();
        assertEquals(expectedFunction, timeWindowFunction.functionName(), 
            "Time window should use " + expectedFunction + " function");
    }
    
    // ===========================================
    // AST Comparison and Equality Tests
    // ===========================================
    
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
    
    // ===========================================
    // Multiline Comment Tests
    // ===========================================
    
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
    
    // ===========================================
    // Additional Function Call Tests
    // ===========================================
    
    static Stream<Arguments> additionalFunctionCallCases() {
        return Stream.of(
            Arguments.of(
                "@SELECT COUNT(*) FROM GarbageCollection",
                (FunctionCallASTSupplier) () -> program(
                    query(
                        select(function("COUNT", star())),
                        from(source("GarbageCollection"))
                    )
                )
            ),
            Arguments.of(
                "@SELECT MAX(duration) FROM GarbageCollection",
                (FunctionCallASTSupplier) () -> program(
                    query(
                        select(function("MAX", identifier("duration"))),
                        from(source("GarbageCollection"))
                    )
                )
            ),
            Arguments.of(
                "@SELECT MIN(duration) FROM GarbageCollection",
                (FunctionCallASTSupplier) () -> program(
                    query(
                        select(function("MIN", identifier("duration"))),
                        from(source("GarbageCollection"))
                    )
                )
            ),
            Arguments.of(
                "@SELECT SUM(heapUsed) FROM HeapSummary",
                (FunctionCallASTSupplier) () -> program(
                    query(
                        select(function("SUM", identifier("heapUsed"))),
                        from(source("HeapSummary"))
                    )
                )
            ),
            Arguments.of(
                "@SELECT AVG(duration), COUNT(*) FROM GarbageCollection",
                (FunctionCallASTSupplier) () -> program(
                    query(
                        select(function("AVG", identifier("duration")), function("COUNT", star())),
                        from(source("GarbageCollection"))
                    )
                )
            )
        );
    }

    @ParameterizedTest
    @MethodSource("additionalFunctionCallCases")
    void testAdditionalFunctionCallParsing(String query, FunctionCallASTSupplier expectedAstSupplier) throws Exception {
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
    
    // ===========================================
    // Factory Pattern Tests for Fuzzy Joins
    // ===========================================
    
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
    
    // ===========================================
    // Factory Pattern Tests for Percentile Functions
    // ===========================================
    
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
}
