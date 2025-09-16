package me.bechberger.jfr.extended;

import me.bechberger.jfr.extended.ast.ASTNodes.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test class for multi-line query parsing behavior.
 * Tests that queries spanning multiple lines (without explicit separators) are parsed as single statements,
 * and that queries separated by empty lines or semicolons are parsed as multiple statements.
 */
public class MultiLineQueryTest {
    
    private Parser createParser(String query) throws Exception {
        Lexer lexer = new Lexer(query);
        List<Token> tokens = lexer.tokenize();
        return new Parser(tokens, query);
    }
    
    @Test
    public void testMultiLineRawQuery() throws Exception {
        String query = """
            SELECT *
            FROM GarbageCollection
            WHERE duration > 10ms
            """;
        Parser parser = createParser(query);
        ProgramNode program = parser.parse();
        
        assertEquals(1, program.statements().size(), "Multi-line raw query should be parsed as single statement");
        assertTrue(program.statements().get(0) instanceof RawJfrQueryNode, "Should be a RawJfrQueryNode");
        
        RawJfrQueryNode raw = (RawJfrQueryNode) program.statements().get(0);
        String rawContent = raw.rawQuery().trim();
        assertTrue(rawContent.contains("SELECT *"), "Raw query should contain SELECT *");
        assertTrue(rawContent.contains("FROM GarbageCollection"), "Raw query should contain FROM clause");
        assertTrue(rawContent.contains("WHERE duration > 10ms"), "Raw query should contain WHERE clause");
    }
    
    @Test
    public void testMultiLineExtendedQuery() throws Exception {
        String query = """
            @SELECT *
            FROM GarbageCollection
            WHERE duration > 10ms
            """;
        Parser parser = createParser(query);
        ProgramNode program = parser.parse();
        
        assertEquals(1, program.statements().size(), "Multi-line extended query should be parsed as single statement");
        assertTrue(program.statements().get(0) instanceof QueryNode, "Should be a QueryNode");
        
        QueryNode queryNode = (QueryNode) program.statements().get(0);
        assertTrue(queryNode.isExtended(), "Should be extended query");
        assertEquals(1, queryNode.from().sources().size(), "Should have one source");
        
        SourceNodeBase source = queryNode.from().sources().get(0);
        assertTrue(source instanceof SourceNode, "Should be a SourceNode");
        assertEquals("GarbageCollection", ((SourceNode) source).source(), "Source should be GarbageCollection");
    }
    
    @ParameterizedTest
    @ValueSource(strings = {
        """
        @SELECT * FROM GarbageCollection
        WHERE duration > 10ms
        AND name LIKE '%gc%'
        """,
        """
        @SELECT duration, name
        FROM GarbageCollection
        WHERE duration > 10ms
        ORDER BY duration DESC
        """,
        """
        @SELECT *
        FROM GarbageCollection AS g
        JOIN ExecutionSample AS e ON g.startTime = e.startTime
        WHERE g.duration > 10ms
        """
    })
    public void testVariousMultiLineExtendedQueries(String query) throws Exception {
        Parser parser = createParser(query);
        ProgramNode program = parser.parse();
        
        assertEquals(1, program.statements().size(), "Multi-line extended query should be parsed as single statement");
        assertTrue(program.statements().get(0) instanceof QueryNode, "Should be a QueryNode");
        
        QueryNode queryNode = (QueryNode) program.statements().get(0);
        assertTrue(queryNode.isExtended(), "Should be extended query");
        assertNotNull(queryNode.select(), "Should have select clause");
        assertNotNull(queryNode.from(), "Should have from clause");
        assertFalse(queryNode.from().sources().isEmpty(), "Should have at least one source");
    }
    
    @Test
    public void testExtendedQueriesSeparatedByEmptyLine() throws Exception {
        String query = """
            @SELECT * FROM GarbageCollection
            
            @SELECT * FROM ExecutionSample
            """;
        Parser parser = createParser(query);
        ProgramNode program = parser.parse();
        
        assertEquals(2, program.statements().size(), "Queries separated by empty line should be parsed as two statements");
        
        assertTrue(program.statements().get(0) instanceof QueryNode, "First should be a QueryNode");
        assertTrue(program.statements().get(1) instanceof QueryNode, "Second should be a QueryNode");
        
        QueryNode firstQuery = (QueryNode) program.statements().get(0);
        QueryNode secondQuery = (QueryNode) program.statements().get(1);
        
        assertTrue(firstQuery.isExtended(), "First should be extended query");
        assertTrue(secondQuery.isExtended(), "Second should be extended query");
        
        // Verify the source tables are correct
        assertEquals("GarbageCollection", 
            ((SourceNode) firstQuery.from().sources().get(0)).source(), 
            "First query should be from GarbageCollection");
        assertEquals("ExecutionSample", 
            ((SourceNode) secondQuery.from().sources().get(0)).source(), 
            "Second query should be from ExecutionSample");
    }
    
    @Test
    public void testRawQueriesSeparatedByEmptyLine() throws Exception {
        String query = """
            SELECT * FROM GarbageCollection
            
            SELECT * FROM ExecutionSample
            """;
        Parser parser = createParser(query);
        ProgramNode program = parser.parse();
        
        // Expected behavior: raw queries separated by empty lines should be parsed as separate statements
        assertEquals(2, program.statements().size(), "Raw queries with empty line should be parsed as separate statements");
        assertTrue(program.statements().get(0) instanceof RawJfrQueryNode, "First should be a RawJfrQueryNode");
        assertTrue(program.statements().get(1) instanceof RawJfrQueryNode, "Second should be a RawJfrQueryNode");
        
        RawJfrQueryNode firstRaw = (RawJfrQueryNode) program.statements().get(0);
        RawJfrQueryNode secondRaw = (RawJfrQueryNode) program.statements().get(1);
        
        assertEquals("SELECT * FROM GarbageCollection", firstRaw.rawQuery().trim(), "First query should be GarbageCollection");
        assertEquals("SELECT * FROM ExecutionSample", secondRaw.rawQuery().trim(), "Second query should be ExecutionSample");
    }
    
    @Test
    public void testMixedQueriesSeparatedByEmptyLine() throws Exception {
        String query = """
            @SELECT * FROM GarbageCollection
            
            SHOW EVENTS
            """;
        Parser parser = createParser(query);
        ProgramNode program = parser.parse();
        
        assertEquals(2, program.statements().size(), "Mixed queries separated by empty line should be parsed as two statements");
        
        assertTrue(program.statements().get(0) instanceof QueryNode, "First should be a QueryNode");
        assertTrue(program.statements().get(1) instanceof ShowEventsNode, "Second should be a ShowEventsNode");
        
        QueryNode firstQuery = (QueryNode) program.statements().get(0);
        assertTrue(firstQuery.isExtended(), "First should be extended query");
        assertEquals("GarbageCollection", 
            ((SourceNode) firstQuery.from().sources().get(0)).source(), 
            "First query should be from GarbageCollection");
    }
    
    @Test
    public void testMultiLineQueryWithComments() throws Exception {
        String query = """
            @SELECT * -- Select all fields
            FROM GarbageCollection -- From GC events
            WHERE duration > 10ms -- Only long GCs
            """;
        Parser parser = createParser(query);
        ProgramNode program = parser.parse();
        
        assertEquals(1, program.statements().size(), "Multi-line query with comments should be parsed as single statement");
        assertTrue(program.statements().get(0) instanceof QueryNode, "Should be a QueryNode");
        
        QueryNode queryNode = (QueryNode) program.statements().get(0);
        assertTrue(queryNode.isExtended(), "Should be extended query");
        assertEquals("GarbageCollection", 
            ((SourceNode) queryNode.from().sources().get(0)).source(), 
            "Source should be GarbageCollection");
    }
    
    @Test
    public void testMultipleEmptyLinesSeparation() throws Exception {
        String query = """
            @SELECT * FROM GarbageCollection
            
            
            @SELECT * FROM ExecutionSample
            """;
        Parser parser = createParser(query);
        ProgramNode program = parser.parse();
        
        assertEquals(2, program.statements().size(), "Queries separated by multiple empty lines should be parsed as two statements");
        
        assertTrue(program.statements().get(0) instanceof QueryNode, "First should be a QueryNode");
        assertTrue(program.statements().get(1) instanceof QueryNode, "Second should be a QueryNode");
        
        QueryNode firstQuery = (QueryNode) program.statements().get(0);
        QueryNode secondQuery = (QueryNode) program.statements().get(1);
        
        assertEquals("GarbageCollection", 
            ((SourceNode) firstQuery.from().sources().get(0)).source(), 
            "First query should be from GarbageCollection");
        assertEquals("ExecutionSample", 
            ((SourceNode) secondQuery.from().sources().get(0)).source(), 
            "Second query should be from ExecutionSample");
    }
    
    @Test
    public void testMultiLineShowQuery() throws Exception {
        String query = """
            SHOW FIELDS
            jdk.GarbageCollection
            """;
        Parser parser = createParser(query);
        ProgramNode program = parser.parse();
        
        assertEquals(1, program.statements().size(), "Multi-line SHOW query should be parsed as single statement");
        assertTrue(program.statements().get(0) instanceof ShowFieldsNode, "Should be a ShowFieldsNode");
        
        ShowFieldsNode show = (ShowFieldsNode) program.statements().get(0);
        assertEquals("jdk.GarbageCollection", show.eventType(), "Event type should be jdk.GarbageCollection");
    }
    
    @Test
    public void testMultiLineAssignmentQuery() throws Exception {
        String query = """
            $var := @SELECT *
            FROM GarbageCollection
            WHERE duration > 10ms
            """;
        Parser parser = createParser(query);
        ProgramNode program = parser.parse();
        
        assertEquals(1, program.statements().size(), "Multi-line assignment should be parsed as single statement");
        assertTrue(program.statements().get(0) instanceof AssignmentNode, "Should be an AssignmentNode");
        
        AssignmentNode assignment = (AssignmentNode) program.statements().get(0);
        assertEquals("var", assignment.variable(), "Variable should be 'var'");
        assertTrue(assignment.query() instanceof QueryNode, "Query should be a QueryNode");
        
        QueryNode queryNode = (QueryNode) assignment.query();
        assertTrue(queryNode.isExtended(), "Assigned query should be extended");
        assertEquals("GarbageCollection", 
            ((SourceNode) queryNode.from().sources().get(0)).source(), 
            "Source should be GarbageCollection");
    }
    
    // ===========================================
    // Multi-Statement Semicolon-Separated Tests
    // ===========================================
    
    static Stream<Arguments> multiStatementCases() {
        return Stream.of(
            Arguments.of(
                "@SELECT * FROM GarbageCollection; @SELECT duration FROM ExecutionSample",
                "Two extended queries separated by semicolon",
                2
            ),
            Arguments.of(
                "SELECT * FROM GarbageCollection; SELECT * FROM ExecutionSample",
                "Two raw queries separated by semicolon",
                2
            ),
            Arguments.of(
                "SHOW EVENTS; SHOW FIELDS jdk.GarbageCollection",
                "Two show statements separated by semicolon",
                2
            ),
            Arguments.of(
                "@SELECT * FROM GarbageCollection; SELECT * FROM ExecutionSample; SHOW EVENTS",
                "Mixed extended, raw, and show statements",
                3
            )
        );
    }
    
    @ParameterizedTest
    @MethodSource("multiStatementCases")
    public void testMultiStatementParsing(String query, String description, int expectedStatements) throws Exception {
        Parser parser = createParser(query);
        ProgramNode program = parser.parse();
        
        assertEquals(expectedStatements, program.statements().size(), 
            description + " should parse into " + expectedStatements + " statements");
        
        // Verify all statements are valid
        for (int i = 0; i < program.statements().size(); i++) {
            StatementNode stmt = program.statements().get(i);
            assertNotNull(stmt, "Statement " + i + " should not be null");
            assertTrue(stmt instanceof QueryNode || stmt instanceof RawJfrQueryNode || 
                      stmt instanceof ShowEventsNode || stmt instanceof ShowFieldsNode ||
                      stmt instanceof AssignmentNode || stmt instanceof ViewDefinitionNode,
                      "Statement " + i + " should be a valid statement type");
        }
    }
    
    @Test
    public void testExtendedQueriesWithSemicolon() throws Exception {
        String query = "@SELECT * FROM GarbageCollection; @SELECT duration FROM ExecutionSample";
        Parser parser = createParser(query);
        ProgramNode program = parser.parse();
        
        assertEquals(2, program.statements().size(), "Should parse into two statements");
        
        // Verify first statement
        assertTrue(program.statements().get(0) instanceof QueryNode, "First should be QueryNode");
        QueryNode first = (QueryNode) program.statements().get(0);
        assertTrue(first.isExtended(), "First should be extended query");
        assertEquals(1, first.from().sources().size(), "First should have one source");
        assertTrue(first.from().sources().get(0) instanceof SourceNode, "Source should be SourceNode");
        assertEquals("GarbageCollection", ((SourceNode) first.from().sources().get(0)).source());
        
        // Verify second statement
        assertTrue(program.statements().get(1) instanceof QueryNode, "Second should be QueryNode");
        QueryNode second = (QueryNode) program.statements().get(1);
        assertTrue(second.isExtended(), "Second should be extended query");
        assertEquals(1, second.from().sources().size(), "Second should have one source");
        assertTrue(second.from().sources().get(0) instanceof SourceNode, "Source should be SourceNode");
        assertEquals("ExecutionSample", ((SourceNode) second.from().sources().get(0)).source());
    }
    
    @Test
    public void testRawQueriesWithSemicolon() throws Exception {
        String query = "SELECT * FROM GarbageCollection; SELECT * FROM ExecutionSample";
        Parser parser = createParser(query);
        ProgramNode program = parser.parse();
        
        assertEquals(2, program.statements().size(), "Should parse into two statements");
        
        // Verify both are raw queries
        assertTrue(program.statements().get(0) instanceof RawJfrQueryNode, "First should be RawJfrQueryNode");
        assertTrue(program.statements().get(1) instanceof RawJfrQueryNode, "Second should be RawJfrQueryNode");
        
        RawJfrQueryNode first = (RawJfrQueryNode) program.statements().get(0);
        RawJfrQueryNode second = (RawJfrQueryNode) program.statements().get(1);
        
        assertTrue(first.rawQuery().contains("GarbageCollection"), "First should contain GarbageCollection");
        assertTrue(second.rawQuery().contains("ExecutionSample"), "Second should contain ExecutionSample");
    }
    
    @Test
    public void testShowStatementsWithSemicolon() throws Exception {
        String query = "SHOW EVENTS; SHOW FIELDS jdk.GarbageCollection";
        Parser parser = createParser(query);
        ProgramNode program = parser.parse();
        
        assertEquals(2, program.statements().size(), "Should parse into two statements");
        
        // Verify first is SHOW EVENTS
        assertTrue(program.statements().get(0) instanceof ShowEventsNode, "First should be ShowEventsNode");
        
        // Verify second is SHOW FIELDS with dotted identifier
        assertTrue(program.statements().get(1) instanceof ShowFieldsNode, "Second should be ShowFieldsNode");
        ShowFieldsNode showFields = (ShowFieldsNode) program.statements().get(1);
        assertEquals("jdk.GarbageCollection", showFields.eventType(), "Should support dotted event type");
    }
    
    // ===========================================
    // Multi-Line Single Statement Tests
    // ===========================================
    
    @Test
    public void testMultiLineRawQueryAsSingleStatement() throws Exception {
        String query = """
            SELECT *
            FROM GarbageCollection
            WHERE duration > 10ms
            """;
        Parser parser = createParser(query);
        ProgramNode program = parser.parse();
        
        assertEquals(1, program.statements().size(), "Multi-line raw query should be parsed as single statement");
        assertTrue(program.statements().get(0) instanceof RawJfrQueryNode, "Should be a RawJfrQueryNode");
        
        RawJfrQueryNode raw = (RawJfrQueryNode) program.statements().get(0);
        String rawContent = raw.rawQuery().trim();
        assertTrue(rawContent.contains("SELECT *"), "Raw query should contain SELECT *");
        assertTrue(rawContent.contains("FROM GarbageCollection"), "Raw query should contain FROM clause");
        assertTrue(rawContent.contains("WHERE duration > 10ms"), "Raw query should contain WHERE clause");
    }
    
    @Test
    public void testMultiLineExtendedQueryAsSingleStatement() throws Exception {
        String query = """
            @SELECT *
            FROM GarbageCollection
            WHERE duration > 10ms
            """;
        Parser parser = createParser(query);
        ProgramNode program = parser.parse();
        
        assertEquals(1, program.statements().size(), "Multi-line extended query should be parsed as single statement");
        assertTrue(program.statements().get(0) instanceof QueryNode, "Should be a QueryNode");
        
        QueryNode queryNode = (QueryNode) program.statements().get(0);
        assertTrue(queryNode.isExtended(), "Should be extended query");
        assertEquals(1, queryNode.from().sources().size(), "Should have one source");
        
        SourceNodeBase source = queryNode.from().sources().get(0);
        assertTrue(source instanceof SourceNode, "Should be a SourceNode");
        assertEquals("GarbageCollection", ((SourceNode) source).source(), "Source should be GarbageCollection");
    }
    
    // ===========================================
    // Empty Line Separation Tests (Current Behavior)
    // ===========================================
    
    // ===========================================
    // Assignment and View Statement Tests
    // ===========================================
    
    @Test
    public void testAssignmentAndViewDefinitionStatements() throws Exception {
        String query = """
            $gc := @SELECT * FROM GarbageCollection;
            VIEW myGcView AS @SELECT duration FROM GarbageCollection;
            SHOW EVENTS
            """;
        Parser parser = createParser(query);
        ProgramNode program = parser.parse();
        
        assertEquals(3, program.statements().size(), "Should parse into three statements");
        
        // Verify assignment
        assertTrue(program.statements().get(0) instanceof AssignmentNode, "First should be AssignmentNode");
        AssignmentNode assignment = (AssignmentNode) program.statements().get(0);
        assertEquals("gc", assignment.variable(), "Variable should be 'gc'");
        assertTrue(assignment.query().isExtended(), "Assigned query should be extended");
        
        // Verify view definition
        assertTrue(program.statements().get(1) instanceof ViewDefinitionNode, "Second should be ViewDefinitionNode");
        ViewDefinitionNode viewDef = (ViewDefinitionNode) program.statements().get(1);
        assertEquals("myGcView", viewDef.viewName(), "View name should be 'myGcView'");
        assertTrue(viewDef.query().isExtended(), "View query should be extended");
        
        // Verify show
        assertTrue(program.statements().get(2) instanceof ShowEventsNode, "Third should be ShowEventsNode");
    }
}

