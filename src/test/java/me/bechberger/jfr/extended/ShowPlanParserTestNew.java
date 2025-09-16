package me.bechberger.jfr.extended;

import me.bechberger.jfr.extended.ast.ASTNodes.*;
import me.bechberger.jfr.extended.engine.framework.QueryTestFramework;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Lightweight tests for SHOW PLAN and EXPLAIN command parsing.
 * 
 * Focuses on parser correctness rather than implementation details to allow
 * flexibility for future visualization changes.
 * 
 * @author JFR Query Language Testing Team
 * @since 3.0
 */
public class ShowPlanParserTestNew {
    
    private QueryTestFramework framework;
    
    @BeforeEach
    void setUp() {
        framework = new QueryTestFramework();
    }
    
    private ProgramNode parseQuery(String query) {
        try {
            return Parser.parseAndValidate(query);
        } catch (Exception e) {
            throw new RuntimeException("Failed to parse query: " + query, e);
        }
    }
    
    // ===== BASIC SHOW PLAN PARSING TESTS =====
    
    @Test
    @DisplayName("SHOW PLAN with simple query should parse correctly")
    void testShowPlanBasic() {
        String query = "SHOW PLAN @SELECT * FROM Events";
        
        assertDoesNotThrow(() -> {
            ProgramNode program = parseQuery(query);
            assertNotNull(program);
            assertEquals(1, program.statements().size());
            
            StatementNode stmt = program.statements().get(0);
            assertInstanceOf(ShowPlanNode.class, stmt);
            
            ShowPlanNode showPlan = (ShowPlanNode) stmt;
            assertNotNull(showPlan.query());
            assertEquals(PlanFormat.SIMPLE, showPlan.planFormat()); // Default format
        });
    }
    
    @ParameterizedTest
    @ValueSource(strings = {
        "SHOW PLAN SIMPLE @SELECT * FROM Events",
        "SHOW PLAN VERBOSE @SELECT * FROM Events", 
        "SHOW PLAN ASCII @SELECT * FROM Events",
        "SHOW PLAN PERFORMANCE @SELECT * FROM Events"
    })
    @DisplayName("SHOW PLAN with different formats should parse correctly")
    void testShowPlanFormats(String queryText) {
        assertDoesNotThrow(() -> {
            ProgramNode program = parseQuery(queryText);
            assertNotNull(program);
            assertEquals(1, program.statements().size());
            
            StatementNode stmt = program.statements().get(0);
            assertInstanceOf(ShowPlanNode.class, stmt);
            
            ShowPlanNode showPlan = (ShowPlanNode) stmt;
            assertNotNull(showPlan.query());
            assertNotNull(showPlan.planFormat());
        });
    }
    
    @Test
    @DisplayName("SHOW PLAN ASCII should parse with correct format")
    void testShowPlanAsciiFormat() {
        String query = "SHOW PLAN ASCII @SELECT name, COUNT(*) FROM Events GROUP BY name";
        
        ProgramNode program = parseQuery(query);
        ShowPlanNode showPlan = (ShowPlanNode) program.statements().get(0);
        
        assertEquals(PlanFormat.ASCII, showPlan.planFormat());
        assertNotNull(showPlan.query());
    }
    
    @Test
    @DisplayName("EXPLAIN command should parse correctly")
    void testExplainBasic() {
        String query = "EXPLAIN @SELECT * FROM Events WHERE duration > 100";
        
        assertDoesNotThrow(() -> {
            ProgramNode program = parseQuery(query);
            assertNotNull(program);
            assertEquals(1, program.statements().size());
            
            StatementNode stmt = program.statements().get(0);
            assertInstanceOf(ExplainNode.class, stmt);
            
            ExplainNode explain = (ExplainNode) stmt;
            assertNotNull(explain.query());
        });
    }
    
    // ===== FORMAT METHOD TESTS =====
    
    @Test
    @DisplayName("ShowPlanNode format method should work correctly")
    void testShowPlanNodeFormat() {
        String query = "SHOW PLAN ASCII @SELECT * FROM Events";
        
        ProgramNode program = parseQuery(query);
        ShowPlanNode showPlan = (ShowPlanNode) program.statements().get(0);
        
        String formatted = showPlan.format();
        assertTrue(formatted.contains("SHOW PLAN ASCII"));
        assertTrue(formatted.contains("@SELECT * FROM Events"));
    }
    
    @Test
    @DisplayName("ExplainNode format method should work correctly")
    void testExplainNodeFormat() {
        String query = "EXPLAIN @SELECT name FROM Events";
        
        ProgramNode program = parseQuery(query);
        ExplainNode explain = (ExplainNode) program.statements().get(0);
        
        String formatted = explain.format();
        assertTrue(formatted.contains("EXPLAIN"));
        assertTrue(formatted.contains("@SELECT name FROM Events"));
    }
    
    // ===== VALIDATION TESTS =====
    
    @Test
    @DisplayName("Parser should handle case-insensitive format names")
    void testCaseInsensitiveFormats() {
        String[] queries = {
            "SHOW PLAN ascii @SELECT * FROM Events",
            "SHOW PLAN Verbose @SELECT * FROM Events",
            "SHOW PLAN PERFORMANCE @SELECT * FROM Events"
        };
        
        for (String query : queries) {
            assertDoesNotThrow(() -> {
                ProgramNode program = parseQuery(query);
                ShowPlanNode showPlan = (ShowPlanNode) program.statements().get(0);
                assertNotNull(showPlan.planFormat());
            }, "Query should parse successfully: " + query);
        }
    }
    
    @Test
    @DisplayName("Parser should reject invalid format names")
    void testInvalidFormats() {
        String[] invalidQueries = {
            "SHOW PLAN INVALID @SELECT * FROM Events",
            "SHOW PLAN DETAILED @SELECT * FROM Events",
            "SHOW PLAN TREE @SELECT * FROM Events"
        };
        
        for (String query : invalidQueries) {
            assertThrows(Exception.class, () -> {
                parseQuery(query);
            }, "Query should fail to parse: " + query);
        }
    }
    
    // ===== INTEGRATION TESTS =====
    
    @Test
    @DisplayName("SHOW PLAN should work with query framework")
    void testShowPlanWithFramework() {
        // Create test data
        framework.mockTable("Events")
            .withStringColumn("name")
            .withNumberColumn("duration")
            .withRow("GC", 100L)
            .withRow("CPU", 50L)
            .build();
        
        // Test that parsing works with framework
        String query = "SHOW PLAN @SELECT * FROM Events";
        
        assertDoesNotThrow(() -> {
            // This tests that the parser integration works
            ProgramNode program = parseQuery(query);
            assertNotNull(program);
            
            // Verify the structure
            ShowPlanNode showPlan = (ShowPlanNode) program.statements().get(0);
            assertEquals(PlanFormat.SIMPLE, showPlan.planFormat());
        });
    }
    
    @Test
    @DisplayName("Multiple SHOW PLAN statements should parse correctly")
    void testMultipleShowPlanStatements() {
        String query = """
            SHOW PLAN @SELECT * FROM Events;
            SHOW PLAN ASCII @SELECT COUNT(*) FROM Events;
            EXPLAIN @SELECT name FROM Events WHERE duration > 100
            """;
        
        assertDoesNotThrow(() -> {
            ProgramNode program = parseQuery(query);
            assertEquals(3, program.statements().size());
            
            assertInstanceOf(ShowPlanNode.class, program.statements().get(0));
            assertInstanceOf(ShowPlanNode.class, program.statements().get(1));
            assertInstanceOf(ExplainNode.class, program.statements().get(2));
        });
    }
}
