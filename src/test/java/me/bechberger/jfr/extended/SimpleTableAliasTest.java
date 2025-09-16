package me.bechberger.jfr.extended;

import me.bechberger.jfr.extended.ast.ASTNodes.ProgramNode;
import me.bechberger.jfr.extended.ast.ASTNodes.QueryNode;
import me.bechberger.jfr.extended.ast.ASTNodes.FromNode;
import me.bechberger.jfr.extended.ast.ASTNodes.SourceNode;
import me.bechberger.jfr.extended.ast.ASTNodes.StatementNode;
import me.bechberger.jfr.extended.ast.ASTNodes.SourceNodeBase;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Simple parser test for table alias AS keyword requirement.
 */
class SimpleTableAliasTest {
    
    @Test
    @DisplayName("Parser should accept explicit AS syntax")
    void testExplicitAsSyntaxAccepted() throws Exception {
        Parser parser = new Parser("@SELECT u.name FROM Users AS u");
        
        // This should parse successfully
        ProgramNode result = parser.parse();
        
        assertNotNull(result);
        assertEquals(1, result.statements().size());
        
        StatementNode stmt = result.statements().get(0);
        assertTrue(stmt instanceof QueryNode);
        QueryNode query = (QueryNode) stmt;
        FromNode from = query.from();
        assertEquals(1, from.sources().size());
        
        SourceNodeBase sourceBase = from.sources().get(0);
        assertTrue(sourceBase instanceof SourceNode);
        SourceNode source = (SourceNode) sourceBase;
        assertEquals("Users", source.source());
        assertEquals("u", source.alias());
    }
    
    @Test
    @DisplayName("Parser should accept implicit alias syntax")
    void testImplicitAliasSyntaxAccepted() {
        try {
            Parser parser = new Parser("@SELECT u.name FROM Users u");
            ProgramNode result = parser.parse();
            
            // Verify the query parsed successfully with implicit alias
            assertNotNull(result, "Query should parse successfully");
            assertEquals(1, result.statements().size());
            
            StatementNode stmt = result.statements().get(0);
            assertTrue(stmt instanceof QueryNode);
            QueryNode query = (QueryNode) stmt;
            FromNode from = query.from();
            assertEquals(1, from.sources().size());
            
            SourceNodeBase sourceBase = from.sources().get(0);
            assertTrue(sourceBase instanceof SourceNode);
            SourceNode source = (SourceNode) sourceBase;
            assertEquals("Users", source.source());
            assertEquals("u", source.alias());
            
        } catch (Exception e) {
            fail("Parser should accept implicit alias syntax, but threw: " + e.getClass().getSimpleName() + " - " + e.getMessage());
        }
    }
}
