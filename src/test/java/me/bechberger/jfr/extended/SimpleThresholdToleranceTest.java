package me.bechberger.jfr.extended;

import me.bechberger.jfr.extended.ast.ASTNodes.*;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class SimpleThresholdToleranceTest {

    @Test 
    public void testThresholdBeforeToleranceParsing() throws Exception {
        String query = "@SELECT * FROM GarbageCollection FUZZY JOIN ExecutionSample ON timestamp WITH NEAREST THRESHOLD 50ms TOLERANCE 10ms";
        
        try {
            // Parse the query
            Lexer lexer = new Lexer(query);
            var tokens = lexer.tokenize();
            Parser parser = new Parser(tokens);
            ProgramNode program = parser.parse();
            
            // Extract the fuzzy join node
            QueryNode queryNode = (QueryNode) program.statements().get(0);
            FromNode fromNode = queryNode.from();
            var sources = fromNode.sources();
            FuzzyJoinSourceNode fuzzy = (FuzzyJoinSourceNode) sources.get(1);
            
            // Check that both tolerance and threshold are present
            assertNotNull(fuzzy.tolerance(), "Tolerance should be present");
            assertNotNull(fuzzy.threshold(), "Threshold should be present");
            
            // Check the values
            if (fuzzy.tolerance() instanceof LiteralNode toleranceLit) {
                assertEquals("10ms", toleranceLit.value().toString(), "Tolerance should be 10ms");
            } else {
                fail("Tolerance should be a literal node");
            }
            
            if (fuzzy.threshold() instanceof LiteralNode thresholdLit) {
                assertEquals("50ms", thresholdLit.value().toString(), "Threshold should be 50ms");
            } else {
                fail("Threshold should be a literal node");
            }
            
            System.out.println("✓ Successfully parsed THRESHOLD before TOLERANCE");
            
        } catch (Exception e) {
            System.err.println("Failed to parse query: " + e.getMessage());
            e.printStackTrace();
            fail("Parsing failed: " + e.getMessage());
        }
    }
    
    @Test 
    public void testToleranceBeforeThresholdParsing() throws Exception {
        String query = "@SELECT * FROM GarbageCollection FUZZY JOIN ExecutionSample ON timestamp WITH NEAREST TOLERANCE 10ms THRESHOLD 50ms";
        
        try {
            // Parse the query
            Lexer lexer = new Lexer(query);
            var tokens = lexer.tokenize();
            Parser parser = new Parser(tokens);
            ProgramNode program = parser.parse();
            
            // Extract the fuzzy join node
            QueryNode queryNode = (QueryNode) program.statements().get(0);
            FromNode fromNode = queryNode.from();
            var sources = fromNode.sources();
            FuzzyJoinSourceNode fuzzy = (FuzzyJoinSourceNode) sources.get(1);
            
            // Check that both tolerance and threshold are present
            assertNotNull(fuzzy.tolerance(), "Tolerance should be present");
            assertNotNull(fuzzy.threshold(), "Threshold should be present");
            
            // Check the values
            if (fuzzy.tolerance() instanceof LiteralNode toleranceLit) {
                assertEquals("10ms", toleranceLit.value().toString(), "Tolerance should be 10ms");
            } else {
                fail("Tolerance should be a literal node");
            }
            
            if (fuzzy.threshold() instanceof LiteralNode thresholdLit) {
                assertEquals("50ms", thresholdLit.value().toString(), "Threshold should be 50ms");
            } else {
                fail("Threshold should be a literal node");
            }
            
            System.out.println("✓ Successfully parsed TOLERANCE before THRESHOLD");
            
        } catch (Exception e) {
            System.err.println("Failed to parse query: " + e.getMessage());
            e.printStackTrace();
            fail("Parsing failed: " + e.getMessage());
        }
    }
}
