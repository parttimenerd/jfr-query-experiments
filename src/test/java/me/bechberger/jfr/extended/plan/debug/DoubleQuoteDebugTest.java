package me.bechberger.jfr.extended.plan.debug;

import me.bechberger.jfr.extended.Lexer;
import me.bechberger.jfr.extended.Parser;
import me.bechberger.jfr.extended.Token;
import me.bechberger.jfr.extended.TokenType;
import me.bechberger.jfr.extended.ast.ASTNodes.*;
import org.junit.jupiter.api.Test;

import java.util.List;

/**
 * Debug double-quoted string tokenization and parsing
 */
class DoubleQuoteDebugTest {

    @Test
    void debugDoubleQuoteTokenization() {
        System.out.println("=== DEBUGGING DOUBLE QUOTE TOKENIZATION ===");
        
        // Test 1: Simple double-quoted string
        String query1 = "\"admin\"";
        System.out.println("\n1. Tokenizing: " + query1);
        
        try {
            Lexer lexer1 = new Lexer(query1);
            List<Token> tokens1 = lexer1.tokenize();
            for (Token token : tokens1) {
                System.out.println("   Token: " + token.type() + " = '" + token.value() + "'");
            }
        } catch (Exception e) {
            System.out.println("   ERROR: " + e.getMessage());
        }
        
        // Test 2: Double-quoted string in WHERE clause
        String query2 = "WHERE role = \"admin\"";
        System.out.println("\n2. Tokenizing: " + query2);
        
        try {
            Lexer lexer2 = new Lexer(query2);
            List<Token> tokens2 = lexer2.tokenize();
            for (Token token : tokens2) {
                System.out.println("   Token: " + token.type() + " = '" + token.value() + "'");
            }
        } catch (Exception e) {
            System.out.println("   ERROR: " + e.getMessage());
        }
        
        // Test 3: Full query with double quotes
        String query3 = "@SELECT name FROM Users WHERE role = \"admin\"";
        System.out.println("\n3. Tokenizing: " + query3);
        
        try {
            Lexer lexer3 = new Lexer(query3);
            List<Token> tokens3 = lexer3.tokenize();
            for (Token token : tokens3) {
                System.out.println("   Token: " + token.type() + " = '" + token.value() + "'");
            }
            
            // Try parsing
            System.out.println("\n   Parsing the query:");
            Parser parser = new Parser(tokens3, query3);
            ProgramNode ast = parser.parse();
            System.out.println("   Parsed successfully: " + ast.getClass().getSimpleName());
            
        } catch (Exception e) {
            System.out.println("   ERROR: " + e.getMessage());
        }
        
        // Test 4: Compare with single quotes
        String query4 = "@SELECT name FROM Users WHERE role = 'admin'";
        System.out.println("\n4. Tokenizing (single quotes): " + query4);
        
        try {
            Lexer lexer4 = new Lexer(query4);
            List<Token> tokens4 = lexer4.tokenize();
            for (Token token : tokens4) {
                System.out.println("   Token: " + token.type() + " = '" + token.value() + "'");
            }
        } catch (Exception e) {
            System.out.println("   ERROR: " + e.getMessage());
        }
        
        System.out.println("\n=== DEBUG COMPLETE ===");
    }
}
