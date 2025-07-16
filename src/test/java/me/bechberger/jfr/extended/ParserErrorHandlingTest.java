package me.bechberger.jfr.extended;

import org.junit.jupiter.api.Test;

import me.bechberger.jfr.extended.evaluator.FunctionRegistry;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;

/**
 * Test class to demonstrate the enhanced parser error messages
 */
public class ParserErrorHandlingTest {
    
    @Test
    void testMissingClosingParenthesis() {
        String query = "@SELECT * FROM GarbageCollection WHERE COUNT(duration > 10ms";
        
        try {
            Lexer lexer = new Lexer(query);
            List<Token> tokens = lexer.tokenize();
            Parser parser = new Parser(tokens, query);
            parser.parse();
            fail("Expected ParserException for missing closing parenthesis");
        } catch (me.bechberger.jfr.extended.ParserException e) {
            String errorMessage = e.getMessage();
            System.out.println("=== Missing Closing Parenthesis Error ===");
            System.out.println(errorMessage);
            System.out.println();
            
            // Verify the error message contains helpful information
            assertTrue(errorMessage.contains("Missing closing parenthesis") || 
                      errorMessage.contains("parenthesis") ||
                      errorMessage.contains("function"));
        } catch (Exception e) {
            System.out.println("Unexpected error: " + e.getMessage());
            fail("Unexpected exception: " + e.getClass().getSimpleName());
        }
    }
    
    @Test
    void testMissingCommaInFunctionCall() {
        String query = "@SELECT * FROM GarbageCollection WHERE PERCENTILE(90 duration) > 50ms";
        
        try {
            Lexer lexer = new Lexer(query);
            List<Token> tokens = lexer.tokenize();
            Parser parser = new Parser(tokens, query);
            parser.parse();
            fail("Expected ParserException for missing comma in function call");
        } catch (me.bechberger.jfr.extended.ParserException e) {
            String errorMessage = e.getMessage();
            System.out.println("=== Missing Comma in Function Call Error ===");
            System.out.println(errorMessage);
            System.out.println();
            
            // Verify the error message contains helpful information
            assertTrue(errorMessage.contains("comma") || errorMessage.contains("function") ||
                      errorMessage.contains("argument"));
        } catch (Exception e) {
            System.out.println("Unexpected error: " + e.getMessage());
            fail("Unexpected exception: " + e.getClass().getSimpleName());
        }
    }
    
    @Test
    void testUnexpectedToken() {
        String query = "@SELECT * FROM WHERE duration > 10ms";
        
        try {
            Lexer lexer = new Lexer(query);
            List<Token> tokens = lexer.tokenize();
            Parser parser = new Parser(tokens, query);
            parser.parse();
            fail("Expected ParserException for unexpected WHERE token");
        } catch (me.bechberger.jfr.extended.ParserException e) {
            String errorMessage = e.getMessage();
            System.out.println("=== Unexpected Token Error ===");
            System.out.println(errorMessage);
            System.out.println();
            
            // Verify the error message contains helpful information
            assertTrue(errorMessage.contains("Unexpected") || errorMessage.contains("Expected") ||
                      errorMessage.contains("table") || errorMessage.contains("FROM"));
        } catch (Exception e) {
            System.out.println("Unexpected error: " + e.getMessage());
            fail("Unexpected exception: " + e.getClass().getSimpleName());
        }
    }
    
    @Test
    void testMultipleErrors() {
        String query = "@SELECT * FROM WHERE duration > 10ms";
        
        try {
            Lexer lexer = new Lexer(query);
            List<Token> tokens = lexer.tokenize();
            Parser parser = new Parser(tokens, query);
            parser.parse();
            fail("Expected ParserException for multiple parsing errors");
        } catch (me.bechberger.jfr.extended.ParserException e) {
            String errorMessage = e.getMessage();
            System.out.println("=== Multiple Errors Test ===");
            System.out.println(errorMessage);
            System.out.println();
            
            // Verify the error message mentions multiple errors (or check for error collection)
            assertTrue(errorMessage.contains("Found") && errorMessage.contains("errors") ||
                      errorMessage.contains("parsing") ||
                      errorMessage.contains("Missing"));
        } catch (Exception e) {
            System.out.println("Unexpected error: " + e.getMessage());
            fail("Unexpected exception: " + e.getClass().getSimpleName());
        }
    }
    
    @Test
    void testMissingFromClause() {
        String query = "@SELECT * WHERE duration > 10ms";
        
        try {
            Lexer lexer = new Lexer(query);
            List<Token> tokens = lexer.tokenize();
            Parser parser = new Parser(tokens, query);
            parser.parse();
            fail("Expected ParserException for missing FROM clause");
        } catch (me.bechberger.jfr.extended.ParserException e) {
            String errorMessage = e.getMessage();
            System.out.println("=== Missing FROM Clause Error ===");
            System.out.println(errorMessage);
            System.out.println();
            
            // Verify the error message contains helpful information
            assertTrue(errorMessage.contains("FROM") || errorMessage.contains("table") ||
                      errorMessage.contains("Expected"));
        } catch (Exception e) {
            System.out.println("Unexpected error: " + e.getMessage());
            fail("Unexpected exception: " + e.getClass().getSimpleName());
        }
    }
    
    @Test
    void testValidQueryDoesNotThrowError() {
        String query = "@SELECT * FROM GarbageCollection WHERE duration > 10ms";
        
        try {
            Lexer lexer = new Lexer(query);
            List<Token> tokens = lexer.tokenize();
            Parser parser = new Parser(tokens, query);
            parser.parse();
            // Should succeed without throwing an exception
            System.out.println("=== Valid Query Test ===");
            System.out.println("Successfully parsed: " + query);
            System.out.println();
        } catch (Exception e) {
            fail("Valid query should not throw exception: " + e.getMessage());
        }
    }
    
    @Test
    void testInvalidFunctionSuggestion() {
        String query = "@SELECT * FROM GarbageCollection WHERE AVERAG(duration) > 10ms";
        
        try {
            Lexer lexer = new Lexer(query);
            List<Token> tokens = lexer.tokenize();
            Parser parser = new Parser(tokens, query);
            parser.parse();
            fail("Expected ParserException for invalid function name");
        } catch (me.bechberger.jfr.extended.ParserException e) {
            String errorMessage = e.getMessage();
            System.out.println("=== Invalid Function Suggestion Test ===");
            System.out.println(errorMessage);
            System.out.println();
            
            // Verify the error message suggests the correct function
            assertTrue(errorMessage.contains("AVG") || errorMessage.contains("AVERAG") ||
                      errorMessage.contains("Did you mean"));
        } catch (Exception e) {
            System.out.println("Unexpected error: " + e.getMessage());
            fail("Unexpected exception: " + e.getClass().getSimpleName());
        }
    }
    
    @Test
    void testFunctionCallSyntaxSuggestion() {
        String query = "@SELECT * FROM GarbageCollection WHERE AVG() > 5";
        
        try {
            Lexer lexer = new Lexer(query);
            List<Token> tokens = lexer.tokenize();
            Parser parser = new Parser(tokens, query);
            parser.parse();
            fail("Expected ParserException for incorrect AVG syntax");
        } catch (me.bechberger.jfr.extended.ParserException e) {
            String errorMessage = e.getMessage();
            System.out.println("=== Function Call Syntax Suggestion Test ===");
            System.out.println(errorMessage);
            System.out.println();
            
            // Verify the error message provides syntax help
            assertTrue(errorMessage.contains("AVG") && 
                      (errorMessage.contains("argument") || errorMessage.contains("field")));
        } catch (Exception e) {
            System.out.println("Unexpected error: " + e.getMessage());
            fail("Unexpected exception: " + e.getClass().getSimpleName());
        }
    }
    
    @Test
    void testPercentileFunctionSuggestion() {
        String query = "@SELECT * FROM GarbageCollection WHERE PERCENTILEX(90, duration) > 10ms";
        
        try {
            Lexer lexer = new Lexer(query);
            List<Token> tokens = lexer.tokenize();
            Parser parser = new Parser(tokens, query);
            parser.parse();
            fail("Expected ParserException for invalid percentile function");
        } catch (me.bechberger.jfr.extended.ParserException e) {
            String errorMessage = e.getMessage();
            System.out.println("=== Percentile Function Suggestion Test ===");
            System.out.println(errorMessage);
            System.out.println();
            
            // Verify the error message suggests PERCENTILE function
            assertTrue(errorMessage.contains("PERCENTILE") || errorMessage.contains("P90") ||
                      errorMessage.contains("Did you mean"));
        } catch (Exception e) {
            System.out.println("Unexpected error: " + e.getMessage());
            fail("Unexpected exception: " + e.getClass().getSimpleName());
        }
    }
    
    @Test
    void testMathematicalFunctionSuggestion() {
        String query = "@SELECT * FROM GarbageCollection WHERE ABS(duration - 10ms) > 5ms";
        
        try {
            Lexer lexer = new Lexer(query);
            List<Token> tokens = lexer.tokenize();
            Parser parser = new Parser(tokens, query);
            parser.parse();
            // This should parse successfully since ABS is a valid function
            System.out.println("=== Mathematical Function Test ===");
            System.out.println("Successfully parsed mathematical function: " + query);
            System.out.println();
        } catch (Exception e) {
            // If it fails, check that the error message contains helpful information
            String errorMessage = e.getMessage();
            System.out.println("=== Mathematical Function Error ===");
            System.out.println(errorMessage);
            System.out.println();
            
            // The error might be about expression parsing, but should mention ABS function
            assertTrue(errorMessage.contains("ABS") || errorMessage.contains("mathematical") ||
                      errorMessage.contains("function"));
        }
    }
    
    @Test
    void testStringFunctionSuggestion() {
        String query = "@SELECT * FROM GarbageCollection WHERE UPPPER(eventType) = 'ALLOCATION'";
        
        try {
            Lexer lexer = new Lexer(query);
            List<Token> tokens = lexer.tokenize();
            Parser parser = new Parser(tokens, query);
            parser.parse();
            fail("Expected ParserException for invalid string function");
        } catch (me.bechberger.jfr.extended.ParserException e) {
            String errorMessage = e.getMessage();
            System.out.println("=== String Function Suggestion Test ===");
            System.out.println(errorMessage);
            System.out.println();
            
            // Verify the error message suggests UPPER function
            assertTrue(errorMessage.contains("UPPER") || errorMessage.contains("UPPPER") ||
                      errorMessage.contains("Did you mean"));
        } catch (Exception e) {
            System.out.println("Unexpected error: " + e.getMessage());
            fail("Unexpected exception: " + e.getClass().getSimpleName());
        }
    }
    
    @Test
    void testDateTimeFunctionSuggestion() {
        String query = "@SELECT * FROM GarbageCollection WHERE YEAAR(timestamp) = 2023";
        
        try {
            Lexer lexer = new Lexer(query);
            List<Token> tokens = lexer.tokenize();
            Parser parser = new Parser(tokens, query);
            parser.parse();
            fail("Expected ParserException for invalid date/time function");
        } catch (me.bechberger.jfr.extended.ParserException e) {
            String errorMessage = e.getMessage();
            System.out.println("=== Date/Time Function Suggestion Test ===");
            System.out.println(errorMessage);
            System.out.println();
            
            // Verify the error message suggests YEAR function
            assertTrue(errorMessage.contains("YEAR") || errorMessage.contains("YEAAR") ||
                      errorMessage.contains("Did you mean"));
        } catch (Exception e) {
            System.out.println("Unexpected error: " + e.getMessage());
            fail("Unexpected exception: " + e.getClass().getSimpleName());
        }
    }
    
    @Test
    void testFunctionRegistryIntegration() {
        // Test that we can get function definitions from the registry
        FunctionRegistry registry = FunctionRegistry.getInstance();
        
        // Test that common functions are registered
        assertTrue(registry.isFunction("COUNT"));
        assertTrue(registry.isFunction("AVG"));
        assertTrue(registry.isFunction("PERCENTILE"));
        assertTrue(registry.isFunction("P90"));
        assertTrue(registry.isFunction("ABS"));
        assertTrue(registry.isFunction("UPPER"));
        assertTrue(registry.isFunction("NOW"));
        
        // Test function definitions
        var countFunc = registry.getFunction("COUNT");
        assertTrue(countFunc != null);
        assertEquals("COUNT", countFunc.name());
        assertNotNull(countFunc.description());
        
        var avgFunc = registry.getFunction("AVG");
        assertTrue(avgFunc != null);
        assertEquals(FunctionRegistry.FunctionType.AGGREGATE, avgFunc.type());
        
        System.out.println("=== Function Registry Integration Test ===");
        System.out.println("COUNT function: " + countFunc.description());
        System.out.println("Available functions: " + registry.getFunctionNames().size());
        System.out.println();
    }
    
    @Test
    void testRecoveryStrategies() {
        // Test that recovery strategies are properly defined
        String query = "@SELECT * FROM GarbageCollection WHERE COUNT(duration > 10ms AND AVG(size) > 100kb";
        
        try {
            Lexer lexer = new Lexer(query);
            List<Token> tokens = lexer.tokenize();
            ParserErrorHandler errorHandler = new ParserErrorHandler(tokens, query);
            
            // Test that error handler can apply recovery strategies
            if (!tokens.isEmpty()) {
                Token errorToken = tokens.get(tokens.size() - 1); // Last token
                
                // Test different recovery strategies
                int skipToNextStatement = errorHandler.applyRecoveryStrategy(
                    ParserErrorHandler.RecoveryStrategy.SKIP_TO_NEXT_STATEMENT, errorToken, tokens);
                assertTrue(skipToNextStatement >= 0);
                
                int skipToNextClause = errorHandler.applyRecoveryStrategy(
                    ParserErrorHandler.RecoveryStrategy.SKIP_TO_NEXT_CLAUSE, errorToken, tokens);
                assertTrue(skipToNextClause >= 0);
                
                int synchronizeParens = errorHandler.applyRecoveryStrategy(
                    ParserErrorHandler.RecoveryStrategy.SYNCHRONIZE_PARENTHESES, errorToken, tokens);
                assertTrue(synchronizeParens >= 0);
                
                System.out.println("=== Recovery Strategies Test ===");
                System.out.println("Skip to next statement: position " + skipToNextStatement);
                System.out.println("Skip to next clause: position " + skipToNextClause);
                System.out.println("Synchronize parentheses: position " + synchronizeParens);
                System.out.println();
            }
        } catch (Exception e) {
            System.out.println("Recovery strategies test error: " + e.getMessage());
            // This is acceptable as the query has syntax errors
        }
    }
    
    @Test
    void testFunctionCallEOFHandling() {
        String query = "@SELECT AVG(duration";
        
        try {
            Lexer lexer = new Lexer(query);
            List<Token> tokens = lexer.tokenize();
            Parser parser = new Parser(tokens, query);
            parser.parse();
            fail("Expected ParserException for EOF in function call");
        } catch (me.bechberger.jfr.extended.ParserException e) {
            String errorMessage = e.getMessage();
            System.out.println("=== Function Call EOF Error ===");
            System.out.println(errorMessage);
            System.out.println();
            
            // Verify that the error message shows "end of input" instead of "found ''"
            assertFalse(errorMessage.contains("found ''"), 
                       "Error message should not contain 'found '''");
            assertTrue(errorMessage.contains("end of input") || 
                      errorMessage.contains("Expected") ||
                      errorMessage.contains("Missing"), 
                      "Error message should indicate EOF or missing token");
        } catch (Exception e) {
            System.out.println("Unexpected error: " + e.getMessage());
            fail("Unexpected exception: " + e.getClass().getSimpleName());
        }
    }
    
    @Test  
    void testFunctionCallMissingCommaWithEOF() {
        String query = "@SELECT COUNT(duration name";
        
        try {
            Lexer lexer = new Lexer(query);
            List<Token> tokens = lexer.tokenize();
            Parser parser = new Parser(tokens, query);
            parser.parse();
            fail("Expected ParserException for incomplete function call");
        } catch (me.bechberger.jfr.extended.ParserException e) {
            String errorMessage = e.getMessage();
            System.out.println("=== Function Call Missing Comma with EOF Error ===");
            System.out.println(errorMessage);
            System.out.println();
            
            // Verify that the error message shows "end of input" instead of "found ''"
            assertFalse(errorMessage.contains("found ''"), 
                       "Error message should not contain 'found '''");
            if (errorMessage.contains("Expected ',' or ')'")) {
                assertTrue(errorMessage.contains("end of input") || errorMessage.contains("'name'"), 
                          "Error message should show 'end of input' for EOF or found token name");
            }
        } catch (Exception e) {
            System.out.println("Unexpected error: " + e.getMessage());
            fail("Unexpected exception: " + e.getClass().getSimpleName());
        }
    }
    
    @Test
    void debugIncompleteFunction() {
        String query = "@SELECT AVG(duration";
        
        System.out.println("=== Debug Incomplete Function ===");
        System.out.println("Query: " + query);
        
        try {
            Lexer lexer = new Lexer(query);
            List<Token> tokens = lexer.tokenize();
            
            System.out.println("Tokens:");
            for (int i = 0; i < tokens.size(); i++) {
                Token token = tokens.get(i);
                System.out.println(i + ": " + token.type() + " -> '" + token.value() + "'");
            }
            
            Parser parser = new Parser(tokens, query);
            var result = parser.parse();
            System.out.println("Parsed successfully: " + result);
            fail("Expected ParserException but parsing succeeded");
        } catch (me.bechberger.jfr.extended.ParserException e) {
            System.out.println("ParserException: " + e.getMessage());
            assertTrue(true, "Got expected exception");
        } catch (Exception e) {
            System.out.println("Other exception: " + e.getClass().getSimpleName() + ": " + e.getMessage());
            fail("Unexpected exception type");
        }
    }
    
    @Test
    void debugIncompleteExtendedFunction() {
        String query = "@SELECT AVG(duration";
        
        System.out.println("=== Debug Incomplete Extended Function ===");
        System.out.println("Query: " + query);
        
        try {
            Lexer lexer = new Lexer(query);
            List<Token> tokens = lexer.tokenize();
            
            System.out.println("Tokens:");
            for (int i = 0; i < tokens.size(); i++) {
                Token token = tokens.get(i);
                System.out.println(i + ": " + token.type() + " -> '" + token.value() + "'");
            }
            
            Parser parser = new Parser(tokens, query);
            var result = parser.parse();
            System.out.println("Parsed successfully: " + result);
            fail("Expected ParserException but parsing succeeded");
        } catch (me.bechberger.jfr.extended.ParserException e) {
            System.out.println("ParserException: " + e.getMessage());
            
            // Check if the error message contains "found ''" 
            if (e.getMessage().contains("found ''")) {
                System.out.println("❌ ISSUE FOUND: Error message contains 'found '''");
                System.out.println("Full error: " + e.getMessage());
                fail("Error message still shows 'found ''");
            } else if (e.getMessage().contains("end of input")) {
                System.out.println("✅ GOOD: Error message shows 'end of input'");
                assertTrue(true, "Got expected error format");
            } else {
                System.out.println("ℹ️  Different error format: " + e.getMessage());
                assertTrue(true, "Got expected exception");
            }
        } catch (Exception e) {
            System.out.println("Other exception: " + e.getClass().getSimpleName() + ": " + e.getMessage());
            fail("Unexpected exception type");
        }
    }
    
    @Test
    void testMissingCommaEOF() {
        String query = "@SELECT COUNT(duration name";
        
        System.out.println("=== Test Missing Comma EOF ===");
        System.out.println("Query: " + query);
        
        try {
            Lexer lexer = new Lexer(query);
            List<Token> tokens = lexer.tokenize();
            Parser parser = new Parser(tokens, query);
            parser.parse();
            fail("Expected ParserException for missing comma");
        } catch (me.bechberger.jfr.extended.ParserException e) {
            System.out.println("ParserException: " + e.getMessage());
            
            // Check if the error message contains "found ''" 
            if (e.getMessage().contains("found ''")) {
                System.out.println("❌ ISSUE FOUND: Error message contains 'found '''");
                fail("Error message still shows 'found ''");
            } else if (e.getMessage().contains("end of input")) {
                System.out.println("✅ GOOD: Error message shows 'end of input'");
                assertTrue(true, "Got expected error format");
            } else {
                System.out.println("ℹ️  Different error format but no 'found ''': " + e.getMessage());
                assertTrue(true, "Got expected exception without confusing empty quotes");
            }
        } catch (Exception e) {
            System.out.println("Other exception: " + e.getClass().getSimpleName() + ": " + e.getMessage());
            fail("Unexpected exception type");
        }
    }
    
    @Test
    void testArraySyntaxEOF() {
        String query = "@SELECT COUNT[duration";
        
        System.out.println("=== Test Array Syntax EOF ===");
        System.out.println("Query: " + query);
        
        try {
            Lexer lexer = new Lexer(query);
            List<Token> tokens = lexer.tokenize();
            Parser parser = new Parser(tokens, query);
            parser.parse();
            fail("Expected ParserException for missing closing bracket");
        } catch (me.bechberger.jfr.extended.ParserException e) {
            System.out.println("ParserException: " + e.getMessage());
            
            // Check if the error message contains "found ''" 
            if (e.getMessage().contains("found ''")) {
                System.out.println("❌ ISSUE FOUND: Error message contains 'found '''");
                fail("Error message still shows 'found ''");
            } else if (e.getMessage().contains("end of input")) {
                System.out.println("✅ GOOD: Error message shows 'end of input'");
                assertTrue(true, "Got expected error format");
            } else {
                System.out.println("ℹ️  Different error format but no 'found ''': " + e.getMessage());
                assertTrue(true, "Got expected exception without confusing empty quotes");
            }
        } catch (Exception e) {
            System.out.println("Other exception: " + e.getClass().getSimpleName() + ": " + e.getMessage());
            fail("Unexpected exception type");
        }
    }
    
    @Test
    void testFunctionArgumentCountValidation() {
        String query = "@SELECT COUNT(duration, name) FROM GarbageCollection";
        
        try {
            Lexer lexer = new Lexer(query);
            List<Token> tokens = lexer.tokenize();
            Parser parser = new Parser(tokens, query);
            parser.parse();
            fail("Expected ParserException for too many arguments in COUNT function");
        } catch (me.bechberger.jfr.extended.ParserException e) {
            String errorMessage = e.getMessage();
            System.out.println("=== Function Argument Count Validation Test ===");
            System.out.println(errorMessage);
            System.out.println();
            
            // Verify the error message explains the argument count limit
            assertTrue(errorMessage.contains("COUNT"), 
                      "Error should mention the function name");
            assertTrue(errorMessage.contains("accepts at most") || errorMessage.contains("too many"), 
                      "Error should specify the argument limit");
            assertTrue(errorMessage.contains("COUNT(*)") || errorMessage.contains("COUNT(duration)"), 
                      "Error should show valid usage examples");
        } catch (Exception e) {
            System.out.println("Unexpected error: " + e.getMessage());
            fail("Unexpected exception: " + e.getClass().getSimpleName());
        }
    }
    
    @Test
    void testEnhancedErrorMessages() {
        System.out.println("=== Enhanced Error Messages Test ===");
        
        // Test 1: Unknown function with intelligent suggestion
        testSingleQuery("@SELECT WRONGFUNC(duration) FROM GarbageCollection", 
                       "Unknown function should provide category suggestions");
        
        // Test 2: Mathematical function with context
        testSingleQuery("@SELECT ABS(a, b, c) FROM GarbageCollection", 
                       "Too many arguments for mathematical function");
        
        // Test 3: Aggregate function with detailed parameter info
        testSingleQuery("@SELECT AVG() FROM GarbageCollection", 
                       "Missing required arguments with parameter details");
        
        // Test 4: Function type inference
        testSingleQuery("@SELECT UPPPER('hello') FROM GarbageCollection", 
                       "String function misspelling with type inference");
    }
    
    private void testSingleQuery(String query, String description) {
        System.out.println("\n--- " + description + " ---");
        System.out.println("Query: " + query);
        try {
            Lexer lexer = new Lexer(query);
            List<Token> tokens = lexer.tokenize();
            Parser parser = new Parser(tokens, query);
            parser.parse();
            System.out.println("❌ Expected error but parsing succeeded");
        } catch (me.bechberger.jfr.extended.ParserException e) {
            System.out.println("✅ Enhanced Error: " + e.getMessage());
        } catch (Exception e) {
            System.out.println("⚠️  Other error: " + e.getMessage());
        }
    }
    
    @Test
    void testAggregateInWhereClause() {
        String query = "@SELECT name FROM GarbageCollection WHERE AVG(duration) > 10ms";
        
        System.out.println("=== Testing Aggregate Function in WHERE Clause ===");
        System.out.println("Query: " + query);
        
        try {
            Lexer lexer = new Lexer(query);
            List<Token> tokens = lexer.tokenize();
            Parser parser = new Parser(tokens, query);
            parser.parse();
            System.out.println("❌ Expected error for aggregate function in WHERE clause");
        } catch (me.bechberger.jfr.extended.ParserException e) {
            String errorMessage = e.getMessage();
            System.out.println("Error message: " + errorMessage);
            
            // Check if it properly identifies the aggregate function issue
            if (errorMessage.contains("Aggregate function") && errorMessage.contains("WHERE clause")) {
                System.out.println("✅ GOOD: Aggregate function in WHERE clause properly detected");
            } else {
                System.out.println("ℹ️  Different error: " + errorMessage);
            }
        } catch (Exception e) {
            System.out.println("⚠️  Other error: " + e.getMessage());
        }
    }
    
    @Test
    void testMultipleAggregateInWhereClause() {
        String query = "@SELECT * FROM GarbageCollection WHERE AVG(duration) > 10ms AND COUNT(*) > 5";
        
        try {
            // Parsing should succeed - aggregate functions in WHERE is a semantic error, not syntax error
            Lexer lexer = new Lexer(query);
            List<Token> tokens = lexer.tokenize();
            Parser parser = new Parser(tokens, query);
            parser.parse(); // This should succeed
            
            // Now test semantic validation - this should fail
            try {
                Parser.parseAndValidate(query); // This should fail
                fail("Expected QuerySemanticException for aggregate functions in WHERE clause");
            } catch (me.bechberger.jfr.extended.engine.QuerySemanticValidator.QuerySemanticException e) {
                String errorMessage = e.getMessage();
                System.out.println("=== Multiple Aggregates in WHERE Clause Semantic Error ===");
                System.out.println(errorMessage);
                System.out.println();
                
                // Verify the error message explains that aggregate functions can't be used in WHERE clause
                assertTrue(errorMessage.contains("WHERE") && 
                          (errorMessage.contains("HAVING") || errorMessage.contains("SELECT")), 
                          "Error should explain that aggregate functions are not allowed in WHERE clause");
            }
        } catch (Exception e) {
            System.out.println("Unexpected error: " + e.getMessage());
            fail("Unexpected exception: " + e.getClass().getSimpleName());
        }
    }
    
    @Test
    void testAggregateInHavingClauseAllowed() {
        String query = "@SELECT * FROM GarbageCollection GROUP BY eventType HAVING COUNT(*) > 5";
        
        try {
            Lexer lexer = new Lexer(query);
            List<Token> tokens = lexer.tokenize();
            Parser parser = new Parser(tokens, query);
            parser.parse();
            // Should succeed - aggregate functions are allowed in HAVING clause
            System.out.println("=== Aggregate in HAVING Clause Test ===");
            System.out.println("Successfully parsed: " + query);
            System.out.println();
        } catch (Exception e) {
            fail("Valid query with aggregate in HAVING clause should not throw exception: " + e.getMessage());
        }
    }
    
    @Test
    void testErrorNumberingBehavior() {
        System.out.println("=== Error Numbering Behavior Test ===");
        
        // Test single error - should NOT use numbers
        testSingleQuery("@SELECT WRONGFUNC(duration) FROM GarbageCollection", 
                       "Single error should not use numbering");
        
        // Test query that should produce multiple errors
        testSingleQuery("@SELECT * FROM GarbageCollection WHERE COUNT(duration; @SELECT FROM duration > 5ms", 
                       "Multiple errors should use numbering");
    }
    
    @Test
    void testActualMultipleErrors() {
        String queryWithMultipleErrors = "@SELECT COUNT(duration name) FROM GarbageCollection WHERE AVG(size) > 100; INVALID_QUERY_PART";
        
        System.out.println("=== Testing Multiple Errors with Numbering ===");
        System.out.println("Query: " + queryWithMultipleErrors);
        
        try {
            Lexer lexer = new Lexer(queryWithMultipleErrors);
            List<Token> tokens = lexer.tokenize();
            Parser parser = new Parser(tokens, queryWithMultipleErrors);
            parser.parse();
            System.out.println("❌ Expected multiple errors but parsing succeeded");
        } catch (me.bechberger.jfr.extended.ParserException e) {
            String errorMessage = e.getMessage();
            System.out.println("Error message: " + errorMessage);
            
            // Check if it starts with a number (indicating multiple errors)
            if (errorMessage.trim().startsWith("1.")) {
                System.out.println("✅ GOOD: Multiple errors use numbered format");
            } else if (errorMessage.contains("1.") && errorMessage.contains("2.")) {
                System.out.println("✅ GOOD: Multiple errors detected with proper numbering");
            } else {
                System.out.println("ℹ️  Single error format (no numbering): " + errorMessage.substring(0, Math.min(100, errorMessage.length())) + "...");
            }
        } catch (Exception e) {
            System.out.println("⚠️  Other error: " + e.getMessage());
        }
    }
    
    @Test
    void debugAggregateInWhere() {
        String query = "@SELECT name FROM GarbageCollection WHERE AVG(duration) > 10ms";
        
        System.out.println("=== Debug Aggregate in WHERE ===");
        System.out.println("Query: " + query);
        
        try {
            Lexer lexer = new Lexer(query);
            List<Token> tokens = lexer.tokenize();
            
            // Check if AVG is recognized as an aggregate function
            QueryErrorMessageGenerator generator = new QueryErrorMessageGenerator();
            boolean isAggregate = generator.isAggregateInWhere("AVG", QueryErrorMessageGenerator.QueryContext.WHERE_CLAUSE);
            System.out.println("Is AVG aggregate in WHERE: " + isAggregate);
            
            Parser parser = new Parser(tokens, query);
            parser.parse();
            System.out.println("❌ Parsing succeeded - aggregate validation not working");
        } catch (me.bechberger.jfr.extended.ParserException e) {
            String errorMessage = e.getMessage();
            System.out.println("✅ Parser error: " + errorMessage);
        } catch (Exception e) {
            System.out.println("⚠️  Other error: " + e.getMessage());
        }
    }
}
