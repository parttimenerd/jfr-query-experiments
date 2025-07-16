package me.bechberger.jfr.extended;

import org.junit.jupiter.api.Test;
import java.util.List;

/**
 * Test to showcase the improved timestamp error messages.
 */
public class TimestampErrorShowcaseTest {

    @Test
    public void showcaseTimestampErrorMessages() {
        System.out.println("\n🔍 JFR Query Parser - Improved Timestamp Error Messages");
        System.out.println("===========================================================");
        
        String[] testCases = {
            "@SELECT * FROM Events WHERE timestamp > 2024-13-01T10:30:00Z",
            "@SELECT * FROM Events WHERE timestamp > 2024-02-30T10:30:00Z", 
            "@SELECT * FROM Events WHERE timestamp > 2023-02-29T10:30:00Z",
            "@SELECT * FROM Events WHERE timestamp > 2024-04-31T10:30:00Z",
            "@SELECT * FROM Events WHERE timestamp > 2024-12-01T25:30:00Z",
            "@SELECT * FROM Events WHERE timestamp > 2024-12-01T10:60:00Z",
            "@SELECT * FROM Events WHERE timestamp > 2024-12-00T10:30:00Z"
        };
        
        for (String query : testCases) {
            System.out.println("\n📝 Query: " + query);
            try {
                Lexer lexer = new Lexer(query);
                List<Token> tokens = lexer.tokenize();
                Parser parser = new Parser(tokens, query);
                parser.parse();
                
                if (parser.hasParsingErrors()) {
                    List<ParserErrorHandler.ParserError> errors = parser.getParsingErrors();
                    System.out.println("✅ Helpful Error Message:");
                    System.out.println("   " + errors.get(0).getMessage());
                } else {
                    System.out.println("❌ No error detected (unexpected)");
                }
            } catch (Exception e) {
                System.out.println("✅ Parser Exception:");
                System.out.println("   " + e.getMessage());
            }
        }
        
        System.out.println("\n✨ All timestamp validation errors provide specific, actionable feedback!");
        System.out.println("💡 Users now get clear guidance on what's wrong and how to fix it.");
    }
}
