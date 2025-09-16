package me.bechberger.jfr.extended;

import me.bechberger.jfr.extended.Parser;
import me.bechberger.jfr.extended.ast.ASTNodes.*;
import org.junit.jupiter.api.Test;

/**
 * Debug test to check CROSS JOIN parsing
 */
public class DebugCrossJoinParsing {
    
    @Test
    void debugCrossJoinParsing() {
        try {
            String query = "@SELECT * FROM A CROSS JOIN B";
            System.out.println("Parsing query: " + query);
            
            ProgramNode program = Parser.parseAndValidate(query);
            System.out.println("Parse successful!");
            
            QueryNode queryNode = (QueryNode) program.statements().get(0);
            FromNode fromNode = queryNode.from();
            
            System.out.println("Number of sources: " + fromNode.sources().size());
            for (int i = 0; i < fromNode.sources().size(); i++) {
                SourceNodeBase source = fromNode.sources().get(i);
                System.out.println("Source " + i + ": " + source.getClass().getSimpleName());
                if (source instanceof StandardJoinSourceNode sjsn) {
                    System.out.println("  Join type: " + sjsn.joinType());
                    System.out.println("  Source: " + sjsn.source());
                    System.out.println("  Left field: " + sjsn.leftJoinField());
                    System.out.println("  Right field: " + sjsn.rightJoinField());
                } else if (source instanceof SourceNode sn) {
                    System.out.println("  Source: " + sn.source());
                }
            }
        } catch (Exception e) {
            System.out.println("Parse failed: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
