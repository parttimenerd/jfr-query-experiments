package me.bechberger.jfr.extended;

import me.bechberger.jfr.extended.engine.framework.QueryTestFramework;
import me.bechberger.jfr.extended.plan.debug.QueryPlanVisualizer;
import me.bechberger.jfr.extended.plan.AstToPlanConverter;
import me.bechberger.jfr.extended.plan.QueryExecutionContext;
import me.bechberger.jfr.extended.plan.plans.StreamingQueryPlan;
import me.bechberger.jfr.extended.Parser;
import me.bechberger.jfr.extended.ast.ASTNodes.StatementNode;
import me.bechberger.jfr.extended.ast.ASTNodes.ProgramNode;
import me.bechberger.jfr.extended.engine.framework.MockRawJfrQueryExecutor;

/**
 * Debug program to visualize query plan execution for the double-quoted string issue.
 * This will help us understand where the column access and WHERE filtering problems occur.
 */
public class DoubleQuoteDebugVisualizer {
    public static void main(String[] args) {
        try {
            System.out.println("🔬 DOUBLE-QUOTED STRING DEBUG VISUALIZER");
            System.out.println("═══════════════════════════════════════════");
            System.out.println();
            
            // Set up the framework and create test data
            var framework = new QueryTestFramework();
            framework.mockTable("Events")
                .withStringColumn("name")
                .withStringColumn("type")
                .withRow("Alice", "user")
                .withRow("Bob", "admin")
                .build();
                
            framework.mockTable("Users")
                .withStringColumn("name")
                .withStringColumn("role")
                .withRow("Alice", "admin")
                .withRow("Bob", "user")
                .build();
            
            // Create plan executor and converter
            var mockExecutor = new MockRawJfrQueryExecutor();
            var planConverter = new AstToPlanConverter(mockExecutor);
            var context = new QueryExecutionContext(null); // Use null metadata for testing
            var visualizer = new QueryPlanVisualizer();
            
            // Test 1: Simple SELECT with double quotes (failing - returns N/A)
            System.out.println("🧪 TEST 1: SELECT with double-quoted literal");
            System.out.println("Query: @SELECT \"constant\" AS literal, name FROM Events");
            System.out.println("Expected: literal='constant', name='Alice'");
            System.out.println("Actual issue: name='N/A'");
            System.out.println();
            
            debugQuery("@SELECT \"constant\" AS literal, name FROM Events", 
                      planConverter, context, visualizer);
            
            System.out.println("\n" + "═".repeat(80) + "\n");
            
            // Test 2: WHERE clause with double quotes (failing - returns 0 rows)
            System.out.println("🧪 TEST 2: WHERE clause with double-quoted string");
            System.out.println("Query: @SELECT name FROM Users WHERE role = \"admin\"");
            System.out.println("Expected: 1 row with name='Alice'");
            System.out.println("Actual issue: 0 rows returned");
            System.out.println();
            
            debugQuery("@SELECT name FROM Users WHERE role = \"admin\"", 
                      planConverter, context, visualizer);
            
            System.out.println("\n" + "═".repeat(80) + "\n");
            
            // Test 3: Compare with single quotes (working)
            System.out.println("🧪 TEST 3: WHERE clause with single-quoted string (WORKING)");
            System.out.println("Query: @SELECT name FROM Users WHERE role = 'admin'");
            System.out.println("Expected: 1 row with name='Alice'");
            System.out.println();
            
            debugQuery("@SELECT name FROM Users WHERE role = 'admin'", 
                      planConverter, context, visualizer);
                      
        } catch (Exception e) {
            System.err.println("Debug failed: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    private static void debugQuery(String queryString, AstToPlanConverter planConverter, 
                                 QueryExecutionContext context, QueryPlanVisualizer visualizer) {
        try {
            System.out.println("📝 Parsing query...");
            Parser parser = new Parser(queryString);
            ProgramNode program = parser.parse();
            
            if (program.statements().isEmpty()) {
                System.out.println("❌ No statements found in query");
                return;
            }
            
            StatementNode statement = program.statements().get(0);
            System.out.println("✅ Parsed successfully: " + statement.getClass().getSimpleName());
            System.out.println();
            
            System.out.println("🏗️  Converting to streaming plan...");
            StreamingQueryPlan plan = planConverter.convertStatementToPlan(statement);
            System.out.println("✅ Plan created: " + plan.getClass().getSimpleName());
            System.out.println();
            
            System.out.println("📊 DETAILED PLAN VISUALIZATION:");
            System.out.println("─".repeat(50));
            String visualization = visualizer.visualizeExecution(plan, context);
            System.out.println(visualization);
            
        } catch (Exception e) {
            System.err.println("❌ Error during debugging: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
