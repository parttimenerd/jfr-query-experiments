package me.bechberger.jfr.extended.plan;

import me.bechberger.jfr.extended.engine.framework.QueryTestFramework;
import me.bechberger.jfr.extended.Parser;
import me.bechberger.jfr.extended.engine.QueryEvaluatorUtils;
import me.bechberger.jfr.extended.ast.ASTNodes.ProgramNode;
import me.bechberger.jfr.extended.ast.ASTNodes.SelectNode;
import me.bechberger.jfr.extended.ast.ASTNodes.SelectItemNode;
import me.bechberger.jfr.extended.ast.ASTNodes.CaseExpressionNode;
import me.bechberger.jfr.extended.ast.ASTNodes.QueryNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Debug to check if CASE expressions are properly detected as containing aggregates
 */
public class DebugCaseAggregateDetection {
    
    private QueryTestFramework framework;
    
    @BeforeEach
    void setUp() {
        framework = new QueryTestFramework();
    }
    
    @Test
    void checkAggregateDetection() {
        System.out.println("=== Checking if CASE expressions are detected as containing aggregates ===");
        
        String query = """
            @SELECT 
                eventType,
                COUNT(*) as count,
                CASE 
                    WHEN COUNT(*) > 4 THEN 'High'
                    WHEN COUNT(*) > 2 THEN 'Medium'
                    ELSE 'Low' 
                END as frequency
            FROM Events 
            GROUP BY eventType
            """;
        
        try {
            ProgramNode program = Parser.parseAndValidate(query);
            QueryNode queryNode = (QueryNode) program.statements().get(0);
            SelectNode selectNode = queryNode.select();
            
            for (int i = 0; i < selectNode.items().size(); i++) {
                SelectItemNode item = selectNode.items().get(i);
                boolean containsAggregate = QueryEvaluatorUtils.containsAggregateFunction(item.expression());
                System.out.printf("SelectItem %d (%s): contains aggregate = %s%n", 
                    i, item.alias() != null ? item.alias() : "no-alias", containsAggregate);
                
                if (item.expression() instanceof CaseExpressionNode caseExpr) {
                    System.out.println("  This is a CASE expression");
                    for (int j = 0; j < caseExpr.whenClauses().size(); j++) {
                        var whenClause = caseExpr.whenClauses().get(j);
                        boolean conditionHasAggregate = QueryEvaluatorUtils.containsAggregateFunction(whenClause.condition());
                        boolean resultHasAggregate = QueryEvaluatorUtils.containsAggregateFunction(whenClause.result());
                        System.out.printf("    WHEN clause %d: condition has aggregate = %s, result has aggregate = %s%n",
                            j, conditionHasAggregate, resultHasAggregate);
                    }
                }
            }
            
        } catch (Exception e) {
            System.err.println("Parse error: " + e.getMessage());
        }
    }
}
