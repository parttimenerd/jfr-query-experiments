package me.bechberger.jfr.extended;

import me.bechberger.jfr.extended.AutoCompletionService.CompletionContext;
import me.bechberger.jfr.extended.AutoCompletionService.CompletionItem;
import me.bechberger.jfr.extended.AutoCompletionService.CompletionType;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import java.util.List;

/**
 * Unit tests for the AutoCompletionService
 */
public class AutoCompletionServiceTest {
    
    private AutoCompletionService service = new AutoCompletionService();
    
    @Test
    public void testKeywordCompletion() {
        CompletionContext context = new CompletionContext(
            "SEL", 3, "SEL", List.of(), List.of(), List.of()
        );
        
        List<CompletionItem> completions = service.getCompletions(context);
        
        // Should suggest SELECT
        boolean foundSelect = false;
        for (CompletionItem item : completions) {
            if (item.text().equals("SELECT") && item.type() == CompletionType.KEYWORD) {
                foundSelect = true;
                break;
            }
        }
        
        assertTrue(foundSelect, "Should suggest SELECT keyword");
    }
    
    @Test
    public void testFunctionCompletion() {
        CompletionContext context = new CompletionContext(
            "SELECT COU", 10, "COU", List.of(), List.of(), List.of()
        );
        
        List<CompletionItem> completions = service.getCompletions(context);
        
        // Should suggest COUNT
        boolean foundCount = false;
        for (CompletionItem item : completions) {
            if (item.text().equals("COUNT(*)") && item.type() == CompletionType.FUNCTION) {
                foundCount = true;
                break;
            }
        }
        
        assertTrue(foundCount, "Should suggest COUNT function");
    }
    
    @Test
    public void testEventTypeCompletion() {
        CompletionContext context = new CompletionContext(
            "SELECT * FROM Garbage", 21, "Garbage", List.of(), List.of(), List.of()
        );
        
        List<CompletionItem> completions = service.getCompletions(context);
        
        // Should suggest GarbageCollection
        boolean foundGC = false;
        for (CompletionItem item : completions) {
            if (item.text().contains("GarbageCollection") && item.type() == CompletionType.EVENT_TYPE) {
                foundGC = true;
                break;
            }
        }
        
        assertTrue(foundGC, "Should suggest GarbageCollection event type");
    }
    
    @Test
    public void testFieldCompletion() {
        CompletionContext context = new CompletionContext(
            "SELECT dur FROM GarbageCollection", 10, "dur", List.of(), List.of(), List.of()
        );
        
        List<CompletionItem> completions = service.getCompletions(context);
        
        // Should suggest duration
        boolean foundDuration = false;
        for (CompletionItem item : completions) {
            if (item.text().equals("duration") && item.type() == CompletionType.FIELD) {
                foundDuration = true;
                break;
            }
        }
        
        assertTrue(foundDuration, "Should suggest duration field");
    }
    
    @Test
    public void testOperatorCompletion() {
        CompletionContext context = new CompletionContext(
            "SELECT * FROM GarbageCollection WHERE duration ", 49, "", List.of(), List.of(), List.of()
        );
        
        List<CompletionItem> completions = service.getCompletions(context);
        
        // Should suggest operators
        boolean foundEquals = false;
        boolean foundGreater = false;
        
        for (CompletionItem item : completions) {
            if (item.text().equals("=") && item.type() == CompletionType.OPERATOR) {
                foundEquals = true;
            } else if (item.text().equals(">") && item.type() == CompletionType.OPERATOR) {
                foundGreater = true;
            }
        }
        
        assertTrue(foundEquals, "Should suggest equals operator");
        assertTrue(foundGreater, "Should suggest greater than operator");
    }
    
    @Test
    public void testSnippetCompletion() {
        CompletionContext context = new CompletionContext(
            "SELECT", 6, "", List.of(), List.of(), List.of()
        );
        
        List<CompletionItem> completions = service.getCompletions(context);
        
        // Should suggest snippets
        boolean foundSelectAll = false;
        boolean foundSelectCount = false;
        
        for (CompletionItem item : completions) {
            if (item.text().equals("SELECT * FROM ") && item.type() == CompletionType.SNIPPET) {
                foundSelectAll = true;
            } else if (item.text().equals("SELECT COUNT(*) FROM ") && item.type() == CompletionType.SNIPPET) {
                foundSelectCount = true;
            }
        }
        
        assertTrue(foundSelectAll, "Should suggest SELECT * FROM snippet");
        assertTrue(foundSelectCount, "Should suggest SELECT COUNT(*) FROM snippet");
    }
    
    @Test
    public void testVariableCompletion() {
        List<String> variables = List.of("slowGCs", "fastGCs", "allEvents");
        
        CompletionContext context = new CompletionContext(
            "SELECT * FROM slow", 17, "slow", List.of(), List.of(), variables
        );
        
        List<CompletionItem> completions = service.getCompletions(context);
        
        // Should suggest slowGCs variable
        boolean foundSlowGCs = false;
        for (CompletionItem item : completions) {
            if (item.text().equals("slowGCs") && item.type() == CompletionType.VARIABLE) {
                foundSlowGCs = true;
                break;
            }
        }
        
        assertTrue(foundSlowGCs, "Should suggest slowGCs variable");
    }
    
    @Test
    public void testCustomEventTypes() {
        List<String> customEventTypes = List.of("MyCustomEvent", "AnotherEvent");
        
        CompletionContext context = new CompletionContext(
            "SELECT * FROM My", 14, "My", customEventTypes, List.of(), List.of()
        );
        
        List<CompletionItem> completions = service.getCompletions(context);
        
        // Should suggest custom event type
        boolean foundCustomEvent = false;
        for (CompletionItem item : completions) {
            if (item.text().equals("MyCustomEvent") && item.type() == CompletionType.EVENT_TYPE) {
                foundCustomEvent = true;
                break;
            }
        }
        
        assertTrue(foundCustomEvent, "Should suggest custom event type");
    }
    
    @Test
    public void testCustomFields() {
        List<String> customFields = List.of("customField1", "customField2", "duration");
        
        CompletionContext context = new CompletionContext(
            "SELECT custom", 13, "custom", List.of(), customFields, List.of()
        );
        
        List<CompletionItem> completions = service.getCompletions(context);
        
        // Should suggest custom fields
        boolean foundCustomField1 = false;
        boolean foundCustomField2 = false;
        
        for (CompletionItem item : completions) {
            if (item.text().equals("customField1") && item.type() == CompletionType.FIELD) {
                foundCustomField1 = true;
            } else if (item.text().equals("customField2") && item.type() == CompletionType.FIELD) {
                foundCustomField2 = true;
            }
        }
        
        assertTrue(foundCustomField1, "Should suggest customField1");
        assertTrue(foundCustomField2, "Should suggest customField2");
    }
    
    @Test
    public void testCompletionPriority() {
        CompletionContext context = new CompletionContext(
            "SELECT", 6, "", List.of(), List.of(), List.of()
        );
        
        List<CompletionItem> completions = service.getCompletions(context);
        
        // Should be sorted by priority (descending)
        assertTrue(completions.size() > 1, "Should have completions");
        
        for (int i = 1; i < completions.size(); i++) {
            assertTrue( 
                      completions.get(i-1).priority() >= completions.get(i).priority(), "Should be sorted by priority");
        }
    }
    
    @Test
    public void testCompletionLimit() {
        CompletionContext context = new CompletionContext(
            "", 0, "", List.of(), List.of(), List.of()
        );
        
        List<CompletionItem> completions = service.getCompletions(context);
        
        // Should limit to 50 completions
        assertTrue(completions.size() <= 50, "Should limit completions");
    }
    
    @Test
    public void testEmptyPrefix() {
        CompletionContext context = new CompletionContext(
            "SELECT ", 7, "", List.of(), List.of(), List.of()
        );
        
        List<CompletionItem> completions = service.getCompletions(context);
        
        // Should still provide completions even with empty prefix
        assertTrue(completions.size() > 0, "Should provide completions for empty prefix");
    }
    
    @Test
    public void testCaseInsensitiveMatching() {
        CompletionContext context = new CompletionContext(
            "select", 6, "select", List.of(), List.of(), List.of()
        );
        
        List<CompletionItem> completions = service.getCompletions(context);
        
        // Should match SELECT even with lowercase input
        boolean foundSelect = false;
        for (CompletionItem item : completions) {
            if (item.text().equals("SELECT") && item.type() == CompletionType.KEYWORD) {
                foundSelect = true;
                break;
            }
        }
        
        assertTrue(foundSelect, "Should match SELECT with lowercase input");
    }
    
    @Test
    public void testPartialMatching() {
        CompletionContext context = new CompletionContext(
            "GRO", 3, "GRO", List.of(), List.of(), List.of()
        );
        
        List<CompletionItem> completions = service.getCompletions(context);
        
        // Should suggest GROUP BY
        boolean foundGroupBy = false;
        for (CompletionItem item : completions) {
            if (item.text().equals("GROUP BY") && item.type() == CompletionType.KEYWORD) {
                foundGroupBy = true;
                break;
            }
        }
        
        assertTrue(foundGroupBy, "Should match GROUP BY with partial input");
    }
    
    @Test
    public void testPercentileCompletion() {
        CompletionContext context = new CompletionContext(
            "P9", 2, "P9", List.of(), List.of(), List.of()
        );
        
        List<CompletionItem> completions = service.getCompletions(context);
        
        // Should suggest P90, P95, P99, P999
        boolean foundP90 = false;
        boolean foundP95 = false;
        boolean foundP99 = false;
        boolean foundP999 = false;
        
        for (CompletionItem item : completions) {
            if (item.type() == CompletionType.FUNCTION) {
                switch (item.text()) {
                    case "P90(" -> foundP90 = true;
                    case "P95(" -> foundP95 = true;
                    case "P99(" -> foundP99 = true;
                    case "P999(" -> foundP999 = true;
                }
            }
        }
        
        assertTrue(foundP90, "Should suggest P90");
        assertTrue(foundP95, "Should suggest P95");
        assertTrue(foundP99, "Should suggest P99");
        assertTrue(foundP999, "Should suggest P999");
    }
    
    @Test
    public void testComplexQueryCompletion() {
        String query = """
            SELECT 
                COUNT(*) AS count,
                AVG(duration) AS avg_duration
            FROM jdk.ExecutionSample AS es
            WHERE 
                duration > 10 AND 
                stackTrace LIKE '%MyClass%'
            GROUP BY thread
            ORDER BY avg_duration DESC
            LI
            """;
        
        CompletionContext context = new CompletionContext(
            query, query.length(), "LI", List.of(), List.of(), List.of()
        );
        
        List<CompletionItem> completions = service.getCompletions(context);
        
        // Should suggest LIMIT
        boolean foundLimit = false;
        for (CompletionItem item : completions) {
            if (item.text().equals("LIMIT") && item.type() == CompletionType.KEYWORD) {
                foundLimit = true;
                break;
            }
        }
        
        assertTrue(foundLimit, "Should suggest LIMIT keyword");
    }
}
