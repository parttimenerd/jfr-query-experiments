package me.bechberger.jfr.extended.test;

import me.bechberger.jfr.extended.engine.framework.QueryTestFramework;
import me.bechberger.jfr.extended.table.CellValue;
import me.bechberger.jfr.extended.table.JfrTable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;

import java.util.logging.Logger;
import java.util.logging.Level;
import java.util.logging.ConsoleHandler;
import java.util.logging.SimpleFormatter;

public class ManualQueryTest {
    
    private static final Logger logger = Logger.getLogger(ManualQueryTest.class.getName());
    
    @BeforeEach
    void setupLogging() {
        // Configure detailed logging for debugging
        logger.setLevel(Level.ALL);
        ConsoleHandler handler = new ConsoleHandler();
        handler.setLevel(Level.ALL);
        handler.setFormatter(new SimpleFormatter());
        logger.addHandler(handler);
        logger.setUseParentHandlers(false);
    }
    
    @Test
    @DisplayName("Test COLLECT with COUNT(*) and GROUP BY - Advanced Debugging")
    void testOriginalFailingQuery() {
        logger.info("=== STARTING MANUAL QUERY TEST ===");
        
        var framework = new QueryTestFramework();
        logger.info("✓ QueryTestFramework initialized");
        
        try {
            // Create the same test data as CollectFunctionEndToEndTest
            logger.info("Creating Events table with test data...");
            framework.customTable("Events")
                .withStringColumn("eventType")
                .withNumberColumn("duration")
                .withStringColumn("thread")
                .withNumberColumn("timestamp")
                .withStringColumn("stackFrame")
                .withRow("GarbageCollection", 150L, "GC-Thread-1", 1000L, "java.lang.System.gc")
                .withRow("AllocationSample", 50L, "Main-Thread", 1100L, "com.example.App.allocate")
                .withRow("GarbageCollection", 200L, "GC-Thread-1", 1200L, "java.lang.System.gc")
                .withRow("ExecutionSample", 75L, "Worker-Thread-1", 1300L, "com.example.Worker.process")
                .withRow("AllocationSample", 30L, "Main-Thread", 1400L, "com.example.App.allocate")
                .withRow("GarbageCollection", 180L, "GC-Thread-1", 1500L, "java.lang.System.gc")
                .withRow("ExecutionSample", 120L, "Worker-Thread-2", 1600L, "com.example.Worker.process")
                .build();
            logger.info("✓ Events table created successfully with 7 rows");
            
            // Test the problematic query
            String query = """
                @SELECT
                    eventType,
                    COLLECT(duration) as durations,
                    COUNT(*) as event_count
                FROM Events
                GROUP BY eventType
                ORDER BY event_count DESC
                """;
            
            logger.info("=== EXECUTING QUERY ===");
            logger.info("Query text:\n" + query);
            
            var result = framework.executeQuery(query);
            
            if (result.isSuccess()) {
                logger.info("✅ Query executed successfully!");
                var table = result.getTable();
                
                // Log basic table info
                logger.info("Table structure:");
                logger.info("  - Column count: " + table.getColumns().size());
                logger.info("  - Row count: " + table.getRowCount());
                
                // Log column details
                logger.info("Column definitions:");
                for (int i = 0; i < table.getColumns().size(); i++) {
                    var col = table.getColumns().get(i);
                    logger.info("  [" + i + "] " + col.name() + " (" + col.type() + ")");
                }
                
                // Log row data with detailed type information
                logger.info("Row data:");
                for (int i = 0; i < table.getRowCount(); i++) {
                    var row = table.getRows().get(i);
                    logger.info("  Row " + i + ":");
                    for (int j = 0; j < row.getCells().size(); j++) {
                        CellValue cell = row.getCells().get(j);
                        String colName = table.getColumns().get(j).name();
                        logger.info("    " + colName + " = " + cell + " (type: " + cell.getClass().getSimpleName() + ")");
                        
                        // Special handling for arrays to show contents
                        if (cell instanceof CellValue.ArrayValue arrayValue) {
                            logger.info("      Array contents (" + arrayValue.elements().size() + " elements):");
                            for (int k = 0; k < arrayValue.elements().size(); k++) {
                                CellValue element = arrayValue.elements().get(k);
                                logger.info("        [" + k + "] = " + element + " (" + element.getClass().getSimpleName() + ")");
                            }
                        }
                    }
                }
                
                logger.info("=== QUERY EXECUTION SUCCESSFUL ===");
                
            } else {
                logger.severe("❌ Query execution failed!");
                logger.severe("Error message: " + result.getError().getMessage());
                logger.severe("Error type: " + result.getError().getClass().getName());
                
                // Log stack trace with line numbers
                logger.severe("Full stack trace:");
                for (StackTraceElement element : result.getError().getStackTrace()) {
                    logger.severe("  at " + element.getClassName() + "." + element.getMethodName() + 
                        "(" + element.getFileName() + ":" + element.getLineNumber() + ")");
                }
                
                // Also test executeAndAssertSuccess to see if it provides more details
                logger.info("Testing executeAndAssertSuccess for additional error details...");
                try {
                    framework.executeAndAssertSuccess(query);
                    logger.info("✓ executeAndAssertSuccess also worked!");
                } catch (Exception e2) {
                    logger.severe("❌ executeAndAssertSuccess failed with: " + e2.getClass().getName());
                    logger.severe("executeAndAssertSuccess error message: " + e2.getMessage());
                    
                    if (e2.getCause() != null) {
                        logger.severe("Root cause: " + e2.getCause().getClass().getName() + " - " + e2.getCause().getMessage());
                    }
                }
                
                // Fail the test
                throw new AssertionError("Query should have succeeded but failed with: " + result.getError().getMessage(), result.getError());
            }
            
        } catch (Exception e) {
            logger.severe("❌ Unexpected exception during test execution!");
            logger.severe("Exception type: " + e.getClass().getName());
            logger.severe("Exception message: " + e.getMessage());
            
            if (e.getCause() != null) {
                logger.severe("Root cause: " + e.getCause().getClass().getName() + " - " + e.getCause().getMessage());
            }
            
            logger.severe("Full exception stack trace:");
            for (StackTraceElement element : e.getStackTrace()) {
                logger.severe("  at " + element.getClassName() + "." + element.getMethodName() + 
                    "(" + element.getFileName() + ":" + element.getLineNumber() + ")");
            }
            
            throw e; // Re-throw to fail the test
        } finally {
            logger.info("=== MANUAL QUERY TEST COMPLETED ===");
        }
    }
}
