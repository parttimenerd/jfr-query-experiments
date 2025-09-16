package me.bechberger.jfr.extended.plan.plans;

import me.bechberger.jfr.extended.ast.ASTNodes.*;
import me.bechberger.jfr.extended.ast.Location;
import me.bechberger.jfr.extended.plan.QueryExecutionContext;
import me.bechberger.jfr.extended.plan.QueryResult;
import me.bechberger.jfr.extended.table.JfrTable;
import me.bechberger.jfr.extended.table.StandardJfrTable;
import me.bechberger.jfr.extended.table.CellValue;
import me.bechberger.jfr.extended.table.CellType;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test class for SortPlan functionality.
 */
public class SortPlanTest {
    
    private static final Location TEST_LOCATION = new Location(1, 1);
    
    @Test
    public void testSimpleSortByColumnAscending() throws Exception {
        // Create a test table
        JfrTable testTable = createTestTable();
        
        // Create a mock input plan that returns the test table
        StreamingQueryPlan inputPlan = new MockInputPlan(testTable);
        
        // Create ORDER BY duration ASC
        OrderFieldNode orderField = new OrderFieldNode(
            new IdentifierNode("duration", TEST_LOCATION),
            SortOrder.ASC,
            TEST_LOCATION
        );
        
        OrderByNode orderByNode = new OrderByNode(
            List.of(orderField),
            TEST_LOCATION
        );
        
        // Create and execute the sort plan
        SortPlan sortPlan = new SortPlan(orderByNode, inputPlan);
        QueryExecutionContext context = new QueryExecutionContext(null);
        
        QueryResult result = sortPlan.execute(context);
        
        assertTrue(result.isSuccess());
        JfrTable sortedTable = result.getTable();
        
        // Verify the table is sorted by duration in ascending order
        List<JfrTable.Row> rows = sortedTable.getRows();
        assertEquals(3, rows.size());
        
        // Check that durations are in ascending order
        double previousDuration = -1;
        for (JfrTable.Row row : rows) {
            double currentDuration = ((Number) row.getCells().get(1).getValue()).doubleValue(); // duration is column 1
            assertTrue(currentDuration >= previousDuration);
            previousDuration = currentDuration;
        }
    }
    
    @Test
    public void testSimpleSortByColumnDescending() throws Exception {
        // Create a test table
        JfrTable testTable = createTestTable();
        
        // Create a mock input plan that returns the test table
        StreamingQueryPlan inputPlan = new MockInputPlan(testTable);
        
        // Create ORDER BY duration DESC
        OrderFieldNode orderField = new OrderFieldNode(
            new IdentifierNode("duration", TEST_LOCATION),
            SortOrder.DESC,
            TEST_LOCATION
        );
        
        OrderByNode orderByNode = new OrderByNode(
            List.of(orderField),
            TEST_LOCATION
        );
        
        // Create and execute the sort plan
        SortPlan sortPlan = new SortPlan(orderByNode, inputPlan);
        QueryExecutionContext context = new QueryExecutionContext(null);
        
        QueryResult result = sortPlan.execute(context);
        
        assertTrue(result.isSuccess());
        JfrTable sortedTable = result.getTable();
        
        // Verify the table is sorted by duration in descending order
        List<JfrTable.Row> rows = sortedTable.getRows();
        assertEquals(3, rows.size());
        
        // Check that durations are in descending order
        double previousDuration = Double.MAX_VALUE;
        for (JfrTable.Row row : rows) {
            double currentDuration = ((Number) row.getCells().get(1).getValue()).doubleValue(); // duration is column 1
            assertTrue(currentDuration <= previousDuration);
            previousDuration = currentDuration;
        }
    }
    
    @Test
    public void testMultiFieldSort() throws Exception {
        // Create a test table with multiple events of same name but different durations
        JfrTable testTable = createMultiFieldTestTable();
        
        // Create a mock input plan that returns the test table
        StreamingQueryPlan inputPlan = new MockInputPlan(testTable);
        
        // Create ORDER BY name ASC, duration DESC
        OrderFieldNode nameField = new OrderFieldNode(
            new IdentifierNode("name", TEST_LOCATION),
            SortOrder.ASC,
            TEST_LOCATION
        );
        
        OrderFieldNode durationField = new OrderFieldNode(
            new IdentifierNode("duration", TEST_LOCATION),
            SortOrder.DESC,
            TEST_LOCATION
        );
        
        OrderByNode orderByNode = new OrderByNode(
            List.of(nameField, durationField),
            TEST_LOCATION
        );
        
        // Create and execute the sort plan
        SortPlan sortPlan = new SortPlan(orderByNode, inputPlan);
        QueryExecutionContext context = new QueryExecutionContext(null);
        
        QueryResult result = sortPlan.execute(context);
        
        assertTrue(result.isSuccess());
        JfrTable sortedTable = result.getTable();
        
        // Verify the table is sorted by name ASC, then duration DESC
        List<JfrTable.Row> rows = sortedTable.getRows();
        assertEquals(4, rows.size());
        
        // Check sorting order
        String previousName = "";
        Double previousDuration = Double.MAX_VALUE;
        
        for (JfrTable.Row row : rows) {
            String currentName = (String) row.getCells().get(0).getValue(); // name is column 0
            Double currentDuration = ((Number) row.getCells().get(1).getValue()).doubleValue(); // duration is column 1
            
            if (!currentName.equals(previousName)) {
                // Name changed, so it should be lexicographically greater
                assertTrue(currentName.compareTo(previousName) >= 0);
                previousName = currentName;
                previousDuration = Double.MAX_VALUE; // Reset for new name group
            }
            
            // Within the same name group, duration should be descending
            assertTrue(currentDuration <= previousDuration);
            previousDuration = currentDuration;
        }
    }
    
    @Test
    public void testExplain() {
        // Create ORDER BY duration ASC
        OrderFieldNode orderField = new OrderFieldNode(
            new IdentifierNode("duration", TEST_LOCATION),
            SortOrder.ASC,
            TEST_LOCATION
        );
        
        OrderByNode orderByNode = new OrderByNode(
            List.of(orderField),
            TEST_LOCATION
        );
        
        // Create a mock input plan
        StreamingQueryPlan inputPlan = new MockInputPlan(createTestTable());
        
        // Create the sort plan
        SortPlan sortPlan = new SortPlan(orderByNode, inputPlan);
        
        String explanation = sortPlan.explain();
        assertTrue(explanation.contains("Sort["));
        assertTrue(explanation.contains("duration"));
        assertTrue(explanation.contains("ASC"));
    }
    
    @Test
    public void testSupportsStreaming() {
        // Create a mock input plan
        StreamingQueryPlan inputPlan = new MockInputPlan(createTestTable());
        
        // Create ORDER BY duration ASC
        OrderFieldNode orderField = new OrderFieldNode(
            new IdentifierNode("duration", TEST_LOCATION),
            SortOrder.ASC,
            TEST_LOCATION
        );
        
        OrderByNode orderByNode = new OrderByNode(
            List.of(orderField),
            TEST_LOCATION
        );
        
        // Create the sort plan
        SortPlan sortPlan = new SortPlan(orderByNode, inputPlan);
        
        // Sorting should not support streaming (needs to materialize all data)
        assertFalse(sortPlan.supportsStreaming());
    }
    
    /**
     * Creates a test table with sample data for testing.
     */
    private JfrTable createTestTable() {
        JfrTable table = new StandardJfrTable(List.of(
            new JfrTable.Column("name", CellType.STRING),
            new JfrTable.Column("duration", CellType.NUMBER)
        ));
        
        // Add test data (intentionally unsorted)
        table.addRow(createRow("Event2", 200L));
        table.addRow(createRow("Event1", 100L));
        table.addRow(createRow("Event3", 300L));
        
        return table;
    }
    
    /**
     * Creates a test table with multiple fields for multi-field sorting.
     */
    private JfrTable createMultiFieldTestTable() {
        JfrTable table = new StandardJfrTable(List.of(
            new JfrTable.Column("name", CellType.STRING),
            new JfrTable.Column("duration", CellType.NUMBER)
        ));
        
        // Add test data with same names but different durations
        table.addRow(createRow("B", 200L));
        table.addRow(createRow("A", 300L));
        table.addRow(createRow("A", 100L));
        table.addRow(createRow("B", 400L));
        
        return table;
    }
    
    /**
     * Creates a table row with the given values.
     */
    private JfrTable.Row createRow(String name, Long duration) {
        List<CellValue> cells = List.of(
            CellValue.of(name),
            CellValue.of(duration)
        );
        return new StandardJfrTable.Row(cells);
    }
    
    /**
     * Mock input plan that returns a fixed table.
     */
    private static class MockInputPlan extends AbstractStreamingPlan {
        private final JfrTable table;
        
        public MockInputPlan(JfrTable table) {
            super(null);
            this.table = table;
        }
        
        @Override
        public QueryResult execute(QueryExecutionContext context) {
            return QueryResult.success(table);
        }
        
        @Override
        public String explain() {
            return "MockInputPlan";
        }
    }
}
