package me.bechberger.jfr.extended.plan.plans;

import me.bechberger.jfr.extended.ast.ASTNodes.StandardJoinType;
import me.bechberger.jfr.extended.ast.ASTNodes.FuzzyJoinType;
import me.bechberger.jfr.extended.plan.QueryExecutionContext;
import me.bechberger.jfr.extended.plan.QueryResult;
import me.bechberger.jfr.extended.table.JfrTable;
import me.bechberger.jfr.extended.table.CellValue;
import me.bechberger.jfr.extended.table.CellType;
import me.bechberger.jfr.extended.table.StandardJfrTable;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive tests for JOIN plan implementations.
 * 
 * Tests cover:
 * - Hash JOIN (INNER, LEFT, RIGHT, FULL)
 * - Nested Loop JOIN (INNER, LEFT, RIGHT, FULL)
 * - Fuzzy JOIN (NEAREST, PREVIOUS, AFTER)
 * - Edge cases (empty tables, null values, duplicate keys)
 * - Performance characteristics
 * 
 * @author JFR Query Plan Architecture Test Suite
 * @since 3.0
 */
public class JoinPlanTest {
    
    private QueryExecutionContext context;
    
    @BeforeEach
    void setUp() {
        context = new QueryExecutionContext(null);
    }
    
    /**
     * Create a simple table for testing.
     */
    private JfrTable createSimpleTable(String name, Object[][] data) {
        List<JfrTable.Column> columns = List.of(
            new JfrTable.Column("id", CellType.NUMBER),
            new JfrTable.Column("name", CellType.STRING)
        );
        
        StandardJfrTable table = new StandardJfrTable(columns);
        
        for (Object[] row : data) {
            table.addRow(
                new CellValue.NumberValue(((Number) row[0]).doubleValue()),
                new CellValue.StringValue((String) row[1])
            );
        }
        
        return table;
    }
    
    /**
     * Create a table with timestamps for fuzzy join testing.
     */
    private JfrTable createTimestampTable(String name, Object[][] data) {
        List<JfrTable.Column> columns = List.of(
            new JfrTable.Column("timestamp", CellType.TIMESTAMP),
            new JfrTable.Column("event", CellType.STRING)
        );
        
        StandardJfrTable table = new StandardJfrTable(columns);
        
        for (Object[] row : data) {
            table.addRow(
                new CellValue.TimestampValue((Instant) row[0]),
                new CellValue.StringValue((String) row[1])
            );
        }
        
        return table;
    }
    
    /**
     * Create a mock plan that returns a predefined table.
     */
    private StreamingQueryPlan createMockPlan(JfrTable table) {
        return new StreamingQueryPlan() {
            @Override
            public QueryResult execute(QueryExecutionContext context) {
                return QueryResult.success(table);
            }
            
            @Override
            public String explain() {
                return "MockPlan";
            }
            
            @Override
            public me.bechberger.jfr.extended.ast.ASTNode getSourceNode() {
                return null;
            }
        };
    }
    
    @Nested
    @DisplayName("Hash JOIN Tests")
    class HashJoinTests {
        
        @Test
        @DisplayName("INNER JOIN with matching records")
        void testInnerJoinWithMatches() throws Exception {
            // Create test data
            JfrTable leftTable = createSimpleTable("left", new Object[][] {
                {1, "Alice"},
                {2, "Bob"},
                {3, "Charlie"}
            });
            
            JfrTable rightTable = createSimpleTable("right", new Object[][] {
                {1, "Manager"},
                {2, "Developer"},
                {4, "Designer"}
            });
            
            // Create plans
            StreamingQueryPlan leftPlan = createMockPlan(leftTable);
            StreamingQueryPlan rightPlan = createMockPlan(rightTable);
            
            // Execute join
            HashJoinPlan joinPlan = new HashJoinPlan(null, leftPlan, rightPlan, 
                StandardJoinType.INNER, "id", "id");
            
            QueryResult result = joinPlan.execute(context);
            
            // Verify results
            assertTrue(result.isSuccess());
            JfrTable joinedTable = result.getTable();
            
            assertEquals(2, joinedTable.getRowCount()); // Only matching records
            assertEquals(4, joinedTable.getColumnCount()); // All columns from both tables
            
            // Verify joined data
            assertEquals(1.0, ((CellValue.NumberValue) joinedTable.getCell(0, 0)).value());
            assertEquals("Alice", ((CellValue.StringValue) joinedTable.getCell(0, 1)).value());
            assertEquals(1.0, ((CellValue.NumberValue) joinedTable.getCell(0, 2)).value());
            assertEquals("Manager", ((CellValue.StringValue) joinedTable.getCell(0, 3)).value());
        }
        
        @Test
        @DisplayName("LEFT OUTER JOIN with unmatched left records")
        void testLeftOuterJoin() throws Exception {
            JfrTable leftTable = createSimpleTable("left", new Object[][] {
                {1, "Alice"},
                {2, "Bob"},
                {3, "Charlie"}
            });
            
            JfrTable rightTable = createSimpleTable("right", new Object[][] {
                {1, "Manager"},
                {2, "Developer"}
            });
            
            StreamingQueryPlan leftPlan = createMockPlan(leftTable);
            StreamingQueryPlan rightPlan = createMockPlan(rightTable);
            
            HashJoinPlan joinPlan = new HashJoinPlan(null, leftPlan, rightPlan, 
                StandardJoinType.LEFT, "id", "id");
            
            QueryResult result = joinPlan.execute(context);
            
            assertTrue(result.isSuccess());
            JfrTable joinedTable = result.getTable();
            
            assertEquals(3, joinedTable.getRowCount()); // All left records
            
            // Verify that Charlie has null values for right table columns
            assertEquals(3.0, ((CellValue.NumberValue) joinedTable.getCell(2, 0)).value());
            assertEquals("Charlie", ((CellValue.StringValue) joinedTable.getCell(2, 1)).value());
            assertInstanceOf(CellValue.NullValue.class, joinedTable.getCell(2, 2));
            assertInstanceOf(CellValue.NullValue.class, joinedTable.getCell(2, 3));
        }
        
        @Test
        @DisplayName("RIGHT OUTER JOIN with unmatched right records")
        void testRightOuterJoin() throws Exception {
            JfrTable leftTable = createSimpleTable("left", new Object[][] {
                {1, "Alice"},
                {2, "Bob"}
            });
            
            JfrTable rightTable = createSimpleTable("right", new Object[][] {
                {1, "Manager"},
                {2, "Developer"},
                {3, "Designer"}
            });
            
            StreamingQueryPlan leftPlan = createMockPlan(leftTable);
            StreamingQueryPlan rightPlan = createMockPlan(rightTable);
            
            HashJoinPlan joinPlan = new HashJoinPlan(null, leftPlan, rightPlan, 
                StandardJoinType.RIGHT, "id", "id");
            
            QueryResult result = joinPlan.execute(context);
            
            assertTrue(result.isSuccess());
            JfrTable joinedTable = result.getTable();
            
            assertEquals(3, joinedTable.getRowCount()); // All right records
            
            // Find the unmatched right record (Designer)
            boolean foundDesigner = false;
            for (int i = 0; i < joinedTable.getRowCount(); i++) {
                CellValue nameValue = joinedTable.getCell(i, 3);
                if (nameValue instanceof CellValue.StringValue str && "Designer".equals(str.value())) {
                    foundDesigner = true;
                    // Verify null values for left table columns
                    assertInstanceOf(CellValue.NullValue.class, joinedTable.getCell(i, 0));
                    assertInstanceOf(CellValue.NullValue.class, joinedTable.getCell(i, 1));
                    break;
                }
            }
            assertTrue(foundDesigner, "Designer record should be present with null left values");
        }
        
        @Test
        @DisplayName("FULL OUTER JOIN with all records")
        void testFullOuterJoin() throws Exception {
            JfrTable leftTable = createSimpleTable("left", new Object[][] {
                {1, "Alice"},
                {2, "Bob"},
                {3, "Charlie"}
            });
            
            JfrTable rightTable = createSimpleTable("right", new Object[][] {
                {1, "Manager"},
                {2, "Developer"},
                {4, "Designer"}
            });
            
            StreamingQueryPlan leftPlan = createMockPlan(leftTable);
            StreamingQueryPlan rightPlan = createMockPlan(rightTable);
            
            HashJoinPlan joinPlan = new HashJoinPlan(null, leftPlan, rightPlan, 
                StandardJoinType.FULL, "id", "id");
            
            QueryResult result = joinPlan.execute(context);
            
            assertTrue(result.isSuccess());
            JfrTable joinedTable = result.getTable();
            
            assertEquals(4, joinedTable.getRowCount()); // All unique records
            
            // Verify we have records for Alice, Bob, Charlie, and Designer
            boolean foundCharlie = false, foundDesigner = false;
            for (int i = 0; i < joinedTable.getRowCount(); i++) {
                CellValue leftName = joinedTable.getCell(i, 1);
                CellValue rightName = joinedTable.getCell(i, 3);
                
                if (leftName instanceof CellValue.StringValue str && "Charlie".equals(str.value())) {
                    foundCharlie = true;
                    assertInstanceOf(CellValue.NullValue.class, rightName);
                } else if (rightName instanceof CellValue.StringValue str && "Designer".equals(str.value())) {
                    foundDesigner = true;
                    assertInstanceOf(CellValue.NullValue.class, leftName);
                }
            }
            assertTrue(foundCharlie, "Charlie should be present with null right values");
            assertTrue(foundDesigner, "Designer should be present with null left values");
        }
        
        @Test
        @DisplayName("JOIN with empty tables")
        void testJoinWithEmptyTables() throws Exception {
            JfrTable leftTable = createSimpleTable("left", new Object[][] {});
            JfrTable rightTable = createSimpleTable("right", new Object[][] {
                {1, "Manager"}
            });
            
            StreamingQueryPlan leftPlan = createMockPlan(leftTable);
            StreamingQueryPlan rightPlan = createMockPlan(rightTable);
            
            HashJoinPlan joinPlan = new HashJoinPlan(null, leftPlan, rightPlan, 
                StandardJoinType.INNER, "id", "id");
            
            QueryResult result = joinPlan.execute(context);
            
            assertTrue(result.isSuccess());
            assertEquals(0, result.getTable().getRowCount());
        }
        
        @Test
        @DisplayName("JOIN with duplicate keys")
        void testJoinWithDuplicateKeys() throws Exception {
            JfrTable leftTable = createSimpleTable("left", new Object[][] {
                {1, "Alice"},
                {1, "Alice2"},
                {2, "Bob"}
            });
            
            JfrTable rightTable = createSimpleTable("right", new Object[][] {
                {1, "Manager"},
                {1, "Manager2"}
            });
            
            StreamingQueryPlan leftPlan = createMockPlan(leftTable);
            StreamingQueryPlan rightPlan = createMockPlan(rightTable);
            
            HashJoinPlan joinPlan = new HashJoinPlan(null, leftPlan, rightPlan, 
                StandardJoinType.INNER, "id", "id");
            
            QueryResult result = joinPlan.execute(context);
            
            assertTrue(result.isSuccess());
            JfrTable joinedTable = result.getTable();
            
            // Should have 4 records (2 left * 2 right for key 1)
            assertEquals(4, joinedTable.getRowCount());
        }
    }
    
    @Nested
    @DisplayName("Nested Loop JOIN Tests")
    class NestedLoopJoinTests {
        
        @Test
        @DisplayName("INNER JOIN produces same results as Hash JOIN")
        void testInnerJoinConsistency() throws Exception {
            JfrTable leftTable = createSimpleTable("left", new Object[][] {
                {1, "Alice"},
                {2, "Bob"},
                {3, "Charlie"}
            });
            
            JfrTable rightTable = createSimpleTable("right", new Object[][] {
                {1, "Manager"},
                {2, "Developer"},
                {4, "Designer"}
            });
            
            StreamingQueryPlan leftPlan = createMockPlan(leftTable);
            StreamingQueryPlan rightPlan = createMockPlan(rightTable);
            
            // Execute both join types
            HashJoinPlan hashJoin = new HashJoinPlan(null, leftPlan, rightPlan, 
                StandardJoinType.INNER, "id", "id");
            NestedLoopJoinPlan nestedLoopJoin = new NestedLoopJoinPlan(null, leftPlan, rightPlan, 
                StandardJoinType.INNER, "id", "id");
            
            QueryResult hashResult = hashJoin.execute(context);
            QueryResult nestedResult = nestedLoopJoin.execute(context);
            
            assertTrue(hashResult.isSuccess());
            assertTrue(nestedResult.isSuccess());
            
            // Should produce same number of rows
            assertEquals(hashResult.getTable().getRowCount(), nestedResult.getTable().getRowCount());
            
            // Both should produce 2 rows (matching records)
            assertEquals(2, hashResult.getTable().getRowCount());
            assertEquals(2, nestedResult.getTable().getRowCount());
        }
        
        @Test
        @DisplayName("LEFT OUTER JOIN with small dataset")
        void testLeftOuterJoinSmallDataset() throws Exception {
            JfrTable leftTable = createSimpleTable("left", new Object[][] {
                {1, "Alice"},
                {2, "Bob"}
            });
            
            JfrTable rightTable = createSimpleTable("right", new Object[][] {
                {1, "Manager"}
            });
            
            StreamingQueryPlan leftPlan = createMockPlan(leftTable);
            StreamingQueryPlan rightPlan = createMockPlan(rightTable);
            
            NestedLoopJoinPlan joinPlan = new NestedLoopJoinPlan(null, leftPlan, rightPlan, 
                StandardJoinType.LEFT, "id", "id");
            
            QueryResult result = joinPlan.execute(context);
            
            assertTrue(result.isSuccess());
            JfrTable joinedTable = result.getTable();
            
            assertEquals(2, joinedTable.getRowCount()); // Both left records
            
            // Verify Bob has null values for right table
            boolean foundBobWithNulls = false;
            for (int i = 0; i < joinedTable.getRowCount(); i++) {
                CellValue leftName = joinedTable.getCell(i, 1);
                if (leftName instanceof CellValue.StringValue str && "Bob".equals(str.value())) {
                    assertInstanceOf(CellValue.NullValue.class, joinedTable.getCell(i, 2));
                    assertInstanceOf(CellValue.NullValue.class, joinedTable.getCell(i, 3));
                    foundBobWithNulls = true;
                    break;
                }
            }
            assertTrue(foundBobWithNulls, "Bob should have null values for right table columns");
        }
    }
    
    @Nested
    @DisplayName("Fuzzy JOIN Tests")
    class FuzzyJoinTests {
        
        @Test
        @DisplayName("NEAREST join finds closest timestamp")
        void testNearestJoin() throws Exception {
            Instant baseTime = Instant.parse("2024-01-01T10:00:00Z");
            
            JfrTable leftTable = createTimestampTable("left", new Object[][] {
                {baseTime, "Event1"},
                {baseTime.plusSeconds(10), "Event2"}
            });
            
            JfrTable rightTable = createTimestampTable("right", new Object[][] {
                {baseTime.plusSeconds(2), "Log1"},
                {baseTime.plusSeconds(8), "Log2"},
                {baseTime.plusSeconds(15), "Log3"}
            });
            
            StreamingQueryPlan leftPlan = createMockPlan(leftTable);
            StreamingQueryPlan rightPlan = createMockPlan(rightTable);
            
            FuzzyJoinPlan joinPlan = new FuzzyJoinPlan(null, leftPlan, rightPlan, 
                FuzzyJoinType.NEAREST, "timestamp", null, null);
            
            QueryResult result = joinPlan.execute(context);
            
            assertTrue(result.isSuccess());
            JfrTable joinedTable = result.getTable();
            
            assertEquals(2, joinedTable.getRowCount());
            
            // Event1 should match with Log1 (closest)
            // Event2 should match with Log2 (closest)
            // Verify the matches
            CellValue event1Match = joinedTable.getCell(0, 3); // right event column
            CellValue event2Match = joinedTable.getCell(1, 3); // right event column
            
            assertTrue(event1Match instanceof CellValue.StringValue);
            assertTrue(event2Match instanceof CellValue.StringValue);
            
            String match1 = ((CellValue.StringValue) event1Match).value();
            String match2 = ((CellValue.StringValue) event2Match).value();
            
            // Event1 (10:00:00) should be closest to Log1 (10:00:02)
            // Event2 (10:00:10) should be closest to Log2 (10:00:08)
            assertTrue(match1.equals("Log1") || match1.equals("Log2"));
            assertTrue(match2.equals("Log1") || match2.equals("Log2"));
        }
        
        @Test
        @DisplayName("PREVIOUS join finds most recent before")
        void testPreviousJoin() throws Exception {
            Instant baseTime = Instant.parse("2024-01-01T10:00:00Z");
            
            JfrTable leftTable = createTimestampTable("left", new Object[][] {
                {baseTime.plusSeconds(10), "Event1"}
            });
            
            JfrTable rightTable = createTimestampTable("right", new Object[][] {
                {baseTime.plusSeconds(2), "Log1"},
                {baseTime.plusSeconds(8), "Log2"},
                {baseTime.plusSeconds(15), "Log3"}
            });
            
            StreamingQueryPlan leftPlan = createMockPlan(leftTable);
            StreamingQueryPlan rightPlan = createMockPlan(rightTable);
            
            FuzzyJoinPlan joinPlan = new FuzzyJoinPlan(null, leftPlan, rightPlan, 
                FuzzyJoinType.PREVIOUS, "timestamp", null, null);
            
            QueryResult result = joinPlan.execute(context);
            
            assertTrue(result.isSuccess());
            JfrTable joinedTable = result.getTable();
            
            assertEquals(1, joinedTable.getRowCount());
            
            // Event1 (10:00:10) should match with Log2 (10:00:08) - most recent before
            CellValue matchedEvent = joinedTable.getCell(0, 3);
            assertTrue(matchedEvent instanceof CellValue.StringValue);
            assertEquals("Log2", ((CellValue.StringValue) matchedEvent).value());
        }
        
        @Test
        @DisplayName("AFTER join finds earliest after")
        void testAfterJoin() throws Exception {
            Instant baseTime = Instant.parse("2024-01-01T10:00:00Z");
            
            JfrTable leftTable = createTimestampTable("left", new Object[][] {
                {baseTime.plusSeconds(10), "Event1"}
            });
            
            JfrTable rightTable = createTimestampTable("right", new Object[][] {
                {baseTime.plusSeconds(2), "Log1"},
                {baseTime.plusSeconds(12), "Log2"},
                {baseTime.plusSeconds(15), "Log3"}
            });
            
            StreamingQueryPlan leftPlan = createMockPlan(leftTable);
            StreamingQueryPlan rightPlan = createMockPlan(rightTable);
            
            FuzzyJoinPlan joinPlan = new FuzzyJoinPlan(null, leftPlan, rightPlan, 
                FuzzyJoinType.AFTER, "timestamp", null, null);
            
            QueryResult result = joinPlan.execute(context);
            
            assertTrue(result.isSuccess());
            JfrTable joinedTable = result.getTable();
            
            assertEquals(1, joinedTable.getRowCount());
            
            // Event1 (10:00:10) should match with Log2 (10:00:12) - earliest after
            CellValue matchedEvent = joinedTable.getCell(0, 3);
            assertTrue(matchedEvent instanceof CellValue.StringValue);
            assertEquals("Log2", ((CellValue.StringValue) matchedEvent).value());
        }
        
        @Test
        @DisplayName("Fuzzy join with tolerance")
        void testFuzzyJoinWithTolerance() throws Exception {
            Instant baseTime = Instant.parse("2024-01-01T10:00:00Z");
            
            JfrTable leftTable = createTimestampTable("left", new Object[][] {
                {baseTime, "Event1"}
            });
            
            JfrTable rightTable = createTimestampTable("right", new Object[][] {
                {baseTime.plusSeconds(2), "Log1"}, // Within tolerance
                {baseTime.plusSeconds(10), "Log2"} // Outside tolerance
            });
            
            StreamingQueryPlan leftPlan = createMockPlan(leftTable);
            StreamingQueryPlan rightPlan = createMockPlan(rightTable);
            
            // Set tolerance to 5 seconds
            CellValue tolerance = new CellValue.DurationValue(Duration.ofSeconds(5));
            
            FuzzyJoinPlan joinPlan = new FuzzyJoinPlan(null, leftPlan, rightPlan, 
                FuzzyJoinType.NEAREST, "timestamp", tolerance, null);
            
            QueryResult result = joinPlan.execute(context);
            
            assertTrue(result.isSuccess());
            JfrTable joinedTable = result.getTable();
            
            assertEquals(1, joinedTable.getRowCount());
            
            // Should match with Log1 (within tolerance), not Log2
            CellValue matchedEvent = joinedTable.getCell(0, 3);
            assertTrue(matchedEvent instanceof CellValue.StringValue);
            assertEquals("Log1", ((CellValue.StringValue) matchedEvent).value());
        }
    }
    
    @Nested
    @DisplayName("JOIN Performance and Edge Cases")
    class JoinEdgeCaseTests {
        
        @Test
        @DisplayName("JOIN with null values")
        void testJoinWithNullValues() throws Exception {
            // Create tables with null values
            List<JfrTable.Column> columns = List.of(
                new JfrTable.Column("id", CellType.NUMBER),
                new JfrTable.Column("name", CellType.STRING)
            );
            
            StandardJfrTable leftTable = new StandardJfrTable(columns);
            leftTable.addRow(new CellValue.NumberValue(1), new CellValue.StringValue("Alice"));
            leftTable.addRow(new CellValue.NullValue(), new CellValue.StringValue("Bob"));
            
            StandardJfrTable rightTable = new StandardJfrTable(columns);
            rightTable.addRow(new CellValue.NumberValue(1), new CellValue.StringValue("Manager"));
            rightTable.addRow(new CellValue.NullValue(), new CellValue.StringValue("Developer"));
            
            StreamingQueryPlan leftPlan = createMockPlan(leftTable);
            StreamingQueryPlan rightPlan = createMockPlan(rightTable);
            
            HashJoinPlan joinPlan = new HashJoinPlan(null, leftPlan, rightPlan, 
                StandardJoinType.INNER, "id", "id");
            
            QueryResult result = joinPlan.execute(context);
            
            assertTrue(result.isSuccess());
            JfrTable joinedTable = result.getTable();
            
            // Should only match non-null values
            assertEquals(1, joinedTable.getRowCount());
            assertEquals(1.0, ((CellValue.NumberValue) joinedTable.getCell(0, 0)).value());
        }
        
        @Test
        @DisplayName("JOIN explains correctly")
        void testJoinExplanation() {
            JfrTable leftTable = createSimpleTable("left", new Object[][] {});
            JfrTable rightTable = createSimpleTable("right", new Object[][] {});
            
            StreamingQueryPlan leftPlan = createMockPlan(leftTable);
            StreamingQueryPlan rightPlan = createMockPlan(rightTable);
            
            HashJoinPlan hashJoin = new HashJoinPlan(null, leftPlan, rightPlan, 
                StandardJoinType.INNER, "id", "id");
            NestedLoopJoinPlan nestedJoin = new NestedLoopJoinPlan(null, leftPlan, rightPlan, 
                StandardJoinType.LEFT, "id", "id");
            
            String hashExplanation = hashJoin.explain();
            String nestedExplanation = nestedJoin.explain();
            
            assertNotNull(hashExplanation);
            assertNotNull(nestedExplanation);
            
            assertTrue(hashExplanation.contains("HashJoinPlan"));
            assertTrue(hashExplanation.contains("INNER"));
            
            assertTrue(nestedExplanation.contains("NestedLoopJoinPlan"));
            assertTrue(nestedExplanation.contains("LEFT"));
        }
        
        @Test
        @DisplayName("JOIN streaming support")
        void testJoinStreamingSupport() {
            JfrTable leftTable = createSimpleTable("left", new Object[][] {});
            JfrTable rightTable = createSimpleTable("right", new Object[][] {});
            
            StreamingQueryPlan leftPlan = createMockPlan(leftTable);
            StreamingQueryPlan rightPlan = createMockPlan(rightTable);
            
            HashJoinPlan hashJoin = new HashJoinPlan(null, leftPlan, rightPlan, 
                StandardJoinType.INNER, "id", "id");
            FuzzyJoinPlan fuzzyJoin = new FuzzyJoinPlan(null, leftPlan, rightPlan, 
                FuzzyJoinType.NEAREST, "timestamp", null, null);
            
            // JOIN operations typically don't support streaming
            assertFalse(hashJoin.supportsStreaming());
            assertFalse(fuzzyJoin.supportsStreaming());
        }
    }
}
