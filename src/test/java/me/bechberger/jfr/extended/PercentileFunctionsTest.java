package me.bechberger.jfr.extended;

import me.bechberger.jfr.extended.ast.ASTNodes.*;
import me.bechberger.jfr.extended.evaluator.FunctionRegistry;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;
import java.util.stream.Stream;

import static me.bechberger.jfr.extended.ASTBuilder.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive test class for all percentile and array function parsing and evaluation.
 * 
 * This class tests:
 * 1. Basic PERCENTILE function for aggregation
 * 2. Array manipulation functions (HEAD, TAIL, SLICE, etc.)
 * 3. Percentile selection functions (P90SELECT, P95SELECT, P99SELECT, etc.)
 * 4. Combined usage of these functions
 * 5. Function registry integration
 * 
 * The tests use parameterized tests where possible and builder patterns for AST construction
 * to improve test readability and maintainability.
 */
@DisplayName("Percentile and Array Functions Tests")
public class PercentileFunctionsTest {
    
    private Parser createParser(String query) throws Exception {
        Lexer lexer = new Lexer(query);
        List<Token> tokens = lexer.tokenize();
        return new Parser(tokens, query);
    }
    
    @Nested
    @DisplayName("Basic Percentile Function Tests")
    class BasicPercentileTests {
        
        @Test
        @DisplayName("Generic PERCENTILE function should parse correctly")
        public void testGenericPercentileFunction() throws Exception {
            Parser parser = createParser("@SELECT PERCENTILE(95.0, duration) FROM ExecutionSample");
            ProgramNode program = parser.parse();
            
            ProgramNode expected = program(
                query(
                    select(percentile(95.0, identifier("duration"))),
                    from(source("ExecutionSample"))
                ).extended()
            );
            
            assertTrue(astEquals(expected, program), "Should parse generic PERCENTILE function correctly");
        }
        
        @ParameterizedTest(name = "PERCENTILE({0}, {1}) should parse correctly")
        @CsvSource({
            "50.0, duration",
            "90.0, latency", 
            "95.0, response_time",
            "99.0, cpu_usage",
            "99.9, memory_usage"
        })
        public void testGenericPercentileWithDifferentValues(String percentile, String field) throws Exception {
            String query = "@SELECT PERCENTILE(" + percentile + ", " + field + ") FROM Events";
            Parser parser = createParser(query);
            ProgramNode program = parser.parse();

            ProgramNode expected = program(
                query(
                    select(percentile(Double.parseDouble(percentile), identifier(field))),
                    from(source("Events"))
                ).extended()
            );
            assertTrue(astEquals(expected, program), "AST should match for PERCENTILE(" + percentile + ", " + field + ")");
        }
        
        @Test
        @DisplayName("PERCENTILE function with alias should parse correctly")
        public void testPercentileWithAlias() throws Exception {
            Parser parser = createParser("@SELECT PERCENTILE(90.0, duration) AS p90_duration FROM ExecutionSample");
            ProgramNode program = parser.parse();
            
            ProgramNode expected = program(
                query(
                    select(percentileWithAlias("p90_duration", 90.0, identifier("duration"))),
                    from(source("ExecutionSample"))
                ).extended()
            );
            
            assertTrue(astEquals(expected, program), "Should parse PERCENTILE with alias correctly");
        }
        
        @Test
        @DisplayName("PERCENTILE functions in complex query should parse correctly")
        public void testPercentileInComplexQuery() throws Exception {
            Parser parser = createParser("""
                @SELECT 
                    thread,
                    PERCENTILE(90.0, duration) AS p90_duration,
                    PERCENTILE(95.0, duration) AS p95_duration,
                    PERCENTILE(99.0, duration) AS p99_duration
                FROM ExecutionSample 
                GROUP BY thread
                """);
            ProgramNode program = parser.parse();

            ProgramNode expected = program(
                query(
                    select(
                        selectItem(identifier("thread")),
                        percentileWithAlias("p90_duration", 90.0, identifier("duration")),
                        percentileWithAlias("p95_duration", 95.0, identifier("duration")),
                        percentileWithAlias("p99_duration", 99.0, identifier("duration"))
                    ),
                    from(source("ExecutionSample"))
                ).groupBy(groupBy(identifier("thread"))).extended()
            );
            assertTrue(astEquals(expected, program), "AST should match for complex percentile query");
        }
    }
    
    @Nested
    @DisplayName("Array Manipulation Function Tests")
    class ArrayManipulationTests {
        
        @ParameterizedTest(name = "{0}({1}, {2}) should parse correctly")
        @CsvSource({
            "HEAD, events, 10",
            "TAIL, events, 5"
        })
        public void testHeadAndTailFunctions(String functionName, String field, String count) throws Exception {
            String query = String.format("@SELECT %s(%s, %s) FROM LogData", functionName, field, count);
            Parser parser = createParser(query);
            ProgramNode program = parser.parse();

            ProgramNode expected = program(
                query(
                    select(function(functionName, identifier(field), numberLiteral(Double.parseDouble(count)))),
                    from(source("LogData"))
                ).extended()
            );
            assertTrue(astEquals(expected, program), "AST should match for " + functionName + "(" + field + ", " + count + ")");
        }

        @Test
        @DisplayName("SLICE function should parse correctly")
        public void testSliceFunction() throws Exception {
            Parser parser = createParser("@SELECT SLICE(events, 10, 20) FROM LogData");
            ProgramNode program = parser.parse();

            ProgramNode expected = program(
                query(
                    select(function("SLICE", identifier("events"), numberLiteral(10.0), numberLiteral(20.0))),
                    from(source("LogData"))
                ).extended()
            );
            assertTrue(astEquals(expected, program), "AST should match for SLICE(events, 10, 20)");
        }
        
        @ParameterizedTest(name = "{0} function should parse correctly")
        @ValueSource(strings = {"HEAD", "TAIL", "SLICE", "FIRST", "LAST", "UNIQUE", "LIST"})
        public void testDataAccessFunctions(String functionName) throws Exception {
            String query = "@SELECT " + functionName + "(events) FROM LogData";
            if (functionName.equals("SLICE")) {
                query = "@SELECT " + functionName + "(events, 0, 10) FROM LogData";
            } else if (functionName.equals("HEAD") || functionName.equals("TAIL")) {
                query = "@SELECT " + functionName + "(events, 5) FROM LogData";
            }
            
            Parser parser = createParser(query);
            ProgramNode program = parser.parse();
            
            assertNotNull(program, "Program should not be null");
            
            // Build expected AST instead of manual checking
            ProgramNode expected = program(
                query(
                    select(function(functionName, buildFunctionArgs(functionName))),
                    from(source("LogData"))
                ).extended()
            );
            assertTrue(astEquals(expected, program), "AST should match for " + functionName + " function");
        }
    }
    
    @Nested
    @DisplayName("Percentile Selection Function Tests")
    class PercentileSelectionTests {
        
        @ParameterizedTest(name = "{0} with percentile {1} should parse correctly")
        @CsvSource({
            "PERCENTILE_SELECT, 95.0",
            "P90SELECT, 90.0",
            "P95SELECT, 95.0",
            "P99SELECT, 99.0",
            "P999SELECT, 99.9"
        })
        public void testPercentileSelectFunctionsParameterized(String functionName, double expectedPercentile) throws Exception {
            String query;
            if (functionName.equals("PERCENTILE_SELECT")) {
                query = "@SELECT * FROM ExecutionSample WHERE gcId IN " + functionName + "(" + expectedPercentile + ", GarbageCollection, id, duration)";
            } else {
                query = "@SELECT * FROM ExecutionSample WHERE gcId IN " + functionName + "(GarbageCollection, id, duration)";
            }
            Parser parser = createParser(query);
            ProgramNode program = parser.parse();

            // Build expected AST instead of manual checking
            ProgramNode expected = program(
                query(
                    selectAll(),
                    from(source("ExecutionSample"))
                ).where(where(condition(
                    in(identifier("gcId"), buildPercentileSelectionNode(functionName, expectedPercentile))
                ))).extended()
            );
            assertTrue(astEquals(expected, program), "AST should match for " + functionName);
        }
    }
    
    @Nested
    @DisplayName("Combined Function Usage Tests")
    class CombinedFunctionTests {
        
        @Test
        @DisplayName("Combined percentile and array functions should parse correctly")
        public void testCombinedPercentileAndArrayFunctions() throws Exception {
            Parser parser = createParser("""
                @SELECT 
                    HEAD(P99SELECT(GarbageCollection, id, duration), 5) AS top_gc_ids,
                    PERCENTILE(95.0, latency) AS p95_latency,
                    TAIL(event_ids, 10) AS recent_events
                FROM PerformanceData
                """);
            ProgramNode program = parser.parse();
            
            assertNotNull(program, "Program should not be null");
            QueryNode query = (QueryNode) program.statements().get(0);
            assertEquals(3, query.select().items().size(), "Should have 3 select items");
            
            // Build expected AST instead of manual checking
            ProgramNode expected = program(
                query(
                    select(
                        functionWithAlias("HEAD", "top_gc_ids", 
                            p99select("GarbageCollection", "id", identifier("duration")),
                            numberLiteral(5.0)
                        ),
                        percentileWithAlias("p95_latency", 95.0, identifier("latency")),
                        functionWithAlias("TAIL", "recent_events", identifier("event_ids"), numberLiteral(10.0))
                    ),
                    from(source("PerformanceData"))
                ).extended()
            );
            assertTrue(astEquals(expected, program), "AST should match for combined functions");
        }
        
        @Test
        @DisplayName("Percentile function in WHERE clause should parse correctly")
        public void testPercentileInWhere() throws Exception {
            Parser parser = createParser("@SELECT * FROM Events WHERE latency > PERCENTILE(90.0, baseline_latency)");
            ProgramNode program = parser.parse();

            ProgramNode expected = program(
                query(
                    selectAll(),
                    from(source("Events"))
                ).where(
                    where(
                        condition(
                            binary(
                                identifier("latency"),
                                BinaryOperator.GREATER_THAN,
                                percentile(90.0, identifier("baseline_latency"))
                            )
                        )
                    )
                ).extended()
            );
            assertTrue(astEquals(expected, program), "AST should match for percentile in WHERE");
        }
        
        @Test
        @DisplayName("Multiple percentile selections with list operations should parse correctly")
        public void testMultiplePercentileSelectionsWithListOperations() throws Exception {
            String query = """
                @SELECT 
                    HEAD(P99SELECT(GarbageCollection, id, duration), 3) AS top_slow_gcs,
                    TAIL(P90SELECT(ExecutionSample, threadId, cpu_time), 5) AS bottom_cpu_intensive
                FROM SystemMetrics
                """;
            
            Parser parser = createParser(query);
            ProgramNode program = parser.parse();
            
            ProgramNode expected = program(
                query(
                    select(
                        functionWithAlias("HEAD", "top_slow_gcs",
                            p99select("GarbageCollection", "id", identifier("duration")),
                            numberLiteral(3.0)
                        ),
                        functionWithAlias("TAIL", "bottom_cpu_intensive",
                            p90select("ExecutionSample", "threadId", identifier("cpu_time")),
                            numberLiteral(5.0)
                        )
                    ),
                    from(source("SystemMetrics"))
                )
                .extended()
            );
            
            assertTrue(astEquals(expected, program), "Should parse multiple percentile selections with list operations");
        }
    }
    
    @Nested
    @DisplayName("Function Registry Tests")
    class FunctionRegistryTests {
        
        @Test
        @DisplayName("Percentile function should be registered")
        public void testPercentileFunctionRegistration() {
            FunctionRegistry registry = FunctionRegistry.getInstance();
            
            // Test generic PERCENTILE function
            assertTrue(registry.isFunction("PERCENTILE"), "PERCENTILE should be registered");
            assertTrue(registry.isFunction("PERCENTILE"), "PERCENTILE function should be available");
            
            // Test data access functions
            assertTrue(registry.isFunction("HEAD"), "HEAD should be registered");
            assertTrue(registry.isFunction("TAIL"), "TAIL should be registered");
            assertTrue(registry.isFunction("SLICE"), "SLICE should be registered");
            assertTrue(registry.isFunction("FIRST"), "FIRST should be registered");
            assertTrue(registry.isFunction("LAST"), "LAST should be registered");
            assertTrue(registry.isFunction("UNIQUE"), "UNIQUE should be registered");
            assertTrue(registry.isFunction("LIST"), "LIST should be registered");
        }
        
        @ParameterizedTest(name = "{0} function should be registered")
        @ValueSource(strings = {"PERCENTILE_SELECT", "P90SELECT", "P95SELECT", "P99SELECT", "P999SELECT"})
        public void testPercentileSelectionFunctionRegistration(String functionName) {
            FunctionRegistry registry = FunctionRegistry.getInstance();
            assertTrue(registry.isFunction(functionName), functionName + " should be registered");
        }
    }
    
    @Nested
    @DisplayName("Comprehensive Parameterized Tests")
    class ComprehensiveParameterizedTests {
        
        static Stream<Arguments> percentileAndArrayFunctionCases() {
            return Stream.of(
                Arguments.of(
                    "@SELECT PERCENTILE(95.0, duration) FROM ExecutionSample",
                    (ASTBuilderSupplier) () -> program(
                        query(
                            select(percentile(95.0, identifier("duration"))),
                            from(source("ExecutionSample"))
                        ).extended()
                    )
                ),
                Arguments.of(
                    "@SELECT PERCENTILE(90.0, duration) AS p90_duration FROM ExecutionSample",
                    (ASTBuilderSupplier) () -> program(
                        query(
                            select(percentileWithAlias("p90_duration", 90.0, identifier("duration"))),
                            from(source("ExecutionSample"))
                        ).extended()
                    )
                ),
                Arguments.of(
                    "@SELECT HEAD(events, 10) FROM LogData",
                    (ASTBuilderSupplier) () -> program(
                        query(
                            select(function("HEAD", identifier("events"), numberLiteral(10.0))),
                            from(source("LogData"))
                        ).extended()
                    )
                ),
                Arguments.of(
                    "@SELECT SLICE(events, 10, 20) FROM LogData",
                    (ASTBuilderSupplier) () -> program(
                        query(
                            select(function("SLICE", identifier("events"), numberLiteral(10.0), numberLiteral(20.0))),
                            from(source("LogData"))
                        ).extended()
                    )
                ),
                Arguments.of(
                    "@SELECT * FROM ExecutionSample WHERE gcId IN P99SELECT(GarbageCollection, id, duration)",
                    (ASTBuilderSupplier) () -> program(
                        query(
                            selectAll(),
                            from(source("ExecutionSample"))
                        ).where(where(condition(
                            in(
                                identifier("gcId"),
                                p99select("GarbageCollection", "id", identifier("duration"))
                            )
                        ))).extended()
                    )
                ),
                Arguments.of(
                    "@SELECT * FROM LogEvents WHERE id IN SLICE(P95SELECT(LogEvents, id, latency), 10, 20)",
                    (ASTBuilderSupplier) () -> program(
                        query(
                            selectAll(),
                            from(source("LogEvents"))
                        ).where(where(condition(
                            in(
                                identifier("id"),
                                slice(
                                    p95select("LogEvents", "id", identifier("latency"))
                                ).start(10).end(20).build()
                            )
                        ))).extended()
                    )
                ),
                Arguments.of(
                    "@SELECT * FROM ExecutionSample WHERE gc_id IN HEAD(P99SELECT(GarbageCollection, id, duration), 5)",
                    (ASTBuilderSupplier) () -> program(
                        query(
                            selectAll(),
                            from(source("ExecutionSample"))
                        ).where(where(condition(
                            in(
                                identifier("gc_id"),
                                head(
                                    p99select("GarbageCollection", "id", identifier("duration"))
                                ).count(5).build()
                            )
                        ))).extended()
                    )
                )
            );
        }
    
        @ParameterizedTest(name = "Query {index}: {0}")
        @MethodSource("percentileAndArrayFunctionCases")
        void testPercentileAndArrayFunctionParsing(String query, ASTBuilderSupplier expectedAstSupplier) throws Exception {
            Parser parser = createParser(query);
            ProgramNode program = parser.parse();
            ProgramNode expected = expectedAstSupplier.get();
            assertTrue(astEquals(expected, program), "AST should match for query: " + query);
        }
    }
    
    // Helper methods
    
    private ExpressionNode[] buildFunctionArgs(String functionName) {
        return switch (functionName) {
            case "SLICE" -> new ExpressionNode[]{identifier("events"), numberLiteral(0.0), numberLiteral(10.0)};
            case "HEAD", "TAIL" -> new ExpressionNode[]{identifier("events"), numberLiteral(5.0)};
            default -> new ExpressionNode[]{identifier("events")};
        };
    }
    
    private PercentileSelectionNode buildPercentileSelectionNode(String functionName, double expectedPercentile) {
        return switch (functionName) {
            case "PERCENTILE_SELECT" -> percentileSelect(expectedPercentile, "GarbageCollection", "id", "duration");
            case "P90SELECT" -> p90select("GarbageCollection", "id", identifier("duration"));
            case "P95SELECT" -> p95select("GarbageCollection", "id", identifier("duration"));
            case "P99SELECT" -> p99select("GarbageCollection", "id", identifier("duration"));
            case "P999SELECT" -> p999select("GarbageCollection", "id", identifier("duration"));
            default -> throw new IllegalArgumentException("Unknown function: " + functionName);
        };
    }
    
    // Helper functional interface for AST builder lambdas
    @FunctionalInterface
    interface ASTBuilderSupplier {
        ProgramNode get();
    }
}
