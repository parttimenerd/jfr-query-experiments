package me.bechberger.jfr.extended;

import me.bechberger.jfr.extended.ast.ASTNodes.*;
import me.bechberger.jfr.extended.ast.ASTNode;
import me.bechberger.jfr.extended.ast.ASTVisitor;
import me.bechberger.jfr.extended.table.CellValue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import me.bechberger.jfr.extended.ast.Location;

/**
 * A tiny DSL for building expected AST structures in tests
 */
public class ASTBuilder {
    private static Location location(int line, int column) {
        return new Location(line, column);
    }

    
    // Factory methods for creating nodes
    public static ProgramNode program(StatementNode... statements) {
        return new ProgramNode(Arrays.asList(statements), location(1, 1));
    }
    
    // Overloaded method to handle QueryBuilder objects
    public static ProgramNode program(Object... statements) {
        List<StatementNode> stmtNodes = new ArrayList<>();
        for (Object stmt : statements) {
            if (stmt instanceof StatementNode) {
                stmtNodes.add((StatementNode) stmt);
            } else if (stmt instanceof QueryBuilder) {
                stmtNodes.add(((QueryBuilder) stmt).build());
            } else {
                throw new IllegalArgumentException("Expected StatementNode or QueryBuilder, got: " + stmt.getClass());
            }
        }
        return new ProgramNode(stmtNodes, location(1, 1));
    }
    
    /**
     * Builder pattern for creating QueryNode instances
     */
    public static class QueryBuilder {
        private boolean extended = false;
        private List<String> columns = List.of();
        private List<FormatterNode> formatters = List.of();
        private SelectNode select;
        private FromNode from;
        private WhereNode where;
        private GroupByNode groupBy;
        private HavingNode having;
        private OrderByNode orderBy;
        private LimitNode limit;
        
        private QueryBuilder(SelectNode select, FromNode from) {
            this.select = select;
            this.from = from;
        }
        
        public QueryBuilder extended() {
            this.extended = true;
            return this;
        }
        
        public QueryBuilder extended(boolean extended) {
            this.extended = extended;
            return this;
        }
        
        public QueryBuilder columns(List<String> columns) {
            this.columns = columns;
            return this;
        }
        
        public QueryBuilder formatters(List<FormatterNode> formatters) {
            this.formatters = formatters;
            return this;
        }
        
        public QueryBuilder where(WhereNode where) {
            this.where = where;
            return this;
        }
        
        public QueryBuilder groupBy(GroupByNode groupBy) {
            this.groupBy = groupBy;
            return this;
        }
        
        public QueryBuilder having(HavingNode having) {
            this.having = having;
            return this;
        }
        
        public QueryBuilder orderBy(OrderByNode orderBy) {
            this.orderBy = orderBy;
            return this;
        }
        
        public QueryBuilder limit(LimitNode limit) {
            this.limit = limit;
            return this;
        }
        
        public QueryNode build() {
            return new QueryNode(extended, columns, formatters, select, from, where, groupBy, having, orderBy, limit, location(1, 1));
        }
    }
    
    // Factory method to start building a raw query (without extended features)
    public static QueryBuilder queryBuilder(SelectNode select, FromNode from) {
        return new QueryBuilder(select, from);
    }
    
    // Factory method for extended queries (@ prefix queries) - automatically extended by default
    public static QueryBuilder query(SelectNode select, FromNode from) {
        return new QueryBuilder(select, from).extended();
    }
    
    // Convenience methods for common query patterns
    public static QueryNode simpleQuery(SelectNode select, FromNode from) {
        return queryBuilder(select, from).build();
    }
    
    public static SelectNode selectAll() {
        return new SelectNode(List.of(), true, location(1, 1));
    }
    
    public static SelectNode select(ExpressionNode... expressions) {
        List<SelectItemNode> items = Arrays.stream(expressions)
                .map(expr -> selectItem(expr))
                .toList();
        return new SelectNode(items, false, location(1, 1));
    }
    
    public static SelectNode select(SelectItemNode... items) {
        return new SelectNode(Arrays.asList(items), false, location(1, 1));
    }
    
    public static FromNode from(SourceNodeBase... sources) {
        return new FromNode(Arrays.asList(sources), location(1, 1));
    }
    
    public static SourceNode source(String name) {
        return new SourceNode(name, null, location(1, 1));
    }
    
    public static SourceNode source(String name, String alias) {
        return new SourceNode(name, alias, location(1, 1));
    }
    
    public static SubquerySourceNode subquery(QueryNode query, String alias) {
        return new SubquerySourceNode(query, alias, location(1, 1));
    }
    
    public static SubquerySourceNode subquery(QueryNode query) {
        return new SubquerySourceNode(query, null, location(1, 1));
    }
    
    public static SubquerySourceNode subquery(RawJfrQueryNode rawQuery, String alias) {
        return new SubquerySourceNode(rawQuery, alias, location(1, 1));
    }
    
    public static SubquerySourceNode subquery(RawJfrQueryNode rawQuery) {
        return new SubquerySourceNode(rawQuery, null, location(1, 1));
    }
    
    // Overloaded methods to accept QueryBuilder
    public static SubquerySourceNode subquery(QueryBuilder queryBuilder, String alias) {
        return new SubquerySourceNode(queryBuilder.build(), alias, location(1, 1));
    }
    
    public static SubquerySourceNode subquery(QueryBuilder queryBuilder) {
        return new SubquerySourceNode(queryBuilder.build(), null, location(1, 1));
    }
    
    /**
     * Factory pattern for creating FuzzyJoinSourceNode instances
     */
    public static class FuzzyJoinBuilder {
        private final String source;
        private final String joinField;
        private String alias;
        private FuzzyJoinType joinType = FuzzyJoinType.NEAREST;
        private ExpressionNode tolerance;
        private ExpressionNode threshold;
        
        private FuzzyJoinBuilder(String source, String joinField) {
            this.source = source;
            this.joinField = joinField;
        }
        
        public FuzzyJoinBuilder alias(String alias) {
            this.alias = alias;
            return this;
        }
        
        public FuzzyJoinBuilder with(FuzzyJoinType joinType) {
            this.joinType = joinType;
            return this;
        }
        
        public FuzzyJoinBuilder withNearest() {
            this.joinType = FuzzyJoinType.NEAREST;
            return this;
        }
        
        public FuzzyJoinBuilder withPrevious() {
            this.joinType = FuzzyJoinType.PREVIOUS;
            return this;
        }
        
        public FuzzyJoinBuilder withAfter() {
            this.joinType = FuzzyJoinType.AFTER;
            return this;
        }
        
        public FuzzyJoinBuilder tolerance(ExpressionNode tolerance) {
            this.tolerance = tolerance;
            return this;
        }
        
        public FuzzyJoinBuilder tolerance(String duration) {
            this.tolerance = durationLiteral(duration);
            return this;
        }
        
        public FuzzyJoinBuilder threshold(ExpressionNode threshold) {
            this.threshold = threshold;
            return this;
        }
        
        public FuzzyJoinBuilder threshold(String duration) {
            this.threshold = durationLiteral(duration);
            return this;
        }
        
        public FuzzyJoinSourceNode build() {
            return new FuzzyJoinSourceNode(source, alias, joinType, joinField, tolerance, threshold, location(1, 1));
        }
    }
    
    // Fuzzy join factory method
    public static FuzzyJoinBuilder fuzzyJoin(String source, String joinField) {
        return new FuzzyJoinBuilder(source, joinField);
    }
    
    // Backward compatibility methods for existing fuzzy join overloads
    public static FuzzyJoinSourceNode fuzzyJoin(String source, String alias, String joinField) {
        return fuzzyJoin(source, joinField).alias(alias).build();
    }
    
    public static FuzzyJoinSourceNode fuzzyJoin(String source, String joinField, FuzzyJoinType joinType) {
        return fuzzyJoin(source, joinField).with(joinType).build();
    }
    
    public static FuzzyJoinSourceNode fuzzyJoin(String source, String alias, String joinField, FuzzyJoinType joinType) {
        return fuzzyJoin(source, joinField).alias(alias).with(joinType).build();
    }
    
    public static FuzzyJoinSourceNode fuzzyJoin(String source, String joinField, FuzzyJoinType joinType, ExpressionNode tolerance) {
        return fuzzyJoin(source, joinField).with(joinType).tolerance(tolerance).build();
    }
    
    public static FuzzyJoinSourceNode fuzzyJoin(String source, String alias, String joinField, FuzzyJoinType joinType, ExpressionNode tolerance) {
        return fuzzyJoin(source, joinField).alias(alias).with(joinType).tolerance(tolerance).build();
    }
    
    public static WhereNode where(ConditionNode condition) {
        return new WhereNode(condition, location(1, 1));
    }
    
    public static GroupByNode groupBy(ExpressionNode... fields) {
        return new GroupByNode(Arrays.asList(fields), location(1, 1));
    }
    
    public static HavingNode having(ConditionNode condition) {
        return new HavingNode(condition, location(1, 1));
    }
    
    public static OrderByNode orderBy(OrderFieldNode... fields) {
        return new OrderByNode(Arrays.asList(fields), location(1, 1));
    }
    
    public static OrderFieldNode orderField(ExpressionNode field, SortOrder order) {
        return new OrderFieldNode(field, order, location(1, 1));
    }
    
    public static LimitNode limit(int count) {
        return new LimitNode(count, location(1, 1));
    }
    
    public static AssignmentNode assignment(String variable, QueryNode query) {
        return new AssignmentNode(variable, query, location(1, 1));
    }
    
    public static ViewDefinitionNode view(String name, QueryNode query) {
        return new ViewDefinitionNode(name, query, location(1, 1));
    }
    
    public static ShowEventsNode showEvents() {
        return new ShowEventsNode(location(1, 1));
    }
    
    public static ShowFieldsNode showFields(String eventType) {
        return new ShowFieldsNode(eventType, location(1, 1));
    }
    
    // Expression builders
    public static IdentifierNode identifier(String name) {
        return new IdentifierNode(name, location(1, 1));
    }
    
    // SELECT item builders
    public static SelectItemNode selectItem(ExpressionNode expression) {
        return new SelectItemNode(expression, null, location(1, 1));
    }
    
    public static SelectItemNode selectItem(ExpressionNode expression, String alias) {
        return new SelectItemNode(expression, alias, location(1, 1));
    }
    
    // Helper for identifier with alias in SELECT
    public static SelectItemNode identifierWithAlias(String name, String alias) {
        return selectItem(identifier(name), alias);
    }
    
    // Helper for function with alias in SELECT  
    public static SelectItemNode functionWithAlias(String name, String alias, ExpressionNode... args) {
        return selectItem(function(name, args), alias);
    }
    
    // Helper for percentile with alias in SELECT
    public static SelectItemNode percentileWithAlias(String alias, double percentile, ExpressionNode field) {
        return selectItem(percentile(percentile, field), alias);
    }
    
    public static LiteralNode literal(CellValue value) {
        return new LiteralNode(value, location(1, 1));
    }
    
    public static LiteralNode stringLiteral(String value) {
        return new LiteralNode(new CellValue.StringValue(value), location(1, 1));
    }
    
    public static LiteralNode numberLiteral(double value) {
        return new LiteralNode(new CellValue.NumberValue(value), location(1, 1));
    }
    
    public static LiteralNode numberLiteral(int value) {
        return new LiteralNode(new CellValue.NumberValue((double) value), location(1, 1));
    }
    
    public static LiteralNode durationLiteral(String value) {
        try {
            long nanoseconds = me.bechberger.jfr.util.Utils.parseTimespan(value);
            java.time.Duration duration = java.time.Duration.ofNanos(nanoseconds);
            return new LiteralNode(new CellValue.DurationValue(duration), location(1, 1));
        } catch (Exception e) {
            return new LiteralNode(new CellValue.DurationValue(java.time.Duration.ZERO), location(1, 1));
        }
    }
    
    public static LiteralNode memorySizeLiteral(String value) {
        try {
            long bytes = me.bechberger.jfr.util.Utils.parseMemorySize(value);
            return new LiteralNode(new CellValue.MemorySizeValue(bytes), location(1, 1));
        } catch (Exception e) {
            return new LiteralNode(new CellValue.MemorySizeValue(0L), location(1, 1));
        }
    }
    
    public static LiteralNode timestampLiteral(String value) {
        try {
            java.time.Instant instant = java.time.Instant.parse(value);
            return new LiteralNode(new CellValue.TimestampValue(instant), location(1, 1));
        } catch (Exception e) {
            return new LiteralNode(new CellValue.TimestampValue(java.time.Instant.EPOCH), location(1, 1));
        }
    }
    
    public static LiteralNode booleanLiteral(boolean value) {
        return new LiteralNode(new CellValue.BooleanValue(value), location(1, 1));
    }
    
    public static LiteralNode numberLiteral(long value) {
        return new LiteralNode(new CellValue.NumberValue(value), location(1, 1));
    }
    
    public static BinaryExpressionNode binary(ExpressionNode left, BinaryOperator op, ExpressionNode right) {
        return new BinaryExpressionNode(left, op, right, location(1, 1));
    }
    
    public static UnaryExpressionNode unary(UnaryOperator op, ExpressionNode expr) {
        return new UnaryExpressionNode(op, expr, location(1, 1));
    }
    
    public static FunctionCallNode function(String name, ExpressionNode... args) {
        return new FunctionCallNode(name, Arrays.asList(args), false, location(1, 1));
    }
    
    public static PercentileFunctionNode percentile(double percentile, ExpressionNode field) {
        return new PercentileFunctionNode("PERCENTILE", field, percentile, null, location(1, 1));
    }
    
    public static PercentileSelectionNode percentileSelection(String functionName, String tableName, String idField, ExpressionNode valueExpression, double percentile) {
        return new PercentileSelectionNode(functionName, tableName, idField, valueExpression, percentile, location(1, 1));
    }
    
    // Convenience methods for specific percentile selection functions
    public static PercentileSelectionNode p90select(String tableName, String idField, ExpressionNode valueExpression) {
        return percentileSelection("P90SELECT", tableName, idField, valueExpression, 90.0);
    }
    
    public static PercentileSelectionNode p95select(String tableName, String idField, ExpressionNode valueExpression) {
        return percentileSelection("P95SELECT", tableName, idField, valueExpression, 95.0);
    }
    
    public static PercentileSelectionNode p99select(String tableName, String idField, ExpressionNode valueExpression) {
        return percentileSelection("P99SELECT", tableName, idField, valueExpression, 99.0);
    }
    
    public static PercentileSelectionNode p999select(String tableName, String idField, ExpressionNode valueExpression) {
        return percentileSelection("P999SELECT", tableName, idField, valueExpression, 99.9);
    }
    
    public static FieldAccessNode fieldAccess(String object, String field) {
        return new FieldAccessNode(object, field, location(1, 1));
    }
    
    public static NestedQueryNode nestedQuery(String jfrQuery) {
        return new NestedQueryNode(jfrQuery, location(1, 1));
    }
    
    // Condition builders
    public static ExpressionConditionNode condition(ExpressionNode expr) {
        return new ExpressionConditionNode(expr, location(1, 1));
    }
    
    public static VariableDeclarationNode varDecl(String name, ExpressionNode value) {
        return new VariableDeclarationNode(name, value, location(1, 1));
    }
    
    public static StatementNode variableDeclaration(String name, ExpressionNode value) {
        final VariableDeclarationNode decl = varDecl(name, value);
        return new StatementNode() {
            @Override
            public <T> T accept(ASTVisitor<T> visitor) {
                return decl.accept(visitor);
            }
            
            @Override
            public int getLine() {
                return decl.getLine();
            }
            
            @Override
            public int getColumn() {
                return decl.getColumn();
            }
            
            @Override
            public String format() {
                return decl.format();
            }
        };
    }
    
    // Operator shortcuts
    public static BinaryExpressionNode add(ExpressionNode left, ExpressionNode right) {
        return binary(left, BinaryOperator.ADD, right);
    }
    
    public static BinaryExpressionNode subtract(ExpressionNode left, ExpressionNode right) {
        return binary(left, BinaryOperator.SUBTRACT, right);
    }
    
    public static BinaryExpressionNode multiply(ExpressionNode left, ExpressionNode right) {
        return binary(left, BinaryOperator.MULTIPLY, right);
    }
    
    public static BinaryExpressionNode divide(ExpressionNode left, ExpressionNode right) {
        return binary(left, BinaryOperator.DIVIDE, right);
    }
    
    public static BinaryExpressionNode equals(ExpressionNode left, ExpressionNode right) {
        return binary(left, BinaryOperator.EQUALS, right);
    }
    
    public static BinaryExpressionNode notEquals(ExpressionNode left, ExpressionNode right) {
        return binary(left, BinaryOperator.NOT_EQUALS, right);
    }
    
    public static BinaryExpressionNode lessThan(ExpressionNode left, ExpressionNode right) {
        return binary(left, BinaryOperator.LESS_THAN, right);
    }
    
    public static BinaryExpressionNode greaterThan(ExpressionNode left, ExpressionNode right) {
        return binary(left, BinaryOperator.GREATER_THAN, right);
    }
    
    public static BinaryExpressionNode lessEqual(ExpressionNode left, ExpressionNode right) {
        return binary(left, BinaryOperator.LESS_EQUAL, right);
    }
    
    public static BinaryExpressionNode greaterEqual(ExpressionNode left, ExpressionNode right) {
        return binary(left, BinaryOperator.GREATER_EQUAL, right);
    }
    
    public static BinaryExpressionNode like(ExpressionNode left, ExpressionNode right) {
        return binary(left, BinaryOperator.LIKE, right);
    }
    
    public static BinaryExpressionNode in(ExpressionNode left, ExpressionNode right) {
        return binary(left, BinaryOperator.IN, right);
    }
    
    public static BinaryExpressionNode and(ExpressionNode left, ExpressionNode right) {
        return binary(left, BinaryOperator.AND, right);
    }
    
    public static BinaryExpressionNode or(ExpressionNode left, ExpressionNode right) {
        return binary(left, BinaryOperator.OR, right);
    }
    
    public static UnaryExpressionNode minus(ExpressionNode expr) {
        return unary(UnaryOperator.MINUS, expr);
    }
    
    /**
     * Factory pattern for creating PercentileFunctionNode instances
     */
    public static class PercentileFunctionBuilder {
        private final String functionName;
        private final ExpressionNode valueExpression;
        private final double percentile;
        private ExpressionNode timeSliceFilter;
        
        private PercentileFunctionBuilder(String functionName, ExpressionNode valueExpression, double percentile) {
            this.functionName = functionName;
            this.valueExpression = valueExpression;
            this.percentile = percentile;
        }
        
        public PercentileFunctionBuilder minTimeSlice(ExpressionNode timeSliceFilter) {
            this.timeSliceFilter = timeSliceFilter;
            return this;
        }
        
        public PercentileFunctionBuilder minTimeSlice(String duration) {
            this.timeSliceFilter = durationLiteral(duration);
            return this;
        }
        
        public PercentileFunctionNode build() {
            return new PercentileFunctionNode(functionName, valueExpression, percentile, timeSliceFilter, location(1, 1));
        }
    }
    
    // Percentile function factory methods
    public static PercentileFunctionBuilder percentile(String functionName, ExpressionNode valueExpression, double percentile) {
        return new PercentileFunctionBuilder(functionName, valueExpression, percentile);
    }
    
    public static PercentileFunctionBuilder percentile(String functionName, String valueField, double percentile) {
        return new PercentileFunctionBuilder(functionName, identifier(valueField), percentile);
    }
    
    // Specific percentile factory methods
    public static PercentileFunctionBuilder p90(ExpressionNode valueExpression) {
        return percentile("P90", valueExpression, 90.0);
    }
    
    public static PercentileFunctionBuilder p90(String valueField) {
        return percentile("P90", valueField, 90.0);
    }
    
    public static PercentileFunctionBuilder p95(ExpressionNode valueExpression) {
        return percentile("P95", valueExpression, 95.0);
    }
    
    public static PercentileFunctionBuilder p95(String valueField) {
        return percentile("P95", valueField, 95.0);
    }
    
    public static PercentileFunctionBuilder p99(ExpressionNode valueExpression) {
        return percentile("P99", valueExpression, 99.0);
    }
    
    public static PercentileFunctionBuilder p99(String valueField) {
        return percentile("P99", valueField, 99.0);
    }
    
    public static PercentileFunctionBuilder p999(ExpressionNode valueExpression) {
        return percentile("P999", valueExpression, 99.9);
    }
    
    public static PercentileFunctionBuilder p999(String valueField) {
        return percentile("P999", valueField, 99.9);
    }
    
    // Backward compatibility methods for existing percentile overloads
    public static PercentileFunctionNode percentile(String functionName, ExpressionNode valueExpression, double percentile, ExpressionNode timeSliceFilter) {
        return percentile(functionName, valueExpression, percentile).minTimeSlice(timeSliceFilter).build();
    }
    
    // Specific percentile shortcuts - backward compatibility
    public static PercentileFunctionNode p90(ExpressionNode valueExpression, ExpressionNode timeSliceFilter) {
        return p90(valueExpression).minTimeSlice(timeSliceFilter).build();
    }
    
    public static PercentileFunctionNode p95(ExpressionNode valueExpression, ExpressionNode timeSliceFilter) {
        return p95(valueExpression).minTimeSlice(timeSliceFilter).build();
    }
    
    public static PercentileFunctionNode p99(ExpressionNode valueExpression, ExpressionNode timeSliceFilter) {
        return p99(valueExpression).minTimeSlice(timeSliceFilter).build();
    }
    
    public static PercentileFunctionNode p999(ExpressionNode valueExpression, ExpressionNode timeSliceFilter) {
        return p999(valueExpression).minTimeSlice(timeSliceFilter).build();
    }

    // Helper method to compare AST nodes (ignoring line/column information)
    public static boolean astEquals(ASTNode expected, ASTNode actual) {
        if (expected == null && actual == null) return true;
        if (expected == null || actual == null) return false;
        
        // Compare types first
        if (!expected.getClass().equals(actual.getClass())) {
            return false;
        }
        
        // For records, we need to compare field by field, ignoring line/column
        return astEqualsImpl(expected, actual);
    }
    
    // Overloaded method with message (for compatibility with existing tests)
    public static boolean astEquals(ASTNode expected, ASTNode actual, String message) {
        return astEquals(expected, actual);
    }
    
    @SuppressWarnings("unchecked")
    private static boolean astEqualsImpl(Object expected, Object actual) {
        if (expected == null && actual == null) return true;
        if (expected == null || actual == null) return false;
        
        // Handle different types
        if (expected instanceof List && actual instanceof List) {
            List<Object> expectedList = (List<Object>) expected;
            List<Object> actualList = (List<Object>) actual;
            if (expectedList.size() != actualList.size()) return false;
            
            for (int i = 0; i < expectedList.size(); i++) {
                if (!astEqualsImpl(expectedList.get(i), actualList.get(i))) return false;
            }
            return true;
        }
        
        // For non-list types, ensure classes are exactly equal
        if (!expected.getClass().equals(actual.getClass())) return false;
        
        // Check if it's an AST node by checking the class name or interface
        String className = expected.getClass().getName();
        boolean isASTNode = expected instanceof ASTNode || 
            className.contains("me.bechberger.jfr.extended.ast.ASTNodes$") ||
            className.contains("ASTNode");
        
        if (isASTNode) {
            return astEqualsRecord(expected, actual);
        }
        
        // For primitives and other objects
        return expected.equals(actual);
    }
    
    private static boolean astEqualsRecord(Object expected, Object actual) {
        try {
            var expectedClass = expected.getClass();
            var actualClass = actual.getClass();
            
            if (!expectedClass.equals(actualClass)) return false;
            
            // Get all record components except line and column
            var components = expectedClass.getRecordComponents();
            if (components == null || components.length == 0) {
                // Not a record, fall back to equals
                return expected.equals(actual);
            }
            
            for (var component : components) {
                String name = component.getName();
                if ("location".equals(name)) {
                    continue; // Skip location
                }
                
                var method = component.getAccessor();
                Object expectedValue = method.invoke(expected);
                Object actualValue = method.invoke(actual);
                
                if (!astEqualsImpl(expectedValue, actualValue)) {
                    return false;
                }
            }
            
            return true;
        } catch (Exception e) {
            throw new RuntimeException("Failed to compare AST nodes", e);
        }
    }
    
    public static RawJfrQueryNode rawJfrQuery(String query) {
        return new RawJfrQueryNode(query, location(1, 1));
    }
    
    // Percentile selection factory methods
    public static PercentileSelectionNode percentileSelect(double percentile, String tableName, String idField, ExpressionNode valueExpression) {
        return new PercentileSelectionNode("PERCENTILE_SELECT", tableName, idField, valueExpression, percentile, location(1, 1));
    }
    
    public static PercentileSelectionNode percentileSelect(double percentile, String tableName, String idField, String valueField) {
        return new PercentileSelectionNode("PERCENTILE_SELECT", tableName, idField, identifier(valueField), percentile, location(1, 1));
    }
    
    /**
     * Factory pattern for creating list manipulation function calls
     */
    public static class ListFunctionBuilder {
        private final String functionName;
        private final ExpressionNode list;
        private ExpressionNode count;
        private ExpressionNode start;
        private ExpressionNode end;
        
        private ListFunctionBuilder(String functionName, ExpressionNode list) {
            this.functionName = functionName;
            this.list = list;
        }
        
        public ListFunctionBuilder count(ExpressionNode count) {
            this.count = count;
            return this;
        }
        
        public ListFunctionBuilder count(int count) {
            this.count = numberLiteral(count);
            return this;
        }
        
        public ListFunctionBuilder start(ExpressionNode start) {
            this.start = start;
            return this;
        }
        
        public ListFunctionBuilder start(int start) {
            this.start = numberLiteral(start);
            return this;
        }
        
        public ListFunctionBuilder end(ExpressionNode end) {
            this.end = end;
            return this;
        }
        
        public ListFunctionBuilder end(int end) {
            this.end = numberLiteral(end);
            return this;
        }
        
        public FunctionCallNode build() {
            switch (functionName) {
                case "HEAD":
                    return count != null ? function("HEAD", list, count) : function("HEAD", list);
                case "TAIL":
                    return count != null ? function("TAIL", list, count) : function("TAIL", list);
                case "SLICE":
                    if (start == null || end == null) {
                        throw new IllegalArgumentException("SLICE requires both start and end parameters");
                    }
                    return function("SLICE", list, start, end);
                default:
                    throw new IllegalArgumentException("Unknown list function: " + functionName);
            }
        }
    }
    
    // List function factory methods
    public static ListFunctionBuilder head(ExpressionNode list) {
        return new ListFunctionBuilder("HEAD", list);
    }
    
    public static ListFunctionBuilder tail(ExpressionNode list) {
        return new ListFunctionBuilder("TAIL", list);
    }
    
    public static ListFunctionBuilder slice(ExpressionNode list) {
        return new ListFunctionBuilder("SLICE", list);
    }
    
    // Backward compatibility methods for existing list functions
    public static FunctionCallNode head(ExpressionNode list, ExpressionNode count) {
        return head(list).count(count).build();
    }
    
    public static FunctionCallNode head(ExpressionNode list, int count) {
        return head(list).count(count).build();
    }
    
    public static FunctionCallNode tail(ExpressionNode list, ExpressionNode count) {
        return tail(list).count(count).build();
    }
    
    public static FunctionCallNode tail(ExpressionNode list, int count) {
        return tail(list).count(count).build();
    }
    
    public static FunctionCallNode slice(ExpressionNode list, ExpressionNode start, ExpressionNode end) {
        return slice(list).start(start).end(end).build();
    }
    
    public static FunctionCallNode slice(ExpressionNode list, int start, int end) {
        return slice(list).start(start).end(end).build();
    }
    
    public static StarNode star() {
        return new StarNode(location(1, 1));
    }
    
    /**
     * Factory pattern for creating StandardJoinSourceNode instances
     */
    public static class StandardJoinBuilder {
        private final String source;
        private final String leftJoinField;
        private final String rightJoinField;
        private String alias;
        private StandardJoinType joinType = StandardJoinType.INNER;
        
        private StandardJoinBuilder(String source, String leftJoinField, String rightJoinField) {
            this.source = source;
            this.leftJoinField = leftJoinField;
            this.rightJoinField = rightJoinField;
        }
        
        public StandardJoinBuilder alias(String alias) {
            this.alias = alias;
            return this;
        }
        
        public StandardJoinBuilder inner() {
            this.joinType = StandardJoinType.INNER;
            return this;
        }
        
        public StandardJoinBuilder left() {
            this.joinType = StandardJoinType.LEFT;
            return this;
        }
        
        public StandardJoinBuilder right() {
            this.joinType = StandardJoinType.RIGHT;
            return this;
        }
        
        public StandardJoinBuilder full() {
            this.joinType = StandardJoinType.FULL;
            return this;
        }
        
        public StandardJoinBuilder with(StandardJoinType joinType) {
            this.joinType = joinType;
            return this;
        }
        
        public StandardJoinSourceNode build() {
            return new StandardJoinSourceNode(source, alias, joinType, leftJoinField, rightJoinField, location(1, 1));
        }
    }
    
    // Standard join factory method
    public static StandardJoinBuilder standardJoin(String source, String leftJoinField, String rightJoinField) {
        return new StandardJoinBuilder(source, leftJoinField, rightJoinField);
    }
    
    // Convenience methods for different join types
    public static StandardJoinSourceNode innerJoin(String source, String leftJoinField, String rightJoinField) {
        return standardJoin(source, leftJoinField, rightJoinField).inner().build();
    }
    
    public static StandardJoinSourceNode leftJoin(String source, String leftJoinField, String rightJoinField) {
        return standardJoin(source, leftJoinField, rightJoinField).left().build();
    }
    
    public static StandardJoinSourceNode rightJoin(String source, String leftJoinField, String rightJoinField) {
        return standardJoin(source, leftJoinField, rightJoinField).right().build();
    }
    
    public static StandardJoinSourceNode fullJoin(String source, String leftJoinField, String rightJoinField) {
        return standardJoin(source, leftJoinField, rightJoinField).full().build();
    }
    
    public static BinaryExpressionNode within(ExpressionNode timeField, ExpressionNode duration, ExpressionNode referenceTime) {
        return binary(
            timeField,
            BinaryOperator.WITHIN,
            binary(duration, BinaryOperator.OF, referenceTime)
        );
    }
}
