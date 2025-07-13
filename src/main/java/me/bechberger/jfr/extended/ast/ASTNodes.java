package me.bechberger.jfr.extended.ast;

import java.util.List;
import me.bechberger.jfr.extended.table.CellValue;

/**
 * AST node records for the extended JFR query language
 * 
 * This file contains all AST node definitions with:
 * - Location record instead of separate line/column fields
 * - Built-in format() methods for pretty printing
 * - Complete visitor pattern support
 */
public class ASTNodes {
    
    /**
     * Root node representing the entire program
     */
    public record ProgramNode(List<StatementNode> statements, Location location) implements ASTNode {
        @Override
        public <T> T accept(ASTVisitor<T> visitor) {
            return visitor.visitProgram(this);
        }
        
        @Override
        public int getLine() { return location.line(); }
        
        @Override
        public int getColumn() { return location.column(); }
        
        @Override
        public String format() {
            return statements.stream()
                .map(StatementNode::format)
                .reduce((a, b) -> a + "\n" + b)
                .orElse("");
        }
    }
    
    /**
     * Base interface for all statement nodes
     */
    public interface StatementNode extends ASTNode {
    }
    
    /**
     * Assignment statement (var = query)
     */
    public record AssignmentNode(String variable, QueryNode query, Location location) implements StatementNode {
        @Override
        public <T> T accept(ASTVisitor<T> visitor) {
            return visitor.visitAssignment(this);
        }
        
        @Override
        public int getLine() { return location.line(); }
        
        @Override
        public int getColumn() { return location.column(); }
        
        @Override
        public String format() {
            return variable + " := " + query.format();
        }
    }
    
    /**
     * View definition statement
     */
    public record ViewDefinitionNode(String viewName, QueryNode query, Location location) implements StatementNode {
        @Override
        public <T> T accept(ASTVisitor<T> visitor) {
            return visitor.visitViewDefinition(this);
        }
        
        @Override
        public int getLine() { return location.line(); }
        
        @Override
        public int getColumn() { return location.column(); }
        
        @Override
        public String format() {
            return "VIEW " + viewName + " AS " + query.format();
        }
    }
    
    /**
     * Query statement
     */
    public record QueryNode(
        boolean isExtended,
        List<String> columns,
        List<FormatterNode> formatters,
        SelectNode select,
        FromNode from,
        WhereNode where,
        GroupByNode groupBy,
        HavingNode having,
        OrderByNode orderBy,
        LimitNode limit,
        Location location
    ) implements StatementNode {
        @Override
        public <T> T accept(ASTVisitor<T> visitor) {
            return visitor.visitQuery(this);
        }
        
        @Override
        public int getLine() { return location.line(); }
        
        @Override
        public int getColumn() { return location.column(); }
        
        @Override
        public String format() {
            StringBuilder sb = new StringBuilder();
            
            // Extended query marker
            if (isExtended) {
                sb.append("@");
            }
            
            // SELECT clause
            if (select != null) {
                sb.append(select.format());
            }
            
            // FROM clause
            if (from != null) {
                sb.append("\n").append(from.format());
            }
            
            // WHERE clause
            if (where != null) {
                sb.append("\n").append(where.format());
            }
            
            // GROUP BY clause
            if (groupBy != null) {
                sb.append("\n").append(groupBy.format());
            }
            
            // HAVING clause
            if (having != null) {
                sb.append("\n").append(having.format());
            }
            
            // ORDER BY clause
            if (orderBy != null) {
                sb.append("\n").append(orderBy.format());
            }
            
            // LIMIT clause
            if (limit != null) {
                sb.append("\n").append(limit.format());
            }
            
            return sb.toString();
        }
    }
    
    /**
     * Formatter node for FORMAT clause
     */
    public record FormatterNode(List<PropertyNode> properties, Location location) implements ASTNode {
        @Override
        public <T> T accept(ASTVisitor<T> visitor) {
            return visitor.visitFormatter(this);
        }
        
        @Override
        public int getLine() { return location.line(); }
        
        @Override
        public int getColumn() { return location.column(); }
        
        @Override
        public String format() {
            String props = properties.stream()
                    .map(PropertyNode::format)
                    .reduce((a, b) -> a + ", " + b)
                    .orElse("");
            return "FORMAT(" + props + ")";
        }
    }
    
    /**
     * Property node for formatter properties
     */
    public record PropertyNode(String name, String value, Location location) implements ASTNode {
        @Override
        public <T> T accept(ASTVisitor<T> visitor) {
            return visitor.visitProperty(this);
        }
        
        @Override
        public int getLine() { return location.line(); }
        
        @Override
        public int getColumn() { return location.column(); }
        
        @Override
        public String format() {
            return name + "=" + value;
        }
    }
    
    /**
     * SELECT clause
     */
    public record SelectNode(List<SelectItemNode> items, boolean isSelectAll, Location location) implements ASTNode {
        @Override
        public <T> T accept(ASTVisitor<T> visitor) {
            return visitor.visitSelect(this);
        }
        
        @Override
        public int getLine() { return location.line(); }
        
        @Override
        public int getColumn() { return location.column(); }
        
        @Override
        public String format() {
            if (isSelectAll) {
                return "SELECT *";
            } else {
                String itemsStr = items.stream()
                        .map(SelectItemNode::format)
                        .reduce((a, b) -> a + ", " + b)
                        .orElse("");
                return "SELECT " + itemsStr;
            }
        }
    }
    
    /**
     * Item in SELECT clause (expression with optional alias)
     */
    public record SelectItemNode(ExpressionNode expression, String alias, Location location) implements ASTNode {
        @Override
        public <T> T accept(ASTVisitor<T> visitor) {
            return visitor.visitSelectItem(this);
        }
        
        @Override
        public int getLine() { return location.line(); }
        
        @Override
        public int getColumn() { return location.column(); }
        
        @Override
        public String format() {
            String result = expression.format();
            if (alias != null) {
                result += " AS " + alias;
            }
            return result;
        }
    }
    
    /**
     * FROM clause
     */
    public record FromNode(List<SourceNodeBase> sources, Location location) implements ASTNode {
        @Override
        public <T> T accept(ASTVisitor<T> visitor) {
            return visitor.visitFrom(this);
        }
        
        @Override
        public int getLine() { return location.line(); }
        
        @Override
        public int getColumn() { return location.column(); }
        
        @Override
        public String format() {
            StringBuilder sb = new StringBuilder("FROM ");
            
            // First source
            if (!sources.isEmpty()) {
                sb.append(sources.get(0).format());
            }
            
            // Remaining sources (usually joins)
            for (int i = 1; i < sources.size(); i++) {
                sb.append("\n  ").append(sources.get(i).format());
            }
            
            return sb.toString();
        }
    }
    
    /**
     * Base interface for source nodes
     */
    public interface SourceNodeBase extends ASTNode {
        String alias();
    }
    
    /**
     * Source in FROM clause
     */
    public record SourceNode(String source, String alias, Location location) implements SourceNodeBase {
        @Override
        public <T> T accept(ASTVisitor<T> visitor) {
            return visitor.visitSource(this);
        }
        
        @Override
        public int getLine() { return location.line(); }
        
        @Override
        public int getColumn() { return location.column(); }
        
        @Override
        public String format() {
            String result = source;
            if (alias != null) {
                result += " AS " + alias;
            }
            return result;
        }
    }
    
    /**
     * Subquery source in FROM clause
     */
    public record SubquerySourceNode(StatementNode query, String alias, Location location) implements SourceNodeBase {
        @Override
        public <T> T accept(ASTVisitor<T> visitor) {
            return visitor.visitSubquerySource(this);
        }
        
        @Override
        public int getLine() { return location.line(); }
        
        @Override
        public int getColumn() { return location.column(); }
        
        @Override
        public String format() {
            String result = "(" + query.format() + ")";
            if (alias != null) {
                result += " AS " + alias;
            }
            return result;
        }
    }
    
    /**
     * WHERE clause
     */
    public record WhereNode(ConditionNode condition, Location location) implements ASTNode {
        @Override
        public <T> T accept(ASTVisitor<T> visitor) {
            return visitor.visitWhere(this);
        }
        
        @Override
        public int getLine() { return location.line(); }
        
        @Override
        public int getColumn() { return location.column(); }
        
        @Override
        public String format() {
            return "WHERE " + condition.format();
        }
    }
    
    /**
     * GROUP BY clause
     */
    public record GroupByNode(List<ExpressionNode> fields, Location location) implements ASTNode {
        @Override
        public <T> T accept(ASTVisitor<T> visitor) {
            return visitor.visitGroupBy(this);
        }
        
        @Override
        public int getLine() { return location.line(); }
        
        @Override
        public int getColumn() { return location.column(); }
        
        @Override
        public String format() {
            String fieldsStr = fields.stream()
                    .map(ExpressionNode::format)
                    .reduce((a, b) -> a + ", " + b)
                    .orElse("");
            return "GROUP BY " + fieldsStr;
        }
    }
    
    /**
     * HAVING clause
     */
    public record HavingNode(ConditionNode condition, Location location) implements ASTNode {
        @Override
        public <T> T accept(ASTVisitor<T> visitor) {
            return visitor.visitHaving(this);
        }
        
        @Override
        public int getLine() { return location.line(); }
        
        @Override
        public int getColumn() { return location.column(); }
        
        @Override
        public String format() {
            return "HAVING " + condition.format();
        }
    }
    
    /**
     * ORDER BY clause
     */
    public record OrderByNode(List<OrderFieldNode> fields, Location location) implements ASTNode {
        @Override
        public <T> T accept(ASTVisitor<T> visitor) {
            return visitor.visitOrderBy(this);
        }
        
        @Override
        public int getLine() { return location.line(); }
        
        @Override
        public int getColumn() { return location.column(); }
        
        @Override
        public String format() {
            String fieldsStr = fields.stream()
                    .map(OrderFieldNode::format)
                    .reduce((a, b) -> a + ", " + b)
                    .orElse("");
            return "ORDER BY " + fieldsStr;
        }
    }
    
    /**
     * Field in ORDER BY clause
     */
    public record OrderFieldNode(ExpressionNode field, SortOrder order, Location location) implements ASTNode {
        @Override
        public <T> T accept(ASTVisitor<T> visitor) {
            return visitor.visitOrderField(this);
        }
        
        @Override
        public int getLine() { return location.line(); }
        
        @Override
        public int getColumn() { return location.column(); }
        
        @Override
        public String format() {
            String result = field.format();
            if (order == SortOrder.DESC) {
                result += " DESC";
            } else {
                result += " ASC";
            }
            return result;
        }
    }
    
    /**
     * Sort order enumeration
     */
    public enum SortOrder {
        ASC, DESC
    }
    
    /**
     * LIMIT clause
     */
    public record LimitNode(int limit, Location location) implements ASTNode {
        @Override
        public <T> T accept(ASTVisitor<T> visitor) {
            return visitor.visitLimit(this);
        }
        
        @Override
        public int getLine() { return location.line(); }
        
        @Override
        public int getColumn() { return location.column(); }
        
        @Override
        public String format() {
            return "LIMIT " + limit;
        }
    }
    
    /**
     * Base interface for all expression nodes
     */
    public interface ExpressionNode extends ASTNode {
    }
    
    /**
     * Binary expression (e.g., a + b, a > b)
     */
    public record BinaryExpressionNode(
        ExpressionNode left,
        BinaryOperator operator,
        ExpressionNode right,
        Location location
    ) implements ExpressionNode {
        @Override
        public <T> T accept(ASTVisitor<T> visitor) {
            return visitor.visitBinaryExpression(this);
        }
        
        @Override
        public int getLine() { return location.line(); }
        
        @Override
        public int getColumn() { return location.column(); }
        
        @Override
        public String format() {
            String leftStr = left.format();
            String rightStr = right.format();
            String operatorStr = formatBinaryOperator(operator);
            return "(" + leftStr + " " + operatorStr + " " + rightStr + ")";
        }
        
        private String formatBinaryOperator(BinaryOperator operator) {
            return switch (operator) {
                case ADD -> "+";
                case SUBTRACT -> "-";
                case MULTIPLY -> "*";
                case DIVIDE -> "/";
                case MODULO -> "%";
                case EQUALS -> "=";
                case NOT_EQUALS -> "!=";
                case LESS_THAN -> "<";
                case LESS_EQUAL -> "<=";
                case GREATER_THAN -> ">";
                case GREATER_EQUAL -> ">=";
                case LIKE -> "LIKE";
                case IN -> "IN";
                case AND -> "AND";
                case OR -> "OR";
                case WITHIN -> "WITHIN";
                case OF -> "OF";
            };
        }
    }
    
    /**
     * Binary operators
     */
    public enum BinaryOperator {
        // Arithmetic
        ADD, SUBTRACT, MULTIPLY, DIVIDE, MODULO,
        // Comparison
        EQUALS, NOT_EQUALS, LESS_THAN, GREATER_THAN, LESS_EQUAL, GREATER_EQUAL,
        // Logical
        AND, OR,
        // String
        LIKE,
        // Set
        IN,
        // Temporal
        WITHIN, OF
    }
    
    /**
     * Unary expression (e.g., -x, NOT x)
     */
    public record UnaryExpressionNode(
        UnaryOperator operator,
        ExpressionNode operand,
        Location location
    ) implements ExpressionNode {
        @Override
        public <T> T accept(ASTVisitor<T> visitor) {
            return visitor.visitUnaryExpression(this);
        }
        
        @Override
        public int getLine() { return location.line(); }
        
        @Override
        public int getColumn() { return location.column(); }
        
        @Override
        public String format() {
            String operandStr = operand.format();
            return switch (operator) {
                case MINUS -> "-" + operandStr;
                case NOT -> "NOT " + operandStr;
            };
        }
    }
    
    /**
     * Unary operators
     */
    public enum UnaryOperator {
        MINUS, NOT
    }
    
    /**
     * Field access (e.g., event.field, table.column)
     */
    public record FieldAccessNode(String qualifier, String field, Location location) implements ExpressionNode {
        @Override
        public <T> T accept(ASTVisitor<T> visitor) {
            return visitor.visitFieldAccess(this);
        }
        
        @Override
        public int getLine() { return location.line(); }
        
        @Override
        public int getColumn() { return location.column(); }
        
        @Override
        public String format() {
            return qualifier + "." + field;
        }
    }
    
    /**
     * Function call (e.g., COUNT(*), MAX(duration))
     */
    public record FunctionCallNode(
        String functionName,
        List<ExpressionNode> arguments,
        Location location
    ) implements ExpressionNode {
        @Override
        public <T> T accept(ASTVisitor<T> visitor) {
            return visitor.visitFunctionCall(this);
        }
        
        @Override
        public int getLine() { return location.line(); }
        
        @Override
        public int getColumn() { return location.column(); }
        
        @Override
        public String format() {
            String args = arguments.stream()
                    .map(ExpressionNode::format)
                    .reduce((a, b) -> a + ", " + b)
                    .orElse("");
            
            return functionName + "(" + args + ")";
        }
    }
    
    /**
     * Literal value
     */
    public record LiteralNode(CellValue value, Location location) implements ExpressionNode {
        @Override
        public <T> T accept(ASTVisitor<T> visitor) {
            return visitor.visitLiteral(this);
        }
        
        @Override
        public int getLine() { return location.line(); }
        
        @Override
        public int getColumn() { return location.column(); }
        
        @Override
        public String format() {
            return switch (value.getType()) {
                case STRING -> "'" + value.toString().replace("'", "''") + "'";
                case NUMBER -> value.toString(); // Use NumberValue's toString() for proper formatting
                case FLOAT -> value.toString(); // Use FloatValue's toString() for proper formatting
                case DURATION, MEMORY_SIZE, RATE -> value.toString();
                case BOOLEAN -> value.toString().toUpperCase();
                case NULL -> "NULL"; // Explicitly format NULL 
                default -> value.toString();
            };
        }
    }
    
    /**
     * Identifier
     */
    public record IdentifierNode(String name, Location location) implements ExpressionNode {
        @Override
        public <T> T accept(ASTVisitor<T> visitor) {
            return visitor.visitIdentifier(this);
        }
        
        @Override
        public int getLine() { return location.line(); }
        
        @Override
        public int getColumn() { return location.column(); }
        
        @Override
        public String format() {
            return name;
        }
    }
    
    /**
     * Nested JFR query [original JFR query]
     */
    public record NestedQueryNode(String jfrQuery, Location location) implements ExpressionNode {
        @Override
        public <T> T accept(ASTVisitor<T> visitor) {
            return visitor.visitNestedQuery(this);
        }
        
        @Override
        public int getLine() { return location.line(); }
        
        @Override
        public int getColumn() { return location.column(); }
        
        @Override
        public String format() {
            String query = jfrQuery;
            return query;
        }
    }
    
    /**
     * Base interface for all condition nodes
     */
    public interface ConditionNode extends ASTNode {
    }
    
    /**
     * GC correlation condition
     */
    public record GCCorrelationNode(
        String field,
        GCCorrelationType type,
        ExpressionNode value,
        Location location
    ) implements ConditionNode {
        @Override
        public <T> T accept(ASTVisitor<T> visitor) {
            return visitor.visitGCCorrelation(this);
        }
        
        @Override
        public int getLine() { return location.line(); }
        
        @Override
        public int getColumn() { return location.column(); }
        
        @Override
        public String format() {
            return field + "_" + type.name().toLowerCase() + 
                   "(" + value.format() + ")";
        }
    }
    
    /**
     * GC correlation types
     */
    public enum GCCorrelationType {
        BEFORE_GC, AFTER_GC, BEFORE_GC_IN_PERCENTILE
    }
    
    /**
     * Variable declaration (x := expression)
     */
    public record VariableDeclarationNode(
        String variable,
        ExpressionNode value,
        Location location
    ) implements ConditionNode {
        @Override
        public <T> T accept(ASTVisitor<T> visitor) {
            return visitor.visitVariableDeclaration(this);
        }
        
        @Override
        public int getLine() { return location.line(); }
        
        @Override
        public int getColumn() { return location.column(); }
        
        @Override
        public String format() {
            return variable + " := " + value.format();
        }
    }
    
    /**
     * Expression condition (wraps an expression as a condition)
     */
    public record ExpressionConditionNode(ExpressionNode expression, Location location) implements ConditionNode {
        @Override
        public <T> T accept(ASTVisitor<T> visitor) {
            return visitor.visitExpressionCondition(this);
        }
        
        @Override
        public int getLine() { return location.line(); }
        
        @Override
        public int getColumn() { return location.column(); }
        
        @Override
        public String format() {
            return expression.format();
        }
    }
    
    /**
     * SHOW EVENTS query
     */
    public record ShowEventsNode(Location location) implements StatementNode {
        @Override
        public <T> T accept(ASTVisitor<T> visitor) {
            return visitor.visitShowEvents(this);
        }
        
        @Override
        public int getLine() { return location.line(); }
        
        @Override
        public int getColumn() { return location.column(); }
        
        @Override
        public String format() {
            return "SHOW EVENTS";
        }
    }
    
    /**
     * SHOW FIELDS query
     */
    public record ShowFieldsNode(String eventType, Location location) implements StatementNode {
        @Override
        public <T> T accept(ASTVisitor<T> visitor) {
            return visitor.visitShowFields(this);
        }
        
        @Override
        public int getLine() { return location.line(); }
        
        @Override
        public int getColumn() { return location.column(); }
        
        @Override
        public String format() {
            return "SHOW FIELDS " + eventType;
        }
    }
    
    /**
     * HELP query - general help
     */
    public record HelpNode(Location location) implements StatementNode {
        @Override
        public <T> T accept(ASTVisitor<T> visitor) {
            return visitor.visitHelp(this);
        }
        
        @Override
        public int getLine() { return location.line(); }
        
        @Override
        public int getColumn() { return location.column(); }
        
        @Override
        public String format() {
            return "HELP";
        }
    }
    
    /**
     * HELP FUNCTION query
     */
    public record HelpFunctionNode(String functionName, Location location) implements StatementNode {
        @Override
        public <T> T accept(ASTVisitor<T> visitor) {
            return visitor.visitHelpFunction(this);
        }
        
        @Override
        public int getLine() { return location.line(); }
        
        @Override
        public int getColumn() { return location.column(); }
        
        @Override
        public String format() {
            return "HELP FUNCTION " + functionName;
        }
    }
    
    /**
     * HELP GRAMMAR query
     */
    public record HelpGrammarNode(Location location) implements StatementNode {
        @Override
        public <T> T accept(ASTVisitor<T> visitor) {
            return visitor.visitHelpGrammar(this);
        }
        
        @Override
        public int getLine() { return location.line(); }
        
        @Override
        public int getColumn() { return location.column(); }
        
        @Override
        public String format() {
            return "HELP GRAMMAR";
        }
    }

    /**
     * Fuzzy join source node
     */
    public record FuzzyJoinSourceNode(
        String source,
        String alias,
        FuzzyJoinType joinType,
        String joinField,
        ExpressionNode tolerance,
        ExpressionNode threshold,
        Location location
    ) implements SourceNodeBase {
        @Override
        public <T> T accept(ASTVisitor<T> visitor) {
            return visitor.visitFuzzyJoinSource(this);
        }
        
        @Override
        public int getLine() { return location.line(); }
        
        @Override
        public int getColumn() { return location.column(); }
        
        @Override
        public String format() {
            StringBuilder sb = new StringBuilder();
            
            // Join type
            sb.append("FUZZY JOIN ");
            
            // Source
            sb.append(source);
            
            // Alias
            if (alias != null) {
                sb.append(" AS ").append(alias);
            }
            
            // Join field
            sb.append(" ON ").append(joinField);
            
            // Add join type with "WITH" only if not NEAREST (which is the default)
            if (joinType != null && joinType != FuzzyJoinType.NEAREST) {
                sb.append(" WITH ").append(joinType);
            }
            
            // Add tolerance if present
            if (tolerance != null) {
                sb.append(" TOLERANCE ").append(tolerance.format());
            }
            
            // Add threshold if present
            if (threshold != null) {
                sb.append(" THRESHOLD ").append(threshold.format());
            }
            
            return sb.toString();
        }
    }
    
    /**
     * Fuzzy join types
     */
    public enum FuzzyJoinType {
        NEAREST,    // Find closest event by time
        PREVIOUS,   // Find latest event before
        AFTER       // Find earliest event after
    }
    
    /**
     * Percentile function with time slice filtering
     */
    public record PercentileFunctionNode(
        String functionName,
        ExpressionNode valueExpression,
        double percentile,
        ExpressionNode timeSliceFilter,
        Location location
    ) implements ExpressionNode {
        @Override
        public <T> T accept(ASTVisitor<T> visitor) {
            return visitor.visitPercentileFunction(this);
        }
        
        @Override
        public int getLine() { return location.line(); }
        
        @Override
        public int getColumn() { return location.column(); }
        
        @Override
        public String format() {
            StringBuilder sb = new StringBuilder();
            sb.append(functionName).append("(");
            sb.append(valueExpression.format());
            sb.append(", ").append(percentile);
            
            if (timeSliceFilter != null) {
                sb.append(" MIN_TIME_SLICE ").append(timeSliceFilter.format());
            }
            
            sb.append(")");
            
            return sb.toString();
        }
    }
    
    /**
     * Percentile selection function that returns a list of IDs of records in the specified percentile
     * Examples: P99SELECT(GarbageCollection, id, duration), PERCENTILE_SELECT(95, Events, eventId, latency)
     * Returns a list that can be used with IN, HEAD(), TAIL(), SLICE() operators
     */
    public record PercentileSelectionNode(
        String functionName,
        String tableName,
        String idField,
        ExpressionNode valueExpression,
        double percentile,
        Location location
    ) implements ExpressionNode {
        @Override
        public <T> T accept(ASTVisitor<T> visitor) {
            return visitor.visitPercentileSelection(this);
        }
        
        @Override
        public int getLine() { return location.line(); }
        
        @Override
        public int getColumn() { return location.column(); }
        
        @Override
        public String format() {
            StringBuilder sb = new StringBuilder();
            sb.append(functionName).append("(");
            sb.append(tableName).append(", ");
            sb.append(idField).append(", ");
            sb.append(valueExpression.format());
            sb.append(", ").append(percentile);
            sb.append(")");
            
            return sb.toString();
        }
    }
    
    /**
     * Raw JFR query statement (passed directly to JFR engine)
     */
    public record RawJfrQueryNode(String rawQuery, Location location) implements StatementNode {
        @Override
        public <T> T accept(ASTVisitor<T> visitor) {
            return visitor.visitRawJfrQuery(this);
        }
        
        @Override
        public int getLine() { return location.line(); }
        
        @Override
        public int getColumn() { return location.column(); }
        
        @Override
        public String format() {
            return rawQuery;
        }
    }
    
    /**
     * Star node representing the '*' symbol (e.g., COUNT(*))
     */
    public record StarNode(Location location) implements ExpressionNode {
        @Override
        public <T> T accept(ASTVisitor<T> visitor) {
            // If you have a visitStar method, call it; otherwise, return null or throw
            return visitor instanceof StarNodeVisitor<T> starVisitor ? starVisitor.visitStar(this) : null;
        }
        
        @Override
        public int getLine() { return location.line(); }
        
        @Override
        public int getColumn() { return location.column(); }
        
        @Override
        public String format() {
            return "*";
        }
    }

    /**
     * Visitor interface for StarNode (optional, only if needed)
     */
    public interface StarNodeVisitor<T> extends ASTVisitor<T> {
        T visitStar(StarNode node);
    }
    
    /**
     * Standard join types
     */
    public enum StandardJoinType {
        INNER,      // Inner join
        LEFT,       // Left outer join
        RIGHT,      // Right outer join
        FULL        // Full outer join
    }
    
    /**
     * Standard join source node
     */
    public record StandardJoinSourceNode(
        String source,
        String alias,
        StandardJoinType joinType,
        String leftJoinField,
        String rightJoinField,
        Location location
    ) implements SourceNodeBase {
        @Override
        public <T> T accept(ASTVisitor<T> visitor) {
            return visitor.visitStandardJoinSource(this);
        }
        
        @Override
        public int getLine() { return location.line(); }
        
        @Override
        public int getColumn() { return location.column(); }
        
        @Override
        public String format() {
            StringBuilder sb = new StringBuilder();
            
            // Join type
            sb.append(joinType.name()).append(" JOIN ");
            
            // Source
            sb.append(source);
            
            // Alias
            if (alias != null) {
                sb.append(" AS ").append(alias);
            }
            
            // Join condition
            sb.append(" ON ").append(leftJoinField).append(" = ").append(rightJoinField);
            
            return sb.toString();
        }
    }
    
    /**
     * Represents an array literal in the AST.
     */
    public record ArrayLiteralNode(List<ExpressionNode> elements, Location location) implements ExpressionNode {
        @Override
        public <T> T accept(ASTVisitor<T> visitor) {
            return visitor.visitArrayLiteral(this);
        }

        @Override
        public int getLine() { return location.line(); }

        @Override
        public int getColumn() { return location.column(); }

        @Override
        public String format() {
            return "[" + elements.stream().map(ExpressionNode::format).reduce((a, b) -> a + ", " + b).orElse("") + "]";
        }
    }
    
    /**
     * WITHIN condition for checking if a value falls within a time window of a reference time
     * Example: timestamp WITHIN 5m OF startTime
     */
    public record WithinConditionNode(
        ExpressionNode value,
        ExpressionNode timeWindow,
        ExpressionNode referenceTime,
        Location location
    ) implements ConditionNode {
        @Override
        public <T> T accept(ASTVisitor<T> visitor) {
            return visitor.visitWithinCondition(this);
        }
        
        @Override
        public int getLine() { return location.line(); }
        
        @Override
        public int getColumn() { return location.column(); }
        
        @Override
        public String format() {
            return value.format() + " WITHIN " + timeWindow.format() + " OF " + referenceTime.format();
        }
    }
}
