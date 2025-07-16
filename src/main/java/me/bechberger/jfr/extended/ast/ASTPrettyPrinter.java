package me.bechberger.jfr.extended.ast;

import me.bechberger.jfr.extended.ast.ASTNodes.*;

/**
 * A pretty printer for AST nodes that reconstructs formatted query strings.
 * 
 * This class implements the visitor pattern to traverse AST nodes and generate
 * properly formatted, human-readable query strings. The output is formatted with
 * appropriate indentation, spacing, and line breaks.
 * 
 * Features:
 * - Maintains proper SQL-like formatting
 * - Handles complex nested structures (subqueries, joins, expressions)
 * - Preserves semantic meaning while improving readability
 * - Supports both raw JFR queries and extended queries
 * 
 * Usage:
 * ```java
 * ASTNode ast = parser.parse();
 * ASTPrettyPrinter printer = new ASTPrettyPrinter();
 * String formattedQuery = printer.format(ast);
 * ```
 */
public class ASTPrettyPrinter implements ASTVisitor<String> {
    
    private int indentLevel = 0;
    private static final String INDENT = "  "; // 2 spaces per level
    
    /**
     * Format an AST node into a pretty-printed string
     */
    public String format(ASTNode node) {
        indentLevel = 0;
        return node.accept(this);
    }
    
    private String indent() {
        return INDENT.repeat(indentLevel);
    }
    
    private String withIndent(String content) {
        return indent() + content;
    }
    
    // ===========================================
    // Top-level Program and Statement Nodes
    // ===========================================
    
    @Override
    public String visitProgram(ProgramNode node) {
        return node.statements().stream()
                .map(stmt -> formatTopLevelStatement(stmt))
                .reduce((a, b) -> a + ";\n\n" + b)
                .orElse("");
    }
    
    private String formatTopLevelStatement(StatementNode stmt) {
        if (stmt instanceof QueryNode) {
            QueryNode query = (QueryNode) stmt;
            if (query.isExtended()) {
                return "@" + stmt.accept(this);
            }
        }
        return stmt.accept(this);
    }
    
    @Override
    public String visitQuery(QueryNode node) {
        StringBuilder sb = new StringBuilder();
        
        // SELECT clause (@ prefix is handled in specific contexts)
        sb.append(node.select().accept(this));
        
        // FROM clause
        if (node.from() != null) {
            sb.append("\n").append(node.from().accept(this));
        }
        
        // WHERE clause
        if (node.where() != null) {
            sb.append("\n").append(node.where().accept(this));
        }
        
        // GROUP BY clause
        if (node.groupBy() != null) {
            sb.append("\n").append(node.groupBy().accept(this));
        }
        
        // HAVING clause
        if (node.having() != null) {
            sb.append("\n").append(node.having().accept(this));
        }
        
        // ORDER BY clause
        if (node.orderBy() != null) {
            sb.append("\n").append(node.orderBy().accept(this));
        }
        
        // LIMIT clause
        if (node.limit() != null) {
            sb.append("\n").append(node.limit().accept(this));
        }
        
        return sb.toString();
    }
    
    @Override
    public String visitRawJfrQuery(RawJfrQueryNode node) {
        return node.rawQuery(); // Raw queries are returned as-is
    }
    
    @Override
    public String visitAssignment(AssignmentNode node) {
        // For assignments, the query should be formatted without @ prefix
        String query = formatQueryWithoutPrefix(node.query());
        return node.variable() + " := " + query;
    }
    
    @Override
    public String visitViewDefinition(ViewDefinitionNode node) {
        // For view definitions, the query should be formatted without @ prefix
        String query = formatQueryWithoutPrefix(node.query());
        return "VIEW " + node.viewName() + " AS " + query;
    }
    
    private String formatQueryWithoutPrefix(ASTNode queryNode) {
        if (queryNode instanceof QueryNode) {
            QueryNode query = (QueryNode) queryNode;
            // Temporarily override isExtended to format without @
            return formatQueryContent(query);
        }
        return queryNode.accept(this);
    }
    
    private String formatQueryContent(QueryNode node) {
        StringBuilder sb = new StringBuilder();
        
        // SELECT clause (no @ prefix for assignments/views)
        sb.append(node.select().accept(this));
        
        // FROM clause
        if (node.from() != null) {
            sb.append("\n").append(node.from().accept(this));
        }
        
        // WHERE clause
        if (node.where() != null) {
            sb.append("\n").append(node.where().accept(this));
        }
        
        // GROUP BY clause
        if (node.groupBy() != null) {
            sb.append("\n").append(node.groupBy().accept(this));
        }
        
        // HAVING clause
        if (node.having() != null) {
            sb.append("\n").append(node.having().accept(this));
        }
        
        // ORDER BY clause
        if (node.orderBy() != null) {
            sb.append("\n").append(node.orderBy().accept(this));
        }
        
        // LIMIT clause
        if (node.limit() != null) {
            sb.append("\n").append(node.limit().accept(this));
        }
        
        return sb.toString();
    }
    
    @Override
    public String visitShowEvents(ShowEventsNode node) {
        return "SHOW EVENTS";
    }
    
    @Override
    public String visitShowFields(ShowFieldsNode node) {
        return "SHOW FIELDS " + node.eventType();
    }
    
    @Override
    public String visitHelp(HelpNode node) {
        return "HELP";
    }
    
    @Override
    public String visitHelpFunction(HelpFunctionNode node) {
        return "HELP FUNCTION " + node.functionName();
    }
    
    @Override
    public String visitHelpGrammar(HelpGrammarNode node) {
        return "HELP GRAMMAR";
    }

    // ===========================================
    // SELECT Clause
    // ===========================================
    
    @Override
    public String visitSelect(SelectNode node) {
        if (node.isSelectAll()) {
            return "SELECT *";
        }
        
        String items = node.items().stream()
                .map(item -> item.accept(this))
                .reduce((a, b) -> a + ", " + b)
                .orElse("");
        
        return "SELECT " + items;
    }
    
    @Override
    public String visitSelectItem(SelectItemNode node) {
        String result = node.expression().accept(this);
        if (node.alias() != null) {
            result += " AS " + node.alias();
        }
        return result;
    }
    
    // ===========================================
    // FROM Clause and Sources
    // ===========================================
    
    @Override
    public String visitFrom(FromNode node) {
        String sources = node.sources().stream()
                .map(source -> source.accept(this))
                .reduce((a, b) -> a + "\n" + withIndent(b))
                .orElse("");
        
        return "FROM " + sources;
    }
    
    @Override
    public String visitSource(SourceNode node) {
        String result = node.source();
        if (node.alias() != null) {
            result += " AS " + node.alias();
        }
        return result;
    }
    
    @Override
    public String visitSubquerySource(SubquerySourceNode node) {
        // Subqueries should not include the @ prefix - they are formatted as raw queries
        String query = node.query().accept(this);
        String result = "(" + query + ")";
        if (node.alias() != null) {
            result += " AS " + node.alias();
        }
        return result;
    }
    
    @Override
    public String visitFuzzyJoinSource(FuzzyJoinSourceNode node) {
        StringBuilder sb = new StringBuilder();
        sb.append("FUZZY JOIN ").append(node.source());
        
        if (node.alias() != null) {
            sb.append(" AS ").append(node.alias());
        }
        
        sb.append(" ON ").append(node.joinField());
        
        if (node.joinType() != FuzzyJoinType.NEAREST) {
            sb.append(" WITH ").append(node.joinType().name());
        }
        
        if (node.tolerance() != null) {
            sb.append(" TOLERANCE ").append(node.tolerance().accept(this));
        }
        
        return sb.toString();
    }
    
    @Override
    public String visitStandardJoinSource(StandardJoinSourceNode node) {
        StringBuilder sb = new StringBuilder();
        
        // Join type
        switch (node.joinType()) {
            case INNER -> sb.append("INNER JOIN ");
            case LEFT -> sb.append("LEFT JOIN ");
            case RIGHT -> sb.append("RIGHT JOIN ");
            case FULL -> sb.append("FULL JOIN ");
        }
        
        sb.append(node.source());
        
        if (node.alias() != null) {
            sb.append(" AS ").append(node.alias());
        }
        
        sb.append(" ON ").append(node.leftJoinField())
          .append(" = ").append(node.rightJoinField());
        
        return sb.toString();
    }
    
    // ===========================================
    // WHERE, GROUP BY, HAVING, ORDER BY, LIMIT
    // ===========================================
    
    @Override
    public String visitWhere(WhereNode node) {
        return "WHERE " + node.condition().accept(this);
    }
    
    @Override
    public String visitGroupBy(GroupByNode node) {
        String fields = node.fields().stream()
                .map(field -> field.accept(this))
                .reduce((a, b) -> a + ", " + b)
                .orElse("");
        
        return "GROUP BY " + fields;
    }
    
    @Override
    public String visitHaving(HavingNode node) {
        return "HAVING " + node.condition().accept(this);
    }
    
    @Override
    public String visitOrderBy(OrderByNode node) {
        String fields = node.fields().stream()
                .map(field -> field.accept(this))
                .reduce((a, b) -> a + ", " + b)
                .orElse("");
        
        return "ORDER BY " + fields;
    }
    
    @Override
    public String visitOrderField(OrderFieldNode node) {
        String result = node.field().accept(this);
        if (node.order() == SortOrder.DESC) {
            result += " DESC";
        }
        return result;
    }
    
    @Override
    public String visitLimit(LimitNode node) {
        return "LIMIT " + node.limit();
    }
    
    // ===========================================
    // Expressions
    // ===========================================
    
    @Override
    public String visitIdentifier(IdentifierNode node) {
        return node.name();
    }
    
    @Override
    public String visitLiteral(LiteralNode node) {
        return node.format();
    }
    
    @Override
    public String visitBinaryExpression(BinaryExpressionNode node) {
        String left = node.left().accept(this);
        String right = node.right().accept(this);
        String operator = formatBinaryOperator(node.operator());
        
        // Add parentheses for clarity in complex expressions
        return "(" + left + " " + operator + " " + right + ")";
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
            case NOT_LIKE -> "NOT LIKE";
            case BETWEEN -> "BETWEEN";
            case IN -> "IN";
            case AND -> "AND";
            case OR -> "OR";
            case WITHIN -> "WITHIN";
            case OF -> "OF";
        };
    }
    
    @Override
    public String visitUnaryExpression(UnaryExpressionNode node) {
        String expr = node.operand().accept(this);
        return switch (node.operator()) {
            case MINUS -> "-" + expr;
            case NOT -> "NOT " + expr;
        };
    }
    
    @Override
    public String visitFunctionCall(FunctionCallNode node) {
        String args = node.arguments().stream()
                .map(arg -> arg.accept(this))
                .reduce((a, b) -> a + ", " + b)
                .orElse("");
        
        return node.functionName() + "(" + args + ")";
    }
    
    @Override
    public String visitFieldAccess(FieldAccessNode node) {
        return node.qualifier() + "." + node.field();
    }
    
    @Override
    public String visitNestedQuery(NestedQueryNode node) {
        // For nested queries, we need to determine if it's a raw JFR query or an extended query
        // Raw JFR queries: (SELECT ...)
        // Extended queries: (@SELECT ...)
        String query = node.jfrQuery();
        if (query.trim().startsWith("@")) {
            return "(" + query + ")";
        } else {
            return "(" + query + ")";
        }
    }
    
    // ===========================================
    // Missing Abstract Methods
    // ===========================================
    
    @Override
    public String visitStatement(StatementNode node) {
        return node.accept(this);
    }
    
    @Override
    public String visitExpression(ExpressionNode node) {
        return node.accept(this);
    }
    
    @Override
    public String visitCondition(ConditionNode node) {
        return node.accept(this);
    }
    
    @Override
    public String visitFormatter(FormatterNode node) {
        String properties = node.properties().stream()
                .map(prop -> prop.accept(this))
                .reduce((a, b) -> a + ", " + b)
                .orElse("");
        
        return "FORMAT(" + properties + ")";
    }
    
    @Override
    public String visitProperty(PropertyNode node) {
        return node.name() + "=" + node.value();
    }
    
    // ===========================================
    // Conditions
    // ===========================================
    
    @Override
    public String visitExpressionCondition(ExpressionConditionNode node) {
        return node.expression().accept(this);
    }
    
    @Override
    public String visitVariableDeclaration(VariableDeclarationNode node) {
        return node.variable() + " := " + node.value().accept(this);
    }
    
    // ===========================================
    // Advanced Function Nodes
    // ===========================================
    
    @Override
    public String visitPercentileFunction(PercentileFunctionNode node) {
        StringBuilder sb = new StringBuilder();
        sb.append(node.functionName()).append("(");
        sb.append(node.valueExpression().accept(this));
        sb.append(", ").append(node.percentile());
        
        if (node.timeSliceFilter() != null) {
            sb.append(" MIN_TIME_SLICE ").append(node.timeSliceFilter().accept(this));
        }
        
        sb.append(")");
        
        return sb.toString();
    }
    
    @Override
    public String visitPercentileSelection(PercentileSelectionNode node) {
        StringBuilder sb = new StringBuilder();
        sb.append(node.functionName()).append("(");
        sb.append(node.tableName()).append(", ");
        sb.append(node.idField()).append(", ");
        sb.append(node.valueExpression().accept(this));
        sb.append(", ").append(node.percentile());
        sb.append(")");
        
        return sb.toString();
    }
    
    @Override
    public String visitArrayLiteral(ArrayLiteralNode node) {
        String elements = node.elements().stream()
                .map(element -> element.accept(this))
                .reduce((a, b) -> a + ", " + b)
                .orElse("");
        return "[" + elements + "]";
    }
    
    @Override
    public String visitStar(StarNode node) {
        return "*";
    }
    
    @Override
    public String visitWithinCondition(WithinConditionNode node) {
        return node.value().accept(this) + " WITHIN " + node.timeWindow().accept(this) + " OF " + node.referenceTime().accept(this);
    }
    
    @Override
    public String visitCaseExpression(CaseExpressionNode node) {
        StringBuilder sb = new StringBuilder();
        sb.append("CASE");
        
        // Add the expression for simple CASE (CASE expression WHEN ...)
        if (node.expression() != null) {
            sb.append(" ").append(node.expression().accept(this));
        }
        
        // Add WHEN clauses
        for (WhenClauseNode whenClause : node.whenClauses()) {
            sb.append(" WHEN ").append(whenClause.condition().accept(this));
            sb.append(" THEN ").append(whenClause.result().accept(this));
        }
        
        // Add ELSE clause if present
        if (node.elseExpression() != null) {
            sb.append(" ELSE ").append(node.elseExpression().accept(this));
        }
        
        sb.append(" END");
        return sb.toString();
    }
}
