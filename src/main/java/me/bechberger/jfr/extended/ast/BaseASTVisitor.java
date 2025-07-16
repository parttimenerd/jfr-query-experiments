package me.bechberger.jfr.extended.ast;

import me.bechberger.jfr.extended.ast.ASTNodes.*;

/**
 * Base implementation of ASTVisitor that provides default traversal behavior.
 * 
 * This class implements the visitor pattern with proper tree traversal, ensuring
 * that all child nodes are visited. Subclasses can override specific visit methods
 * to customize behavior while inheriting proper traversal for unhandled nodes.
 * 
 * The default behavior is to:
 * 1. Visit all child nodes recursively
 * 2. Return null for the generic type T
 * 
 * Subclasses should override visit methods for node types they care about
 * and call super.visitXxx() if they want to continue traversal to child nodes.
 */
public class BaseASTVisitor<T> implements ASTVisitor<T> {
    
    /**
     * Default return value for visit methods. Can be overridden by subclasses.
     */
    protected T defaultReturn() {
        return null;
    }
    
    // Program structure
    
    @Override
    public T visitProgram(ProgramNode node) {
        for (StatementNode statement : node.statements()) {
            statement.accept(this);
        }
        return defaultReturn();
    }
    
    @Override
    public T visitStatement(StatementNode node) {
        // Delegate to the actual statement type
        return node.accept(this);
    }
    
    @Override
    public T visitAssignment(AssignmentNode node) {
        node.query().accept(this);
        return defaultReturn();
    }
    
    @Override
    public T visitViewDefinition(ViewDefinitionNode node) {
        node.query().accept(this);
        return defaultReturn();
    }
    
    @Override
    public T visitRawJfrQuery(RawJfrQueryNode node) {
        // Raw queries don't have child nodes to traverse
        return defaultReturn();
    }
    
    // Query structure
    
    @Override
    public T visitQuery(QueryNode node) {
        if (node.select() != null) {
            node.select().accept(this);
        }
        if (node.from() != null) {
            node.from().accept(this);
        }
        if (node.where() != null) {
            node.where().accept(this);
        }
        if (node.groupBy() != null) {
            node.groupBy().accept(this);
        }
        if (node.having() != null) {
            node.having().accept(this);
        }
        if (node.orderBy() != null) {
            node.orderBy().accept(this);
        }
        if (node.limit() != null) {
            node.limit().accept(this);
        }
        return defaultReturn();
    }
    
    @Override
    public T visitSelect(SelectNode node) {
        for (SelectItemNode item : node.items()) {
            item.accept(this);
        }
        return defaultReturn();
    }
    
    @Override
    public T visitSelectItem(SelectItemNode node) {
        node.expression().accept(this);
        return defaultReturn();
    }
    
    @Override
    public T visitFrom(FromNode node) {
        for (SourceNodeBase source : node.sources()) {
            source.accept(this);
        }
        return defaultReturn();
    }
    
    @Override
    public T visitSource(SourceNode node) {
        // Simple source nodes don't have child nodes
        return defaultReturn();
    }
    
    @Override
    public T visitSubquerySource(SubquerySourceNode node) {
        node.query().accept(this);
        return defaultReturn();
    }
    
    @Override
    public T visitWhere(WhereNode node) {
        node.condition().accept(this);
        return defaultReturn();
    }
    
    @Override
    public T visitGroupBy(GroupByNode node) {
        for (ExpressionNode field : node.fields()) {
            field.accept(this);
        }
        return defaultReturn();
    }
    
    @Override
    public T visitHaving(HavingNode node) {
        node.condition().accept(this);
        return defaultReturn();
    }
    
    @Override
    public T visitOrderBy(OrderByNode node) {
        for (OrderFieldNode field : node.fields()) {
            field.accept(this);
        }
        return defaultReturn();
    }
    
    @Override
    public T visitOrderField(OrderFieldNode node) {
        node.field().accept(this);
        return defaultReturn();
    }
    
    @Override
    public T visitLimit(LimitNode node) {
        // Limit nodes don't have child nodes
        return defaultReturn();
    }
    
    // Formatting
    
    @Override
    public T visitFormatter(FormatterNode node) {
        for (PropertyNode property : node.properties()) {
            property.accept(this);
        }
        return defaultReturn();
    }
    
    @Override
    public T visitProperty(PropertyNode node) {
        // Property nodes don't have child nodes
        return defaultReturn();
    }
    
    // Expressions
    
    @Override
    public T visitExpression(ExpressionNode node) {
        // Delegate to the actual expression type
        return node.accept(this);
    }
    
    @Override
    public T visitBinaryExpression(BinaryExpressionNode node) {
        node.left().accept(this);
        node.right().accept(this);
        return defaultReturn();
    }
    
    @Override
    public T visitUnaryExpression(UnaryExpressionNode node) {
        node.operand().accept(this);
        return defaultReturn();
    }
    
    @Override
    public T visitFieldAccess(FieldAccessNode node) {
        // Field access nodes don't have child nodes
        return defaultReturn();
    }
    
    @Override
    public T visitFunctionCall(FunctionCallNode node) {
        for (ExpressionNode arg : node.arguments()) {
            arg.accept(this);
        }
        return defaultReturn();
    }
    
    @Override
    public T visitLiteral(LiteralNode node) {
        // Literal nodes don't have child nodes
        return defaultReturn();
    }
    
    @Override
    public T visitIdentifier(IdentifierNode node) {
        // Identifier nodes don't have child nodes
        return defaultReturn();
    }
    
    @Override
    public T visitNestedQuery(NestedQueryNode node) {
        // Nested query content is opaque for traversal purposes
        return defaultReturn();
    }
    
    @Override
    public T visitArrayLiteral(ArrayLiteralNode node) {
        for (ExpressionNode element : node.elements()) {
            element.accept(this);
        }
        return defaultReturn();
    }
    
    @Override
    public T visitStar(StarNode node) {
        // Star node has no children to traverse
        return defaultReturn();
    }
    
    // Conditions
    
    @Override
    public T visitCondition(ConditionNode node) {
        // Delegate to the actual condition type
        return node.accept(this);
    }
    
    @Override
    public T visitVariableDeclaration(VariableDeclarationNode node) {
        // Variable declaration nodes don't have child expressions
        return defaultReturn();
    }
    
    @Override
    public T visitExpressionCondition(ExpressionConditionNode node) {
        node.expression().accept(this);
        return defaultReturn();
    }
    
    // Help and information
    
    @Override
    public T visitShowEvents(ShowEventsNode node) {
        // Show events nodes don't have child nodes
        return defaultReturn();
    }
    
    @Override
    public T visitShowFields(ShowFieldsNode node) {
        // Show fields nodes don't have child nodes
        return defaultReturn();
    }
    
    @Override
    public T visitHelp(HelpNode node) {
        // Help nodes don't have child nodes
        return defaultReturn();
    }
    
    @Override
    public T visitHelpFunction(HelpFunctionNode node) {
        // Help function nodes don't have child nodes
        return defaultReturn();
    }
    
    @Override
    public T visitHelpGrammar(HelpGrammarNode node) {
        // Help grammar nodes don't have child nodes
        return defaultReturn();
    }
    
    // Join sources
    
    @Override
    public T visitFuzzyJoinSource(FuzzyJoinSourceNode node) {
        // Visit the tolerance and threshold expressions
        if (node.tolerance() != null) {
            node.tolerance().accept(this);
        }
        if (node.threshold() != null) {
            node.threshold().accept(this);
        }
        return defaultReturn();
    }
    
    @Override
    public T visitStandardJoinSource(StandardJoinSourceNode node) {
        // Standard join fields are strings, not expressions, so no traversal needed
        return defaultReturn();
    }
    
    // Percentile functions
    
    @Override
    public T visitPercentileFunction(PercentileFunctionNode node) {
        node.valueExpression().accept(this);
        if (node.timeSliceFilter() != null) {
            node.timeSliceFilter().accept(this);
        }
        return defaultReturn();
    }
    
    @Override
    public T visitPercentileSelection(PercentileSelectionNode node) {
        node.valueExpression().accept(this);
        return defaultReturn();
    }
    
    // Special conditions
    
    @Override
    public T visitWithinCondition(WithinConditionNode node) {
        node.value().accept(this);
        node.timeWindow().accept(this);
        node.referenceTime().accept(this);
        return defaultReturn();
    }
    
    @Override
    public T visitCaseExpression(CaseExpressionNode node) {
        // Visit the expression (for simple CASE)
        if (node.expression() != null) {
            node.expression().accept(this);
        }
        
        // Visit all when clauses
        for (var whenClause : node.whenClauses()) {
            whenClause.condition().accept(this);
            whenClause.result().accept(this);
        }
        
        // Visit the else expression
        if (node.elseExpression() != null) {
            node.elseExpression().accept(this);
        }
        
        return defaultReturn();
    }
}
