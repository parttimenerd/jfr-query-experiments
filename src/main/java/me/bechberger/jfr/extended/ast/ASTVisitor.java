package me.bechberger.jfr.extended.ast;

import me.bechberger.jfr.extended.ast.ASTNodes.*;

/**
 * Visitor interface for traversing the AST
 */
public interface ASTVisitor<T> {
    
    // Program structure
    T visitProgram(ProgramNode node);
    T visitStatement(StatementNode node);
    T visitAssignment(AssignmentNode node);
    T visitViewDefinition(ViewDefinitionNode node);
    T visitRawJfrQuery(RawJfrQueryNode node);
    
    // Query structure
    T visitQuery(QueryNode node);
    T visitSelect(SelectNode node);
    T visitSelectItem(SelectItemNode node);
    T visitFrom(FromNode node);
    T visitSource(SourceNode node);
    T visitSubquerySource(SubquerySourceNode node);
    T visitWhere(WhereNode node);
    T visitGroupBy(GroupByNode node);
    T visitHaving(HavingNode node);
    T visitOrderBy(OrderByNode node);
    T visitOrderField(OrderFieldNode node);
    T visitLimit(LimitNode node);
    
    // Formatting
    T visitFormatter(FormatterNode node);
    T visitProperty(PropertyNode node);
    
    // Expressions
    T visitExpression(ExpressionNode node);
    T visitBinaryExpression(BinaryExpressionNode node);
    T visitUnaryExpression(UnaryExpressionNode node);
    T visitFieldAccess(FieldAccessNode node);
    T visitFunctionCall(FunctionCallNode node);
    T visitLiteral(LiteralNode node);
    T visitIdentifier(IdentifierNode node);
    T visitNestedQuery(NestedQueryNode node);
    T visitArrayLiteral(ArrayLiteralNode node);
    T visitStar(StarNode node);
    
    // Conditions
    T visitCondition(ConditionNode node);
    T visitVariableDeclaration(VariableDeclarationNode node);
    T visitExpressionCondition(ExpressionConditionNode node);
    
    // Special queries
    T visitShowEvents(ShowEventsNode node);
    T visitShowFields(ShowFieldsNode node);
    T visitHelp(HelpNode node);
    T visitHelpFunction(HelpFunctionNode node);
    T visitHelpGrammar(HelpGrammarNode node);

    // New AST nodes
    T visitFuzzyJoinSource(FuzzyJoinSourceNode node);
    T visitStandardJoinSource(StandardJoinSourceNode node);
    T visitPercentileFunction(PercentileFunctionNode node);
    T visitPercentileSelection(PercentileSelectionNode node);
    
    // Case expression
    T visitCaseExpression(CaseExpressionNode node);
    
    // Conditions
    T visitWithinCondition(WithinConditionNode node);
}
