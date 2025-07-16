package me.bechberger.jfr.extended.engine;

import me.bechberger.jfr.extended.ast.ASTNodes.*;
import me.bechberger.jfr.extended.ast.ASTVisitor;
import me.bechberger.jfr.extended.engine.util.StringSimilarity;
import me.bechberger.jfr.extended.evaluator.FunctionRegistry;
import me.bechberger.jfr.extended.evaluator.FunctionRegistry.FunctionType;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.HashSet;

/**
 * Semantic validator for JFR queries.
 * 
 * This class validates the semantic correctness of queries after parsing,
 * including rules such as:
 * - No aggregate functions in WHERE clause
 * - Proper function usage and argument validation
 * - Field reference validation
 * - No aggregate functions in ORDER BY clause without GROUP BY clause
 * - Runtime error detection (non-existent fields/functions, division by zero, etc.)
 */
public class QuerySemanticValidator {
    
    // Common error messages
    private static final String AGGREGATE_IN_WHERE_MSG = "Aggregate functions cannot be used in WHERE clause.";
    private static final String AGGREGATE_IN_WHERE_SUGGESTION = "Use aggregate functions in SELECT, HAVING, or ORDER BY (with GROUP BY) clauses instead.";
    private static final String AGGREGATE_IN_WHERE_CONTEXT = "WHERE clause should contain field comparisons and logical operations";
    private static final String AGGREGATE_IN_WHERE_EXAMPLES = "Examples: WHERE field > value; SELECT COUNT(*) FROM table; SELECT * FROM table GROUP BY field HAVING COUNT(*) > 5";
    
    // Percentile function names
    private static final Set<String> PERCENTILE_FUNCTION_NAMES = Set.of("P99", "P95", "P90", "P75", "P50", "PERCENTILE");
    
    private final FunctionRegistry functionRegistry;
    private final List<SemanticError> errors;
    
    public QuerySemanticValidator() {
        this.functionRegistry = FunctionRegistry.getInstance();
        this.errors = new ArrayList<>();
    }
    
    /**
     * Validates a query node for semantic correctness.
     * 
     * @param query The query node to validate
     * @throws QuerySemanticException if semantic validation fails
     */
    public void validate(QueryNode query) throws QuerySemanticException {
        errors.clear();
        
        // Validate the main query and all nested subqueries recursively
        validateQueryRecursively(query);
        
        // If any errors were found, throw an exception
        if (!errors.isEmpty()) {
            throw new QuerySemanticException(errors);
        }
    }
    
    /**
     * Recursively validates a query and all its nested subqueries.
     */
    private void validateQueryRecursively(QueryNode query) {
        // Validate main query components
        validateWhereClause(query);
        validateOrderByWithGroupBy(query);
        validateOrderByWithoutGroupBy(query);
        validateSelectWithGroupBy(query);
        validateFunctionCalls(query);
        
        // Validate nested subqueries
        SubqueryValidator subqueryValidator = new SubqueryValidator();
        query.accept(subqueryValidator);
    }
    
    /**
     * Utility method to collect field names from a list of expressions.
     */
    private Set<String> collectFieldsFromExpressions(Iterable<ExpressionNode> expressions) {
        Set<String> fields = new HashSet<>();
        for (ExpressionNode expr : expressions) {
            FieldCollector collector = new FieldCollector();
            expr.accept(collector);
            fields.addAll(collector.getFields());
        }
        return fields;
    }
    
    /**
     * Utility method to collect field names from a single expression.
     */
    private Set<String> collectFieldsFromExpression(ExpressionNode expression) {
        FieldCollector collector = new FieldCollector();
        expression.accept(collector);
        return collector.getFields();
    }

    /**
     * Helper visitor to collect all field references in an expression.
     * Uses a base visitor that traverses all children by default.
     */
    private static class FieldCollector extends BaseASTVisitor<Void> {
        private final Set<String> fields = new HashSet<>();
        public Set<String> getFields() { return fields; }

        @Override
        public Void visitIdentifier(IdentifierNode node) {
            fields.add(node.name());
            return super.visitIdentifier(node);
        }

        @Override
        public Void visitFieldAccess(FieldAccessNode node) {
            fields.add(node.field());
            return super.visitFieldAccess(node);
        }
    }

    /**
     * Base visitor that traverses all children by default.
     * Extend this for analysis/collection tasks.
     */
    private static class BaseASTVisitor<T> implements ASTVisitor<T> {
        // For each node type, call accept on children if present
        @Override public T visitProgram(ProgramNode node) {
            for (StatementNode stmt : node.statements()) stmt.accept(this);
            return null;
        }
        @Override public T visitStatement(StatementNode node) { return null; }
        @Override public T visitQuery(QueryNode node) {
            if (node.select() != null) node.select().accept(this);
            if (node.where() != null) node.where().accept(this);
            if (node.groupBy() != null) node.groupBy().accept(this);
            if (node.having() != null) node.having().accept(this);
            if (node.orderBy() != null) node.orderBy().accept(this);
            return null;
        }
        @Override public T visitSelect(SelectNode node) {
            for (SelectItemNode item : node.items()) item.accept(this);
            return null;
        }
        @Override public T visitSelectItem(SelectItemNode node) {
            node.expression().accept(this);
            return null;
        }
        @Override public T visitWhere(WhereNode node) {
            node.condition().accept(this);
            return null;
        }
        @Override public T visitGroupBy(GroupByNode node) {
            for (ExpressionNode expr : node.fields()) expr.accept(this);
            return null;
        }
        @Override public T visitHaving(HavingNode node) {
            node.condition().accept(this);
            return null;
        }
        @Override public T visitOrderBy(OrderByNode node) {
            for (OrderFieldNode field : node.fields()) field.accept(this);
            return null;
        }
        @Override public T visitOrderField(OrderFieldNode node) {
            node.field().accept(this);
            return null;
        }
        @Override public T visitLimit(LimitNode node) { return null; }
        @Override public T visitFormatter(FormatterNode node) { return null; }
        @Override public T visitProperty(PropertyNode node) { return null; }
        @Override public T visitExpression(ExpressionNode node) { return null; }
        @Override public T visitIdentifier(IdentifierNode node) { return null; }
        @Override public T visitFieldAccess(FieldAccessNode node) { return null; }
        @Override public T visitFunctionCall(FunctionCallNode node) {
            for (ExpressionNode arg : node.arguments()) arg.accept(this);
            return null;
        }
        @Override public T visitBinaryExpression(BinaryExpressionNode node) {
            node.left().accept(this);
            node.right().accept(this);
            return null;
        }
        @Override public T visitUnaryExpression(UnaryExpressionNode node) {
            node.operand().accept(this);
            return null;
        }
        @Override public T visitLiteral(LiteralNode node) { return null; }
        @Override public T visitArrayLiteral(ArrayLiteralNode node) {
            for (ExpressionNode elem : node.elements()) elem.accept(this);
            return null;
        }
        @Override public T visitStar(StarNode node) { return null; }
        @Override public T visitCondition(ConditionNode node) { return null; }
        @Override public T visitExpressionCondition(ExpressionConditionNode node) {
            node.expression().accept(this);
            return null;
        }
        @Override public T visitSubquerySource(SubquerySourceNode node) { return null; }
        @Override public T visitSource(SourceNode node) { return null; }
        @Override public T visitFrom(FromNode node) { return null; }
        @Override public T visitShowEvents(ShowEventsNode node) { return null; }
        @Override public T visitShowFields(ShowFieldsNode node) { return null; }
        @Override public T visitHelp(HelpNode node) { return null; }
        @Override public T visitHelpFunction(HelpFunctionNode node) { return null; }
        @Override public T visitHelpGrammar(HelpGrammarNode node) { return null; }
        @Override public T visitFuzzyJoinSource(FuzzyJoinSourceNode node) { return null; }
        @Override public T visitVariableDeclaration(VariableDeclarationNode node) { return null; }
        @Override public T visitAssignment(AssignmentNode node) { return null; }
        @Override public T visitViewDefinition(ViewDefinitionNode node) { return null; }
        @Override public T visitPercentileFunction(PercentileFunctionNode node) {
            node.valueExpression().accept(this);
            if (node.timeSliceFilter() != null) node.timeSliceFilter().accept(this);
            return null;
        }
        @Override public T visitPercentileSelection(PercentileSelectionNode node) {
            node.valueExpression().accept(this);
            return null;
        }
        @Override public T visitCaseExpression(CaseExpressionNode node) {
            if (node.expression() != null) node.expression().accept(this);
            for (var whenClause : node.whenClauses()) {
                whenClause.condition().accept(this);
                whenClause.result().accept(this);
            }
            if (node.elseExpression() != null) node.elseExpression().accept(this);
            return null;
        }
        @Override public T visitWithinCondition(WithinConditionNode node) { return null; }
        @Override public T visitStandardJoinSource(StandardJoinSourceNode node) { return null; }
        @Override public T visitNestedQuery(NestedQueryNode node) { return null; }
        @Override public T visitRawJfrQuery(RawJfrQueryNode node) { return null; }
    }
    
    /**
     * Validates that fields used in ORDER BY expressions are either:
     * 1. Present in the GROUP BY clause, or
     * 2. Aggregate functions, or
     * 3. Aliases referring to grouped fields or aggregate functions
     * when a GROUP BY clause exists.
     */
    private void validateOrderByWithGroupBy(QueryNode query) {
        // If there's no ORDER BY clause, nothing to validate
        if (query.orderBy() == null) {
            return;
        }
        
        // If there's no GROUP BY clause, this validation doesn't apply
        if (query.groupBy() == null) {
            return;
        }
        
        // Collect grouped fields
        Set<String> groupedFields = collectFieldsFromExpressions(query.groupBy().fields());
        
        // Build alias mapping from SELECT clause
        AliasMapping aliasMapping = buildAliasMapping(query, groupedFields);
        
        // Check each ORDER BY expression
        for (OrderFieldNode orderField : query.orderBy().fields()) {
            validateOrderByExpression(orderField.field(), groupedFields, aliasMapping);
        }
    }
    
    /**
     * Validates a single ORDER BY expression against grouped fields and aliases.
     */
    private void validateOrderByExpression(ExpressionNode expression, Set<String> groupedFields, AliasMapping aliasMapping) {
        // First check if this is a simple identifier that might be an alias
        if (expression instanceof IdentifierNode identifier) {
            String fieldName = identifier.name();
            
            // Check if it's an alias
            if (aliasMapping.isAlias(fieldName)) {
                // If it's an alias, check what the alias refers to
                if (aliasMapping.isAliasForAggregate(fieldName) || aliasMapping.isAliasForGroupedField(fieldName)) {
                    return; // Valid alias reference
                }
                // If alias refers to non-grouped field, it's an error
                errors.add(new SemanticError(
                    "Alias '" + fieldName + "' refers to a field that is not in GROUP BY clause",
                    "Either add the underlying field to GROUP BY or use an aggregate function",
                    expression,
                    "Aliases in ORDER BY must refer to grouped fields or aggregate functions when GROUP BY is present",
                    SemanticErrorType.ORDER_BY_NON_GROUPED_FIELD,
                    "Examples: SELECT name, COUNT(*) as count FROM table GROUP BY name ORDER BY count; or SELECT name, age FROM table GROUP BY name, age ORDER BY age"
                ));
                return;
            }
            
            // If not an alias, fall through to regular field validation
        }
        
        // Check if expression contains aggregate functions - if so, it's valid
        FunctionAnalysisResult aggregateResult = analyzeAggregateFunctions(expression);
        if (!aggregateResult.aggregateFunctions.isEmpty() || !aggregateResult.percentileFunctions.isEmpty()) {
            return; // Aggregate functions are allowed
        }
        
        // Collect all field references in the ORDER BY expression
        Set<String> orderByFields = collectFieldsFromExpression(expression);
        
        // Check if all fields in ORDER BY are present in GROUP BY
        for (String field : orderByFields) {
            if (!groupedFields.contains(field)) {
                errors.add(new SemanticError(
                    "Field '" + field + "' in ORDER BY expression must appear in GROUP BY clause or be an aggregate function",
                    "Add '" + field + "' to the GROUP BY clause, or use an aggregate function like COUNT(), SUM(), etc.",
                    expression,
                    "When GROUP BY is present, ORDER BY can only reference grouped fields or aggregate functions",
                    SemanticErrorType.ORDER_BY_NON_GROUPED_FIELD,
                    "Examples: GROUP BY field1 ORDER BY field1; or GROUP BY field1 ORDER BY COUNT(*); or GROUP BY field1, field2 ORDER BY field1, field2"
                ));
            }
        }
    }

    /**
     * Validates that ORDER BY clause does not contain aggregate functions 
     * when there is no GROUP BY clause.
     */
    private void validateOrderByWithoutGroupBy(QueryNode query) {
        // If there's no ORDER BY clause, nothing to validate
        if (query.orderBy() == null) {
            return;
        }
        
        // If there's a GROUP BY clause, this validation doesn't apply
        if (query.groupBy() != null) {
            return;
        }
        
        // Check each ORDER BY expression for aggregate functions
        for (OrderFieldNode orderField : query.orderBy().fields()) {
            validateOrderByExpressionWithoutGroupBy(orderField.field());
        }
    }
    
    /**
     * Validates a single ORDER BY expression to ensure it doesn't contain
     * aggregate functions when there's no GROUP BY clause.
     */
    private void validateOrderByExpressionWithoutGroupBy(ExpressionNode expression) {
        // Check if expression contains aggregate functions
        FunctionAnalysisResult aggregateResult = analyzeAggregateFunctions(expression);
        
        // If aggregate functions are found, it's an error
        if (!aggregateResult.aggregateFunctions.isEmpty()) {
            String functionNames = String.join(", ", aggregateResult.aggregateFunctions);
            errors.add(new SemanticError(
                "Aggregate function(s) " + functionNames + " cannot be used in ORDER BY clause without GROUP BY",
                "Add a GROUP BY clause to use aggregate functions in ORDER BY, or remove the aggregate function",
                expression,
                "Aggregate functions in ORDER BY require a GROUP BY clause",
                SemanticErrorType.AGGREGATE_WITHOUT_GROUP_BY,
                "Examples: SELECT * FROM table GROUP BY field ORDER BY COUNT(*); or SELECT * FROM table ORDER BY field"
            ));
        }
        
        // If percentile functions are found, it's also an error
        if (!aggregateResult.percentileFunctions.isEmpty()) {
            String functionNames = String.join(", ", aggregateResult.percentileFunctions);
            errors.add(new SemanticError(
                "Percentile function(s) " + functionNames + " cannot be used in ORDER BY clause without GROUP BY",
                "Add a GROUP BY clause to use percentile functions in ORDER BY, or remove the percentile function",
                expression,
                "Percentile functions in ORDER BY require a GROUP BY clause",
                SemanticErrorType.AGGREGATE_WITHOUT_GROUP_BY,
                "Examples: SELECT * FROM table GROUP BY field ORDER BY P99(value); or SELECT * FROM table ORDER BY field"
            ));
        }
    }

    /**
     * Validates that fields used in SELECT expressions are either:
     * 1. Present in the GROUP BY clause, or
     * 2. Aggregate functions
     * when a GROUP BY clause exists.
     */
    private void validateSelectWithGroupBy(QueryNode query) {
        // If there's no SELECT clause, nothing to validate
        if (query.select() == null) {
            return;
        }
        
        // If there's no GROUP BY clause, this validation doesn't apply
        if (query.groupBy() == null) {
            return;
        }
        
        // Collect grouped fields
        Set<String> groupedFields = collectFieldsFromExpressions(query.groupBy().fields());
        
        // Check each SELECT expression
        for (SelectItemNode item : query.select().items()) {
            validateSelectExpression(item.expression(), groupedFields);
        }
    }
    
    /**
     * Validates a single SELECT expression against grouped fields.
     */
    private void validateSelectExpression(ExpressionNode expression, Set<String> groupedFields) {
        // Check if expression contains aggregate functions - if so, it's valid
        FunctionAnalysisResult aggregateResult = analyzeAggregateFunctions(expression);
        if (!aggregateResult.aggregateFunctions.isEmpty() || !aggregateResult.percentileFunctions.isEmpty()) {
            return; // Aggregate functions are allowed
        }
        
        // Collect all field references in the SELECT expression
        Set<String> selectFields = collectFieldsFromExpression(expression);
        
        // Check if all fields in SELECT are present in GROUP BY
        for (String field : selectFields) {
            if (!groupedFields.contains(field)) {
                errors.add(new SemanticError(
                    "Field '" + field + "' in SELECT expression must appear in GROUP BY clause or be an aggregate function",
                    "Add '" + field + "' to the GROUP BY clause, or use an aggregate function like COUNT(), SUM(), etc.",
                    expression,
                    "When GROUP BY is present, SELECT can only reference grouped fields or aggregate functions",
                    SemanticErrorType.ORDER_BY_NON_GROUPED_FIELD, // Reusing error type for consistency
                    "Examples: SELECT field1, COUNT(*) FROM table GROUP BY field1; or SELECT field1, field2 FROM table GROUP BY field1, field2"
                ));
            }
        }
    }
    
    /**
     * Builds a mapping of aliases to their underlying expressions for validation purposes.
     */
    private AliasMapping buildAliasMapping(QueryNode query, Set<String> groupedFields) {
        AliasMapping mapping = new AliasMapping();
        
        if (query.select() != null) {
            for (SelectItemNode item : query.select().items()) {
                if (item.alias() != null) {
                    String alias = item.alias();
                    ExpressionNode expression = item.expression();
                    
                    // Check if this expression is an aggregate function
                    FunctionAnalysisResult aggregateResult = analyzeAggregateFunctions(expression);
                    boolean isAggregate = !aggregateResult.aggregateFunctions.isEmpty() || !aggregateResult.percentileFunctions.isEmpty();
                    
                    if (isAggregate) {
                        mapping.addAggregateAlias(alias);
                    } else {
                        // Check if all fields in the expression are grouped
                        Set<String> expressionFields = collectFieldsFromExpression(expression);
                        
                        boolean allFieldsGrouped = expressionFields.isEmpty() || groupedFields.containsAll(expressionFields);
                        if (allFieldsGrouped) {
                            mapping.addGroupedFieldAlias(alias);
                        } else {
                            mapping.addNonGroupedFieldAlias(alias);
                        }
                    }
                }
            }
        }
        
        return mapping;
    }
    
    /**
     * Helper class to track alias mappings and their types.
     */
    private static class AliasMapping {
        private final Set<String> aggregateAliases = new HashSet<>();
        private final Set<String> groupedFieldAliases = new HashSet<>();
        private final Set<String> nonGroupedFieldAliases = new HashSet<>();
        
        public void addAggregateAlias(String alias) {
            aggregateAliases.add(alias);
        }
        
        public void addGroupedFieldAlias(String alias) {
            groupedFieldAliases.add(alias);
        }
        
        public void addNonGroupedFieldAlias(String alias) {
            nonGroupedFieldAliases.add(alias);
        }
        
        public boolean isAlias(String name) {
            return aggregateAliases.contains(name) || 
                   groupedFieldAliases.contains(name) || 
                   nonGroupedFieldAliases.contains(name);
        }
        
        public boolean isAliasForAggregate(String alias) {
            return aggregateAliases.contains(alias);
        }
        
        public boolean isAliasForGroupedField(String alias) {
            return groupedFieldAliases.contains(alias);
        }
    }
    
    /**
     * Validates WHERE clause conditions, ensuring aggregate functions are not used.
     * Aggregate functions are only allowed in SELECT, HAVING, and ORDER BY (with GROUP BY) clauses.
     */
    private void validateWhereClause(QueryNode query) {
        if (query.where() != null) {
            if (containsAggregateFunction(query.where().condition())) {
                // Extract ExpressionNode for error reporting
                ExpressionNode errorExpression = null;
                if (query.where().condition() instanceof ExpressionConditionNode exprCondition) {
                    errorExpression = exprCondition.expression();
                }
                
                // Use a dummy identifier if we can't extract the expression
                if (errorExpression == null) {
                    errorExpression = new IdentifierNode("WHERE_CONDITION", query.where().location());
                }
                
                errors.add(new SemanticError(
                    AGGREGATE_IN_WHERE_MSG,
                    AGGREGATE_IN_WHERE_SUGGESTION,
                    errorExpression,
                    AGGREGATE_IN_WHERE_CONTEXT,
                    SemanticErrorType.AGGREGATE_IN_WHERE_CLAUSE,
                    AGGREGATE_IN_WHERE_EXAMPLES
                ));
            }
        }
    }
    
    /**
     * Helper method to check if a condition contains aggregate functions.
     */
    private boolean containsAggregateFunction(ConditionNode condition) {
        if (condition instanceof ExpressionConditionNode exprCondition) {
            return containsAggregateFunction(exprCondition.expression());
        }
        return false;
    }
    
    /**
     * Helper method to check if an expression contains aggregate functions.
     */
    private boolean containsAggregateFunction(ExpressionNode expression) {
        if (expression instanceof FunctionCallNode functionCall) {
            // Check if this function is an aggregate function
            String functionName = functionCall.functionName().toUpperCase();
            boolean isAggregate = functionRegistry.getAllFunctions().stream()
                .anyMatch(f -> f.name().equals(functionName) && f.type() == FunctionRegistry.FunctionType.AGGREGATE);
            
            if (isAggregate) {
                return true;
            }
            
            // Check percentile functions
            if (PERCENTILE_FUNCTION_NAMES.contains(functionName)) {
                return true;
            }
            
            // Recursively check function arguments
            for (ExpressionNode arg : functionCall.arguments()) {
                if (containsAggregateFunction(arg)) {
                    return true;
                }
            }
        } else if (expression instanceof PercentileFunctionNode) {
            // Percentile functions are aggregate-like functions that should not be in WHERE clause
            return true;
        } else if (expression instanceof PercentileSelectionNode) {
            // Percentile selection functions are also aggregate-like
            return true;
        } else if (expression instanceof BinaryExpressionNode binaryExpr) {
            return containsAggregateFunction(binaryExpr.left()) || 
                   containsAggregateFunction(binaryExpr.right());
        } else if (expression instanceof UnaryExpressionNode unaryExpr) {
            return containsAggregateFunction(unaryExpr.operand());
        }
        
        return false;
    }
    
    /**
     * Validates that all function calls in the query are valid.
     * This includes checking function names, argument counts, and types.
     */
    private void validateFunctionCalls(QueryNode query) {
        FunctionCallValidator validator = new FunctionCallValidator(errors, functionRegistry);
        
        // Validate function calls in SELECT clause
        if (query.select() != null) {
            for (SelectItemNode item : query.select().items()) {
                item.expression().accept(validator);
            }
        }
        
        // Validate function calls in WHERE clause
        if (query.where() != null) {
            validateConditionFunctionCalls(query.where().condition(), validator);
        }
        
        // Validate function calls in GROUP BY clause
        if (query.groupBy() != null) {
            for (ExpressionNode expr : query.groupBy().fields()) {
                expr.accept(validator);
            }
        }
        
        // Validate function calls in HAVING clause
        if (query.having() != null) {
            validateConditionFunctionCalls(query.having().condition(), validator);
        }
        
        // Validate function calls in ORDER BY clause
        if (query.orderBy() != null) {
            for (OrderFieldNode orderField : query.orderBy().fields()) {
                orderField.field().accept(validator);
            }
        }
    }
    
    /**
     * Validates function calls within condition nodes.
     */
    private void validateConditionFunctionCalls(ConditionNode condition, FunctionCallValidator validator) {
        if (condition instanceof ExpressionConditionNode exprCondition) {
            exprCondition.expression().accept(validator);
        }
    }
    
    /**
     * Result of analyzing an expression for aggregate and percentile functions.
     */
    private static class FunctionAnalysisResult {
        public final List<String> aggregateFunctions;
        public final List<String> percentileFunctions;
        
        public FunctionAnalysisResult(List<String> aggregateFunctions, List<String> percentileFunctions) {
            this.aggregateFunctions = aggregateFunctions;
            this.percentileFunctions = percentileFunctions;
        }
    }
    
    /**
     * Recursively searches an expression for aggregate and percentile functions.
     * 
     * @param expression The expression to search
     * @return A result containing lists of aggregate and percentile function names found in the expression
     */
    private FunctionAnalysisResult analyzeAggregateFunctions(ExpressionNode expression) {
        AggregateFunctionFinder finder = new AggregateFunctionFinder();
        expression.accept(finder);
        return new FunctionAnalysisResult(finder.getAggregateFunctions(), finder.getPercentileFunctions());
    }
    
    /**
     * Semantic error types for better categorization
     */
    public enum SemanticErrorType {
        AGGREGATE_WITHOUT_GROUP_BY,
        ORDER_BY_NON_GROUPED_FIELD,
        INVALID_FUNCTION_CALL,
        INVALID_FIELD_REFERENCE,
        DIVISION_BY_ZERO,
        FIELD_ACCESS_WITHOUT_ALIAS,
        AGGREGATE_IN_WHERE_CLAUSE,
        RUNTIME_ERROR
    }
    
    /**
     * Represents a semantic error with enhanced information similar to ParserError
     */
    public static class SemanticError {
        private final String message;
        private final String suggestion;
        private final ExpressionNode errorNode;
        private final String context;
        private final SemanticErrorType type;
        private final String examples;
        
        public SemanticError(String message, String suggestion, ExpressionNode errorNode, String context, SemanticErrorType type) {
            this(message, suggestion, errorNode, context, type, null);
        }
        
        public SemanticError(String message, String suggestion, ExpressionNode errorNode, String context, SemanticErrorType type, String examples) {
            this.message = message;
            this.suggestion = suggestion;
            this.errorNode = errorNode;
            this.context = context;
            this.type = type;
            this.examples = examples;
        }
        
        public String getMessage() { return message; }
        public String getSuggestion() { return suggestion; }
        public ExpressionNode getErrorNode() { return errorNode; }
        public String getContext() { return context; }
        public SemanticErrorType getType() { return type; }
        public String getExamples() { return examples; }
        
        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("Semantic Error at line ").append(errorNode.getLine()).append(", column ").append(errorNode.getColumn()).append(":\n");
            if (context != null && !context.isEmpty()) {
                sb.append("Context: ").append(context).append("\n");
            }
            sb.append(message).append("\n");
            if (suggestion != null && !suggestion.isEmpty()) {
                sb.append("Suggestion: ").append(suggestion).append("\n");
            }
            if (examples != null && !examples.isEmpty()) {
                sb.append("Examples: ").append(examples).append("\n");
            }
            return sb.toString();
        }
    }
    
    /**
     * Exception thrown when semantic validation fails.
     * Can contain multiple errors for comprehensive reporting.
     */
    public static class QuerySemanticException extends RuntimeException {
        private final List<SemanticError> errors;
        
        public QuerySemanticException(List<SemanticError> errors) {
            super(createErrorMessage(errors));
            this.errors = new ArrayList<>(errors);
        }
        
        public QuerySemanticException(String message, int line, int column) {
            // Legacy constructor for backward compatibility
            super(message + " at line " + line + ", column " + column);
            this.errors = List.of(); // Empty list for legacy usage
        }
        
        public List<SemanticError> getErrors() {
            return new ArrayList<>(errors);
        }
        
        public boolean hasErrors() {
            return !errors.isEmpty();
        }
        
        public String getErrorReport() {
            return createErrorMessage(errors);
        }
        
        private static String createErrorMessage(List<SemanticError> errors) {
            if (errors.isEmpty()) {
                return "No semantic errors found.";
            }
            
            if (errors.size() == 1) {
                return errors.get(0).toString();
            } else {
                StringBuilder report = new StringBuilder();
                int errorCount = Math.min(3, errors.size()); // Limit to top 3 errors
                
                if (errors.size() > 3) {
                    report.append("Found ").append(errors.size()).append(" semantic errors (showing top ").append(errorCount).append("):\n\n");
                } else {
                    report.append("Found ").append(errorCount).append(" semantic errors:\n\n");
                }
                
                for (int i = 0; i < errorCount; i++) {
                    report.append(i + 1).append(".\n").append(errors.get(i).toString());
                    if (i < errorCount - 1) {
                        report.append("\n--------------------------------------------------\n");
                    }
                }
                
                if (errors.size() > 3) {
                    report.append("\n\n(Additional errors detected but not shown to avoid clutter)");
                }
                
                return report.toString();
            }
        }
    }
    
    /**
     * Visitor that validates function calls in expressions.
     */
    private static class FunctionCallValidator implements ASTVisitor<Void> {
        private final List<SemanticError> errors;
        private final FunctionRegistry functionRegistry;
        
        public FunctionCallValidator(List<SemanticError> errors, FunctionRegistry functionRegistry) {
            this.errors = errors;
            this.functionRegistry = functionRegistry;
        }
        
        @Override
        public Void visitFunctionCall(FunctionCallNode node) {
            String funcName = node.functionName().toUpperCase();
            
            // Check if function exists
            if (!functionRegistry.isFunction(funcName)) {
                // Find similar function names for suggestions
                List<String> suggestions = findSimilarFunctionNames(funcName);
                String suggestion = suggestions.isEmpty() ? 
                    "Check the function name and ensure it's spelled correctly" :
                    "Did you mean: " + String.join(", ", suggestions.subList(0, Math.min(3, suggestions.size()))) + "?";
                    
                errors.add(new SemanticError(
                    "Unknown function: " + funcName,
                    suggestion,
                    node,
                    "Function calls must use valid, registered function names",
                    SemanticErrorType.INVALID_FUNCTION_CALL,
                    "Available functions can be listed with 'HELP FUNCTIONS'"
                ));
            } else {
                // Function exists, validate argument count and types if possible
                FunctionRegistry.FunctionDefinition funcDef = functionRegistry.getFunction(funcName);
                if (funcDef != null) {
                    validateFunctionArguments(node, funcDef);
                }
            }
            
            // Recursively validate function arguments
            for (ExpressionNode arg : node.arguments()) {
                arg.accept(this);
            }
            
            return null;
        }
        
        private void validateFunctionArguments(FunctionCallNode node, FunctionRegistry.FunctionDefinition funcDef) {
            int provided = node.arguments().size();
            int required = (int) funcDef.parameters().stream().filter(p -> !p.optional()).count();
            int maximum = funcDef.parameters().size();
            
            // Check if any parameter is variadic
            boolean hasVariadic = funcDef.parameters().stream().anyMatch(p -> p.variadic());
            
            if (provided < required) {
                errors.add(new SemanticError(
                    "Function " + node.functionName() + " expects at least " + required + " argument(s) but received " + provided,
                    "Add " + (required - provided) + " more argument(s) to the function call",
                    node,
                    "Function calls must provide all required parameters",
                    SemanticErrorType.INVALID_FUNCTION_CALL,
                    "Usage: " + funcDef.signature()
                ));
            } else if (!hasVariadic && provided > maximum) {
                errors.add(new SemanticError(
                    "Function " + node.functionName() + " expects at most " + maximum + " argument(s) but received " + provided,
                    "Remove " + (provided - maximum) + " argument(s) from the function call",
                    node,
                    "Function calls cannot exceed the maximum number of parameters",
                    SemanticErrorType.INVALID_FUNCTION_CALL,
                    "Usage: " + funcDef.signature()
                ));
            }
        }
        
        private List<String> findSimilarFunctionNames(String funcName) {
            // Simple similarity check - in a real implementation you might use Levenshtein distance
            List<String> similar = new ArrayList<>();
            for (var funcDef : functionRegistry.getAllFunctions()) {
                String registeredName = funcDef.name();
                if (registeredName.toLowerCase().contains(funcName.toLowerCase()) || 
                    funcName.toLowerCase().contains(registeredName.toLowerCase()) ||
                    StringSimilarity.levenshteinDistanceIgnoreCase(funcName, registeredName) <= 2) {
                    similar.add(registeredName);
                }
            }
            return similar;
        }
        
        // Other visitor methods that don't need special handling
        @Override public Void visitProgram(ProgramNode node) { return null; }
        @Override public Void visitStatement(StatementNode node) { return null; }
        @Override public Void visitAssignment(AssignmentNode node) { return null; }
        @Override public Void visitViewDefinition(ViewDefinitionNode node) { return null; }
        @Override public Void visitRawJfrQuery(RawJfrQueryNode node) { return null; }
        @Override public Void visitQuery(QueryNode node) { return null; }
        @Override public Void visitSelect(SelectNode node) { return null; }
        @Override public Void visitSelectItem(SelectItemNode node) { return null; }
        @Override public Void visitFrom(FromNode node) { return null; }
        @Override public Void visitSource(SourceNode node) { return null; }
        @Override public Void visitSubquerySource(SubquerySourceNode node) { return null; }
        @Override public Void visitWhere(WhereNode node) { return null; }
        @Override public Void visitGroupBy(GroupByNode node) { return null; }
        @Override public Void visitHaving(HavingNode node) { return null; }
        @Override public Void visitOrderBy(OrderByNode node) { return null; }
        @Override public Void visitOrderField(OrderFieldNode node) { return null; }
        @Override public Void visitLimit(LimitNode node) { return null; }
        @Override public Void visitFormatter(FormatterNode node) { return null; }
        @Override public Void visitProperty(PropertyNode node) { return null; }
        @Override public Void visitExpression(ExpressionNode node) { return null; }
        
        @Override
        public Void visitBinaryExpression(BinaryExpressionNode node) {
            // Check for division by zero
            if (node.operator() == BinaryOperator.DIVIDE) {
                if (node.right() instanceof LiteralNode literal) {
                    if (literal.value().equals(0) || literal.value().equals(0.0) || literal.value().equals("0")) {
                        errors.add(new SemanticError(
                            "Division by zero is not allowed",
                            "Use a non-zero divisor in the expression",
                            node,
                            "Division by zero will cause runtime errors",
                            SemanticErrorType.DIVISION_BY_ZERO,
                            "Examples: age / 2; price / quantity"
                        ));
                    }
                }
            }
            
            node.left().accept(this);
            node.right().accept(this);
            return null;
        }
        
        @Override
        public Void visitUnaryExpression(UnaryExpressionNode node) {
            node.operand().accept(this);
            return null;
        }
        
        @Override 
        public Void visitFieldAccess(FieldAccessNode node) { 
            // Validate field access - check if qualifier is properly defined
            String qualifier = node.qualifier();
            if (qualifier != null && !qualifier.isEmpty()) {
                // Check for common mistakes like using table name instead of alias
                if (qualifier.toLowerCase().contains("users") || qualifier.toLowerCase().contains("employees")) {
                    errors.add(new SemanticError(
                        "Invalid field access: '" + qualifier + "." + node.field() + "'",
                        "Use a proper table alias or remove the prefix for direct field access",
                        node,
                        "Field access requires proper alias definition in FROM clause",
                        SemanticErrorType.FIELD_ACCESS_WITHOUT_ALIAS,
                        "Examples: FROM table AS t, then t.field; or just use field directly"
                    ));
                }
            }
            return null; 
        }
        @Override public Void visitLiteral(LiteralNode node) { return null; }
        @Override public Void visitIdentifier(IdentifierNode node) { return null; }
        @Override public Void visitNestedQuery(NestedQueryNode node) { return null; }
        
        @Override
        public Void visitArrayLiteral(ArrayLiteralNode node) {
            for (ExpressionNode element : node.elements()) {
                element.accept(this);
            }
            return null;
        }
        
        @Override
        public Void visitStar(StarNode node) {
            // Star node is valid in function calls like COUNT(*)
            return null;
        }
        
        @Override public Void visitCondition(ConditionNode node) { return null; }
        @Override public Void visitVariableDeclaration(VariableDeclarationNode node) { return null; }
        @Override public Void visitExpressionCondition(ExpressionConditionNode node) { 
            // Visit the inner expression to find aggregate functions
            node.expression().accept(this);
            return null; 
        }
        @Override public Void visitShowEvents(ShowEventsNode node) { return null; }
        @Override public Void visitShowFields(ShowFieldsNode node) { return null; }
        @Override public Void visitHelp(HelpNode node) { return null; }
        @Override public Void visitHelpFunction(HelpFunctionNode node) { return null; }
        @Override public Void visitHelpGrammar(HelpGrammarNode node) { return null; }
        @Override public Void visitFuzzyJoinSource(FuzzyJoinSourceNode node) { return null; }
        @Override public Void visitStandardJoinSource(StandardJoinSourceNode node) { return null; }
        
        @Override
        public Void visitPercentileFunction(PercentileFunctionNode node) {
            // Validate value expression and time slice filter
            node.valueExpression().accept(this);
            if (node.timeSliceFilter() != null) {
                node.timeSliceFilter().accept(this);
            }
            return null;
        }
        
        @Override
        public Void visitPercentileSelection(PercentileSelectionNode node) {
            node.valueExpression().accept(this);
            return null;
        }
        
        @Override public Void visitWithinCondition(WithinConditionNode node) { return null; }
        
        @Override
        public Void visitCaseExpression(CaseExpressionNode node) {
            // Validate the expression (for simple CASE) and all when/else expressions
            if (node.expression() != null) {
                node.expression().accept(this);
            }
            for (var whenClause : node.whenClauses()) {
                whenClause.condition().accept(this);
                whenClause.result().accept(this);
            }
            if (node.elseExpression() != null) {
                node.elseExpression().accept(this);
            }
            return null;
        }
    }
    
    /**
     * Simple AST visitor that finds aggregate functions in expressions.
     * Uses the BaseASTVisitor for proper tree traversal.
     */
    private class AggregateFunctionFinder extends BaseASTVisitor<Void> {
        private final List<String> aggregateFunctions = new ArrayList<>();
        private final List<String> percentileFunctions = new ArrayList<>();
        private final Set<String> aggregateFunctionNames = new HashSet<>();
        private final Set<String> percentileFunctionNames = new HashSet<>();
        
        public AggregateFunctionFinder() {
            // Initialize the set of aggregate function names
            for (var functionDef : functionRegistry.getAllFunctions()) {
                if (functionDef.type() == FunctionType.AGGREGATE) {
                    aggregateFunctionNames.add(functionDef.name());
                }
            }
            
            // Initialize percentile function names
            percentileFunctionNames.addAll(PERCENTILE_FUNCTION_NAMES);
        }
        
        public List<String> getAggregateFunctions() {
            return aggregateFunctions;
        }
        
        public List<String> getPercentileFunctions() {
            return percentileFunctions;
        }
        
        @Override
        public Void visitFunctionCall(FunctionCallNode node) {
            // Check if this is a percentile function
            if (percentileFunctionNames.contains(node.functionName().toUpperCase())) {
                percentileFunctions.add(node.functionName().toUpperCase());
            }
            // Check if this is an aggregate function
            else if (aggregateFunctionNames.contains(node.functionName().toUpperCase())) {
                aggregateFunctions.add(node.functionName().toUpperCase());
            }
            
            // Continue traversal to check function arguments
            return super.visitFunctionCall(node);
        }
        
        @Override
        public Void visitPercentileFunction(PercentileFunctionNode node) {
            // Percentile functions are percentile functions
            percentileFunctions.add(node.functionName().toUpperCase());
            
            // Continue traversal
            return super.visitPercentileFunction(node);
        }
        
        @Override
        public Void visitPercentileSelection(PercentileSelectionNode node) {
            // Percentile selection functions are percentile functions
            percentileFunctions.add(node.functionName().toUpperCase());
            
            // Continue traversal
            return super.visitPercentileSelection(node);
        }
    }
    
    /**
     * Visitor that finds and validates all nested subqueries recursively.
     */
    private class SubqueryValidator implements ASTVisitor<Void> {
        
        @Override
        public Void visitProgram(ProgramNode node) {
            for (StatementNode statement : node.statements()) {
                statement.accept(this);
            }
            return null;
        }
        
        @Override public Void visitStatement(StatementNode node) { return null; }
        
        @Override
        public Void visitAssignment(AssignmentNode node) {
            node.query().accept(this);
            return null;
        }
        
        @Override
        public Void visitViewDefinition(ViewDefinitionNode node) {
            node.query().accept(this);
            return null;
        }
        
        @Override
        public Void visitRawJfrQuery(RawJfrQueryNode node) {
            // Raw JFR queries don't have extended subqueries to validate
            return null;
        }
        
        @Override
        public Void visitQuery(QueryNode node) {
            // Traverse all parts of the query to find subqueries
            if (node.from() != null) {
                node.from().accept(this);
            }
            if (node.where() != null) {
                node.where().accept(this);
            }
            if (node.having() != null) {
                node.having().accept(this);
            }
            // Note: SELECT, GROUP BY, ORDER BY can contain expressions that might have subqueries
            if (node.select() != null) {
                node.select().accept(this);
            }
            if (node.groupBy() != null) {
                node.groupBy().accept(this);
            }
            if (node.orderBy() != null) {
                node.orderBy().accept(this);
            }
            return null;
        }
        
        @Override
        public Void visitSelect(SelectNode node) {
            for (SelectItemNode item : node.items()) {
                item.accept(this);
            }
            return null;
        }
        
        @Override
        public Void visitSelectItem(SelectItemNode node) {
            node.expression().accept(this);
            return null;
        }
        
        @Override
        public Void visitFrom(FromNode node) {
            for (SourceNodeBase source : node.sources()) {
                source.accept(this);
            }
            return null;
        }
        
        @Override public Void visitSource(SourceNode node) { return null; }
        
        @Override
        public Void visitSubquerySource(SubquerySourceNode node) {
            // This is where we found a nested subquery!
            StatementNode subqueryStatement = node.query();
            
            // Only validate if it's an extended query (QueryNode)
            if (subqueryStatement instanceof QueryNode) {
                QueryNode subquery = (QueryNode) subqueryStatement;
                
                // Only validate extended queries (@SELECT)
                if (subquery.isExtended()) {
                    validateQueryRecursively(subquery);
                }
            }
            return null;
        }
        
        @Override
        public Void visitWhere(WhereNode node) {
            node.condition().accept(this);
            return null;
        }
        
        @Override
        public Void visitGroupBy(GroupByNode node) {
            for (ExpressionNode field : node.fields()) {
                field.accept(this);
            }
            return null;
        }
        
        @Override
        public Void visitHaving(HavingNode node) {
            node.condition().accept(this);
            return null;
        }
        
        @Override
        public Void visitOrderBy(OrderByNode node) {
            for (OrderFieldNode field : node.fields()) {
                field.accept(this);
            }
            return null;
        }
        
        @Override
        public Void visitOrderField(OrderFieldNode node) {
            node.field().accept(this);
            return null;
        }
        
        @Override public Void visitLimit(LimitNode node) { return null; }
        @Override public Void visitFormatter(FormatterNode node) { return null; }
        @Override public Void visitProperty(PropertyNode node) { return null; }
        @Override public Void visitExpression(ExpressionNode node) { return null; }
        
        @Override
        public Void visitBinaryExpression(BinaryExpressionNode node) {
            node.left().accept(this);
            node.right().accept(this);
            return null;
        }
        
        @Override
        public Void visitUnaryExpression(UnaryExpressionNode node) {
            node.operand().accept(this);
            return null;
        }
        
        @Override public Void visitFieldAccess(FieldAccessNode node) { return null; }
        
        @Override
        public Void visitFunctionCall(FunctionCallNode node) {
            for (ExpressionNode arg : node.arguments()) {
                arg.accept(this);
            }
            return null;
        }
        
        @Override public Void visitLiteral(LiteralNode node) { return null; }
        @Override public Void visitIdentifier(IdentifierNode node) { return null; }
        
        @Override
        public Void visitNestedQuery(NestedQueryNode node) {
            // NestedQueryNode contains a raw JFR query string, not a parsed extended query
            // so it doesn't contain subqueries that need validation
            return null;
        }
        
        @Override
        public Void visitArrayLiteral(ArrayLiteralNode node) {
            for (ExpressionNode element : node.elements()) {
                element.accept(this);
            }
            return null;
        }
        
        @Override
        public Void visitStar(StarNode node) {
            // Star node doesn't contain subqueries
            return null;
        }
        
        @Override public Void visitCondition(ConditionNode node) { return null; }
        @Override public Void visitVariableDeclaration(VariableDeclarationNode node) { return null; }
        
        @Override
        public Void visitExpressionCondition(ExpressionConditionNode node) {
            node.expression().accept(this);
            return null;
        }
        
        @Override public Void visitShowEvents(ShowEventsNode node) { return null; }
        @Override public Void visitShowFields(ShowFieldsNode node) { return null; }
        @Override public Void visitHelp(HelpNode node) { return null; }
        @Override public Void visitHelpFunction(HelpFunctionNode node) { return null; }
        @Override public Void visitHelpGrammar(HelpGrammarNode node) { return null; }
        @Override public Void visitFuzzyJoinSource(FuzzyJoinSourceNode node) { return null; }
        @Override public Void visitStandardJoinSource(StandardJoinSourceNode node) { return null; }
        
        @Override
        public Void visitPercentileFunction(PercentileFunctionNode node) {
            node.valueExpression().accept(this);
            if (node.timeSliceFilter() != null) {
                node.timeSliceFilter().accept(this);
            }
            return null;
        }
        
        @Override
        public Void visitPercentileSelection(PercentileSelectionNode node) {
            node.valueExpression().accept(this);
            return null;
        }
        
        @Override public Void visitWithinCondition(WithinConditionNode node) { return null; }
        
        @Override
        public Void visitCaseExpression(CaseExpressionNode node) {
            // Validate subqueries in all expressions of the case expression
            if (node.expression() != null) {
                node.expression().accept(this);
            }
            for (var whenClause : node.whenClauses()) {
                whenClause.condition().accept(this);
                whenClause.result().accept(this);
            }
            if (node.elseExpression() != null) {
                node.elseExpression().accept(this);
            }
            return null;
        }
    }

}
