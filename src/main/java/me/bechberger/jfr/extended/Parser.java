package me.bechberger.jfr.extended;

import me.bechberger.jfr.extended.ast.ASTNodes.*;
import me.bechberger.jfr.extended.Lexer.LexerException;
import me.bechberger.jfr.extended.ast.Location;
import me.bechberger.jfr.extended.ast.ASTNodes.ExpressionNode;
import me.bechberger.jfr.extended.ast.ASTNodes.ArrayLiteralNode;
import me.bechberger.jfr.extended.table.CellValue;
import me.bechberger.jfr.util.Utils;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Parser for the extended JFR query language.
 * 
 * <p>This parser implements a recursive descent parser for the JFR query language grammar.
 * It supports both standard SQL-like queries and extended JFR-specific syntax including:
 * <ul>
 *   <li>Fuzzy joins with temporal matching</li>
 *   <li>Percentile functions and selection</li>
 *   <li>Synthetic field references</li>
 *   <li>Extended query markers (@)</li>
 *   <li>Show statements for metadata</li>
 *   <li>View definitions</li>
 * </ul>
 * 
 * <p>The parser provides comprehensive error handling with context-aware error messages
 * and error recovery to continue parsing after encountering errors.
 */
public class Parser {
    
    private final TokenStream tokens;
    private final ParserErrorHandler errorHandler;
    private final QueryErrorMessageGenerator errorMessageGenerator;
    private final FunctionValidator functionValidator;
    private boolean errorRecoveryMode = false;
    private boolean panicMode = false; // Track if we're in panic mode recovery
    
    // Current parsing context for error messages
    private QueryErrorMessageGenerator.QueryContext currentContext = QueryErrorMessageGenerator.QueryContext.EXPRESSION;
    
    // Track valid aliases from FROM clause for field access validation
    private final Set<String> validAliases = new java.util.HashSet<>();
    
    // Synchronization points for error recovery
    private static final java.util.Set<TokenType> SYNCHRONIZATION_TOKENS = java.util.Set.of(
        TokenType.SELECT, TokenType.FROM, TokenType.WHERE, 
        TokenType.GROUP_BY, TokenType.HAVING, TokenType.ORDER_BY, 
        TokenType.LIMIT, TokenType.SEMICOLON, TokenType.EOF
    );

    public Parser(List<Token> tokens) {
        this(tokens, null);
    }
    
    public Parser(String query) throws LexerException {
        this(new Lexer(query).tokenize());
    }

    public Parser(List<Token> tokens, String originalQuery) {
        this.tokens = new TokenStream(tokens);
        this.errorHandler = new ParserErrorHandler(tokens, originalQuery);
        this.errorMessageGenerator = new QueryErrorMessageGenerator();
        this.functionValidator = new FunctionValidator();
    }
    
    /**
     * Helper method to create a Location from line and column
     */
    private Location location(int line, int column) {
        return ParserUtils.location(line, column);
    }
    
    /**
     * Helper method to create a Location from a token
     */
    private Location location(Token token) {
        return ParserUtils.location(token);
    }
    
    /**
     * Parse the tokens into an AST
     */
    public ProgramNode parse() throws ParserException {
        List<StatementNode> statements = new ArrayList<>();
        
        // Check for empty input first
        if (tokens.isAtEnd()) {
            throw new ParserException("Empty query. Please provide a valid JFR query or statement.");
        }
        
        while (!tokens.isAtEnd()) {
            // Skip any leading semicolons
            while (tokens.match(TokenType.SEMICOLON)) {
                // Just consume and continue
            }
            
            if (!tokens.isAtEnd()) {
                try {
                    StatementNode statement = parseStatement();
                    if (statement != null) {
                        statements.add(statement);
                        
                        // After parsing a statement, handle statement separation
                        if (!tokens.isAtEnd()) {
                            if (tokens.check(TokenType.SEMICOLON)) {
                                tokens.advance(); // consume the semicolon
                            } else if (tokens.check(TokenType.SHOW) || tokens.check(TokenType.VIEW) || 
                                      tokens.check(TokenType.EXTENDED_QUERY) || 
                                      (tokens.check(TokenType.IDENTIFIER) && tokens.checkNext(TokenType.ASSIGN)) ||
                                      (tokens.check(TokenType.SYNTHETIC_FIELD) && tokens.checkNext(TokenType.ASSIGN))) {
                                // Next statement starts - this is OK, no separator needed
                            } else if (isNewStatementStart()) {
                                // Check if this could be the start of a new raw query statement
                                // This handles cases where raw queries are separated by empty lines
                                // without requiring explicit semicolons
                                // Continue parsing without error
                            } else {
                                // Error: unexpected token after statement
                                reportUnexpectedToken(tokens.current(), 
                                    "Expected semicolon (;) to separate statements or end of input");
                                
                                // Try to recover by finding the next statement boundary
                                recoverToStatementBoundary();
                            }
                        }
                    }
                } catch (ParserException e) {
                    // Collect the error and try to continue parsing the next statement
                    if (!errorHandler.hasErrors()) {
                        // If no error was already recorded by the error handler, create a more specific one
                        Token errorToken = tokens.current();
                        if (e instanceof PositionedParserException positioned) {
                            errorToken = positioned.getErrorToken();
                        }
                        ParserErrorHandler.ParserError error = errorHandler.createExpressionError(
                            errorToken, 
                            e.getMessage()
                        );
                        errorHandler.addError(error);
                    }
                    
                    // Try to recover to the next statement
                    recoverToStatementBoundary();
                }
            }
        }
        
        // If we collected errors during parsing, create a comprehensive error report
        if (errorHandler.hasErrors()) {
            throw new ParserException(errorHandler.createErrorReport());
        }
        
        return new ProgramNode(statements, location(1, 1));
    }
    
    /**
     * Parse a single statement (entry point for individual statement parsing).
     * 
     * <p><strong>Grammar Rule:</strong></p>
     * <pre>
     * statement ::= show_query
     *             | view_definition
     *             | assignment
     *             | extended_query
     *             | raw_jfr_query
     * 
     * extended_query ::= EXTENDED_QUERY query
     * raw_jfr_query ::= ( any_tokens_until_statement_boundary )
     * </pre>
     * 
     * 
     * <p><strong>Special Cases:</strong></p>
     * <ul>
     *   <li>SHOW queries for metadata (SHOW EVENTS, SHOW FIELDS)</li>
     *   <li>VIEW definitions for query reuse</li>
     *   <li>Variable assignments with := operator</li>
     *   <li>Extended queries marked with @ prefix</li>
     *   <li>Raw JFR queries for everything else (including plain SELECT)</li>
     *   <li>Statement boundary detection for proper separation</li>
     * </ul>
     * 
     * @return StatementNode representing the parsed statement
     * @throws ParserException if statement syntax is invalid or unrecognized
     */
    private StatementNode parseStatement() throws ParserException {
        try {
            // Check for SHOW queries
            if (tokens.match(TokenType.SHOW)) {
                return parseShowQuery();
            }
            
            // Check for HELP queries
            if (tokens.match(TokenType.HELP)) {
                return parseHelpQuery();
            }

            // Check for VIEW definition
            if (tokens.match(TokenType.VIEW)) {
                return parseViewDefinition();
            }
            
            // Check for assignment (identifier := ... or $variable := ...)
            if ((tokens.check(TokenType.IDENTIFIER) || tokens.check(TokenType.SYNTHETIC_FIELD)) && tokens.checkNext(TokenType.ASSIGN)) {
                return parseAssignment();
            }
            
            // Check if this is an extended query (starts with @ only)
            if (tokens.check(TokenType.EXTENDED_QUERY)) {
                return parseQuery();
            }
            
            // Check if this looks like a malformed SQL query (starts with FROM, WHERE, etc.)
            // These should fail with helpful error messages, not be parsed as raw queries
            if (tokens.check(TokenType.FROM)) {
                throw new ParserException("Missing SELECT clause. Query cannot start with FROM. Use 'SELECT * FROM ...' or 'SELECT field FROM ...'");
            }
            if (tokens.check(TokenType.WHERE)) {
                throw new ParserException("Missing SELECT and FROM clauses. Query cannot start with WHERE. Use 'SELECT * FROM table WHERE ...'");
            }
            if (tokens.check(TokenType.GROUP_BY)) {
                throw new ParserException("Missing SELECT and FROM clauses. Query cannot start with GROUP BY. Use 'SELECT * FROM table GROUP BY ...'");
            }
            if (tokens.check(TokenType.ORDER_BY)) {
                throw new ParserException("Missing SELECT and FROM clauses. Query cannot start with ORDER BY. Use 'SELECT * FROM table ORDER BY ...'");
            }
            if (tokens.check(TokenType.HAVING)) {
                throw new ParserException("Missing SELECT, FROM, and GROUP BY clauses. Query cannot start with HAVING. Use 'SELECT * FROM table GROUP BY field HAVING ...'");
            }
            if (tokens.check(TokenType.LIMIT)) {
                throw new ParserException("Missing SELECT and FROM clauses. Query cannot start with LIMIT. Use 'SELECT * FROM table LIMIT ...'");
            }
            
            // Otherwise, it's a raw JFR query - capture the entire remaining input
            return parseRawJfrQuery();
            
        } catch (ParserException e) {
            // Don't wrap ParserException - just re-throw to avoid nested error messages
            throw e;
        } catch (Exception e) {
            throw new ParserException("Error parsing statement at " + tokens.current().getPositionString() + ": " + e.getMessage(), e);
        }
    }
    
    /**
     * Parse SHOW EVENTS or SHOW FIELDS queries (metadata queries).
     * 
     * <p><strong>Grammar Rule:</strong></p>
     * <pre>
     * show_query ::= SHOW EVENTS
     *              | SHOW FIELDS event_type
     * event_type ::= IDENTIFIER ( DOT IDENTIFIER )*
     * </pre>
     * 
     * 
     * <p><strong>Special Cases:</strong></p>
     * <ul>
     *   <li>SHOW EVENTS lists all available JFR event types</li>
     *   <li>SHOW FIELDS requires an event type name</li>
     *   <li>Event types can be dotted (e.g., jdk.GarbageCollection)</li>
     * </ul>
     * 
     * @return StatementNode representing the SHOW query (ShowEventsNode or ShowFieldsNode)
     * @throws ParserException if syntax is invalid or unsupported SHOW type
     */
    private StatementNode parseShowQuery() throws ParserException {
        Token keyword = tokens.consume(TokenType.IDENTIFIER, "Expected keyword after SHOW");
        
        if ("EVENTS".equalsIgnoreCase(keyword.value())) {
            return new ShowEventsNode(location(keyword));
        } else if ("FIELDS".equalsIgnoreCase(keyword.value())) {
            // Handle dotted event type names like jdk.GarbageCollection
            Token eventTypeToken = tokens.consume(TokenType.IDENTIFIER, "Expected event type after SHOW FIELDS");
            StringBuilder eventType = new StringBuilder(eventTypeToken.value());
            
            // Handle dotted identifiers (e.g., jdk.GarbageCollection)
            while (tokens.match(TokenType.DOT)) {
                eventType.append(".");
                Token part = tokens.consume(TokenType.IDENTIFIER, "Expected identifier after dot");
                eventType.append(part.value());
            }
            
            return new ShowFieldsNode(eventType.toString(), location(eventTypeToken));
        } else {
            throw new ParserException("Expected EVENTS or FIELDS after SHOW");
        }
    }
    
    /**
     * Parse HELP queries (documentation and usage information).
     * 
     * <p><strong>Grammar Rule:</strong></p>
     * <pre>
     * help_query ::= HELP
     *              | HELP FUNCTION function_name
     *              | HELP GRAMMAR
     * function_name ::= IDENTIFIER
     * </pre>
     * 
     * <p><strong>Description:</strong></p>
     * <ul>
     *   <li>HELP shows general query language help and available functions</li>
     *   <li>HELP FUNCTION shows detailed help for a specific function</li>
     *   <li>HELP GRAMMAR shows the complete grammar documentation</li>
     * </ul>
     * 
     * <p><strong>Error Handling Strategy:</strong></p>
     * <ul>
     *   <li><strong>Missing function name:</strong> Clear error message for HELP FUNCTION</li>
     *   <li><strong>Unknown help type:</strong> Throws ParserException with helpful message</li>
     *   <li><strong>Invalid function name:</strong> Accepts any identifier, validation in executor</li>
     * </ul>
     * 
     * @return StatementNode representing the HELP query (HelpNode, HelpFunctionNode, or HelpGrammarNode)
     * @throws ParserException if syntax is invalid or unsupported HELP type
     */
    private StatementNode parseHelpQuery() throws ParserException {
        // Check if there's an optional keyword after HELP
        if (!tokens.check(TokenType.IDENTIFIER)) {
            // Just "HELP" - general help
            return new HelpNode(location(tokens.previous()));
        }
        
        Token keyword = tokens.consume(TokenType.IDENTIFIER, "Expected keyword after HELP");
        
        if ("FUNCTION".equalsIgnoreCase(keyword.value())) {
            Token functionNameToken = tokens.consume(TokenType.IDENTIFIER, "Expected function name after HELP FUNCTION");
            return new HelpFunctionNode(functionNameToken.value(), location(keyword));
        } else if ("GRAMMAR".equalsIgnoreCase(keyword.value())) {
            return new HelpGrammarNode(location(keyword));
        } else {
            throw new ParserException("Expected FUNCTION or GRAMMAR after HELP, or use just HELP for general help. Found: " + keyword.value());
        }
    }

    /**
     * Parse VIEW definition (creates named views for query reuse).
     * 
     * <p><strong>Grammar Rule:</strong></p>
     * <pre>
     * view_definition ::= VIEW IDENTIFIER AS query
     * </pre>
     * 
     * 
     * <p><strong>Special Cases:</strong></p>
     * <ul>
     *   <li>View names must be valid identifiers</li>
     *   <li>Views can contain both extended and raw JFR queries</li>
     *   <li>Views are stored for later reference in other queries</li>
     * </ul>
     * 
     * @return ViewDefinitionNode representing the view definition
     * @throws ParserException if view syntax is invalid
     */
    private ViewDefinitionNode parseViewDefinition() throws ParserException {
        Token viewName = tokens.consume(TokenType.IDENTIFIER, "Expected view name after VIEW");
        tokens.consume(TokenType.AS, "Expected AS after view name");
        QueryNode query = parseQuery();
        return new ViewDefinitionNode(viewName.value(), query, location(viewName.line(), viewName.column()));
    }
    
    /**
     * Parse assignment statement (variable := expression).
     * 
     * <p><strong>Grammar Rule:</strong></p>
     * <pre>
     * assignment ::= ( IDENTIFIER | SYNTHETIC_FIELD ) ASSIGN query
     * </pre>
     * 
     * 
     * <p><strong>Special Cases:</strong></p>
     * <ul>
     *   <li>Supports both regular identifiers and synthetic fields ($variable)</li>
     *   <li>Strips $ prefix from synthetic field names</li>
     *   <li>Assignment operator is := (not =)</li>
     *   <li>Right-hand side must be a complete query</li>
     * </ul>
     * 
     * @return AssignmentNode representing the assignment statement
     * @throws ParserException if assignment syntax is invalid
     */
    private AssignmentNode parseAssignment() throws ParserException {
        Token variable;
        String variableName;
        if (tokens.check(TokenType.IDENTIFIER)) {
            variable = tokens.consume(TokenType.IDENTIFIER, "Expected variable name");
            variableName = variable.value();
        } else {
            variable = tokens.consume(TokenType.SYNTHETIC_FIELD, "Expected variable name");
            // Strip the $ prefix from synthetic field names
            variableName = variable.value().startsWith("$") ? variable.value().substring(1) : variable.value();
        }
        tokens.consume(TokenType.ASSIGN, "Expected := after variable name");
        QueryNode query = parseQuery();
        return new AssignmentNode(variableName, query, location(variable.line(), variable.column()));
    }
    
    /**
     * Parse a query (main query parsing logic).
     * 
     * <p><strong>Grammar Rule:</strong></p>
     * <pre>
     * query ::= ( EXTENDED_QUERY )?
     *           ( column_clause )?
     *           ( format_clause )?
     *           select_clause
     *           from_clause
     *           ( where_clause )?
     *           ( join_clause* )?
     *           ( group_by_clause )?
     *           ( having_clause )?
     *           ( order_by_clause )?
     *           ( limit_clause )?
     * </pre>
     * 
     * 
     * <p><strong>Special Cases:</strong></p>
     * <ul>
     *   <li>Extended queries marked with @ prefix</li>
     *   <li>COLUMN and FORMAT clauses are JFR-specific extensions</li>
     *   <li>JOIN clauses can appear in FROM or after WHERE</li>
     *   <li>HAVING clause sets appropriate context for aggregate functions</li>
     * </ul>
     * 
     * @return QueryNode representing the complete parsed query
     * @throws ParserException if required clauses are missing or syntax errors occur
     */
    private QueryNode parseQuery() throws ParserException {
        int line = tokens.current().line();
        int column = tokens.current().column();
        
        // Clear valid aliases for this query
        validAliases.clear();
        
        // Check for extended query marker @
        boolean isExtended = tokens.match(TokenType.EXTENDED_QUERY);
        
        // Parse COLUMN clause
        List<String> columns = parseColumnClause();
        
        // Parse FORMAT clause
        List<FormatterNode> formatters = parseFormatClause();
        
        // Parse SELECT clause
        SelectNode select = parseSelectClause();
        
        // Parse FROM clause
        FromNode from = parseFromClause();
        
        // Parse WHERE clause
        WhereNode where = parseWhereClause();
        
        // Check for JOINs after the WHERE clause
        if (tokens.check(TokenType.INNER) || tokens.check(TokenType.LEFT) || tokens.check(TokenType.RIGHT) || 
            tokens.check(TokenType.FULL) || tokens.check(TokenType.JOIN) || tokens.check(TokenType.FUZZY)) {
            // Update the FROM clause with additional JOINs
            from = parseAdditionalJoins(from);
        }
        
        // Parse GROUP BY clause
        GroupByNode groupBy = parseGroupByClause();
        
        // Parse HAVING clause
        HavingNode having = parseHavingClause();
        
        // Parse ORDER BY clause
        OrderByNode orderBy = parseOrderByClause();
        
        // Parse LIMIT clause
        LimitNode limit = parseLimitClause();
        
        return new QueryNode(isExtended, columns, formatters, select, from, where, groupBy, having, orderBy, limit, location(line, column));
    }
    
    /**
     * Parse COLUMN clause (extended JFR syntax for column formatting).
     * 
     * <p><strong>Grammar Rule:</strong></p>
     * <pre>
     * column_clause ::= COLUMN string_list
     * string_list ::= STRING ( COMMA STRING )*
     * </pre>
     * 
     * 
     * <p><strong>Special Cases:</strong></p>
     * <ul>
     *   <li>Column names must be quoted strings</li>
     *   <li>Used for output formatting and display customization</li>
     * </ul>
     * 
     * @return List of column names, empty if COLUMN clause not present
     * @throws ParserException if COLUMN keyword present but syntax is invalid
     */
    private List<String> parseColumnClause() throws ParserException {
        List<String> columns = new ArrayList<>();
        
        if (tokens.check(TokenType.IDENTIFIER) && "COLUMN".equalsIgnoreCase(tokens.current().value())) {
            tokens.advance(); // consume COLUMN
            do {
                Token text = tokens.consume(TokenType.STRING, "Expected column name");
                columns.add(text.value());
            } while (tokens.match(TokenType.COMMA));
        }
        
        return columns;
    }
    
    /**
     * Parse FORMAT clause (extended JFR syntax for output formatting).
     * 
     * <p><strong>Grammar Rule:</strong></p>
     * <pre>
     * format_clause ::= FORMAT formatter_list
     * formatter_list ::= formatter ( COMMA formatter )*
     * </pre>
     * 
     * 
     * <p><strong>Special Cases:</strong></p>
     * <ul>
     *   <li>Used for customizing output format and appearance</li>
     *   <li>Each formatter consists of property-value pairs</li>
     * </ul>
     * 
     * @return List of formatter nodes, empty if FORMAT clause not present
     * @throws ParserException if FORMAT keyword present but syntax is invalid
     */
    private List<FormatterNode> parseFormatClause() throws ParserException {
        List<FormatterNode> formatters = new ArrayList<>();
        
        if (tokens.check(TokenType.IDENTIFIER) && "FORMAT".equalsIgnoreCase(tokens.current().value())) {
            tokens.advance(); // consume FORMAT
            do {
                formatters.add(parseFormatter());
            } while (tokens.match(TokenType.COMMA));
        }
        
        return formatters;
    }
    
    /**
     * Parse a formatter (property list for output formatting).
     * 
     * <p><strong>Grammar Rule:</strong></p>
     * <pre>
     * formatter ::= property_list
     * property_list ::= property ( SEMICOLON property )*
     * </pre>
     * 
     * 
     * <p><strong>Special Cases:</strong></p>
     * <ul>
     *   <li>Properties are separated by semicolons within a formatter</li>
     *   <li>Each formatter represents a complete formatting specification</li>
     * </ul>
     * 
     * @return FormatterNode containing the parsed properties
     * @throws ParserException if property syntax is invalid
     */
    private FormatterNode parseFormatter() throws ParserException {
        int line = tokens.current().line();
        int column = tokens.current().column();
        
        List<PropertyNode> properties = new ArrayList<>();
        
        do {
            properties.add(parseProperty());
        } while (tokens.match(TokenType.SEMICOLON));
        
        return new FormatterNode(properties, location(line, column));
    }
    
    /**
     * Parse a property (name-value pair for formatters).
     * 
     * <p><strong>Grammar Rule:</strong></p>
     * <pre>
     * property ::= IDENTIFIER ( EQUALS IDENTIFIER )?
     * </pre>
     * 
     * 
     * <p><strong>Special Cases:</strong></p>
     * <ul>
     *   <li>Property value is optional (boolean properties)</li>
     *   <li>Both name and value must be identifiers</li>
     * </ul>
     * 
     * @return PropertyNode representing the parsed property
     * @throws ParserException if property name is missing or invalid
     */
    private PropertyNode parseProperty() throws ParserException {
        Token name = tokens.consume(TokenType.IDENTIFIER, "Expected property name");
        
        String value = "";
        if (tokens.match(TokenType.EQUALS)) {
            Token valueToken = tokens.consume(TokenType.IDENTIFIER, "Expected property value");
            value = valueToken.value();
        }
        
        return new PropertyNode(name.value(), value, location(name));
    }
    
    /**
     * Parse SELECT clause.
     * 
     * <p><strong>Grammar Rule:</strong></p>
     * <pre>
     * select_clause ::= SELECT ( STAR | select_item_list )
     * select_item_list ::= select_item ( COMMA select_item )*
     * select_item ::= expression ( AS IDENTIFIER )?
     * </pre>
     * 
     * 
     * <p><strong>Synthetic Recovery:</strong> When an expression fails to parse, creates a synthetic
     * IdentifierNode with "__error__" to allow continued parsing of the rest of the SELECT clause.</p>
     * 
     * @return SelectNode representing the parsed SELECT clause
     * @throws ParserException if the SELECT token is missing or unrecoverable errors occur
     */
    private SelectNode parseSelectClause() throws ParserException {
        tokens.consume(TokenType.SELECT, "Expected SELECT");
        
        int line = tokens.previous().line();
        int column = tokens.previous().column();
        
        // Set context for SELECT clause parsing
        QueryErrorMessageGenerator.QueryContext previousContext = currentContext;
        currentContext = QueryErrorMessageGenerator.QueryContext.SELECT_CLAUSE;
        
        try {
            // Check if we have * for SELECT *
            if (tokens.match(TokenType.STAR)) {
                return new SelectNode(List.of(), true, location(line, column));
            }
            
            // Validate that we have SELECT items (not empty SELECT)
            if (tokens.check(TokenType.FROM) || tokens.isAtEnd()) {
                ParserErrorHandler.ParserError error = new ParserErrorHandler.ParserError(
                    "Missing fields in SELECT statement",
                    "Specify what to select: field names, * for all fields, or function calls like COUNT(*).",
                    tokens.current(),
                    errorHandler.extractContext(tokens.current()),
                    ParserErrorHandler.ErrorType.MISSING_TOKEN
                );
                errorHandler.addError(error);
                
                // Create synthetic select item to continue parsing
                SelectItemNode syntheticItem = new SelectItemNode(
                    new IdentifierNode("__missing_field__", location(tokens.current())),
                    null,
                    location(tokens.current())
                );
                return new SelectNode(List.of(syntheticItem), false, location(line, column));
            }
            
            List<SelectItemNode> selectItems = new ArrayList<>();
            
            do {
                try {
                    ExpressionNode expression = parseExpression();
                    String alias = null;
                    
                    // Check for AS alias
                    if (tokens.match(TokenType.AS)) {
                        try {
                            Token aliasToken = tokens.consume(TokenType.IDENTIFIER, "Expected alias after AS");
                            alias = aliasToken.value();
                        } catch (ParserException e) {
                            // Error in alias - add error and continue without alias
                            ParserErrorHandler.ParserError error = errorHandler.createExpressionError(
                                tokens.current(), "Invalid alias in SELECT: " + e.getMessage());
                            errorHandler.addError(error);
                        }
                    }
                    
                    SelectItemNode selectItem = new SelectItemNode(expression, alias, location(line, column));
                    selectItems.add(selectItem);
                } catch (ParserException e) {
                    // Error parsing select item - add error and try to recover
                    ParserErrorHandler.ParserError error = errorHandler.createExpressionError(
                        tokens.current(), "Invalid expression in SELECT: " + e.getMessage());
                    errorHandler.addError(error);
                    
                    // Skip to next comma or FROM clause
                    tokens.skipToRecoveryPoint(TokenType.COMMA, TokenType.FROM);
                    
                    // Add synthetic select item for continued parsing
                    SelectItemNode syntheticItem = new SelectItemNode(
                        new IdentifierNode("__error__", location(tokens.current())),
                        null,
                        location(tokens.current())
                    );
                    selectItems.add(syntheticItem);
                    
                    // If we found a comma, consume it to continue parsing
                    if (tokens.check(TokenType.COMMA)) {
                        tokens.advance();
                    } else {
                        break; // Reached FROM or other clause
                    }
                }
            } while (tokens.check(TokenType.COMMA) && tokens.match(TokenType.COMMA));
            
            return new SelectNode(selectItems, false, location(line, column));
        } finally {
            // Restore previous context
            currentContext = previousContext;
        }
    }
    
    /**
     * Parse FROM clause.
     * 
     * <p><strong>Grammar Rule:</strong></p>
     * <pre>
     * from_clause ::= FROM source_list
     * source_list ::= source ( COMMA source )*
     * source ::= ( LPAREN ( extended_query | raw_query ) RPAREN ( AS IDENTIFIER )? )
     *          | ( IDENTIFIER ( DOT IDENTIFIER )* ( AS IDENTIFIER | IDENTIFIER )? )
     *          | ( source join_clause* )
     * 
     * join_clause ::= fuzzy_join | standard_join
     * fuzzy_join ::= FUZZY JOIN IDENTIFIER ( AS IDENTIFIER )? ON IDENTIFIER 
     *                ( WITH ( NEAREST | PREVIOUS | AFTER ) )? 
     *                ( TOLERANCE expression )? ( threshold expression )?
     * standard_join ::= ( INNER | LEFT | RIGHT | FULL )? JOIN IDENTIFIER ( AS IDENTIFIER )?
     *                   ON field_reference EQUALS field_reference
     * </pre>
     * 
     * 
     * <p><strong>Special Cases:</strong></p>
     * <ul>
     *   <li>Detects missing table names by checking for WHERE/GROUP BY/etc. tokens</li>
     *   <li>Handles complex join sequences with proper precedence</li>
     *   <li>Supports parenthesized subqueries with alias assignment</li>
     * </ul>
     * 
     * @return FromNode representing the parsed FROM clause
     * @throws ParserException if FROM token is missing or unrecoverable syntax errors occur
     */
    private FromNode parseFromClause() throws ParserException {
        // Validate that FROM clause is present
        if (!tokens.check(TokenType.FROM)) {
            ParserErrorHandler.ParserError error = new ParserErrorHandler.ParserError(
                "Missing FROM clause in query",
                "Add 'FROM table_name' to specify which table to query.",
                tokens.current(),
                errorHandler.extractContext(tokens.current()),
                ParserErrorHandler.ErrorType.MISSING_TOKEN
            );
            errorHandler.addError(error);
            
            // Create synthetic FROM clause to continue parsing
            List<SourceNodeBase> syntheticSources = List.of(
                new SourceNode("MISSING_TABLE", null, location(tokens.current()))
            );
            return new FromNode(syntheticSources, location(tokens.current()));
        }
        
        tokens.consume(TokenType.FROM, "Expected FROM");
        
        int line = tokens.previous().line();
        int column = tokens.previous().column();
        
        List<SourceNodeBase> sources = new ArrayList<>();
        
        do {
            SourceNodeBase source = parseSource();
            sources.add(source);
            
            // Handle multiple JOINs in sequence
            while (true) {
                // Check for fuzzy join after the source
                if (tokens.match(TokenType.FUZZY)) {
                    tokens.consume(TokenType.JOIN, "Expected JOIN after FUZZY");
                    Token rightSourceToken = tokens.consume(TokenType.IDENTIFIER, "Expected source name after FUZZY JOIN");
                    
                    // Parse alias for the joined source (before ON clause)
                    String rightAlias = null;
                    if (tokens.match(TokenType.AS)) {
                        Token aliasToken = parseAlias("Expected alias after AS");
                        rightAlias = aliasToken.value();
                        // Add alias to valid aliases set
                        if (rightAlias != null) {
                            validAliases.add(rightAlias);
                        }
                    }
                    
                    // Parse join condition
                    tokens.consume(TokenType.ON, "Expected ON clause after FUZZY JOIN");
                    Token joinField = tokens.consume(TokenType.IDENTIFIER, "Expected field name in ON clause");
                    
                    // Parse fuzzy join type (WITH NEAREST/PREVIOUS/AFTER)
                    FuzzyJoinType joinType = FuzzyJoinType.NEAREST; // default
                    if (tokens.match(TokenType.WITH)) {
                        if (tokens.match(TokenType.NEAREST)) {
                            joinType = FuzzyJoinType.NEAREST;
                        } else if (tokens.match(TokenType.PREVIOUS)) {
                            joinType = FuzzyJoinType.PREVIOUS;
                        } else if (tokens.match(TokenType.AFTER)) {
                            joinType = FuzzyJoinType.AFTER;
                        } else {
                            throw new ParserException("Expected NEAREST, PREVIOUS, or AFTER after WITH");
                        }
                    }
                    
                    // Parse tolerance and threshold (optional, in any order)
                    ExpressionNode tolerance = null;
                    ExpressionNode threshold = null;
                    
                    // Allow tolerance and threshold in any order
                    while (tokens.check(TokenType.TOLERANCE) || (tokens.check(TokenType.IDENTIFIER) && "threshold".equalsIgnoreCase(tokens.current().value()))) {
                        if (tokens.match(TokenType.TOLERANCE)) {
                            if (tolerance != null) {
                                throw new ParserException("TOLERANCE specified multiple times");
                            }
                            tolerance = parseExpression();
                        } else if (tokens.check(TokenType.IDENTIFIER) && "threshold".equalsIgnoreCase(tokens.current().value())) {
                            tokens.advance(); // consume "threshold" identifier
                            if (threshold != null) {
                                throw new ParserException("THRESHOLD specified multiple times");
                            }
                            threshold = parseExpression();
                        }
                    }
                    
                    FuzzyJoinSourceNode fuzzyJoinSource = new FuzzyJoinSourceNode(
                        rightSourceToken.value(), 
                        rightAlias, 
                        joinType, 
                        joinField.value(), 
                        tolerance, 
                        threshold,
                        location(line, column)
                    );
                    
                    sources.add(fuzzyJoinSource);
                }
                
                // Check for standard join after the source
                else if (tokens.match(TokenType.INNER, TokenType.LEFT, TokenType.RIGHT, TokenType.FULL)) {
                    StandardJoinType joinType = switch (tokens.previous().type()) {
                        case INNER -> StandardJoinType.INNER;
                        case LEFT -> StandardJoinType.LEFT;
                        case RIGHT -> StandardJoinType.RIGHT;
                        case FULL -> StandardJoinType.FULL;
                        default -> throw new ParserException("Unexpected join type: " + tokens.previous().value());
                    };
                    
                    tokens.consume(TokenType.JOIN, "Expected JOIN after join type");
                    Token rightSourceToken = tokens.consume(TokenType.IDENTIFIER, "Expected source name after JOIN");
                    
                    String rightAlias = null;
                    if (tokens.match(TokenType.AS)) {
                        rightAlias = tokens.consume(TokenType.IDENTIFIER, "Expected alias after AS").value();
                        // Add alias to valid aliases set
                        if (rightAlias != null) {
                            validAliases.add(rightAlias);
                        }
                    } else if (tokens.check(TokenType.IDENTIFIER) && !tokens.check(TokenType.ON)) {
                        // Handle alias without AS keyword (like "ExecutionSample e")
                        rightAlias = tokens.consume(TokenType.IDENTIFIER, "Expected alias").value();
                        // Add alias to valid aliases set
                        if (rightAlias != null) {
                            validAliases.add(rightAlias);
                        }
                    }
                    
                    tokens.consume(TokenType.ON, "Expected ON clause after JOIN");
                    String leftJoinField = ParserUtils.parseFieldReference(tokens, "ON clause");
                    tokens.consume(TokenType.EQUALS, "Expected = in ON clause");
                    String rightJoinField = ParserUtils.parseFieldReference(tokens, "ON clause");
                    
                    StandardJoinSourceNode standardJoinSource = new StandardJoinSourceNode(
                        rightSourceToken.value(),
                        rightAlias,
                        joinType,
                        leftJoinField,
                        rightJoinField,
                        location(line, column)
                    );
                    
                    sources.add(standardJoinSource);
                }
                
                // Check for standalone JOIN (defaults to INNER)
                else if (tokens.match(TokenType.JOIN)) {
                    Token rightSourceToken = tokens.consume(TokenType.IDENTIFIER, "Expected source name after JOIN");
                    
                    String rightAlias = null;
                    if (tokens.match(TokenType.AS)) {
                        rightAlias = tokens.consume(TokenType.IDENTIFIER, "Expected alias after AS").value();
                        // Add alias to valid aliases set
                        if (rightAlias != null) {
                            validAliases.add(rightAlias);
                        }
                    } else if (tokens.check(TokenType.IDENTIFIER) && !tokens.check(TokenType.ON)) {
                        // Handle alias without AS keyword (like "ExecutionSample e")
                        rightAlias = tokens.consume(TokenType.IDENTIFIER, "Expected alias").value();
                        // Add alias to valid aliases set
                        if (rightAlias != null) {
                            validAliases.add(rightAlias);
                        }
                    }
                    
                    tokens.consume(TokenType.ON, "Expected ON clause after JOIN");
                    String leftJoinField = ParserUtils.parseFieldReference(tokens, "ON clause");
                    tokens.consume(TokenType.EQUALS, "Expected = in ON clause");
                    String rightJoinField = ParserUtils.parseFieldReference(tokens, "ON clause");
                    
                    StandardJoinSourceNode standardJoinSource = new StandardJoinSourceNode(
                        rightSourceToken.value(),
                        rightAlias,
                        StandardJoinType.INNER,  // Default to INNER JOIN
                        leftJoinField,
                        rightJoinField,
                        location(line, column)
                    );
                    
                    sources.add(standardJoinSource);
                }
                else {
                    // No more JOINs, break out of the loop
                    break;
                }
            }
        } while (tokens.match(TokenType.COMMA));
        
        // Validate for duplicate table names without aliases
        validateNoDuplicateTableNames(sources);
        
        return new FromNode(sources, location(line, column));
    }
    
    /**
     * Parse additional JOIN clauses and add them to the existing FROM clause
     */
    private FromNode parseAdditionalJoins(FromNode from) throws ParserException {
        int line = from.location().line();
        int column = from.location().column();
        List<SourceNodeBase> sources = new ArrayList<>(from.sources());
        
        // Continue parsing JOIN clauses until we hit a non-JOIN token
        while (tokens.check(TokenType.INNER) || tokens.check(TokenType.LEFT) || tokens.check(TokenType.RIGHT) || 
               tokens.check(TokenType.FULL) || tokens.check(TokenType.JOIN) || tokens.check(TokenType.FUZZY)) {
            
            // Parse FUZZY JOIN
            if (tokens.match(TokenType.FUZZY)) {
                tokens.consume(TokenType.JOIN, "Expected JOIN after FUZZY");
                Token rightSourceToken = tokens.consume(TokenType.IDENTIFIER, "Expected source name after FUZZY JOIN");
                
                // Parse alias for the joined source (before ON clause)
                String rightAlias = null;
                if (tokens.match(TokenType.AS)) {
                    Token aliasToken = parseAlias("Expected alias after AS");
                    rightAlias = aliasToken.value();
                        // Add alias to valid aliases set
                        if (rightAlias != null) {
                            validAliases.add(rightAlias);
                        }
                }
                
                // Parse join condition
                tokens.consume(TokenType.ON, "Expected ON clause after FUZZY JOIN");
                Token joinField = tokens.consume(TokenType.IDENTIFIER, "Expected field name in ON clause");
                
                // Parse fuzzy join type (WITH NEAREST/PREVIOUS/AFTER)
                FuzzyJoinType joinType = FuzzyJoinType.NEAREST; // default
                if (tokens.match(TokenType.WITH)) {
                    if (tokens.match(TokenType.NEAREST)) {
                        joinType = FuzzyJoinType.NEAREST;
                    } else if (tokens.match(TokenType.PREVIOUS)) {
                        joinType = FuzzyJoinType.PREVIOUS;
                    } else if (tokens.match(TokenType.AFTER)) {
                        joinType = FuzzyJoinType.AFTER;
                    } else {
                        throw new ParserException("Expected NEAREST, PREVIOUS, or AFTER after WITH");
                    }
                }
                
                // Parse tolerance and threshold (optional, in any order)
                ExpressionNode tolerance = null;
                ExpressionNode threshold = null;
                
                // Allow tolerance and threshold in any order
                while (tokens.check(TokenType.TOLERANCE) || (tokens.check(TokenType.IDENTIFIER) && "threshold".equalsIgnoreCase(tokens.current().value()))) {
                    if (tokens.match(TokenType.TOLERANCE)) {
                        if (tolerance != null) {
                            throw new ParserException("TOLERANCE specified multiple times");
                        }
                        tolerance = parseExpression();
                    } else if (tokens.check(TokenType.IDENTIFIER) && "threshold".equalsIgnoreCase(tokens.current().value())) {
                        tokens.advance(); // consume "threshold" identifier
                        if (threshold != null) {
                            throw new ParserException("THRESHOLD specified multiple times");
                        }
                        threshold = parseExpression();
                    }
                }
                
                FuzzyJoinSourceNode fuzzyJoinSource = new FuzzyJoinSourceNode(
                    rightSourceToken.value(), 
                    rightAlias, 
                    joinType, 
                    joinField.value(), 
                    tolerance, 
                    threshold,
                    location(line, column)
                );
                
                sources.add(fuzzyJoinSource);
            }
            
            // Parse standard join (INNER, LEFT, RIGHT, FULL)
            else if (tokens.match(TokenType.INNER, TokenType.LEFT, TokenType.RIGHT, TokenType.FULL)) {
                StandardJoinType joinType = switch (tokens.previous().type()) {
                    case INNER -> StandardJoinType.INNER;
                    case LEFT -> StandardJoinType.LEFT;
                    case RIGHT -> StandardJoinType.RIGHT;
                    case FULL -> StandardJoinType.FULL;
                    default -> throw new ParserException("Unexpected join type: " + tokens.previous().value());
                };
                
                tokens.consume(TokenType.JOIN, "Expected JOIN after join type");
                Token rightSourceToken = tokens.consume(TokenType.IDENTIFIER, "Expected source name after JOIN");
                
                String rightAlias = null;
                if (tokens.match(TokenType.AS)) {
                    rightAlias = tokens.consume(TokenType.IDENTIFIER, "Expected alias after AS").value();
                        // Add alias to valid aliases set
                        if (rightAlias != null) {
                            validAliases.add(rightAlias);
                        }
                }
                
                tokens.consume(TokenType.ON, "Expected ON clause after JOIN");
                String leftJoinField = ParserUtils.parseFieldReference(tokens, "ON clause");
                tokens.consume(TokenType.EQUALS, "Expected = in ON clause");
                String rightJoinField = ParserUtils.parseFieldReference(tokens, "ON clause");
                
                StandardJoinSourceNode standardJoinSource = new StandardJoinSourceNode(
                    rightSourceToken.value(),
                    rightAlias,
                    joinType,
                    leftJoinField,
                    rightJoinField,
                    location(line, column)
                );
                
                sources.add(standardJoinSource);
            }
            
            // Parse standalone JOIN (defaults to INNER)
            else if (tokens.match(TokenType.JOIN)) {
                Token rightSourceToken = tokens.consume(TokenType.IDENTIFIER, "Expected source name after JOIN");
                
                String rightAlias = null;
                if (tokens.match(TokenType.AS)) {
                    rightAlias = tokens.consume(TokenType.IDENTIFIER, "Expected alias after AS").value();
                        // Add alias to valid aliases set
                        if (rightAlias != null) {
                            validAliases.add(rightAlias);
                        }
                } else if (tokens.check(TokenType.IDENTIFIER) && !tokens.check(TokenType.ON)) {
                    // Handle alias without AS keyword (like "ExecutionSample e")
                    rightAlias = tokens.consume(TokenType.IDENTIFIER, "Expected alias").value();
                    // Add alias to valid aliases set
                    if (rightAlias != null) {
                        validAliases.add(rightAlias);
                    }
                }
                
                tokens.consume(TokenType.ON, "Expected ON clause after JOIN");
                String leftJoinField = ParserUtils.parseFieldReference(tokens, "ON clause");
                tokens.consume(TokenType.EQUALS, "Expected = in ON clause");
                String rightJoinField = ParserUtils.parseFieldReference(tokens, "ON clause");
                
                StandardJoinSourceNode standardJoinSource = new StandardJoinSourceNode(
                    rightSourceToken.value(),
                    rightAlias,
                    StandardJoinType.INNER,  // Default to INNER JOIN
                    leftJoinField,
                    rightJoinField,
                    location(line, column)
                );
                
                sources.add(standardJoinSource);
            }
            else {
                break; // No more JOINs
            }
        }
        
        return new FromNode(sources, location(line, column));
    }
    
    /**
     * Parse a source in FROM clause (table, view, or subquery).
     * 
     * <p><strong>Grammar Rule:</strong></p>
     * <pre>
     * source ::= LPAREN ( extended_query | raw_query ) RPAREN ( AS IDENTIFIER )?
     *          | dotted_identifier ( AS IDENTIFIER | IDENTIFIER )?
     * 
     * dotted_identifier ::= IDENTIFIER ( DOT IDENTIFIER )*
     * extended_query ::= EXTENDED_QUERY query
     * raw_query ::= ( any_tokens_until_closing_paren )
     * </pre>
     * 
     * 
     * <p><strong>Special Cases:</strong></p>
     * <ul>
     *   <li>Parenthesized subqueries can be extended (@SELECT) or raw JFR queries</li>
     *   <li>Raw subqueries capture all tokens until closing parenthesis</li>
     *   <li>Dotted identifiers for JFR event types (jdk.GarbageCollection)</li>
     *   <li>Alias detection avoids conflicts with clause keywords</li>
     *   <li>Missing table name detection with helpful error messages</li>
     * </ul>
     * 
     * @return SourceNodeBase representing the parsed source (table, view, or subquery)
     * @throws ParserException if source syntax is invalid or source name is missing
     */
    private SourceNodeBase parseSource() throws ParserException {
        // Check for parenthesized subquery
        if (tokens.match(TokenType.LPAREN)) {
            int line = tokens.previous().line();
            int column = tokens.previous().column();
            
            // Check if this is an extended subquery (starts with @)
            if (tokens.check(TokenType.EXTENDED_QUERY)) {
                // Parse as extended query
                QueryNode nestedQuery = parseQuery();
                
                tokens.consume(TokenType.RPAREN, "Expected ')' after subquery");
                
                String alias = null;
                if (tokens.match(TokenType.AS)) {
                    Token aliasToken = parseAlias("Expected alias after AS");
                    alias = aliasToken.value();
                    // Add alias to valid aliases set
                    if (alias != null) {
                        validAliases.add(alias);
                    }
                }
                
                return new SubquerySourceNode(nestedQuery, alias, location(line, column));
            } else {
                // Parse as raw query - capture the raw string
                StringBuilder rawQuery = new StringBuilder();
                int parenDepth = 1; // We already consumed the opening paren
                
                while (!tokens.isAtEnd() && parenDepth > 0) {
                    Token token = tokens.advance();
                    if (token.type() == TokenType.LPAREN) {
                        parenDepth++;
                    } else if (token.type() == TokenType.RPAREN) {
                        parenDepth--;
                        if (parenDepth == 0) {
                            break; // Don't include the closing paren
                        }
                    }
                    
                    if (rawQuery.length() > 0) {
                        rawQuery.append(" ");
                    }
                    rawQuery.append(token.value());
                }
                
                String alias = null;
                if (tokens.match(TokenType.AS)) {
                    Token aliasToken = parseAlias("Expected alias after AS");
                    alias = aliasToken.value();
                    // Add alias to valid aliases set
                    if (alias != null) {
                        validAliases.add(alias);
                    }
                }
                
                // Create a RawJfrQueryNode and wrap it in SubquerySourceNode
                RawJfrQueryNode rawJfrQuery = new RawJfrQueryNode(rawQuery.toString().trim(), location(line, column));
                return new SubquerySourceNode(rawJfrQuery, alias, location(line, column));
            }
        }
        
        // Regular table/view name (may be dotted like jdk.GarbageCollection)
        // Check for specific missing table name scenario
        if (tokens.check(TokenType.WHERE) || tokens.check(TokenType.GROUP_BY) || tokens.check(TokenType.ORDER_BY) || 
            tokens.check(TokenType.HAVING) || tokens.check(TokenType.LIMIT) || tokens.isAtEnd()) {
            // Missing table name after FROM - provide helpful error message
            String errorMessage = "Missing table or source name after FROM keyword";
            // Create a specific error with helpful suggestion
            ParserErrorHandler.ParserError specificError = new ParserErrorHandler.ParserError(
                errorMessage,
                "Specify a table name, view name, or subquery. Examples: FROM GarbageCollection, FROM jdk.GarbageCollection, FROM (SELECT * FROM Events)",
                tokens.current(),
                errorHandler.extractContext(tokens.current()),
                ParserErrorHandler.ErrorType.MISSING_TOKEN
            );
            errorHandler.addError(specificError);
            
            // Continue parsing by creating a synthetic source node instead of throwing
            return new SourceNode("MISSING_TABLE", null, location(tokens.current()));
        }
        
        Token source = tokens.consume(TokenType.IDENTIFIER, "Expected source name");
        StringBuilder sourceName = new StringBuilder(source.value());
        
        // Handle dotted identifiers (e.g., jdk.GarbageCollection)
        while (tokens.match(TokenType.DOT)) {
            sourceName.append(".");
            Token part = tokens.consume(TokenType.IDENTIFIER, "Expected identifier after dot");
            sourceName.append(part.value());
        }
        
        String alias = null;
        if (tokens.match(TokenType.AS)) {
            Token aliasToken = parseAlias("Expected alias after AS");
            alias = aliasToken.value();
            // Add alias to valid aliases set
            if (alias != null) {
                validAliases.add(alias);
            }
        } else if (tokens.check(TokenType.IDENTIFIER) && 
                   !tokens.check(TokenType.WHERE) && 
                   !tokens.check(TokenType.GROUP_BY) && 
                   !tokens.check(TokenType.HAVING) && 
                   !tokens.check(TokenType.ORDER_BY) && 
                   !tokens.check(TokenType.LIMIT) && 
                   !tokens.check(TokenType.COMMA) && 
                   !tokens.isAtEnd()) {
            // Check if next token could be an alias (identifier that's not a keyword)
            Token nextToken = tokens.current();
            if (nextToken.type() == TokenType.IDENTIFIER) {
                Token aliasToken = parseAlias("Expected alias");
                alias = aliasToken.value();
                
                // Add alias to valid aliases set
                if (alias != null) {
                    validAliases.add(alias);
                }
            }
        }
        
        return new SourceNode(sourceName.toString(), alias, location(source));
    }
    
    /**
     * Parse WHERE clause.
     * 
     * <p><strong>Grammar Rule:</strong></p>
     * <pre>
     * where_clause ::= WHERE condition
     * condition ::= or_condition
     * or_condition ::= and_condition ( OR and_condition )*
     * and_condition ::= primary_condition ( AND primary_condition )*
     * primary_condition ::= NOT condition
     *                     | LPAREN condition RPAREN
     *                     | variable_declaration
     *                     | expression
     * 
     * variable_declaration ::= IDENTIFIER ASSIGN expression
     * </pre>
     * 
     * 
     * <p><strong>Special Features:</strong></p>
     * <ul>
     *   <li>Supports variable declarations (x := expression) within conditions</li>
     *   <li>Provides proper operator precedence (OR < AND < NOT)</li>
     *   <li>Supports WITHIN temporal conditions</li>
     * </ul>
     * 
     * @return WhereNode representing the parsed WHERE clause, or null if no WHERE clause
     * @throws ParserException only for unrecoverable parsing errors
     */
    private WhereNode parseWhereClause() throws ParserException {
        if (!tokens.match(TokenType.WHERE)) {
            return null;
        }
        
        int line = tokens.previous().line();
        int column = tokens.previous().column();
        
        // Check for missing WHERE condition
        if (tokens.check(TokenType.GROUP_BY) || tokens.check(TokenType.HAVING) || tokens.check(TokenType.ORDER_BY) || 
            tokens.check(TokenType.LIMIT) || tokens.isAtEnd()) {
            ParserErrorHandler.ParserError error = new ParserErrorHandler.ParserError(
                "Missing WHERE condition",
                "Add a condition after WHERE. Examples: WHERE field = 'value', WHERE duration > 10ms",
                tokens.current(),
                errorHandler.extractContext(tokens.current()),
                ParserErrorHandler.ErrorType.MISSING_TOKEN
            );
            errorHandler.addError(error);
            
            // Return synthetic condition for continued parsing
            ConditionNode syntheticCondition = new ExpressionConditionNode(
                new LiteralNode(new CellValue.BooleanValue(true), location(line, column)),
                location(line, column)
            );
            return new WhereNode(syntheticCondition, location(line, column));
        }
        
        // Set context for WHERE clause parsing
        QueryErrorMessageGenerator.QueryContext previousContext = currentContext;
        currentContext = QueryErrorMessageGenerator.QueryContext.WHERE_CLAUSE;
        
        try {
            ConditionNode condition = parseCondition();
            return new WhereNode(condition, location(line, column));
        } catch (ParserException e) {
            // Error in WHERE clause - add error and create synthetic condition
            ParserErrorHandler.ParserError error = errorHandler.createExpressionError(
                tokens.current(), "Invalid WHERE clause: " + e.getMessage());
            errorHandler.addError(error);
            
            // Skip to next major clause
            tokens.skipToRecoveryPoint(TokenType.GROUP_BY, TokenType.HAVING, TokenType.ORDER_BY, TokenType.LIMIT);
            
            // Return synthetic condition for continued parsing
            ConditionNode syntheticCondition = new ExpressionConditionNode(
                new LiteralNode(new CellValue.BooleanValue(true), location(line, column)),
                location(line, column)
            );
            return new WhereNode(syntheticCondition, location(line, column));
        } finally {
            // Restore previous context
            currentContext = previousContext;
        }
    }
    
    /**
     * Parse a condition (entry point for condition parsing).
     * 
     * <p><strong>Grammar Rule:</strong></p>
     * <pre>
     * condition ::= or_condition
     * </pre>
     * 
     * 
     * @return ConditionNode representing the parsed condition
     * @throws ParserException if syntax errors occur in condition
     */
    private ConditionNode parseCondition() throws ParserException {
        return parseOrCondition();
    }
    
    /**
     * Parse OR condition (logical OR operations).
     * 
     * <p><strong>Grammar Rule:</strong></p>
     * <pre>
     * or_condition ::= and_condition ( OR and_condition )*
     * </pre>
     * 
     * 
     * <p><strong>Special Cases:</strong></p>
     * <ul>
     *   <li>Short-circuit evaluation during runtime, not parse time</li>
     *   <li>Multiple OR operations chained together</li>
     *   <li>Mixed with AND operations (AND has higher precedence)</li>
     * </ul>
     * 
     * @return ConditionNode representing the parsed OR condition
     * @throws ParserException if unrecoverable syntax errors occur
     */
    private ConditionNode parseOrCondition() throws ParserException {
        ConditionNode condition;
        try {
            condition = parseAndCondition();
        } catch (ParserException e) {
            // Error in left side of OR - add error and create synthetic condition
            ParserErrorHandler.ParserError error = errorHandler.createExpressionError(
                tokens.current(), "Invalid condition in OR expression: " + e.getMessage());
            errorHandler.addError(error);
            condition = new ExpressionConditionNode(
                new LiteralNode(new CellValue.BooleanValue(true), location(tokens.current())),
                location(tokens.current())
            );
        }
        
        while (tokens.match(TokenType.OR)) {
            Token operator = tokens.previous();
            ConditionNode rightCondition;
            try {
                rightCondition = parseAndCondition();
            } catch (ParserException e) {
                // Error in right side of OR - add error and create synthetic condition
                ParserErrorHandler.ParserError error = errorHandler.createExpressionError(
                    tokens.current(), "Invalid condition in OR expression: " + e.getMessage());
                errorHandler.addError(error);
                rightCondition = new ExpressionConditionNode(
                    new LiteralNode(new CellValue.BooleanValue(true), location(tokens.current())),
                    location(tokens.current())
                );
            }
            // For now, we'll create a simple combined condition
            // In a full implementation, we'd want a proper logical operator node
            condition = new ExpressionConditionNode(
                new BinaryExpressionNode(
                    toExpression(condition),
                    BinaryOperator.OR,
                    toExpression(rightCondition),
                    location(operator)
                ),
                location(operator)
            );
        }
        
        return condition;
    }
    
    /**
     * Parse AND condition (logical AND operations).
     * 
     * <p><strong>Grammar Rule:</strong></p>
     * <pre>
     * and_condition ::= primary_condition ( AND primary_condition )*
     * </pre>
     * 
     * 
     * <p><strong>Special Cases:</strong></p>
     * <ul>
     *   <li>Short-circuit evaluation during runtime, not parse time</li>
     *   <li>Multiple AND operations chained together</li>
     *   <li>Higher precedence than OR operations</li>
     * </ul>
     * 
     * @return ConditionNode representing the parsed AND condition
     * @throws ParserException if syntax errors occur in primary conditions
     */
    private ConditionNode parseAndCondition() throws ParserException {
        ConditionNode condition = parsePrimaryCondition();
        
        while (tokens.match(TokenType.AND)) {
            Token operator = tokens.previous();
            ConditionNode right = parsePrimaryCondition();
            // For now, we'll create a simple combined condition
            // In a full implementation, we'd want a proper logical operator node
            condition = new ExpressionConditionNode(
                new BinaryExpressionNode(
                    toExpression(condition),
                    BinaryOperator.AND,
                    toExpression(right),
                    location(operator)
                ),
                location(operator)
            );
        }
        
        return condition;
    }
    
    /**
     * Convert a condition to an expression (helper method for logical operations).
     * 
     * <p>This utility method bridges the gap between condition nodes and expression nodes
     * when building logical operator trees. It extracts the underlying expression from
     * condition nodes or wraps primitive conditions appropriately.</p>
     * 
     * <p><strong>Conversion Rules:</strong></p>
     * <ul>
     *   <li><strong>ExpressionConditionNode:</strong> Returns the wrapped expression directly</li>
     *   <li><strong>Other condition types:</strong> Creates boolean literal as fallback</li>
     * </ul>
     * 
     * @param condition The condition node to convert
     * @return ExpressionNode representing the condition as an expression
     */
    private ExpressionNode toExpression(ConditionNode condition) {
        if (condition instanceof ExpressionConditionNode exprCond) {
            return exprCond.expression();
        }
        // For other condition types, wrap them in an identifier for now
        return new IdentifierNode("condition", location(condition.getLine(), condition.getColumn()));
    }
    
    /**
     * Parse primary condition (atomic conditions, NOT operations, special JFR conditions).
     * 
     * <p><strong>Grammar Rule:</strong></p>
     * <pre>
     * primary_condition ::= NOT ( LPAREN condition RPAREN | expression )
     *                     | IDENTIFIER ASSIGN expression          // Variable declaration
     *                     | IDENTIFIER DOT gc_correlation         // GC correlation
     *                     | expression                            // Expression condition
     * 
     * </pre>
     * 
     * 
     * <p><strong>Special Cases:</strong></p>
     * <ul>
     *   <li>NOT operator with optional parentheses</li>
     *   <li>Variable declarations (x := value) in conditions</li>
     * </ul>
     * 
     * @return ConditionNode representing the parsed primary condition
     * @throws ParserException if syntax errors occur in condition elements
     */
    private ConditionNode parsePrimaryCondition() throws ParserException {
        // Handle NOT operator for conditions
        if (tokens.match(TokenType.NOT)) {
            Token notToken = tokens.previous();
            
            // If we have a parenthesized condition after NOT
            if (tokens.match(TokenType.LPAREN)) {
                ConditionNode innerCondition = parseCondition();
                tokens.consume(TokenType.RPAREN, "Expected ')' after condition");
                
                // Create a NOT unary expression condition
                return new ExpressionConditionNode(
                    new UnaryExpressionNode(
                        UnaryOperator.NOT,
                        toExpression(innerCondition),
                        location(notToken)
                    ),
                    location(notToken)
                );
            } else {
                // If no parentheses, just negate the next expression
                ExpressionNode expr = parseExpression();
                return new ExpressionConditionNode(
                    new UnaryExpressionNode(
                        UnaryOperator.NOT,
                        expr,
                        location(notToken)
                    ),
                    location(notToken)
                );
            }
        }
        
        // Check for variable declaration (x := expression)
        if (tokens.check(TokenType.IDENTIFIER) && tokens.checkNext(TokenType.ASSIGN)) {
            Token variable = tokens.advance();
            tokens.consume(TokenType.ASSIGN, "Expected :=");
            ExpressionNode value = parseExpression();
            return new VariableDeclarationNode(variable.value(), value, location(variable));
        }
        
        // Otherwise, it's an expression condition
        ExpressionNode expr = parseExpression();
        
        // Validate that WHERE conditions are proper comparisons, not just field references
        if (currentContext == QueryErrorMessageGenerator.QueryContext.WHERE_CLAUSE) {
            validateWhereExpression(expr);
        }
        
        return new ExpressionConditionNode(expr, location(expr.getLine(), expr.getColumn()));
    }
    
    /**
     * Validate that a WHERE clause expression is a proper comparison.
     * Rejects simple field references without comparison operators.
     */
    private void validateWhereExpression(ExpressionNode expr) throws ParserException {
        // Allow binary expressions (comparisons), function calls, and literals
        if (expr instanceof BinaryExpressionNode binaryExpr) {
            // Check if it's a comparison operator
            switch (binaryExpr.operator()) {
                case EQUALS, NOT_EQUALS, LESS_THAN, LESS_EQUAL, 
                     GREATER_THAN, GREATER_EQUAL, LIKE, IN, AND, OR, WITHIN, OF -> {
                    // Valid comparison operators for WHERE clause
                    return;
                }
                case ADD, SUBTRACT, MULTIPLY, DIVIDE, MODULO -> {
                    // Arithmetic operators should not be the root of a WHERE condition
                    throw new ParserException("WHERE clause requires a comparison operator. " +
                        "Arithmetic expression '" + binaryExpr.operator() + "' cannot be used as a condition. " +
                        "Use comparison operators like =, <, >, <=, >=, <>, LIKE, etc.");
                }
            }
        } else if (expr instanceof FunctionCallNode || expr instanceof LiteralNode) {
            // Function calls and literals are allowed (e.g., WHERE someBooleanFunction(), WHERE true)
            return;
        } else if (expr instanceof IdentifierNode identifierNode) {
            // Simple identifier without comparison is not allowed in WHERE
            throw new ParserException("Incomplete WHERE condition. Field reference '" + identifierNode.name() + 
                "' requires a comparison operator. " +
                "Examples: WHERE " + identifierNode.name() + " = 'value', " +
                "WHERE " + identifierNode.name() + " > 100");
        } else if (expr instanceof FieldAccessNode fieldAccessNode) {
            // Field access without comparison is not allowed in WHERE
            throw new ParserException("Incomplete WHERE condition. Field reference '" + 
                fieldAccessNode.qualifier() + "." + fieldAccessNode.field() + 
                "' requires a comparison operator. " +
                "Examples: WHERE " + fieldAccessNode.qualifier() + "." + fieldAccessNode.field() + " = 'value'");
        } else if (expr instanceof UnaryExpressionNode) {
            // Allow unary expressions like NOT field
            return;
        }
        
        // For other expression types, allow them (e.g., complex expressions)
    }
    
    /**
     * Parse GROUP BY clause.
     * 
     * <p><strong>Grammar Rule:</strong></p>
     * <pre>
     * group_by_clause ::= GROUP BY expression_list
     * expression_list ::= expression ( COMMA expression )*
     * </pre>
     * 
     * 
     * <p><strong>Special Cases:</strong></p>
     * <ul>
     *   <li>Supports complex expressions in GROUP BY including function calls</li>
     *   <li>Handles field references with dot notation (e.g., event.thread)</li>
     * </ul>
     * 
     * @return GroupByNode representing the parsed GROUP BY clause, or null if not present
     * @throws ParserException if unrecoverable syntax errors occur in expressions
     */
    private GroupByNode parseGroupByClause() throws ParserException {
        if (!tokens.match(TokenType.GROUP_BY)) {
            return null;
        }
        
        int line = tokens.previous().line();
        int column = tokens.previous().column();
        
        List<ExpressionNode> fields = new ArrayList<>();
        
        // Check if there are any expressions after GROUP BY
        if (tokens.isAtEnd() || tokens.isAtEndOfClause()) {
            ParserErrorHandler.ParserError error = new ParserErrorHandler.ParserError(
                "Missing expression after GROUP BY",
                "GROUP BY clause requires at least one field name or expression to group by. Add a field name (e.g., GROUP BY eventType) or expression (e.g., GROUP BY duration / 1000).",
                tokens.current(),
                errorHandler.extractContext(tokens.current()),
                ParserErrorHandler.ErrorType.MISSING_TOKEN
            );
            errorHandler.addError(error);
            // Create a synthetic field to allow recovery
            fields.add(new IdentifierNode("__missing_group_by_field__", location(line, column)));
        } else {
            do {
                fields.add(parseExpression());
            } while (tokens.match(TokenType.COMMA));
        }
        
        return new GroupByNode(fields, location(line, column));
    }
    
    /**
     * Parse HAVING clause.
     * 
     * <p><strong>Grammar Rule:</strong></p>
     * <pre>
     * having_clause ::= HAVING condition
     * condition ::= or_condition
     * </pre>
     * 
     * 
     * <p><strong>Special Cases:</strong></p>
     * <ul>
     *   <li>Allows aggregate functions in HAVING conditions (unlike WHERE)</li>
     *   <li>Properly restores parsing context after completion</li>
     * </ul>
     * 
     * @return HavingNode representing the parsed HAVING clause, or null if not present
     * @throws ParserException if unrecoverable syntax errors occur in condition
     */
    private HavingNode parseHavingClause() throws ParserException {
        if (!tokens.match(TokenType.HAVING)) {
            return null;
        }
        
        int line = tokens.previous().line();
        int column = tokens.previous().column();
        
        // Set context for HAVING clause parsing (aggregate functions allowed)
        QueryErrorMessageGenerator.QueryContext previousContext = currentContext;
        currentContext = QueryErrorMessageGenerator.QueryContext.HAVING_CLAUSE;
        
        try {
            ConditionNode condition = parseCondition();
            return new HavingNode(condition, location(line, column));
        } finally {
            // Restore previous context
            currentContext = previousContext;
        }
    }
    
    /**
     * Parse ORDER BY clause.
     * 
     * <p><strong>Grammar Rule:</strong></p>
     * <pre>
     * order_by_clause ::= ORDER BY order_field_list
     * order_field_list ::= order_field ( COMMA order_field )*
     * order_field ::= expression ( ASC | DESC )?
     * </pre>
     * 
     * 
     * <p><strong>Special Cases:</strong></p>
     * <ul>
     *   <li>Supports complex expressions for ordering including function calls</li>
     *   <li>Defaults to ascending order when no direction specified</li>
     *   <li>Allows multiple fields with different sort directions</li>
     * </ul>
     * 
     * @return OrderByNode representing the parsed ORDER BY clause, or null if not present
     * @throws ParserException if unrecoverable syntax errors occur in expressions
     */
    private OrderByNode parseOrderByClause() throws ParserException {
        if (!tokens.match(TokenType.ORDER_BY)) {
            return null;
        }
        
        int line = tokens.previous().line();
        int column = tokens.previous().column();
        
        List<OrderFieldNode> fields = new ArrayList<>();
        
        // Check if there are any expressions after ORDER BY
        if (tokens.isAtEnd() || tokens.isAtEndOfClause()) {
            ParserErrorHandler.ParserError error = new ParserErrorHandler.ParserError(
                "Missing expression after ORDER BY",
                "ORDER BY clause requires at least one field name or expression to sort by. Add a field name (e.g., ORDER BY eventType) or expression (e.g., ORDER BY duration DESC).",
                tokens.current(),
                errorHandler.extractContext(tokens.current()),
                ParserErrorHandler.ErrorType.MISSING_TOKEN
            );
            errorHandler.addError(error);
            // Create a synthetic field to allow recovery
            ExpressionNode syntheticField = new IdentifierNode("__missing_order_by_field__", location(line, column));
            fields.add(new OrderFieldNode(syntheticField, SortOrder.ASC, location(line, column)));
        } else {
            do {
                ExpressionNode field = parseExpression();
                SortOrder order = SortOrder.ASC;
                
                if (tokens.match(TokenType.ASC)) {
                    order = SortOrder.ASC;
                } else if (tokens.match(TokenType.DESC)) {
                    order = SortOrder.DESC;
                }
                
                fields.add(new OrderFieldNode(field, order, location(field.getLine(), field.getColumn())));
            } while (tokens.match(TokenType.COMMA));
        }
        
        return new OrderByNode(fields, location(line, column));
    }
    
    /**
     * Parse LIMIT clause.
     * 
     * <p><strong>Grammar Rule:</strong></p>
     * <pre>
     * limit_clause ::= LIMIT NUMBER
     * </pre>
     * 
     * 
     * <p><strong>Special Cases:</strong></p>
     * <ul>
     *   <li>Only accepts positive integer values</li>
     *   <li>NumberFormatException converted to helpful error message</li>
     * </ul>
     * 
     * @return LimitNode representing the parsed LIMIT clause, or null if not present
     * @throws ParserException if LIMIT token present but number is missing or invalid
     */
    private LimitNode parseLimitClause() throws ParserException {
        if (!tokens.match(TokenType.LIMIT)) {
            return null;
        }
        
        int line = tokens.previous().line();
        int column = tokens.previous().column();
        
        Token limitToken = tokens.consume(TokenType.NUMBER, "Expected number after LIMIT");
        int limit = Integer.parseInt(limitToken.value());
        
        // Validate LIMIT value
        if (limit <= 0) {
            throw new ParserException("LIMIT value must be a positive integer greater than 0, but was: " + limit);
        }
        
        return new LimitNode(limit, location(line, column));
    }
    
    /**
     * Parse an expression (entry point for expression parsing).
     * 
     * <p><strong>Grammar Rule:</strong></p>
     * <pre>
     * expression ::= comparison
     * </pre>
     * 
     * 
     * @return ExpressionNode representing the parsed expression
     * @throws ParserException if syntax errors occur in expression
     */
    private ExpressionNode parseExpression() throws ParserException {
        return parseComparison();
    }
    
    /**
     * Parse comparison expressions (==, !=, <, >, <=, >=, LIKE, IN, WITHIN).
     * 
     * <p><strong>Grammar Rule:</strong></p>
     * <pre>
     * comparison ::= arithmetic ( comparison_op arithmetic )*
     *              | arithmetic WITHIN arithmetic OF arithmetic
     * comparison_op ::= EQUALS | NOT_EQUALS | LESS_THAN | GREATER_THAN 
     *                 | LESS_EQUAL | GREATER_EQUAL | LIKE | IN
     * </pre>
     * 
     * 
     * <p><strong>Special Cases:</strong></p>
     * <ul>
     *   <li>WITHIN...OF temporal expressions for time-based filtering</li>
     *   <li>Chained comparisons (a < b < c treated as separate expressions)</li>
     *   <li>IN operator for set membership testing</li>
     *   <li>LIKE operator for pattern matching</li>
     * </ul>
     * 
     * @return ExpressionNode representing the parsed comparison expression
     * @throws ParserException if unrecoverable syntax errors occur
     */
    private ExpressionNode parseComparison() throws ParserException {
        ExpressionNode expr;
        try {
            expr = parseArithmetic();
        } catch (ParserException e) {
            // Error in left side of comparison - add error and create synthetic expression
            ParserErrorHandler.ParserError error = errorHandler.createExpressionError(
                tokens.current(), "Invalid expression in comparison: " + e.getMessage());
            errorHandler.addError(error);
            expr = new LiteralNode(new CellValue.NumberValue(0), location(tokens.current()));
        }
        
        // Check for WITHIN condition
        if (tokens.match(TokenType.WITHIN)) {
            Token withinToken = tokens.previous();
            ExpressionNode timeWindow;
            ExpressionNode referenceTime;
            
            try {
                timeWindow = parseArithmetic();
                tokens.consume(TokenType.OF, "Expected 'OF' after time window in WITHIN clause");
                referenceTime = parseArithmetic();
            } catch (ParserException e) {
                // Error in WITHIN clause - add error and create synthetic expressions
                ParserErrorHandler.ParserError error = errorHandler.createExpressionError(
                    tokens.current(), "Invalid WITHIN clause: " + e.getMessage());
                errorHandler.addError(error);
                timeWindow = new LiteralNode(new CellValue.DurationValue(java.time.Duration.ZERO), location(withinToken));
                referenceTime = new LiteralNode(new CellValue.TimestampValue(java.time.Instant.EPOCH), location(withinToken));
            }
            
            // We'll just create a binary expression with WITHIN and OF operators
            // The ASTPrettyPrinter will format it correctly
            return new BinaryExpressionNode(
                expr,
                BinaryOperator.WITHIN,
                new BinaryExpressionNode(timeWindow, BinaryOperator.OF, referenceTime, location(withinToken)),
                location(withinToken)
            );
        }
        
        while (tokens.match(TokenType.EQUALS, TokenType.NOT_EQUALS, TokenType.LESS_THAN, 
                    TokenType.GREATER_THAN, TokenType.LESS_EQUAL, TokenType.GREATER_EQUAL,
                    TokenType.LIKE, TokenType.IN)) {
            Token operator = tokens.previous();
            
            // Check for double equals (==) error - if we just consumed an EQUALS and the next token is also EQUALS
            if (operator.type() == TokenType.EQUALS && tokens.check(TokenType.EQUALS)) {
                throw new ParserException("Unexpected token: EQUALS. " +
                    "Use single '=' for comparison, not '==' (double equals is not supported in this query language)");
            }
            
            ExpressionNode right;
            
            try {
                right = parseArithmetic();
            } catch (ParserException e) {
                // Error in right side of comparison - add error and create synthetic expression
                ParserErrorHandler.ParserError error = errorHandler.createExpressionError(
                    tokens.current(), "Invalid expression in comparison: " + e.getMessage());
                errorHandler.addError(error);
                right = new LiteralNode(new CellValue.NumberValue(0), location(operator));
            }
            
            BinaryOperator op = switch (operator.type()) {
                case EQUALS -> BinaryOperator.EQUALS;
                case NOT_EQUALS -> BinaryOperator.NOT_EQUALS;
                case LESS_THAN -> BinaryOperator.LESS_THAN;
                case GREATER_THAN -> BinaryOperator.GREATER_THAN;
                case LESS_EQUAL -> BinaryOperator.LESS_EQUAL;
                case GREATER_EQUAL -> BinaryOperator.GREATER_EQUAL;
                case LIKE -> BinaryOperator.LIKE;
                case IN -> BinaryOperator.IN;
                default -> throw new ParserException("Unexpected operator: " + operator.type());
            };
            
            expr = new BinaryExpressionNode(expr, op, right, location(operator));
        }
        
        return expr;
    }
    
    /**
     * Parse arithmetic expressions (+ and - operations).
     * 
     * <p><strong>Grammar Rule:</strong></p>
     * <pre>
     * arithmetic ::= term ( ( PLUS | MINUS ) term )*
     * </pre>
     * 
     * 
     * <p><strong>Special Cases:</strong></p>
     * <ul>
     *   <li>Supports both numeric and duration arithmetic</li>
     *   <li>Handles type coercion in binary expression nodes</li>
     * </ul>
     * 
     * @return ExpressionNode representing the parsed arithmetic expression
     * @throws ParserException if syntax errors occur in operands
     */
    private ExpressionNode parseArithmetic() throws ParserException {
        ExpressionNode expr = parseTerm();
        
        while (tokens.match(TokenType.PLUS, TokenType.MINUS)) {
            Token operator = tokens.previous();
            
            // Check for double minus operator pattern: - -
            if (operator.type() == TokenType.MINUS && tokens.check(TokenType.MINUS)) {
                ParserErrorHandler.ParserError error = errorHandler.createExpressionError(
                    tokens.current(), "Invalid double minus operator '- -'. Use parentheses for negation: -(expression)");
                errorHandler.addError(error);
                // Skip the second minus to continue parsing
                tokens.advance();
            }
            
            ExpressionNode right = parseTerm();
            
            BinaryOperator op = operator.type() == TokenType.PLUS ? BinaryOperator.ADD : BinaryOperator.SUBTRACT;
            expr = new BinaryExpressionNode(expr, op, right, location(operator));
        }
        
        return expr;
    }
    
    /**
     * Parse term expressions (*, /, % operations).
     * 
     * <p><strong>Grammar Rule:</strong></p>
     * <pre>
     * term ::= unary ( ( STAR | DIVIDE | MODULO ) unary )*
     * </pre>
     * 
     * 
     * <p><strong>Special Cases:</strong></p>
     * <ul>
     *   <li>Division by zero handled at evaluation time, not parse time</li>
     *   <li>Modulo operation supported for integer expressions</li>
     *   <li>Multiplication can handle duration scaling</li>
     * </ul>
     * 
     * @return ExpressionNode representing the parsed term expression
     * @throws ParserException if syntax errors occur in operands
     */
    private ExpressionNode parseTerm() throws ParserException {
        ExpressionNode expr = parseUnary();
        
        while (tokens.match(TokenType.STAR, TokenType.DIVIDE, TokenType.MODULO)) {
            Token operator = tokens.previous();
            ExpressionNode right = parseUnary();
            
            BinaryOperator op = switch (operator.type()) {
                case STAR -> BinaryOperator.MULTIPLY;
                case DIVIDE -> BinaryOperator.DIVIDE;
                case MODULO -> BinaryOperator.MODULO;
                default -> throw new ParserException("Unexpected operator: " + operator.type());
            };
            
            expr = new BinaryExpressionNode(expr, op, right, location(operator));
        }
        
        return expr;
    }
    
    /**
     * Parse unary expressions (unary minus).
     * 
     * <p><strong>Grammar Rule:</strong></p>
     * <pre>
     * unary ::= MINUS unary | primary
     * </pre>
     * 
     * 
     * <p><strong>Special Cases:</strong></p>
     * <ul>
     *   <li>Supports numeric negation and duration negation</li>
     *   <li>Double negation (--x) handled correctly</li>
     * </ul>
     * 
     * @return ExpressionNode representing the parsed unary expression
     * @throws ParserException if syntax errors occur in operand
     */
    private ExpressionNode parseUnary() throws ParserException {
        if (tokens.match(TokenType.MINUS)) {
            Token operator = tokens.previous();
            ExpressionNode expr = parseUnary();
            return new UnaryExpressionNode(UnaryOperator.MINUS, expr, location(operator));
        }
        
        return parsePrimary();
    }
    
    /**
     * Parse primary expression (literals, identifiers, function calls, parenthesized expressions).
     * 
     * <p><strong>Grammar Rule:</strong></p>
     * <pre>
     * primary ::= NUMBER | STRING | DURATION | TIMESTAMP | TRUE | FALSE | NULL
     *           | IDENTIFIER ( LPAREN argument_list RPAREN )?
     *           | SYNTHETIC_FIELD
     *           | ARRAY_FIELD_REFERENCE
     *           | LBRACKET expression_list RBRACKET
     *           | LPAREN expression RPAREN
     *           | percentile_function
     *           | unary_minus primary
     * 
     * percentile_function ::= ( P90 | P95 | P99 | P999 | PERCENTILE ) 
     *                       ( LPAREN argument_list RPAREN )?
     *                       ( SELECT LPAREN expression RPAREN )?
     * </pre>
     * 
     * 
     * <p><strong>Special Cases:</strong></p>
     * <ul>
     *   <li>Percentile functions with and without parentheses</li>
     *   <li>Percentile selection functions (P90SELECT, etc.)</li>
     *   <li>Array literals with square bracket syntax</li>
     *   <li>Synthetic field references ($field)</li>
     *   <li>Array field references (field[index])</li>
     *   <li>Function call disambiguation from identifiers</li>
     * </ul>
     * 
     * @return ExpressionNode representing the parsed primary expression
     * @throws ParserException if unrecoverable syntax errors occur
     */
    private ExpressionNode parsePrimary() throws ParserException {
        // Parenthesized expression
        if (tokens.match(TokenType.LPAREN)) {
            try {
                ExpressionNode expr = parseExpression();
                tokens.consume(TokenType.RPAREN, "Expected ')' after expression");
                return expr;
            } catch (ParserException e) {
                // Error in parenthesized expression - add error and create synthetic node
                ParserErrorHandler.ParserError error = errorHandler.createExpressionError(
                    tokens.current(), "Invalid parenthesized expression: " + e.getMessage());
                errorHandler.addError(error);
                
                // Skip to closing paren or recovery point
                tokens.skipToRecoveryPoint(TokenType.RPAREN);
                if (tokens.check(TokenType.RPAREN)) {
                    tokens.advance(); // consume closing paren
                }
                
                // Return synthetic identifier for continued parsing
                return new IdentifierNode("__error__", location(tokens.previous()));
            }
        }
        
        // Unary minus
        if (tokens.match(TokenType.MINUS)) {
            Token minus = tokens.previous();
            try {
                return new UnaryExpressionNode(UnaryOperator.MINUS, parsePrimary(), location(minus));
            } catch (ParserException e) {
                // Error in unary expression - add error and create synthetic node
                ParserErrorHandler.ParserError error = errorHandler.createExpressionError(
                    tokens.current(), "Invalid unary expression: " + e.getMessage());
                errorHandler.addError(error);
                return new LiteralNode(new CellValue.NumberValue(0), location(minus));
            }
        }
        
        // Special percentile functions check (these have specific token types)
        if (tokens.check(TokenType.P90SELECT) || tokens.check(TokenType.P95SELECT) || 
            tokens.check(TokenType.P99SELECT) || tokens.check(TokenType.P999SELECT) || 
            tokens.check(TokenType.PERCENTILE_SELECT) ||
            tokens.check(TokenType.P90) || tokens.check(TokenType.P95) || 
            tokens.check(TokenType.P99) || tokens.check(TokenType.P999) || 
            tokens.check(TokenType.PERCENTILE)) {
            Token funcToken = tokens.advance();
            try {
                return parseFunctionCall(funcToken);
            } catch (ParserException e) {
                // Error in function call - already handled by parseFunctionCall recovery
                // Return synthetic function call
                return createSyntheticFunctionCall(funcToken.value(), new ArrayList<>(), location(funcToken));
            }
        }
        
        // Handle identifiers (including function names)
        if (tokens.match(TokenType.IDENTIFIER, TokenType.SYNTHETIC_FIELD)) {
            Token identifier = tokens.previous();
            
            // Validate identifier length
            ParserUtils.validateIdentifierLength(identifier.value(), "identifier");
            
            // Check for common duration units used without a value (common mistake)
            String[] durationUnits = {"ms", "us", "ns", "s", "m", "h", "d", "min", "hour", "hours", "day", "days"};
            for (String unit : durationUnits) {
                if (identifier.value().equals(unit)) {
                    ParserErrorHandler.ParserError error = errorHandler.createExpressionError(
                        identifier, "Duration unit '" + unit + "' without a value. Use a number before the unit (e.g., '5" + unit + "')");
                    errorHandler.addError(error);
                    // Return a synthetic duration literal with value 0
                    return new LiteralNode(new me.bechberger.jfr.extended.table.CellValue.DurationValue(java.time.Duration.ZERO), location(identifier));
                }
            }
            
            // Check if this is a function call - only allow parentheses for function calls
            // Square brackets are reserved for array literals only
            boolean isFunction = tokens.check(TokenType.LPAREN);
            
            if (isFunction) {
                try {
                    return parseFunctionCall(identifier);
                } catch (ParserException e) {
                    // Error in function call - already handled by parseFunctionCall recovery
                    // Return synthetic function call
                    return createSyntheticFunctionCall(identifier.value(), new ArrayList<>(), location(identifier));
                }
            }
                 // Special case: check if this looks like a function name but user forgot delimiters
        if (functionValidator.functionExists(identifier.value()) && 
            !tokens.check(TokenType.DOT) && !tokens.isAtEndOfClause()) {
            // This looks like a function name but missing parentheses
            ParserErrorHandler.ParserError error = errorHandler.createFunctionCallError(
                identifier, "Function '" + identifier.value() + "' is missing parentheses. Use " + 
                identifier.value() + "() for function calls");
            errorHandler.addError(error);
            
            // Create synthetic function call with no arguments
            return createSyntheticFunctionCall(identifier.value(), new ArrayList<>(), location(identifier));
        }
            
            // Check for field access (alias.field) - only allow access via valid aliases
            if (tokens.match(TokenType.DOT)) {
                try {
                    Token field = tokens.consume(TokenType.IDENTIFIER, "Expected field name after dot");
                    
                    // Note: Alias validation is deferred to later phase due to parsing order
                    // (SELECT clause is parsed before FROM clause which defines aliases)
                    
                    return new FieldAccessNode(identifier.value(), field.value(), location(identifier));
                } catch (ParserException e) {
                    // Error in field access - add error and return just the base identifier
                    ParserErrorHandler.ParserError error = errorHandler.createExpressionError(
                        tokens.current(), "Invalid field access: " + e.getMessage());
                    errorHandler.addError(error);
                    return new IdentifierNode(identifier.value(), location(identifier));
                }
            }
            
            return new IdentifierNode(identifier.value(), location(identifier));
        }
        
        // Array literal
        if (tokens.match(TokenType.LBRACKET)) {
            try {
                List<ExpressionNode> elements = new ArrayList<>();
                // Check for empty array
                if (tokens.check(TokenType.RBRACKET)) {
                    tokens.consume(TokenType.RBRACKET, "Expected ']' after array literal");
                    return new ArrayLiteralNode(elements, location(tokens.previous()));
                }
                
                // Parse array elements with error recovery
                do {
                    elements.add(parseExpression());
                } while (tokens.match(TokenType.COMMA));
                
                tokens.consume(TokenType.RBRACKET, "Expected ']' after array literal");
                return new ArrayLiteralNode(elements, location(tokens.previous()));
            } catch (ParserException e) {
                // Error in array literal - add error and create empty array
                ParserErrorHandler.ParserError error = errorHandler.createExpressionError(
                    tokens.current(), "Invalid array literal: " + e.getMessage());
                errorHandler.addError(error);
                
                // Skip to closing bracket
                tokens.skipToRecoveryPoint(TokenType.RBRACKET);
                if (tokens.check(TokenType.RBRACKET)) {
                    tokens.advance();
                }
                
                return new ArrayLiteralNode(new ArrayList<>(), location(tokens.previous()));
            }
        }
        
        // Literals
        if (tokens.match(TokenType.STRING)) {
            Token literal = tokens.previous();
            String value = literal.value();
            // Remove surrounding quotes
            if (value.length() >= 2 && value.startsWith("'") && value.endsWith("'")) {
                value = value.substring(1, value.length() - 1);
            }
            // Validate string literal content
            ParserUtils.validateStringLiteral(value);
            return new LiteralNode(new CellValue.StringValue(value), location(literal));
        }
        
        if (tokens.match(TokenType.DURATION_LITERAL)) {
            Token literal = tokens.previous();
            try {
                String value = literal.value();
                long nanoseconds = Utils.parseTimespan(value);
                java.time.Duration duration = java.time.Duration.ofNanos(nanoseconds);
                return new LiteralNode(new CellValue.DurationValue(duration), location(literal));
            } catch (Exception e) {
                return new LiteralNode(new CellValue.DurationValue(java.time.Duration.ZERO), location(literal));
            }
        }
        
        if (tokens.match(TokenType.MEMORY_SIZE_LITERAL)) {
            Token literal = tokens.previous();
            try {
                String value = literal.value();
                long bytes = Utils.parseMemorySize(value);
                return new LiteralNode(new CellValue.MemorySizeValue(bytes), location(literal));
            } catch (Exception e) {
                return new LiteralNode(new CellValue.MemorySizeValue(0L), location(literal));
            }
        }
        
        if (tokens.match(TokenType.TIMESTAMP_LITERAL)) {
            Token literal = tokens.previous();
            try {
                String value = literal.value();
                // Handle timestamps with or without 'Z' suffix
                if (!value.endsWith("Z")) {
                    value += "Z";  // Assume UTC if no timezone specified
                }
                java.time.Instant instant = java.time.Instant.parse(value);
                return new LiteralNode(new CellValue.TimestampValue(instant), location(literal));
            } catch (Exception e) {
                // Add error to error handler for invalid timestamp
                ParserErrorHandler.ParserError error = errorHandler.createExpressionError(
                    literal, "Invalid timestamp format: " + literal.value() + ". " + e.getMessage() + ". Use format: YYYY-MM-DDTHH:MM:SSZ");
                errorHandler.addError(error);
                return new LiteralNode(new CellValue.TimestampValue(java.time.Instant.EPOCH), location(literal));
            }
        }
        
        if (tokens.match(TokenType.NUMBER)) {
            Token literal = tokens.previous();
            double value = Double.parseDouble(literal.value());
            return new LiteralNode(new CellValue.NumberValue(value), location(literal));
        }
        
        if (tokens.match(TokenType.BOOLEAN)) {
            Token literal = tokens.previous();
            boolean value = Boolean.parseBoolean(literal.value());
            return new LiteralNode(new CellValue.BooleanValue(value), location(literal));
        }
        
        // Handle STAR token (for expressions like COUNT(*))
        if (tokens.match(TokenType.STAR)) {
            Token star = tokens.previous();
            return new StarNode(location(star));
        }
        
        throw new ParserException("Unexpected token: '" + tokens.current().value() + "' at " + tokens.current().getPositionString());
    }
    

    
    /**
     * Parse function call with known function name token.
     * 
     * <p><strong>Grammar Rule:</strong></p>
     * <pre>
     * function_call ::= function_name LPAREN argument_list RPAREN
     *                 | function_name LBRACKET argument_list RBRACKET
     *                 | percentile_function ( SELECT LPAREN expression RPAREN )?
     * 
     * argument_list ::= ( expression ( COMMA expression )* )?
     * percentile_function ::= P90 | P95 | P99 | P999 | PERCENTILE
     * </pre>
     * 
     * 
     * <p><strong>Special Cases:</strong></p>
     * <ul>
     *   <li>Percentile functions can omit parentheses (P90 vs P90())</li>
     *   <li>Percentile selection functions (P90SELECT expression)</li>
     *   <li>Square bracket syntax for some functions</li>
     *   <li>Function name validation with typo suggestions</li>
     *   <li>Recovery from missing delimiters</li>
     * </ul>
     * 
     * @param functionName Token representing the function name
     * @return ExpressionNode representing the parsed function call
     * @throws ParserException if function syntax is invalid or function doesn't exist
     */
    private ExpressionNode parseFunctionCall(Token functionName) throws ParserException {
        String funcName = functionName.value();
        
        // Validate function name case
        ParserUtils.validateFunctionCase(funcName);
        
        // Set context for function call parsing
        QueryErrorMessageGenerator.QueryContext previousContext = currentContext;
        currentContext = QueryErrorMessageGenerator.QueryContext.FUNCTION_CALL;
        
        try {
            List<ExpressionNode> arguments = new ArrayList<>();
            boolean usesParentheses = tokens.check(TokenType.LPAREN);
            
            if (usesParentheses) {
                // Standard function call with parentheses
                try {
                    tokens.consume(TokenType.LPAREN, "Expected '(' after function name");
                } catch (ParserException e) {
                    // Recover by adding error and creating synthetic function call
                    ParserErrorHandler.ParserError error = errorHandler.createFunctionCallError(functionName, e.getMessage());
                    errorHandler.addError(error);
                    return createSyntheticFunctionCall(funcName, arguments, location(functionName));
                }
                
                if (!tokens.check(TokenType.RPAREN)) {
                    arguments = parseFunctionArguments();
                }
                
                try {
                    tokens.consume(TokenType.RPAREN, "Expected ')' after function arguments");
                } catch (ParserException e) {
                    // Recover by adding error but continue parsing
                    String foundToken = tokens.current().value().isEmpty() ? "end of input" : "'" + tokens.current().value() + "'";
                    ParserErrorHandler.ParserError error = errorHandler.createFunctionCallError(
                        tokens.current(), "Expected ')' after function arguments, found " + foundToken);
                    errorHandler.addError(error);
                    // Try to skip to a reasonable recovery point
                    tokens.skipToRecoveryPoint(TokenType.RPAREN, TokenType.COMMA, TokenType.SEMICOLON);
                }
            } else {
                // No delimiters found - add error and create synthetic function call
                ParserErrorHandler.ParserError error = errorHandler.createFunctionCallError(
                    functionName, "Expected '(' after function name");
                errorHandler.addError(error);
                return createSyntheticFunctionCall(funcName, arguments, location(functionName));
            }
             // Validate function and arguments using utility classes
            validateAndSuggestFunction(funcName, arguments, functionName, previousContext);
            
            // Validate that aggregate functions don't contain nested aggregates
            if (functionValidator.isAggregateFunction(funcName)) {
                Set<String> aggregateFunctions = functionValidator.getAggregateFunctions();
                for (ExpressionNode arg : arguments) {
                    try {
                        ParserUtils.validateNoNestedAggregates(arg, aggregateFunctions);
                    } catch (ParserException e) {
                        // Add error to error handler instead of propagating exception
                        ParserErrorHandler.ParserError error = errorHandler.createFunctionCallError(
                            functionName, e.getMessage());
                        errorHandler.addError(error);
                        break; // Stop checking further arguments once we find a nested aggregate
                    }
                }
            }
            
            // Check if this is a percentile function and create the appropriate node type using utility
            if (PercentileFunctionParser.isPercentileFunction(funcName)) {
                return PercentileFunctionParser.createPercentileFunction(funcName, arguments, location(functionName));
            }
            
            // Check if this is a percentile selection function using utility
            if (PercentileFunctionParser.isPercentileSelectionFunction(funcName)) {
                return PercentileFunctionParser.createPercentileSelectionFunction(funcName, arguments, location(functionName));
            }
            
            // For non-percentile functions, create a regular FunctionCallNode
            return new FunctionCallNode(functionName.value(), arguments, location(functionName));
        } finally {
            // Restore previous context
            currentContext = previousContext;
        }
    }

    /**
     * Parse a raw JFR query (capture tokens until semicolon or explicit statement boundary).
     * 
     * <p><strong>Grammar Rule:</strong></p>
     * <pre>
     * raw_jfr_query ::= ( any_token_except_statement_boundary )+
     * statement_boundary ::= SEMICOLON | SHOW | VIEW | assignment_start | EXTENDED_QUERY
     * assignment_start ::= IDENTIFIER ASSIGN
     * </pre>
     * 
     * 
     * <p><strong>Boundary Detection:</strong></p>
     * <ul>
     *   <li><strong>Semicolon (;):</strong> Explicit statement terminator</li>
     *   <li><strong>SHOW:</strong> Start of metadata query</li>
     *   <li><strong>VIEW:</strong> Start of view definition</li>
     *   <li><strong>Assignment (x :=):</strong> Start of variable assignment</li>
     *   <li><strong>Extended query (@):</strong> Start of extended query</li>
     * </ul>
     * 
     * <p><strong>Special Cases:</strong></p>
     * <ul>
     *   <li>Handles plain SELECT queries without @ prefix</li>
     *   <li>Preserves original JFR query syntax exactly</li>
     *   <li>Stops at semicolons but doesn't consume them</li>
     *   <li>Provides context in error messages for debugging</li>
     * </ul>
     * 
     * @return RawJfrQueryNode containing the captured query text
     * @throws ParserException if query is empty or invalid
     */
    private RawJfrQueryNode parseRawJfrQuery() throws ParserException {
        int line = tokens.current().line();
        int column = tokens.current().column();
        
        StringBuilder queryBuilder = new StringBuilder();
        boolean first = true;
        int lastTokenLine = line;
        
        while (!tokens.isAtEnd()) {
            Token token = tokens.current();
            
            // Stop at semicolon (explicit statement separator)
            if (token.type() == TokenType.SEMICOLON) {
                break; // Don't consume the semicolon here - let main parser handle it
            }
            
            // Stop only at explicit statement-starting tokens
            // Be conservative - only split on very clear statement boundaries
            if (token.type() == TokenType.SHOW) {
                break;
            }
            
            // Stop at VIEW definition
            if (token.type() == TokenType.VIEW) {
                break;
            }
            
            // Stop at assignment (identifier followed by :=)
            if (token.type() == TokenType.IDENTIFIER && tokens.checkNext(TokenType.ASSIGN)) {
                break;
            }
            
            // Stop at extended query marker (@)
            if (token.type() == TokenType.EXTENDED_QUERY) {
                break;
            }
            
            // Check for empty line separator (more than 1 line gap)
            // This handles raw queries separated by empty lines
            if (!first && token.line() > lastTokenLine + 1) {
                // We found an empty line gap - this should split the raw query
                // But only if we have some content already
                if (!queryBuilder.toString().trim().isEmpty()) {
                    break;
                }
            }
            
            // Update last token line, but only for non-whitespace tokens
            if (token.type() != TokenType.WHITESPACE) {
                lastTokenLine = token.line();
            }
            
            tokens.advance(); // consume the token
            if (!first) {
                queryBuilder.append(" ");
            }
            queryBuilder.append(token.value());
            first = false;
        }
        
        String queryContent = queryBuilder.toString().trim();
        if (queryContent.isEmpty()) {
            throw new ParserException("Empty raw query at line " + line + ", column " + column + 
                ". Raw queries cannot be empty. Use semicolon (;) to separate multiple statements.");
        }
        
        return new RawJfrQueryNode(queryContent, location(line, column));
    }
    
    /**
    // ==================== UTILITY METHODS ====================
    

    
    /**
     * Report an unexpected token error with enhanced suggestions
     */
    private void reportUnexpectedToken(Token token, String expectedDescription) {
        String enhancedMessage = expectedDescription;
        
        // Check if this might be an operator-related error
        if (token != null && token.value() != null) {
            String tokenValue = token.value();
            
            // Check for common operator mistakes
            if ("/".equals(tokenValue) || "*".equals(tokenValue) || "%".equals(tokenValue)) {
                String operatorSuggestion = ParserSuggestionHelper.createOperatorSuggestion(
                    tokenValue, 
                    getTokenContext(token)
                );
                enhancedMessage = expectedDescription + " " + operatorSuggestion;
            }
        }
        
        ParserErrorHandler.ParserError error = errorHandler.createUnexpectedTokenError(token, enhancedMessage);
        errorHandler.addError(error);
    }
    
    /**
     * Get context around a token for better error messages
     */
    private String getTokenContext(Token token) {
        if (token == null) return "";
        
        // Get a few tokens before and after for context
        int currentPos = tokens.getCurrentPosition();
        StringBuilder context = new StringBuilder();
        
        // Go back a few tokens
        int startPos = Math.max(0, currentPos - 2);
        tokens.setPosition(startPos);
        
        for (int i = 0; i < 5 && !tokens.isAtEnd(); i++) {
            if (tokens.current() != null) {
                context.append(tokens.current().value()).append(" ");
            }
            tokens.advance();
        }
        
        // Restore position
        tokens.setPosition(currentPos);
        
        return context.toString().trim();
    }
    
    /**
     * Recover to a statement boundary after an error
     */
    private void recoverToStatementBoundary() {
        errorRecoveryMode = true;
        panicMode = true;
        
        while (!tokens.isAtEnd()) {
            Token token = tokens.current();
            
            // Stop at statement boundaries
            if (token.type() == TokenType.SEMICOLON ||
                token.type() == TokenType.SELECT ||
                token.type() == TokenType.SHOW ||
                token.type() == TokenType.VIEW ||
                token.type() == TokenType.EXTENDED_QUERY ||
                (token.type() == TokenType.IDENTIFIER && tokens.checkNext(TokenType.ASSIGN))) {
                break;
            }
            
            tokens.advance();
        }
        
        // Consume semicolon if present
        if (tokens.check(TokenType.SEMICOLON)) {
            tokens.advance();
        }
        
        errorRecoveryMode = false;
        panicMode = false;
    }
    
    /**
     * Parse an alias with error message (for backward compatibility)
     */
    private Token parseAlias(String errorMessage) throws ParserException {
        if (tokens.check(TokenType.IDENTIFIER) && !ParserUtils.isReservedKeyword(tokens.current().value())) {
            return tokens.advance();
        }
        throw new ParserException(errorMessage + " at " + tokens.current().getPositionString());
    }
    
    /**
     * Create a synthetic function call for aggregations without explicit parentheses
     */
    private FunctionCallNode createSyntheticFunctionCall(String functionName, List<ExpressionNode> arguments, Location location) {
        return new FunctionCallNode(functionName, arguments, location);
    }
    
    /**
     * Parse function arguments within parentheses (assumes opening paren already consumed)
     */
    private List<ExpressionNode> parseFunctionArguments() throws ParserException {
        List<ExpressionNode> arguments = new ArrayList<>();
        
        // Note: opening parenthesis should already be consumed by caller
        
        if (!tokens.check(TokenType.RPAREN)) {
            do {
                // Check for double parentheses pattern like ((expression))
                // This happens when the first token in arguments is a LPAREN
                if (tokens.check(TokenType.LPAREN)) {
                    // Look ahead to see if this creates a double parentheses pattern
                    // We need to check if after parsing this parenthesized expression,
                    // we would have another RPAREN that would create double closing parens
                    int parenDepth = 0;
                    int currentPos = tokens.getCurrentPosition();
                    
                    // Count parentheses to find the matching closing paren
                    while (!tokens.isAtEnd()) {
                        if (tokens.current().type() == TokenType.LPAREN) {
                            parenDepth++;
                        } else if (tokens.current().type() == TokenType.RPAREN) {
                            parenDepth--;
                            if (parenDepth == 0) {
                                // Found matching closing paren, check if next token is also RPAREN
                                tokens.advance();
                                if (tokens.check(TokenType.RPAREN)) {
                                    // Reset position and add error
                                    tokens.setPosition(currentPos);
                                    
                                    // Find the function name and extract argument text for suggestion
                                    String functionName = getCurrentFunctionName();
                                    String argumentText = extractArgumentTextForSuggestion(currentPos);
                                    String suggestion = ParserSuggestionHelper.createDoubleParenthesesSuggestion(functionName, argumentText);
                                    
                                    ParserErrorHandler.ParserError error = errorHandler.createFunctionCallError(
                                        tokens.current(), "Invalid double parentheses in function call. " +
                                        "Found '((' which creates unnecessary nesting. " + suggestion);
                                    errorHandler.addError(error);
                                    break;
                                }
                                break;
                            }
                        }
                        tokens.advance();
                    }
                    
                    // Reset position for normal parsing
                    tokens.setPosition(currentPos);
                }
                arguments.add(parseExpression());
            } while (tokens.match(TokenType.COMMA));
        }
        
        // Note: closing parenthesis should be consumed by caller
        return arguments;
    }
    
    /**
     * Validate function calls and provide suggestions for common errors using FunctionValidator utility
     */
    private void validateAndSuggestFunction(String funcName, List<ExpressionNode> arguments, 
                                          Token functionToken, QueryErrorMessageGenerator.QueryContext context) {
        // Check if function exists using FunctionValidator
        if (!functionValidator.functionExists(funcName)) {
            // Function not found - suggest similar functions
            String suggestion = functionValidator.createFunctionSuggestion(funcName);
            ParserErrorHandler.ParserError error = errorHandler.createFunctionCallError(
                functionToken, suggestion);
            errorHandler.addError(error);
            return; // Exit early if function doesn't exist
        }
        
        // Check if aggregate function is used in WHERE clause (not allowed)
        if (functionValidator.isAggregateFunction(funcName) && 
            context == QueryErrorMessageGenerator.QueryContext.WHERE_CLAUSE) {
            ParserErrorHandler.ParserError error = errorHandler.createFunctionCallError(
                functionToken, "Aggregate function '" + funcName + "' cannot be used in WHERE clause. " +
                             "Use aggregate functions in SELECT or HAVING clauses instead.");
            errorHandler.addError(error);
        }
        
        // Stricter argument validation for specific functions using FunctionValidator
        try {
            functionValidator.validateFunctionArguments(funcName, arguments.size());
        } catch (ParserException e) {
            ParserErrorHandler.ParserError error = errorHandler.createFunctionCallError(
                functionToken, e.getMessage());
            errorHandler.addError(error);
        }
    }

    /**
     * Check if there were any parsing errors during the last parsing operation.
     * @return true if there were errors, false otherwise
     */
    public boolean hasParsingErrors() {
        return errorHandler.hasErrors();
    }

    /**
     * Get the list of parsing errors from the last parsing operation.
     * @return list of ParserError objects
     */
    public List<ParserErrorHandler.ParserError> getParsingErrors() {
        return errorHandler.getErrors();
    }

    /**
     * Get a formatted error report containing all parsing errors.
     * @return formatted error report string
     */
    public String getErrorReport() {
        return errorHandler.createErrorReport();
    }
    
    /**
     * Validate that there are no duplicate table names without aliases in the FROM clause.
     * This enforces strict SQL syntax where duplicate table references must have aliases.
     */
    private void validateNoDuplicateTableNames(List<SourceNodeBase> sources) throws ParserException {
        java.util.Set<String> tableNames = new java.util.HashSet<>();
        
        for (SourceNodeBase source : sources) {
            if (source instanceof SourceNode sourceNode) {
                String tableName = sourceNode.source();
                String alias = sourceNode.alias();
                
                // Check if alias is the same as table name (confusing and should be avoided)
                if (alias != null && alias.equals(tableName)) {
                    ParserErrorHandler.ParserError error = new ParserErrorHandler.ParserError(
                        "Table alias '" + alias + "' is the same as table name '" + tableName + "'",
                        "Table aliases should be different from table names to avoid confusion. Use a distinct alias like: FROM " + tableName + " t1",
                        tokens.current(),
                        errorHandler.extractContext(tokens.current()),
                        ParserErrorHandler.ErrorType.SYNTAX_ERROR
                    );
                    errorHandler.addError(error);
                }
                
                // If there's no alias and we've seen this table name before, it's a duplicate
                if (alias == null && !tableNames.add(tableName)) {
                    ParserErrorHandler.ParserError error = new ParserErrorHandler.ParserError(
                        "Duplicate table name '" + tableName + "' without alias",
                        "Use table aliases to distinguish between multiple references to the same table. Example: FROM " + tableName + " t1, " + tableName + " t2",
                        tokens.current(),
                        errorHandler.extractContext(tokens.current()),
                        ParserErrorHandler.ErrorType.SYNTAX_ERROR
                    );
                    errorHandler.addError(error);
                }
            }
        }
    }
    
    /**
     * Check if the current token could start a new statement.
     * This is used to detect potential statement boundaries for raw queries
     * separated by empty lines without requiring explicit semicolons.
     */
    private boolean isNewStatementStart() {
        Token current = tokens.current();
        
        // SQL keywords that could start a raw JFR query
        if (current.type() == TokenType.SELECT || 
            current.type() == TokenType.WITH) {
            return true;
        }
        
        // Other statement-starting tokens that we already handle
        if (current.type() == TokenType.SHOW || 
            current.type() == TokenType.VIEW || 
            current.type() == TokenType.EXTENDED_QUERY) {
            return true;
        }
        
        // Assignment statements
        if ((current.type() == TokenType.IDENTIFIER || current.type() == TokenType.SYNTHETIC_FIELD) && 
            tokens.checkNext(TokenType.ASSIGN)) {
            return true;
        }
        
        return false;
    }
    
    // ==================== UTILITY METHODS ====================
    
    /**
     * Get the current function name being parsed (for error suggestions).
     * This helps create context-aware error messages.
     */
    /**
     * Extract argument text for error suggestion based on the current parsing position.
     * This method tries to extract meaningful text from between parentheses for suggestions.
     */
    private String extractArgumentTextForSuggestion(int startPos) {
        int currentPos = tokens.getCurrentPosition();
        
        // Look for the first identifier after the opening parenthesis
        tokens.setPosition(startPos);
        tokens.advance(); // Skip the opening parenthesis
        
        StringBuilder argumentText = new StringBuilder();
        int parenDepth = 0;
        
        while (!tokens.isAtEnd()) {
            Token token = tokens.current();
            
            if (token.type() == TokenType.LPAREN) {
                parenDepth++;
            } else if (token.type() == TokenType.RPAREN) {
                if (parenDepth == 0) {
                    break; // Reached the matching closing parenthesis
                }
                parenDepth--;
            } else if (token.type() == TokenType.IDENTIFIER && argumentText.length() == 0) {
                // Take the first identifier as the main argument
                argumentText.append(token.value());
            }
            
            tokens.advance();
        }
        
        // Restore position
        tokens.setPosition(currentPos);
        return argumentText.toString();
    }

    private String getCurrentFunctionName() {
        // Save current position
        int currentPos = tokens.getCurrentPosition();
        
        // Look backward for the function name that started this call
        for (int i = currentPos - 1; i >= 0; i--) {
            tokens.setPosition(i);
            if (!tokens.isAtEnd() && tokens.current().type() == TokenType.IDENTIFIER) {
                String identifier = tokens.current().value();
                // Check if next token is LPAREN (making this a function call)
                tokens.setPosition(i + 1);
                if (!tokens.isAtEnd() && tokens.current().type() == TokenType.LPAREN) {
                    // Restore position and return the function name
                    tokens.setPosition(currentPos);
                    return identifier;
                }
            }
        }
        
        // Restore position
        tokens.setPosition(currentPos);
        return "function"; // fallback
    }
    
}