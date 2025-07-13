package me.bechberger.jfr.extended;

/**
 * Extended JFR Query Language Grammar
 * 
 * This is an extended version of the JFR query language with additional features
 * for time-aware operations, fuzzy joins, percentile functions, variables, and nested expressions.
 * 
 * For the complete grammar specification, lexer definition, examples, and extended features,
 * see {@link #getGrammarText()}.
 */
public final class Grammar {
    
    private Grammar() {
        // Utility class
    }
    
    /**
     * Returns the complete grammar documentation as a string
     */
    public static String getGrammarText() {
        return """
            Extended JFR Query Language Grammar
            
            This is an extended version of the JFR query language with additional features
            for time-aware operations, fuzzy joins, percentile functions, variables, and nested expressions.
            
            GRAMMAR DEFINITION:
            
            program         ::= statement*
            statement       ::= assignment | query | viewDefinition | showQuery | rawJfrQuery
            assignment      ::= IDENTIFIER ':=' query
            viewDefinition  ::= 'VIEW' IDENTIFIER 'AS' query
            showQuery       ::= 'SHOW' ('EVENTS' | 'FIELDS' IDENTIFIER)
            rawJfrQuery     ::= <any sequence of tokens that doesn't start with '@', VIEW, SHOW, or assignment>
            
            query           ::= ['@'] [column] [format] select from [join*] [where] [groupBy] [having] [orderBy] [limit]
            column          ::= 'COLUMN' STRING (',' STRING)*
            format          ::= 'FORMAT' formatter (',' formatter)*
            formatter       ::= property (';' property)*
            property        ::= IDENTIFIER ['=' IDENTIFIER]
            
            select          ::= 'SELECT' ('*' | selectItem (',' selectItem)*)
            selectItem      ::= expression ['AS' alias]
            alias           ::= IDENTIFIER | keyword | functionName
            
            from            ::= 'FROM' source (',' source)*
            source          ::= (IDENTIFIER | subquery) ['AS' alias] [join*]
            subquery        ::= '(' query ')'
            
            join            ::= [joinType] 'JOIN' source ['AS' alias] 'ON' joinCondition |
                                'FUZZY' 'JOIN' source ['AS' alias] 'ON' IDENTIFIER ['WITH' fuzzyType] ['TOLERANCE' expression]
            joinType        ::= 'INNER' | 'LEFT' | 'RIGHT' | 'FULL'
            joinCondition   ::= IDENTIFIER '=' IDENTIFIER
            fuzzyType       ::= 'NEAREST' | 'PREVIOUS' | 'AFTER'
            
            where           ::= 'WHERE' expression
            groupBy         ::= 'GROUP' 'BY' expression (',' expression)*
            having          ::= 'HAVING' expression
            orderBy         ::= 'ORDER' 'BY' orderExpression (',' orderExpression)*
            orderExpression ::= expression ['ASC' | 'DESC']
            limit           ::= 'LIMIT' NUMBER
            
            expression      ::= comparison
            comparison      ::= arithmetic (('=' | '!=' | '<' | '>' | '<=' | '>=' | 'LIKE' | 'IN') arithmetic)*
            arithmetic      ::= term (('+' | '-') term)*
            term            ::= unary (('*' | '/' | '%') unary)*
            unary           ::= ['-'] primary
            primary         ::= '(' expression ')' |
                                functionCall |
                                fieldAccess |
                                IDENTIFIER |
                                literal
            
            functionCall    ::= (IDENTIFIER | functionKeyword) '(' [argumentList] ')'
            argumentList    ::= ('*' | expression) (',' ('*' | expression))*
            fieldAccess     ::= IDENTIFIER '.' IDENTIFIER
            
            percentileFunction ::= ('P90' | 'P95' | 'P99' | 'P999') '(' expression [',' expression] ')'
            percentileSelect   ::= ('P90SELECT' | 'P95SELECT' | 'P99SELECT' | 'P999SELECT') 
                                  '(' IDENTIFIER ',' IDENTIFIER ',' expression ')' |
                                  'PERCENTILE_SELECT' '(' NUMBER ',' IDENTIFIER ',' IDENTIFIER ',' expression ')'
            
            literal         ::= STRING | NUMBER | DURATION_LITERAL | MEMORY_SIZE_LITERAL
            
            LEXER DEFINITION:
            
            // Keywords
            SELECT, FROM, WHERE, GROUP, BY, HAVING, ORDER, ASC, DESC, LIMIT,
            INNER, LEFT, RIGHT, FULL, JOIN, FUZZY, ON, WITH, TOLERANCE,
            NEAREST, PREVIOUS, AFTER, AS, VIEW, SHOW, EVENTS, FIELDS,
            COLUMN, FORMAT, AND, OR, LIKE, IN, NOT
            
            // Operators
            ASSIGN         ::= ':='
            EQUALS         ::= '='
            NOT_EQUALS     ::= '!='
            LESS_THAN      ::= '<'
            GREATER_THAN   ::= '>'
            LESS_EQUAL     ::= '<='
            GREATER_EQUAL  ::= '>='
            PLUS           ::= '+'
            MINUS          ::= '-'
            STAR           ::= '*'
            DIVIDE         ::= '/'
            MODULO         ::= '%'
            
            // Punctuation
            LPAREN         ::= '('
            RPAREN         ::= ')'
            COMMA          ::= ','
            SEMICOLON      ::= ';'
            DOT            ::= '.'
            EXTENDED_QUERY ::= '@'
            
            // Literals
            STRING          ::= "'" [^']* "'"
            NUMBER          ::= [0-9]+ ('.' [0-9]+)?
            DURATION_LITERAL ::= NUMBER ('ns' | 'us' | 'ms' | 's' | 'm' | 'h' | 'd')
            MEMORY_SIZE_LITERAL ::= NUMBER ('B' | 'KB' | 'MB' | 'GB' | 'TB')
            IDENTIFIER      ::= [a-zA-Z_][a-zA-Z0-9_]*
            
            // Whitespace and comments
            WHITESPACE      ::= [ \\t\\r\\n]+
            COMMENT         ::= '--' [^\\r\\n]* | '/*' .*? '*/'
            
            EXTENDED FEATURES:
            
            1. Extended Query Marker (@):
               - Queries can start with '@' to indicate extended syntax
               - Raw JFR queries are passed through as-is when not using extended syntax
            
            2. COLUMN and FORMAT clauses:
               - COLUMN "name1", "name2" - specify column names for output
               - FORMAT property=value; property2=value2 - output formatting options
            
            3. Advanced JOIN operations:
               - Standard joins: INNER, LEFT, RIGHT, FULL JOIN with ON conditions
               - Fuzzy joins: FUZZY JOIN with temporal matching (NEAREST, PREVIOUS, AFTER)
               - Tolerance specification for fuzzy joins
            
            4. Percentile functions:
               - P90(expression), P95(expression), P99(expression), P999(expression)
               - Optional time slice filtering as second parameter
            
            5. Percentile selection functions:
               - P90SELECT(table, id_field, value_field) - select records at percentile
               - PERCENTILE_SELECT(percentile, table, id_field, value_field) - configurable percentile
            
            6. Variables and views:
               - Variable assignment: variable := query
               - View definitions: VIEW name AS query
            
            7. Enhanced data types:
               - Duration literals: 10ms, 5s, 2m, 1h, 3d
               - Memory size literals: 1GB, 512MB, 1024KB, 2048B
            
            8. Expression system:
               - Full arithmetic expressions with precedence
               - Comparison operators: =, !=, <, >, <=, >=, LIKE, IN
               - Field access: table.field
               - Function calls with arbitrary arguments
            
            9. Special queries:
               - SHOW EVENTS - list available event types
               - SHOW FIELDS eventType - list fields for an event type
            
            10. ORDER BY clause:
                - Supports single and multi-field sorting
                - ASC (ascending) and DESC (descending) sort directions
                - ASC is default if direction not specified
                - Works with simple fields, expressions, and aliases
                - In GROUP BY queries, can order by:
                  * Grouped fields (fields in GROUP BY clause)
                  * Aggregate functions (COUNT, SUM, AVG, MIN, MAX, percentiles)
                  * Complex expressions involving aggregates
                  * Alias references to SELECT items
                - Aggregate functions in ORDER BY require GROUP BY clause
                - Multi-field sorting follows SQL precedence (left to right)
                - Supports complex expressions: (COUNT(*) * 2), ABS(age - 30), etc.
                - Compatible with HAVING and LIMIT clauses
            
            EXAMPLES:
            
            Basic extended query:
            @ SELECT * FROM ExecutionSample WHERE duration > 10ms
            
            Standard join:
            @ SELECT * FROM ExecutionSample es 
              INNER JOIN ThreadStart ts ON es.threadId = ts.threadId
            
            Fuzzy join with tolerance:
            @ SELECT * FROM ExecutionSample es 
              FUZZY JOIN GarbageCollection gc ON startTime WITH NEAREST TOLERANCE 100ms
            
            Percentile function:
            @ SELECT P99(duration) FROM ExecutionSample GROUP BY stackTrace
            
            Percentile selection:
            @ SELECT * FROM P99SELECT(ExecutionSample, id, duration)
            
            Variable assignment and view:
            slowMethods := @ SELECT * FROM ExecutionSample WHERE duration > 1s
            VIEW TopMethods AS @ SELECT stackTrace, COUNT(*) FROM slowMethods GROUP BY stackTrace
            
            Subquery in FROM clause:
            @ SELECT * FROM (@ SELECT * FROM ExecutionSample WHERE duration > 100ms) AS slow
            
            Raw JFR query (no @ prefix):
            SELECT * FROM jdk.ExecutionSample WHERE duration > "100 ms"
            
            Special queries:
            SHOW EVENTS
            SHOW FIELDS ExecutionSample
            
            ORDER BY examples:
            
            Basic field sorting:
            @ SELECT * FROM ExecutionSample ORDER BY duration DESC
            @ SELECT name, age FROM Users ORDER BY name ASC, age DESC
            
            Sorting with expressions:
            @ SELECT * FROM Users ORDER BY (age * 2) DESC
            @ SELECT * FROM ExecutionSample ORDER BY ABS(duration - 100ms) ASC
            
            GROUP BY with ORDER BY (aggregate functions):
            @ SELECT threadId, COUNT(*) FROM ExecutionSample GROUP BY threadId ORDER BY COUNT(*) DESC
            @ SELECT stackTrace, AVG(duration) FROM ExecutionSample GROUP BY stackTrace ORDER BY AVG(duration)
            @ SELECT threadId, P99(duration) as p99 FROM ExecutionSample GROUP BY threadId ORDER BY P99(duration) DESC
            
            GROUP BY with ORDER BY (grouped fields):
            @ SELECT threadId, COUNT(*) FROM ExecutionSample GROUP BY threadId ORDER BY threadId
            @ SELECT stackTrace, SUM(duration) FROM ExecutionSample GROUP BY stackTrace ORDER BY stackTrace ASC
            
            Complex multi-field ORDER BY with GROUP BY:
            @ SELECT threadId, COUNT(*) as count, AVG(duration) as avg_dur 
              FROM ExecutionSample GROUP BY threadId 
              ORDER BY COUNT(*) DESC, threadId ASC, AVG(duration) DESC
            
            ORDER BY with aliases:
            @ SELECT threadId, COUNT(*) as sample_count FROM ExecutionSample 
              GROUP BY threadId ORDER BY sample_count DESC
            
            ORDER BY with complex expressions in GROUP BY context:
            @ SELECT threadId, COUNT(*) FROM ExecutionSample GROUP BY threadId 
              ORDER BY (COUNT(*) * 2 + 1) DESC
            
            ORDER BY with HAVING and LIMIT:
            @ SELECT stackTrace, COUNT(*) FROM ExecutionSample 
              GROUP BY stackTrace 
              HAVING COUNT(*) > 10 
              ORDER BY COUNT(*) DESC 
              LIMIT 5
            """;
    }
}
