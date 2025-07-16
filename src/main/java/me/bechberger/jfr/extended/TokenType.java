package me.bechberger.jfr.extended;

import java.util.EnumSet;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Token attributes for categorizing token types
 */
enum TokenAttribute {
    LITERAL,    // Token represents a literal value
    OPERATOR,   // Token represents an operator
    KEYWORD,    // Token represents a reserved keyword
    FUNCTION    // Token represents a function name
}

/**
 * Token types for the extended JFR query language with built-in attributes
 */
public enum TokenType {
    
    // Literals (ordered from most specific to least specific)
    MEMORY_SIZE_LITERAL("\\d+(\\.\\d+)?(B|KB|MB|GB|TB)", TokenAttribute.LITERAL),
    DURATION_LITERAL("\\d+(\\.\\d+)?(hours|hour|days|day|min|ns|us|ms|s|m|h|d)(\\d+(\\.\\d+)?(hours|hour|days|day|min|ns|us|ms|s|m|h|d))*", TokenAttribute.LITERAL),
    TIME_LITERAL("\\d{1,2}:\\d{2}(:\\d{2})?", TokenAttribute.LITERAL),
    TIMESTAMP_LITERAL("\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}(\\.\\d{3})?Z?", TokenAttribute.LITERAL),
    NUMBER("\\d+(\\.\\d+)?", TokenAttribute.LITERAL),
    STRING("'([^'\\\\]|\\\\.)*'", TokenAttribute.LITERAL),
    SYNTHETIC_FIELD("\\$[a-zA-Z][a-zA-Z0-9_]*"),
    IDENTIFIER("[a-zA-Z_][a-zA-Z0-9_]*"),
    BOOLEAN("\\b(true|false)\\b", TokenAttribute.LITERAL),
    
    // Rate units (kept as they can be standalone in expressions like "per second")
    RATE_UNIT("\\b/\\s*(s|sec|m|min|h|hour|d|day)\\b"),
    
    // Operators (longer operators first to prevent partial matches)
    NOT_EQUALS("!=", TokenAttribute.OPERATOR),
    LESS_EQUAL("<=", TokenAttribute.OPERATOR),
    GREATER_EQUAL(">=", TokenAttribute.OPERATOR),
    EQUALS("=", TokenAttribute.OPERATOR),
    LESS_THAN("<", TokenAttribute.OPERATOR),
    GREATER_THAN(">", TokenAttribute.OPERATOR),
    LIKE("LIKE", TokenAttribute.OPERATOR, TokenAttribute.KEYWORD),
    IN("IN", TokenAttribute.OPERATOR, TokenAttribute.KEYWORD),
    NOT("NOT", TokenAttribute.OPERATOR, TokenAttribute.KEYWORD),
    BETWEEN("BETWEEN", TokenAttribute.OPERATOR, TokenAttribute.KEYWORD),
    DISTINCT("DISTINCT", TokenAttribute.KEYWORD),
    
    // Comments (handled specially in lexer, not by regex)
    COMMENT(""),  // Will be set by lexer manually
    
    // Arithmetic operators
    PLUS("\\+", TokenAttribute.OPERATOR),
    MINUS("-", TokenAttribute.OPERATOR),
    STAR("\\*", TokenAttribute.OPERATOR),  // Moved from below to prioritize over MULTIPLY
    DIVIDE("/", TokenAttribute.OPERATOR),
    MODULO("%", TokenAttribute.OPERATOR),
    
    // Assignment
    ASSIGN(":=", TokenAttribute.OPERATOR),
    
    // Punctuation
    LPAREN("\\("),
    RPAREN("\\)"),
    LBRACKET("\\["),
    RBRACKET("\\]"),
    COMMA(","),
    SEMICOLON(";"),
    DOT("\\."),
    
    // Essential SQL keywords only
    SELECT("SELECT", TokenAttribute.KEYWORD),
    FROM("FROM", TokenAttribute.KEYWORD),
    WHERE("WHERE", TokenAttribute.KEYWORD),
    GROUP_BY("GROUP BY", TokenAttribute.KEYWORD),
    HAVING("HAVING", TokenAttribute.KEYWORD),
    ORDER_BY("ORDER BY", TokenAttribute.KEYWORD),
    LIMIT("LIMIT", TokenAttribute.KEYWORD),
    AS("AS", TokenAttribute.KEYWORD),
    AND("AND", TokenAttribute.OPERATOR, TokenAttribute.KEYWORD),
    OR("OR", TokenAttribute.OPERATOR, TokenAttribute.KEYWORD),
    ASC("ASC", TokenAttribute.KEYWORD),
    DESC("DESC", TokenAttribute.KEYWORD),
    SHOW("SHOW", TokenAttribute.KEYWORD),
    HELP("HELP", TokenAttribute.KEYWORD),
    VIEW("VIEW", TokenAttribute.KEYWORD),
    
    // Essential join keywords
    JOIN("JOIN", TokenAttribute.KEYWORD),
    FUZZY("FUZZY", TokenAttribute.KEYWORD),
    ON("ON", TokenAttribute.KEYWORD),
    WITH("WITH", TokenAttribute.KEYWORD),
    
    // Standard JOIN types
    INNER("INNER", TokenAttribute.KEYWORD),
    LEFT("LEFT", TokenAttribute.KEYWORD),
    RIGHT("RIGHT", TokenAttribute.KEYWORD),
    FULL("FULL", TokenAttribute.KEYWORD),
    OUTER("OUTER", TokenAttribute.KEYWORD),
    
    // Context-sensitive keywords (can be identifiers in other contexts)
    TOLERANCE("TOLERANCE", TokenAttribute.KEYWORD),
    NEAREST("NEAREST", TokenAttribute.KEYWORD),
    PREVIOUS("PREVIOUS", TokenAttribute.KEYWORD),
    AFTER("AFTER", TokenAttribute.KEYWORD),
    
    // Percentile functions (these need to be reserved for parsing)
    PERCENTILE("PERCENTILE", TokenAttribute.KEYWORD, TokenAttribute.FUNCTION),
    P90("P90", TokenAttribute.KEYWORD, TokenAttribute.FUNCTION),
    P95("P95", TokenAttribute.KEYWORD, TokenAttribute.FUNCTION),
    P99("P99", TokenAttribute.KEYWORD, TokenAttribute.FUNCTION),
    P999("P999", TokenAttribute.KEYWORD, TokenAttribute.FUNCTION),
    
    // Advanced percentile selection functions
    P90SELECT("P90SELECT", TokenAttribute.KEYWORD, TokenAttribute.FUNCTION),
    P95SELECT("P95SELECT", TokenAttribute.KEYWORD, TokenAttribute.FUNCTION),
    P99SELECT("P99SELECT", TokenAttribute.KEYWORD, TokenAttribute.FUNCTION),
    P999SELECT("P999SELECT", TokenAttribute.KEYWORD, TokenAttribute.FUNCTION),
    PERCENTILE_SELECT("PERCENTILE_SELECT", TokenAttribute.KEYWORD, TokenAttribute.FUNCTION),
    
    // Temporal window keywords
    WITHIN("WITHIN", TokenAttribute.KEYWORD),
    OF("OF", TokenAttribute.KEYWORD),
    OVER("OVER", TokenAttribute.KEYWORD),
    
    // Case expression keywords
    CASE("CASE", TokenAttribute.KEYWORD),
    WHEN("WHEN", TokenAttribute.KEYWORD),
    THEN("THEN", TokenAttribute.KEYWORD),
    ELSE("ELSE", TokenAttribute.KEYWORD),
    END("END", TokenAttribute.KEYWORD),
    
    // Special
    EXTENDED_QUERY("@"),
    
    // Whitespace
    WHITESPACE("\\s+"),
    
    // End of file
    EOF("$");
    
    private final Pattern pattern;
    private final Set<TokenAttribute> attributes;
    
    TokenType(String regex, TokenAttribute... attributes) {
        this.pattern = Pattern.compile(regex, Pattern.CASE_INSENSITIVE);
        this.attributes = attributes.length > 0 ? EnumSet.of(attributes[0], attributes) : EnumSet.noneOf(TokenAttribute.class);
    }
    
    public Pattern getPattern() {
        return pattern;
    }
    
    public boolean matches(String text) {
        return pattern.matcher(text).matches();
    }
    
    /**
     * Returns true if this token type has the specified attribute
     */
    public boolean hasAttribute(TokenAttribute attribute) {
        return attributes.contains(attribute);
    }
    
    /**
     * Returns true if this token type represents a literal value
     */
    public boolean isLiteral() {
        return hasAttribute(TokenAttribute.LITERAL);
    }
    
    /**
     * Returns true if this token type represents an operator
     */
    public boolean isOperator() {
        return hasAttribute(TokenAttribute.OPERATOR);
    }
    
    /**
     * Returns true if this token type represents a keyword
     */
    public boolean isKeyword() {
        return hasAttribute(TokenAttribute.KEYWORD);
    }
    
    /**
     * Returns true if this token type represents a function
     */
    public boolean isFunction() {
        return hasAttribute(TokenAttribute.FUNCTION);
    }
}
