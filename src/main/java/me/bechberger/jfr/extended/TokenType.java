package me.bechberger.jfr.extended;

import java.util.regex.Pattern;

/**
 * Token types for the extended JFR query language
 */
public enum TokenType {
    // Literals (ordered from most specific to least specific)
    MEMORY_SIZE_LITERAL("\\d+(\\.\\d+)?(B|KB|MB|GB|TB)"),
    DURATION_LITERAL("\\d+(\\.\\d+)?(hours|hour|days|day|min|ns|us|ms|s|m|h|d)(\\d+(\\.\\d+)?(hours|hour|days|day|min|ns|us|ms|s|m|h|d))*"),
    TIME_LITERAL("\\d{1,2}:\\d{2}(:\\d{2})?"),
    TIMESTAMP_LITERAL("\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}(\\.\\d{3})?Z?"),
    NUMBER("\\d+(\\.\\d+)?"),
    STRING("'([^'\\\\]|\\\\.)*'"),
    SYNTHETIC_FIELD("\\$[a-zA-Z][a-zA-Z0-9_]*"),
    IDENTIFIER("[a-zA-Z_][a-zA-Z0-9_]*"),
    BOOLEAN("\\b(true|false)\\b"),
    
    // Rate units (kept as they can be standalone in expressions like "per second")
    RATE_UNIT("\\b/\\s*(s|sec|m|min|h|hour|d|day)\\b"),
    
    // Operators (longer operators first to prevent partial matches)
    NOT_EQUALS("!="),
    LESS_EQUAL("<="),
    GREATER_EQUAL(">="),
    EQUALS("="),
    LESS_THAN("<"),
    GREATER_THAN(">"),
    LIKE("LIKE"),
    IN("IN"),
    NOT("NOT"),
    
    // Comments (handled specially in lexer, not by regex)
    COMMENT(""),  // Will be set by lexer manually
    
    // Arithmetic operators
    PLUS("\\+"),
    MINUS("-"),
    STAR("\\*"),  // Moved from below to prioritize over MULTIPLY
    DIVIDE("/"),
    MODULO("%"),
    
    // Assignment
    ASSIGN(":="),
    
    // Punctuation
    LPAREN("\\("),
    RPAREN("\\)"),
    LBRACKET("\\["),
    RBRACKET("\\]"),
    COMMA(","),
    SEMICOLON(";"),
    DOT("\\."),
    
    // Essential SQL keywords only
    SELECT("SELECT"),
    FROM("FROM"),
    WHERE("WHERE"),
    GROUP_BY("GROUP BY"),
    HAVING("HAVING"),
    ORDER_BY("ORDER BY"),
    LIMIT("LIMIT"),
    AS("AS"),
    AND("AND"),
    OR("OR"),
    ASC("ASC"),
    DESC("DESC"),
    SHOW("SHOW"),
    HELP("HELP"),
    VIEW("VIEW"),
    
    // Essential join keywords
    JOIN("JOIN"),
    FUZZY("FUZZY"),
    ON("ON"),
    WITH("WITH"),
    
    // Standard JOIN types
    INNER("INNER"),
    LEFT("LEFT"),
    RIGHT("RIGHT"),
    FULL("FULL"),
    
    // Context-sensitive keywords (can be identifiers in other contexts)
    TOLERANCE("TOLERANCE"),
    NEAREST("NEAREST"),
    PREVIOUS("PREVIOUS"),
    AFTER("AFTER"),
    
    // Percentile functions (these need to be reserved for parsing)
    PERCENTILE("PERCENTILE"),
    P90("P90"),
    P95("P95"),
    P99("P99"),
    P999("P999"),
    
    // Advanced percentile selection functions
    P90SELECT("P90SELECT"),
    P95SELECT("P95SELECT"),
    P99SELECT("P99SELECT"),
    P999SELECT("P999SELECT"),
    PERCENTILE_SELECT("PERCENTILE_SELECT"),
    
    // Temporal window keywords
    WITHIN("WITHIN"),
    OF("OF"),
    OVER("OVER"),
    
    // Special
    EXTENDED_QUERY("@"),
    
    // Whitespace
    WHITESPACE("\\s+"),
    
    // End of file
    EOF("$");
    
    private final Pattern pattern;
    
    TokenType(String regex) {
        this.pattern = Pattern.compile(regex, Pattern.CASE_INSENSITIVE);
    }
    
    public Pattern getPattern() {
        return pattern;
    }
    
    public boolean matches(String text) {
        return pattern.matcher(text).matches();
    }
}
