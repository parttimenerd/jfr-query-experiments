package me.bechberger.jfr.extended;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import me.bechberger.jfr.extended.engine.util.StringSimilarity;
import me.bechberger.jfr.extended.evaluator.FunctionRegistry;

/**
 * Lexer for the extended JFR query language
 */
public class Lexer {
    
    private final String input;
    private int position;
    private int line;
    private int column;
    
    public Lexer(String input) {
        this.input = input;
        this.position = 0;
        this.line = 1;
        this.column = 1;
    }
    
    /**
     * Tokenizes the entire input and returns a list of tokens
     */
    public List<Token> tokenize() throws LexerException {
        List<Token> tokens = new ArrayList<>();
        List<QueryError> errors = new ArrayList<>();
        
        while (position < input.length()) {
            try {
                Token token = nextToken();
                if (token.type() != TokenType.WHITESPACE && token.type() != TokenType.COMMENT) {
                    tokens.add(token);
                }
            } catch (LexerException e) {
                // Convert LexerException to QueryError
                QueryError error = createLexerErrorFromException(e.getMessage(), position);
                errors.add(error);
                
                // Try to recover by skipping the problematic character
                if (position < input.length()) {
                    advance(1);
                }
            }
        }
        
        // Add EOF token
        tokens.add(new Token(TokenType.EOF, "", line, column, position));
        
        // If there were errors, throw the unified exception
        if (!errors.isEmpty()) {
            throw new LexerException(errors, input);
        }
        
        return tokens;
    }
    
    /**
     * Returns the next token from the input
     */
    private Token nextToken() throws LexerException {
        if (position >= input.length()) {
            return new Token(TokenType.EOF, "", line, column, position);
        }
        
        // Skip whitespace but track position
        if (Character.isWhitespace(input.charAt(position))) {
            return readWhitespace();
        }
        
        // Check for comments
        if (position + 1 < input.length()) {
            char current = input.charAt(position);
            char next = input.charAt(position + 1);
            
            // Multi-line comments /* */
            if (current == '/' && next == '*') {
                return readMultilineComment();
            }
            
            // Single-line comments: --, //, #
            if ((current == '-' && next == '-') || 
                (current == '/' && next == '/')) {
                return readSingleLineComment();
            }
        }
        
        // Hash comments #
        if (input.charAt(position) == '#') {
            return readSingleLineComment();
        }
        
        // Handle square brackets
        if (input.charAt(position) == '[') {
            return new Token(TokenType.LBRACKET, "[", line, column, position++);
        }
        if (input.charAt(position) == ']') {
            return new Token(TokenType.RBRACKET, "]", line, column, position++);
        }
        
        // First, try to match keywords and context-sensitive patterns
        for (TokenType tokenType : TokenType.values()) {
            if (tokenType == TokenType.EOF) continue;
            if (tokenType == TokenType.IDENTIFIER || tokenType == TokenType.WHITESPACE || tokenType == TokenType.COMMENT) continue; // Skip these for now
            
            Matcher matcher = tokenType.getPattern().matcher(input);
            matcher.region(position, input.length());
            
            if (matcher.lookingAt()) {
                String value = matcher.group();
                
                // Special handling for duration and memory literals - only match if actually preceded by numbers
                if (tokenType == TokenType.DURATION_LITERAL || tokenType == TokenType.MEMORY_SIZE_LITERAL) {
                    // Check if this matches because it starts with a digit (valid literal)
                    // or just matches a unit suffix (should be treated as identifier)
                    if (!Character.isDigit(value.charAt(0))) {
                        continue; // Skip this match, let it be handled as IDENTIFIER
                    }
                }
                
                // For keywords, ensure word boundaries (not part of a larger identifier)
                if (tokenType.isKeyword()) {
                    if (position + value.length() < input.length()) {
                        char nextChar = input.charAt(position + value.length());
                        if (Character.isLetterOrDigit(nextChar) || nextChar == '_') {
                            continue; // This is part of a larger identifier
                        }
                    }
                }
                
                Token token = new Token(tokenType, value, line, column, position);
                advance(value.length());
                return token;
            }
        }
        
        // Then try IDENTIFIER and other general patterns
        for (TokenType tokenType : new TokenType[]{TokenType.IDENTIFIER, TokenType.NUMBER, TokenType.STRING, TokenType.WHITESPACE}) {
            Matcher matcher = tokenType.getPattern().matcher(input);
            matcher.region(position, input.length());
            
            if (matcher.lookingAt()) {
                String value = matcher.group();
                Token token = new Token(tokenType, value, line, column, position);
                advance(value.length());
                return token;
            }
        }
        
        // If no token matches, check for special error cases
        char unexpectedChar = input.charAt(position);
        
        // Special handling for unclosed strings
        if (unexpectedChar == '\'') {
            // Look ahead to see if this is an unclosed string
            int end = position + 1;
            while (end < input.length() && input.charAt(end) != '\'' && input.charAt(end) != '\n') {
                end++;
            }
            if (end >= input.length() || input.charAt(end) == '\n') {
                // Reached end of input or newline without closing quote
                String unclosedString = input.substring(position, Math.min(end, input.length()));
                throw new LexerException("Unclosed string literal starting at line " + line + ", column " + column + 
                                       "\nString: " + unclosedString + 
                                       "\nAdd closing single quote (') to complete the string literal." +
                                       "\nExample: 'complete string'");
            }
        }
        
        String helpfulMessage = createHelpfulLexerErrorMessage(unexpectedChar, position);
        throw new LexerException(helpfulMessage);
    }

    /**
     * Reads whitespace and returns a whitespace token
     */
    private Token readWhitespace() {
        int start = position;
        int startLine = line;
        int startColumn = column;
        
        while (position < input.length() && Character.isWhitespace(input.charAt(position))) {
            if (input.charAt(position) == '\n') {
                line++;
                column = 1;
            } else {
                column++;
            }
            position++;
        }
        
        String value = input.substring(start, position);
        return new Token(TokenType.WHITESPACE, value, startLine, startColumn, start);
    }
    
    /**
     * Reads a multi-line comment and returns a comment token
     */
    private Token readMultilineComment() throws LexerException {
        int start = position;
        int startLine = line;
        int startColumn = column;
        
        // Skip the opening /*
        advance(2);
        
        // Look for the closing */
        while (position + 1 < input.length()) {
            if (input.charAt(position) == '*' && input.charAt(position + 1) == '/') {
                // Found closing */
                advance(2);
                String value = input.substring(start, position);
                return new Token(TokenType.COMMENT, value, startLine, startColumn, start);
            }
            advance(1);
        }
        
        // If we reach here, the comment is not closed
        String helpfulCommentError = "Unclosed multi-line comment starting at line " + startLine + ", column " + startColumn + 
                                   "\nMulti-line comments must be closed with */. Add */ to close the comment." +
                                   "\nExample: /* this is a comment */";
        throw new LexerException(helpfulCommentError);
    }
    
    /**
     * Reads a single-line comment and returns a comment token
     */
    private Token readSingleLineComment() {
        int start = position;
        int startLine = line;
        int startColumn = column;
        
        // Skip until end of line or end of input
        while (position < input.length() && input.charAt(position) != '\n' && input.charAt(position) != '\r') {
            position++;
            column++;
        }
        
        String value = input.substring(start, position);
        return new Token(TokenType.COMMENT, value, startLine, startColumn, start);
    }
    
    /**
     * Advances the position by the specified number of characters
     */
    private void advance(int count) {
        for (int i = 0; i < count; i++) {
            if (position < input.length() && input.charAt(position) == '\n') {
                line++;
                column = 1;
            } else {
                column++;
            }
            position++;
        }
    }
    
    /**
     * Returns the current position in the input
     */
    public int getPosition() {
        return position;
    }
    
    /**
     * Returns the current line number
     */
    public int getLine() {
        return line;
    }
    
    /**
     * Returns the current column number
     */
    public int getColumn() {
        return column;
    }
    
    /**
     * Creates a helpful error message for lexer errors by analyzing the context
     * and providing specific guidance about what went wrong.
     */
    private String createHelpfulLexerErrorMessage(char unexpectedChar, int errorPosition) {
        // Create a QueryError for unified formatting
        QueryError queryError = createEnhancedLexerError(createCharacterErrorMessage(unexpectedChar), errorPosition);
        return queryError.getFormattedMessage();
    }
    
    /**
     * Creates a character-specific error message
     */
    private String createCharacterErrorMessage(char unexpectedChar) {
        return "Unexpected character: '" + unexpectedChar + "'";
    }
    
    /**
     * Creates an enhanced QueryError from a lexer error message
     */
    private QueryError createEnhancedLexerError(String message, int errorPosition) {
        char unexpectedChar = errorPosition < input.length() ? input.charAt(errorPosition) : '\0';
        
        // Analyze the context and create enhanced error
        ContextAnalysis context = analyzeContext(errorPosition);
        String suggestion = getEnhancedSuggestion(unexpectedChar, context);
        String typoSuggestion = detectPossibleTypo(errorPosition);
        String tips = getContextualTips(unexpectedChar, context);
        
        // Create a token for the error position
        Token errorToken = new Token(TokenType.IDENTIFIER, String.valueOf(unexpectedChar), line, column, errorPosition);
        
        return new QueryError.Builder(JFRQueryException.ErrorOrigin.LEXER)
                .category(JFRQueryException.ErrorCategory.UNEXPECTED_CHARACTER)
                .errorMessage(message)
                .suggestion(suggestion + (typoSuggestion.isEmpty() ? "" : "\nPossible typo: " + typoSuggestion))
                .context(tips)
                .errorToken(errorToken)
                .originalQuery(input)
                .build();
    }
    
    /**
     * Creates a QueryError from a LexerException for error collection during tokenization
     */
    private QueryError createLexerErrorFromException(String message, int errorPosition) {
        return createEnhancedLexerError(message, errorPosition);
    }

    /**
     * Categorize the type of character error for better error reporting
     */
    /**
     * Enhanced context analysis that provides more detailed information about where the error occurred
     */
    private ContextAnalysis analyzeContext(int errorPosition) {
        // Look backwards to understand the parsing context
        int start = Math.max(0, errorPosition - 30);
        String preceding = errorPosition > start ? input.substring(start, errorPosition) : "";
        
        ContextAnalysis analysis = new ContextAnalysis();
        
        // Analyze what we were likely parsing
        if (preceding.matches(".*\\b(SELECT|FROM|WHERE|GROUP|ORDER|HAVING|LIMIT)\\s*$")) {
            analysis.context = "after_keyword";
            analysis.lastKeyword = preceding.trim().split("\\s+")[preceding.trim().split("\\s+").length - 1];
        } else if (preceding.matches(".*\\b[a-zA-Z][a-zA-Z0-9_]*$")) {
            analysis.context = "in_identifier";
            analysis.partialToken = preceding.replaceAll(".*\\b([a-zA-Z][a-zA-Z0-9_]*)$", "$1");
        } else if (preceding.matches(".*\\d+\\.?\\d*$")) {
            analysis.context = "in_number";
            analysis.partialToken = preceding.replaceAll(".*\\b(\\d+\\.?\\d*)$", "$1");
        } else if (preceding.matches(".*'[^']*$")) {
            analysis.context = "in_string";
        } else if (preceding.matches(".*[=<>!]+$")) {
            analysis.context = "in_operator";
            analysis.partialToken = preceding.replaceAll(".*([=<>!]+)$", "$1");
        } else if (preceding.trim().isEmpty() || preceding.matches(".*\\s$")) {
            analysis.context = "start_of_token";
        } else {
            analysis.context = "unknown";
        }
        
        return analysis;
    }
    
    /**
     * Container class for context analysis results
     */
    private static class ContextAnalysis {
        String context = "";
        String lastKeyword = "";
        String partialToken = "";
    }
    
    /**
     * Provides enhanced suggestions based on character and detailed context analysis
     */
    private String getEnhancedSuggestion(char unexpectedChar, ContextAnalysis context) {
        // Handle specific context-based errors
        switch (context.context) {
            case "after_keyword":
                return getAfterKeywordSuggestion(unexpectedChar, context.lastKeyword);
                
            case "in_identifier":
                return getInIdentifierSuggestion(unexpectedChar, context.partialToken);
                
            case "in_number":
                return getInNumberSuggestion(unexpectedChar, context.partialToken);
                
            case "in_string":
                return getInStringSuggestion(unexpectedChar);
                
            case "in_operator":
                return getInOperatorSuggestion(unexpectedChar, context.partialToken);
                
            default:
                return getGeneralCharacterSuggestion(unexpectedChar);
        }
    }
    
    /**
     * Provides suggestions when error occurs after a keyword
     */
    private String getAfterKeywordSuggestion(char unexpectedChar, String keyword) {
        // Check for special characters that have general suggestions
        String generalSuggestion = getCharacterSpecificSuggestion(unexpectedChar);
        if (!generalSuggestion.isEmpty()) {
            return generalSuggestion;
        }
        
        switch (keyword.toUpperCase()) {
            case "SELECT":
                if (unexpectedChar == '"') {
                    return "After SELECT, use field names or expressions. For string literals, use single quotes.";
                } else if (Character.isDigit(unexpectedChar)) {
                    return "After SELECT, expected field names or expressions, not numeric literals.";
                } else {
                    return "After SELECT, expected field names, expressions, or * for all fields.";
                }
                
            case "FROM":
                if (Character.isDigit(unexpectedChar)) {
                    return "After FROM, expected table or event type name, not a number.";
                } else {
                    return "After FROM, expected table name or event type (e.g., GarbageCollection, ExecutionSample).";
                }
                
            case "WHERE":
                return "After WHERE, expected a condition expression (e.g., field = value, duration > 5ms).";
                
            case "GROUP":
                return "After GROUP, expected BY keyword to complete GROUP BY clause.";
                
            case "ORDER":
                return "After ORDER, expected BY keyword to complete ORDER BY clause.";
                
            default:
                return "After " + keyword + " keyword, unexpected character '" + unexpectedChar + "'.";
        }
    }
    
    /**
     * Gets character-specific suggestions that apply regardless of context
     */
    private String getCharacterSpecificSuggestion(char unexpectedChar) {
        switch (unexpectedChar) {
            case '"':
                return "Double quotes are not supported for strings.\n" +
                       "Fix: Use single quotes instead: 'text' instead of \"text\"";
                       
            case '`':
                return "Backticks are not supported for identifiers.\n" +
                       "Fix: Use standard identifiers or single quotes for strings with spaces.";
                       
            case '{':
            case '}':
                return "Curly braces are not supported.\n" +
                       "Fix: Use parentheses for grouping expressions: (expression) instead of {expression}";
                       
            case '\\':
                return "Backslashes are only allowed inside string literals for escaping.\n" +
                       "Fix: For file paths, use forward slashes or escape within strings: 'C:\\\\path'";
                       
            case '~':
                return "Tilde operator (~) is not supported.\n" +
                       "Fix: Use LIKE for pattern matching or standard comparison operators (=, !=, <, >, <=, >=)";
                       
            case '^':
                return "Caret operator (^) is not supported.\n" +
                       "Fix: Use POW function for exponentiation: POW(base, exponent) instead of base ^ exponent";
                       
            case '&':
                return "Ampersand (&) is not supported for logical operations.\n" +
                       "Fix: Use AND for logical operations: condition1 AND condition2";
                       
            case '|':
                return "Pipe (|) is not supported for logical operations.\n" +
                       "Fix: Use OR for logical operations: condition1 OR condition2";
                       
            default:
                return ""; // No specific suggestion for this character
        }
    }
    
    /**
     * Provides suggestions when error occurs within an identifier
     */
    private String getInIdentifierSuggestion(char unexpectedChar, String partialIdentifier) {
        if (unexpectedChar == '-') {
            return "Hyphens are not allowed in identifiers. Use underscores instead.\n" +
                   "Fix: " + partialIdentifier + "_" + " instead of " + partialIdentifier + "-";
        } else if (unexpectedChar == '.') {
            return "Dots in identifiers are only allowed for qualified names (e.g., jdk.GarbageCollection).\n" +
                   "If this is a field access, ensure proper syntax.";
        } else if (!Character.isLetterOrDigit(unexpectedChar) && unexpectedChar != '_') {
            return "Identifiers can only contain letters (a-z, A-Z), digits (0-9), and underscores (_).\n" +
                   "Invalid character '" + unexpectedChar + "' in identifier '" + partialIdentifier + "'.";
        } else {
            return "Invalid character '" + unexpectedChar + "' in identifier.";
        }
    }
    
    /**
     * Provides suggestions when error occurs within a number literal
     */
    private String getInNumberSuggestion(char unexpectedChar, String partialNumber) {
        if (unexpectedChar == ',') {
            return "Thousand separators are not allowed in numbers.\n" +
                   "Fix: " + partialNumber.replace(",", "") + " instead of " + partialNumber + "," + "...";
        } else if (unexpectedChar == ' ') {
            return "Spaces are not allowed within number literals.\n" +
                   "Fix: Remove spaces from the number.";
        } else if (unexpectedChar == '.') {
            if (partialNumber.contains(".")) {
                return "Numbers cannot have multiple decimal points.\n" +
                       "Fix: Use only one decimal point in the number.";
            } else {
                return "Decimal point position error in number literal.";
            }
        } else if (Character.isLetter(unexpectedChar)) {
            return "Letters in numbers are only allowed for duration/memory units (ms, kb, etc.).\n" +
                   "Fix: Add proper unit suffix or separate the number from text.";
        } else {
            return "Invalid character '" + unexpectedChar + "' in number literal '" + partialNumber + "'.";
        }
    }
    
    /**
     * Provides suggestions when error occurs within a string literal
     */
    private String getInStringSuggestion(char unexpectedChar) {
        if (unexpectedChar == '\n' || unexpectedChar == '\r') {
            return "String literals cannot span multiple lines without proper escaping.\n" +
                   "Fix: Close the string with a single quote (') before the line break.";
        } else if (unexpectedChar == '"') {
            return "Cannot mix quote types within a string. Use only single quotes.\n" +
                   "Fix: Use \\' to include a literal single quote in the string.";
        } else {
            return "Invalid character in string literal. Use escape sequences if needed.";
        }
    }
    
    /**
     * Provides suggestions when error occurs within an operator
     */
    private String getInOperatorSuggestion(char unexpectedChar, String partialOperator) {
        if (partialOperator.equals("=") && unexpectedChar == '=') {
            return "Double equals (==) is not supported. Use single equals (=) for comparison.\n" +
                   "Fix: Change '==' to '=' for equality comparison.";
        } else if (partialOperator.equals("!") && unexpectedChar != '=') {
            return "Incomplete operator. After '!' expected '=' for not-equals operator.\n" +
                   "Fix: Use '!=' for not-equals comparison.";
        } else {
            return "Invalid operator sequence '" + partialOperator + unexpectedChar + "'.\n" +
                   "Valid operators: =, !=, <, >, <=, >=, LIKE";
        }
    }
    
    /**
     * Provides general character-specific suggestions
     */
    private String getGeneralCharacterSuggestion(char unexpectedChar) {
        // First try character-specific suggestions
        String characterSuggestion = getCharacterSpecificSuggestion(unexpectedChar);
        if (!characterSuggestion.isEmpty()) {
            return characterSuggestion;
        }
        
        // Unicode and special character suggestions
        if (unexpectedChar > 127) { // Non-ASCII characters
            if (isCommonUnicodeSymbol(unexpectedChar)) {
                return getUnicodeSymbolSuggestion(unexpectedChar);
            } else if (Character.isLetter(unexpectedChar)) {
                return "Non-ASCII letters are not supported in identifiers.\n" +
                       "Fix: Use only ASCII letters (a-z, A-Z), digits (0-9), and underscores (_).\n" +
                       "For international text, use string literals enclosed in single quotes.";
            } else {
                return "Unicode character '" + getCharacterName(unexpectedChar) + "' is not supported.\n" +
                       "Fix: Use standard ASCII characters in JFR queries.";
            }
        }
        
        // Default case for unhandled characters
        return "Character '" + unexpectedChar + "' is not recognized in JFR query syntax.\n" +
               "Fix: Check the query syntax documentation for valid characters and operators.";
    }
    
    /**
     * Checks if a character is a common Unicode symbol that users might encounter
     */
    private boolean isCommonUnicodeSymbol(char c) {
        // Common currency symbols
        if (c == '€' || c == '¥' || c == '£' || c == '¢') return true;
        
        // Common copyright/trademark symbols  
        if (c == '™' || c == '©' || c == '®') return true;
        
        // Common mathematical/scientific symbols
        if (c == 'µ' || c == 'α' || c == 'β' || c == 'γ' || c == 'δ' || c == 'π' || c == 'Σ') return true;
        
        return false;
    }
    
    /**
     * Gets specific suggestions for common Unicode symbols
     */
    private String getUnicodeSymbolSuggestion(char c) {
        switch (c) {
            case '€':
            case '¥':
            case '£':
            case '¢':
                return "Currency symbols are not supported. Use string literals like 'EUR', 'USD', 'GBP', 'JPY' or numeric values.\n" +
                       "Example: price = 100 or currency = 'USD' instead of price = " + c + "100";
                       
            case '™':
            case '©':
            case '®':
                return "Copyright and trademark symbols are not supported in identifiers. Use text descriptions instead.\n" +
                       "Example: 'trademark' or 'copyright' in string literals";
                       
            case 'µ':
                return "Micro symbol (µ) is not supported in duration literals. Use 'us' for microseconds.\n" +
                       "Example: duration > 5us instead of duration > 5µs";
                       
            case 'α':
            case 'β':
            case 'γ':
            case 'δ':
            case 'π':
            case 'Σ':
                return "Greek letters are not supported in identifiers. Use standard ASCII letters (a-z, A-Z).\n" +
                       "Example: alpha, beta, gamma instead of α, β, γ";
                       
            default:
                return "Unicode symbol '" + getCharacterName(c) + "' is not supported.\n" +
                       "Use standard ASCII characters in JFR queries.";
        }
    }
    
    /**
     * Gets a human-readable name for a character
     */
    private String getCharacterName(char c) {
        switch (c) {
            case '™': return "trademark symbol";
            case '©': return "copyright symbol";
            case '®': return "registered trademark symbol";
            case 'µ': return "micro symbol";
            case '€': return "euro symbol";
            case '¥': return "yen symbol";
            case '£': return "pound symbol";
            case '¢': return "cent symbol";
            default:
                if (Character.isLetter(c)) {
                    return "letter '" + c + "'";
                } else {
                    return "symbol '" + c + "'";
                }
        }
    }
    

    
    /**
     * Detects possible typos by analyzing the context and suggesting corrections
     */
    private String detectPossibleTypo(int errorPosition) {
        // Look for partial keywords that might be misspelled
        int start = Math.max(0, errorPosition - 10);
        String preceding = input.substring(start, errorPosition).trim();
        
        // Extract the last partial word
        String[] words = preceding.split("\\s+");
        if (words.length == 0) return "";
        
        String lastWord = words[words.length - 1];
        if (lastWord.length() < 2) return "";
        
        // Check against common keywords and functions
        String[] keywords = {
            "SELECT", "FROM", "WHERE", "GROUP", "ORDER", "BY", "HAVING", "LIMIT", "AS",
            "AND", "OR", "NOT", "LIKE", "IN", "ASC", "DESC", "INNER", "LEFT", "RIGHT", "FULL",
            "JOIN", "ON", "COUNT", "SUM", "AVG", "MIN", "MAX", "P90", "P95", "P99"
        };
        
        String suggestion = findClosestMatch(lastWord.toUpperCase(), keywords);
        if (suggestion != null) {
            return suggestion + " (possible typo correction for '" + lastWord + "')";
        }
        
        // Check for common operator typos
        if (lastWord.equals("==")) {
            return "= (use single equals for comparison)";
        } else if (lastWord.equals("&&")) {
            return "AND (use AND for logical operations)";
        } else if (lastWord.equals("||")) {
            return "OR (use OR for logical operations)";
        }
        
        return "";
    }
    
    /**
     * Finds the closest matching keyword using simple edit distance
     */
    private String findClosestMatch(String input, String[] candidates) {
        if (input.length() < 2) return null;
        
        return StringSimilarity.findClosestMatch(input, candidates, Math.max(1, input.length() / 3), false);
    }

    /**
     * Provides contextual tips based on the error character and context
     */
    private String getContextualTips(char unexpectedChar, ContextAnalysis context) {
        StringBuilder tips = new StringBuilder();
        
        // General tips based on character type
        if (Character.isLetter(unexpectedChar) && unexpectedChar > 127) {
            tips.append("• International characters should be used within string literals: 'text'\n");
            tips.append("• Identifiers must use ASCII letters (a-z, A-Z) only");
        } else if (unexpectedChar == '"') {
            tips.append("• JFR queries use single quotes for strings: 'text'\n");
            tips.append("• Double quotes are not supported in this query language");
        } else if (unexpectedChar == '-' && context.context.equals("in_identifier")) {
            tips.append("• Use underscores in identifiers: field_name\n");
            tips.append("• Hyphens are reserved for arithmetic operations");
        }
        
        // Context-specific tips
        switch (context.context) {
            case "after_keyword":
                tips.append("• Check the syntax documentation for what can follow ").append(context.lastKeyword);
                break;
            case "in_number":
                tips.append("• Number literals: 123, 123.45, 5ms, 10kb\n");
                tips.append("• No thousand separators or spaces allowed");
                break;
            case "in_string":
                tips.append("• Strings must be enclosed in single quotes: 'text'\n");
                tips.append("• Use \\' for literal quotes within strings");
                break;
        }
        
        return tips.toString();
    }
}
