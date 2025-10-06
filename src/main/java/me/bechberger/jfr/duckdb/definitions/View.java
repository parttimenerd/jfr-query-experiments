package me.bechberger.jfr.duckdb.definitions;

import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.stream.Stream;

public final class View {

    record Alternative(String query, List<String> referencedTables) {

        public boolean isValid(Set<String> existingTables) {
            return existingTables.containsAll(referencedTables);
        }

    }

    private final String name;
    private final String category;
    private final String label;
    private final @Nullable String relatedJFRView;
    private final String definition;
    private final List<String> referencedTables;
    private final List<Alternative> alternatives = new ArrayList<>();
    private boolean hasUnionAlternatives = false;

    public View(String name, String category, String label, @Nullable String relatedJFRView, String definition, String referencedTable, String... referencedTables) {
        this.name = name;
        this.category = category;
        this.label = label;
        this.relatedJFRView = relatedJFRView;
        this.definition = definition;
        this.referencedTables = Stream.concat(Stream.of(referencedTable), Arrays.stream(referencedTables))
                .toList();
    }

    String viewName() {
        return "jfr$" + name;
    }

    public boolean isValid(Set<String> existingTables) {
        return existingTables.containsAll(referencedTables);
    }

    /**
     * If the main definition is not valid, tries to find an alternative that is valid (tries the one with the most referenced tables first).
     *
     * @param existingTables the set of existing tables
     * @return the best matching query, or null if none is valid
     */
    public @Nullable String getBestMatchingQuery(Set<String> existingTables) {
        if (isValid(existingTables)) {
            return definition;
        }
        // return the first alternative that is valid, preferring those with more referenced tables
        var alt = alternatives.stream()
                .filter(a -> a.isValid(existingTables))
                .sorted((a1, a2) -> Integer.compare(a2.referencedTables.size(), a1.referencedTables.size()))
                .map(Alternative::query)
                .findFirst()
                .orElse(null);
        if (alt == null && hasUnionAlternatives) {
            return getUnionAlternative(existingTables);
        }
        return alt;
    }

    /**
     * Generates an alternative query by removing UNION ALL clauses that reference missing tables.
     *
     * @param existingTables the set of existing tables
     * @return a modified query with missing table references removed, or null if no valid query can be generated
     */
    private @Nullable String getUnionAlternative(Set<String> existingTables) {
        String query = definition;

        // Check if this is a UNION ALL query by looking for the pattern
        if (!query.contains("UNION ALL")) {
            return null;
        }

        // Try to find UNION ALL within parentheses - look for various patterns
        // Use regex to handle whitespace and line breaks between JOIN keywords and parentheses
        String[] patterns = {
            "(?i)\\bfrom\\s*\\(",
            "(?i)\\bleft\\s+join\\s*\\(",
            "(?i)\\bright\\s+join\\s*\\(",
            "(?i)\\binner\\s+join\\s*\\(",
            "(?i)\\bjoin\\s*\\("
        };

        for (String pattern : patterns) {
            java.util.regex.Pattern regexPattern = java.util.regex.Pattern.compile(pattern);
            java.util.regex.Matcher matcher = regexPattern.matcher(query);

            if (matcher.find()) {
                int matchStart = matcher.start();
                // Find the opening parenthesis position
                int openParenIndex = -1;
                for (int i = matchStart; i < query.length(); i++) {
                    if (query.charAt(i) == '(') {
                        openParenIndex = i;
                        break;
                    }
                }

                if (openParenIndex != -1) {
                    // Find the matching closing parenthesis using balanced counting
                    int closeParenIndex = findMatchingCloseParen(query, openParenIndex);
                    if (closeParenIndex != -1) {
                        String innerQuery = query.substring(openParenIndex + 1, closeParenIndex);

                        // Only process if the inner query contains UNION ALL
                        if (innerQuery.contains("UNION ALL")) {
                            String processedInnerQuery = processUnionClauses(innerQuery, existingTables);
                            if (processedInnerQuery == null) {
                                return null; // No valid clauses found
                            }

                            // Replace the inner query content while preserving structure
                            return query.substring(0, openParenIndex + 1) +
                                   processedInnerQuery +
                                   query.substring(closeParenIndex);
                        }
                    }
                }
            }
        }

        // Fallback: process as simple UNION ALL query (no parentheses)
        return processUnionClauses(query, existingTables);
    }

    /**
     * Finds the matching closing parenthesis for an opening parenthesis using balanced counting
     */
    private int findMatchingCloseParen(String query, int openParenIndex) {
        int parenCount = 1;
        for (int i = openParenIndex + 1; i < query.length(); i++) {
            char c = query.charAt(i);
            if (c == '(') {
                parenCount++;
            } else if (c == ')') {
                parenCount--;
                if (parenCount == 0) {
                    return i;
                }
            }
        }
        return -1; // No matching closing parenthesis found
    }

    /**
     * Processes UNION ALL clauses by removing clauses that reference missing tables
     */
    private @Nullable String processUnionClauses(String queryWithUnions, Set<String> existingTables) {
        // Split the query by UNION ALL to get individual SELECT clauses
        String[] clauses = queryWithUnions.split("\\s+UNION\\s+ALL\\s+");
        List<String> validClauses = new ArrayList<>();

        for (String clause : clauses) {
            // Extract table names from each SELECT clause
            Set<String> referencedTables = extractTablesFromSelectClause(clause.trim());

            // Check if all referenced tables exist
            if (existingTables.containsAll(referencedTables)) {
                validClauses.add(clause.trim());
            }
        }

        // If no valid clauses remain, return null
        if (validClauses.isEmpty()) {
            return null;
        }

        // If exactly one clause remains, return it without UNION ALL
        if (validClauses.size() == 1) {
            return validClauses.get(0);
        }

        // Join the valid clauses back with UNION ALL
        return String.join("\n                        UNION ALL\n                        ", validClauses);
    }

    /**
     * Processes simple UNION ALL queries (not wrapped in parentheses)
     */
    private @Nullable String processSimpleUnionQuery(String query, Set<String> existingTables) {
        String[] clauses = query.split("\\s+UNION\\s+ALL\\s+");
        List<String> validClauses = new ArrayList<>();

        for (String clause : clauses) {
            Set<String> referencedTables = extractTablesFromSelectClause(clause.trim());
            if (existingTables.containsAll(referencedTables)) {
                validClauses.add(clause.trim());
            }
        }

        if (validClauses.isEmpty()) {
            return null;
        }

        if (validClauses.size() == 1) {
            return validClauses.get(0);
        }

        return String.join("\n                        UNION ALL\n                        ", validClauses);
    }

    /**
     * Extracts table names from a SELECT clause by finding FROM keywords.
     * Handles simple cases like "SELECT ... FROM TableName"
     */
    private Set<String> extractTablesFromSelectClause(String selectClause) {
        Set<String> tables = new HashSet<>();

        // Simple regex to find table names after FROM keyword
        String fromPattern = "(?i)\\bFROM\\s+([A-Za-z_][A-Za-z0-9_]*)";
        java.util.regex.Pattern pattern = java.util.regex.Pattern.compile(fromPattern);
        java.util.regex.Matcher matcher = pattern.matcher(selectClause);

        while (matcher.find()) {
            String tableName = matcher.group(1);
            // Remove any schema prefix if present
            if (tableName.contains(".")) {
                tableName = tableName.substring(tableName.lastIndexOf('.') + 1);
            }
            tables.add(tableName);
        }

        return tables;
    }

    public String name() {
        return name;
    }

    public String category() {
        return category;
    }

    public String label() {
        return label;
    }

    public @Nullable String relatedJFRView() {
        return relatedJFRView;
    }

    public String definition() {
        return definition;
    }

    public List<String> referencedTables() {
        return referencedTables;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || obj.getClass() != this.getClass()) return false;
        var that = (View) obj;
        return Objects.equals(this.name, that.name) &&
               Objects.equals(this.category, that.category) &&
               Objects.equals(this.label, that.label) &&
               Objects.equals(this.relatedJFRView, that.relatedJFRView) &&
               Objects.equals(this.definition, that.definition) &&
               Objects.equals(this.referencedTables, that.referencedTables);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, category, label, relatedJFRView, definition, referencedTables);
    }

    @Override
    public String toString() {
        return "View[" +
               "name=" + name + ", " +
               "category=" + category + ", " +
               "label=" + label + ", " +
               "relatedJFRView=" + relatedJFRView + ", " +
               "definition=" + definition + ", " +
               "referencedTables=" + referencedTables + ']';
    }

    public View addAlternative(String query, String referencedTable, String... referencedTables) {
        alternatives.add(new Alternative(query, Stream.concat(Stream.of(referencedTable), Arrays.stream(referencedTables)).toList()));
        return this;
    }

    /**
     * Enables automatic generation of alternative queries by filtering UNION ALL clauses
     * that reference missing tables. This feature supports and has been tested with the following SQL patterns:
     *
     * <h3>Supported Patterns:</h3>
     * <ul>
     *   <li><b>FROM (UNION ALL)</b> - UNION ALL queries wrapped in parentheses after FROM clause:
     *       <pre>FROM (SELECT ... FROM TableA UNION ALL SELECT ... FROM TableB) alias</pre></li>
     *   <li><b>LEFT JOIN (UNION ALL)</b> - UNION ALL queries in LEFT JOIN subqueries:
     *       <pre>LEFT JOIN (SELECT ... FROM TableA UNION ALL SELECT ... FROM TableB) alias ON ...</pre></li>
     *   <li><b>RIGHT JOIN (UNION ALL)</b> - UNION ALL queries in RIGHT JOIN subqueries:
     *       <pre>RIGHT JOIN (SELECT ... FROM TableA UNION ALL SELECT ... FROM TableB) alias ON ...</pre></li>
     *   <li><b>INNER JOIN (UNION ALL)</b> - UNION ALL queries in INNER JOIN subqueries:
     *       <pre>INNER JOIN (SELECT ... FROM TableA UNION ALL SELECT ... FROM TableB) alias ON ...</pre></li>
     *   <li><b>JOIN (UNION ALL)</b> - UNION ALL queries in simple JOIN subqueries:
     *       <pre>JOIN (SELECT ... FROM TableA UNION ALL SELECT ... FROM TableB) alias ON ...</pre></li>
     *   <li><b>Simple UNION ALL</b> - Top-level UNION ALL queries without parentheses:
     *       <pre>SELECT ... FROM TableA UNION ALL SELECT ... FROM TableB</pre></li>
     * </ul>
     *
     * <h3>Behavior:</h3>
     * <ul>
     *   <li>Removes UNION ALL clauses that reference tables not present in the database</li>
     *   <li>Preserves the overall query structure and JOIN clauses</li>
     *   <li>Returns a single SELECT clause when only one valid table remains (removes UNION ALL)</li>
     *   <li>Returns null if no valid UNION ALL clauses remain</li>
     *   <li>Handles nested parentheses with balanced counting</li>
     *   <li>Preserves query formatting and indentation in the result</li>
     * </ul>
     *
     * <h3>Limitations:</h3>
     * <ul>
     *   <li>Does not support deeply nested UNION ALL structures</li>
     *   <li>Table extraction uses simple regex pattern matching on FROM keywords</li>
     *   <li>Does not handle complex table expressions with schema prefixes in UNION clauses</li>
     * </ul>
     *
     * Be sure to check that the pattern used is tested in UnionAlternativeTest and UnionAlternativeParameterizedTest.
     *
     * @return this View instance for method chaining
     */
    public View addUnionAlternatives() {
        hasUnionAlternatives = true;
        return this;
    }
}