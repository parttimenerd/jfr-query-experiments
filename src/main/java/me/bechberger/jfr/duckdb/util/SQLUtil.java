package me.bechberger.jfr.duckdb.util;

import org.duckdb.DuckDBConnection;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

public class SQLUtil {

    /**
     * Extracts table names from a SQL query string.
     * This is a simplified implementation and may not cover all SQL syntax cases.
     * It essentially looks for patterns like "FROM table_name" and "JOIN table_name"
     * and removes quotation marks and aliases.
     * Also works with multiple tables in the FROM clause separated by commas.
     *
     * @param query the SQL query string
     * @return a set of referenced table names
     */
    public static Set<String> getReferencedTables(String query) {
        query = query.replace("\n", " ").replace("\r", " ");
        String[] tokens = query.split("\\s+");
        Set<String> tables = new HashSet<>();
        for (int i = 0; i < tokens.length; i++) {
            String token = tokens[i].toUpperCase();
            if (token.equals("FROM") || token.equals("JOIN")) {
                // loop as there might be multiple tables separated by commas
                while (i + 1 < tokens.length) {
                    i++;
                    String tableToken = tokens[i];
                    boolean hasComma = tableToken.endsWith(",");
                    // Remove trailing commas
                    tableToken = tableToken.replaceAll(",$", "");
                    // Remove quotes if present
                    tableToken = tableToken.replaceAll("^\"|\"$", "");
                    tables.add(tableToken);

                    // Check if next token is an alias (not a SQL keyword)
                    String nextToken = (i + 1 < tokens.length) ? tokens[i + 1] : "";
                    if (!hasComma && !nextToken.isEmpty() &&
                        !nextToken.equals(",") &&
                        !nextToken.toUpperCase().matches("(WHERE|ORDER|GROUP|HAVING|LIMIT|UNION|INTERSECT|EXCEPT|JOIN|INNER|LEFT|RIGHT|FULL|CROSS|ON|USING|AS).*")) {

                        // Check if it's an alias (identifier pattern with optional comma)
                        String aliasToken = nextToken.replaceAll(",$", "");
                        if (aliasToken.matches("[a-zA-Z_][a-zA-Z0-9_]*")) {
                            i++; // skip the alias
                            hasComma = nextToken.endsWith(",");
                        }
                    }

                    // Handle explicit AS keyword
                    if (!hasComma && nextToken.equalsIgnoreCase("AS") && i + 2 < tokens.length) {
                        i += 2; // skip "AS" and the alias
                        String tokenAfterAlias = (i + 1 < tokens.length) ? tokens[i + 1] : "";
                        hasComma = tokens[i].endsWith(",") || tokenAfterAlias.equals(",");
                        if (tokenAfterAlias.equals(",")) {
                            i++; // skip the comma
                        }
                    }

                    // If no comma in current or previous processing, check if we should continue
                    if (!hasComma) {
                        String checkToken = (i + 1 < tokens.length) ? tokens[i + 1] : "";
                        if (!checkToken.equals(",")) {
                            break; // end of table list
                        } else {
                            i++; // skip comma and continue
                        }
                    }
                }
            }
        }
        return tables.stream().map(String::trim).filter(s -> !s.startsWith("(")).map(s -> s.replace(")", "")).collect(Collectors.toSet());
    }

    public static Set<String> getTableNames(DuckDBConnection connection) throws SQLException {
        ResultSet rs = connection.createStatement().executeQuery("SHOW TABLES");
        Set<String> tableNames = new HashSet<>();
        while (rs.next()) {
            tableNames.add(rs.getString(1));
        }
        return tableNames;
    }
}