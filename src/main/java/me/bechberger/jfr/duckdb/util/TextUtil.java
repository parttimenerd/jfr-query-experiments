package me.bechberger.jfr.duckdb.util;

import java.util.List;
import java.util.stream.Collectors;

public class TextUtil {

    public static List<String> findClosestMatches(String input, List<String> options, int maxDistance, int maxResults) {
        return options.stream()
                .filter(option -> levenshteinDistance(input.toLowerCase(), option.toLowerCase()) <= maxDistance)
                .collect(Collectors.toList());
    }

    public static int levenshteinDistance(String s1, String s2) {
        int[][] dp = new int[s1.length() + 1][s2.length() + 1];

        for (int i = 0; i <= s1.length(); i++) {
            for (int j = 0; j <= s2.length(); j++) {
                if (i == 0) {
                    dp[i][j] = j; // Deletion
                } else if (j == 0) {
                    dp[i][j] = i; // Insertion
                } else if (s1.charAt(i - 1) == s2.charAt(j - 1)) {
                    dp[i][j] = dp[i - 1][j - 1]; // No operation
                } else {
                    dp[i][j] = 1 + Math.min(dp[i - 1][j], // Deletion
                                    Math.min(dp[i][j - 1], // Insertion
                                             dp[i - 1][j - 1])); // Substitution
                }
            }
        }

        return dp[s1.length()][s2.length()];
    }
}