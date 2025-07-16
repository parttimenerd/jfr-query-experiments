package me.bechberger.jfr.extended.engine.util;

/**
 * Utility class for string similarity and distance calculations.
 * 
 * Provides efficient implementations of common string distance algorithms
 * used throughout the query engine for error reporting and suggestions.
 * 
 * @author Query Engine Team
 * @since 1.0
 */
public class StringSimilarity {
    
    /**
     * Calculate the Levenshtein distance between two strings.
     * 
     * The Levenshtein distance is the minimum number of single-character edits
     * (insertions, deletions, or substitutions) required to change one string
     * into another.
     * 
     * @param s1 the first string
     * @param s2 the second string
     * @return the Levenshtein distance between the strings
     */
    public static int levenshteinDistance(String s1, String s2) {
        if (s1 == null || s2 == null) {
            throw new IllegalArgumentException("Input strings cannot be null");
        }
        
        int len1 = s1.length();
        int len2 = s2.length();
        
        // Handle edge cases
        if (len1 == 0) return len2;
        if (len2 == 0) return len1;
        
        // Create distance matrix
        int[][] dp = new int[len1 + 1][len2 + 1];
        
        // Initialize first row and column
        for (int i = 0; i <= len1; i++) {
            dp[i][0] = i;
        }
        for (int j = 0; j <= len2; j++) {
            dp[0][j] = j;
        }
        
        // Fill the distance matrix
        for (int i = 1; i <= len1; i++) {
            for (int j = 1; j <= len2; j++) {
                int cost = (s1.charAt(i - 1) == s2.charAt(j - 1)) ? 0 : 1;
                dp[i][j] = Math.min(Math.min(
                    dp[i - 1][j] + 1,      // deletion
                    dp[i][j - 1] + 1),     // insertion
                    dp[i - 1][j - 1] + cost // substitution
                );
            }
        }
        
        return dp[len1][len2];
    }
    
    /**
     * Calculate normalized similarity score between two strings (0.0 to 1.0).
     * 
     * Returns 1.0 for identical strings and 0.0 for completely different strings.
     * Uses Levenshtein distance normalized by the length of the longer string.
     * 
     * @param s1 the first string
     * @param s2 the second string
     * @return similarity score between 0.0 and 1.0
     */
    public static double similarity(String s1, String s2) {
        if (s1 == null || s2 == null) {
            throw new IllegalArgumentException("Input strings cannot be null");
        }
        
        if (s1.equals(s2)) {
            return 1.0;
        }
        
        int maxLength = Math.max(s1.length(), s2.length());
        if (maxLength == 0) {
            return 1.0; // Both strings are empty
        }
        
        int distance = levenshteinDistance(s1, s2);
        return 1.0 - ((double) distance / maxLength);
    }
    
    /**
     * Check if two strings are similar within a given threshold.
     * 
     * @param s1 the first string
     * @param s2 the second string
     * @param threshold similarity threshold (0.0 to 1.0)
     * @return true if similarity is greater than or equal to threshold
     */
    public static boolean isSimilar(String s1, String s2, double threshold) {
        return similarity(s1, s2) >= threshold;
    }
    
    /**
     * Check if two strings are similar within a given edit distance.
     * 
     * @param s1 the first string
     * @param s2 the second string
     * @param maxDistance maximum allowed edit distance
     * @return true if edit distance is less than or equal to maxDistance
     */
    public static boolean isSimilar(String s1, String s2, int maxDistance) {
        return levenshteinDistance(s1, s2) <= maxDistance;
    }
    
    /**
     * Calculate case-insensitive Levenshtein distance.
     * 
     * @param s1 the first string
     * @param s2 the second string
     * @return the Levenshtein distance between the lowercase strings
     */
    public static int levenshteinDistanceIgnoreCase(String s1, String s2) {
        if (s1 == null || s2 == null) {
            throw new IllegalArgumentException("Input strings cannot be null");
        }
        return levenshteinDistance(s1.toLowerCase(), s2.toLowerCase());
    }
    
    /**
     * Calculate case-insensitive similarity score.
     * 
     * @param s1 the first string
     * @param s2 the second string
     * @return similarity score between 0.0 and 1.0
     */
    public static double similarityIgnoreCase(String s1, String s2) {
        if (s1 == null || s2 == null) {
            throw new IllegalArgumentException("Input strings cannot be null");
        }
        return similarity(s1.toLowerCase(), s2.toLowerCase());
    }
    
    /**
     * Find the closest matching string from a collection within a given edit distance threshold.
     * 
     * @param target the target string to match against
     * @param candidates the collection of candidate strings
     * @param maxDistance maximum allowed edit distance
     * @param ignoreCase whether to perform case-insensitive matching
     * @return the closest matching string, or null if no match within threshold
     */
    public static String findClosestMatch(String target, Iterable<String> candidates, int maxDistance, boolean ignoreCase) {
        if (target == null) {
            throw new IllegalArgumentException("Target string cannot be null");
        }
        if (candidates == null) {
            throw new IllegalArgumentException("Candidates collection cannot be null");
        }
        
        String bestMatch = null;
        int minDistance = Integer.MAX_VALUE;
        
        for (String candidate : candidates) {
            if (candidate == null) continue;
            
            int distance = ignoreCase ? 
                levenshteinDistanceIgnoreCase(target, candidate) :
                levenshteinDistance(target, candidate);
                
            if (distance < minDistance && distance <= maxDistance) {
                minDistance = distance;
                bestMatch = candidate;
            }
        }
        
        return bestMatch;
    }
    
    /**
     * Find the closest matching string from an array within a given edit distance threshold.
     * 
     * @param target the target string to match against
     * @param candidates the array of candidate strings
     * @param maxDistance maximum allowed edit distance
     * @param ignoreCase whether to perform case-insensitive matching
     * @return the closest matching string, or null if no match within threshold
     */
    public static String findClosestMatch(String target, String[] candidates, int maxDistance, boolean ignoreCase) {
        if (candidates == null) {
            throw new IllegalArgumentException("Candidates array cannot be null");
        }
        return findClosestMatch(target, java.util.Arrays.asList(candidates), maxDistance, ignoreCase);
    }
    
    /**
     * Find the closest matching string from a collection with case-insensitive matching.
     * Uses a default maximum distance threshold of 2.
     * 
     * @param target the target string to match against
     * @param candidates the collection of candidate strings
     * @return the closest matching string, or null if no match within threshold
     */
    public static String findClosestMatch(String target, Iterable<String> candidates) {
        return findClosestMatch(target, candidates, 2, true);
    }
    
    /**
     * Find the closest matching string from an array with case-insensitive matching.
     * Uses a default maximum distance threshold of 2.
     * 
     * @param target the target string to match against
     * @param candidates the array of candidate strings
     * @return the closest matching string, or null if no match within threshold
     */
    public static String findClosestMatch(String target, String[] candidates) {
        return findClosestMatch(target, candidates, 2, true);
    }
}
