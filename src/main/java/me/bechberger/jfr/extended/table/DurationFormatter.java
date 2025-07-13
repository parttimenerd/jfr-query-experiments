package me.bechberger.jfr.extended.table;

import java.time.Duration;

/**
 * Utility class for formatting Duration objects as human-readable strings.
 * Supports both compound formats (e.g., "1m30s", "2h15m") and decimal formats (e.g., "1.5s", "2.5ms").
 */
public class DurationFormatter {
    
    /**
     * Format a duration as a compound duration string (e.g., "1m30s", "2h15m") or decimal units (e.g., "1.5s", "1.5ms")
     */
    public static String format(Duration duration) {
        long totalNanos = duration.toNanos();
        
        if (totalNanos == 0) return "0ns";
        
        StringBuilder result = new StringBuilder();
        
        // Days
        if (totalNanos >= 86_400_000_000_000L) {
            long days = totalNanos / 86_400_000_000_000L;
            result.append(days).append("d");
            totalNanos %= 86_400_000_000_000L;
        }
        
        // Hours
        if (totalNanos >= 3_600_000_000_000L) {
            long hours = totalNanos / 3_600_000_000_000L;
            result.append(hours).append("h");
            totalNanos %= 3_600_000_000_000L;
        }
        
        // Minutes  
        if (totalNanos >= 60_000_000_000L) {
            long minutes = totalNanos / 60_000_000_000L;
            result.append(minutes).append("m");
            totalNanos %= 60_000_000_000L;
        }
        
        // If we have larger units, continue with compound format
        if (result.length() > 0) {
            // Handle seconds
            if (totalNanos >= 1_000_000_000L) {
                long seconds = totalNanos / 1_000_000_000L;
                result.append(seconds).append("s");
                totalNanos %= 1_000_000_000L;
            }
            
            // Add remaining smaller units
            if (totalNanos > 0) {
                if (totalNanos >= 1_000_000L) {
                    long millis = totalNanos / 1_000_000L;
                    result.append(millis).append("ms");
                    totalNanos %= 1_000_000L;
                }
                
                if (totalNanos >= 1_000L) {
                    long micros = totalNanos / 1_000L;
                    result.append(micros).append("us");
                    totalNanos %= 1_000L;
                }
                
                if (totalNanos > 0) {
                    result.append(totalNanos).append("ns");
                }
            }
            
            return result.toString();
        }
        
        // No larger units - format in the most appropriate single unit with decimal support
        
        // Seconds (always prefer decimal format for fractional seconds)
        if (totalNanos >= 1_000_000_000L) {
            double seconds = totalNanos / 1_000_000_000.0;
            if (seconds == Math.floor(seconds)) {
                // Whole seconds
                return ((long) seconds) + "s";
            } else {
                // Decimal seconds - always format as decimal
                return formatDecimal(seconds) + "s";
            }
        }
        
        // For sub-second durations, prefer the original unit if it makes sense
        // Only convert to decimal seconds for very specific "nice" fractions
        if (totalNanos < 1_000_000_000L) {
            double seconds = totalNanos / 1_000_000_000.0;
            // Only format as decimal seconds for very specific values like 0.5s 
            if (isVeryNiceDecimalSeconds(seconds)) {
                return formatDecimal(seconds) + "s";
            }
        }
        
        // Milliseconds  
        if (totalNanos >= 1_000_000L) {
            double millis = totalNanos / 1_000_000.0;
            if (millis == Math.floor(millis)) {
                // Whole milliseconds
                return ((long) millis) + "ms";
            } else {
                // Decimal milliseconds
                return formatDecimal(millis) + "ms";
            }
        }
        
        // Microseconds
        if (totalNanos >= 1_000L) {
            double micros = totalNanos / 1_000.0;
            if (micros == Math.floor(micros)) {
                // Whole microseconds
                return ((long) micros) + "us";
            } else {
                // Decimal microseconds
                return formatDecimal(micros) + "us";
            }
        }
        
        // Nanoseconds (always whole numbers)
        return totalNanos + "ns";
    }
    
    /**
     * Check if a fractional seconds value should be formatted as decimal seconds
     * rather than smaller units. Returns true for "nice" decimal values like 0.5, 0.25, 0.1, etc.
     */
    private static boolean isVeryNiceDecimalSeconds(double seconds) {
        // Only allow very specific nice fractions: 0.5, 0.25, 0.75
        return seconds == 0.5 || seconds == 0.25 || seconds == 0.75;
    }
    
    /**
     * Format a decimal value, removing trailing zeros
     */
    private static String formatDecimal(double value) {
        return String.format(java.util.Locale.US, "%.3f", value).replaceAll("\\.?0+$", "");
    }
}
