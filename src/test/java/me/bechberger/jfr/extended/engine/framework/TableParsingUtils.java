package me.bechberger.jfr.extended.engine.framework;

import me.bechberger.jfr.extended.table.CellType;
import me.bechberger.jfr.extended.Lexer;
import me.bechberger.jfr.extended.Token;
import me.bechberger.jfr.extended.TokenType;

import java.util.*;
import java.util.regex.Pattern;

/**
 * Consolidated utilities for parsing table specifications to eliminate code duplication
 */
public class TableParsingUtils {
    
    /**
     * Parse simple table specification (space-separated, types inferred)
     */
    public static ExpectedTable parseSimpleTableSpec(String tableSpec) {
        String[] lines = tableSpec.trim().split("\n");
        if (lines.length < 1) {
            throw new IllegalArgumentException("Table specification must have at least a header line");
        }
        
        // Parse header line for column names
        String headerLine = lines[0].trim();
        String[] columnNames = headerLine.split("\\s+");
        
        // Parse data rows and infer types
        List<List<Object>> expectedRows = new ArrayList<>();
        List<CellType> columnTypes = new ArrayList<>();
        
        // Initialize column types to STRING (will be inferred from first row)
        for (int i = 0; i < columnNames.length; i++) {
            columnTypes.add(CellType.STRING);
        }
        
        for (int lineIdx = 1; lineIdx < lines.length; lineIdx++) {
            String line = lines[lineIdx].trim();
            if (line.isEmpty()) continue;
            
            String[] values = line.split("\\s+");
            List<Object> rowValues = new ArrayList<>();
            
            for (int colIdx = 0; colIdx < values.length && colIdx < columnNames.length; colIdx++) {
                String value = values[colIdx];
                
                // Infer type on first data row
                if (expectedRows.isEmpty()) {
                    columnTypes.set(colIdx, inferColumnType(value));
                }
                
                // Parse value according to inferred type
                Object parsedValue = parseValueByType(value, columnTypes.get(colIdx));
                rowValues.add(parsedValue);
            }
            expectedRows.add(rowValues);
        }
        
        return new ExpectedTable(Arrays.asList(columnNames), columnTypes, expectedRows);
    }
    
    /**
     * Infer column type from a sample value using the lexer for accurate detection
     */
    public static CellType inferColumnType(String value) {
        if (value == null || value.equalsIgnoreCase("null")) {
            return CellType.NULL;
        }
        
        try {
            // Use the lexer to tokenize the value and determine its type
            Lexer lexer = new Lexer(value.trim());
            List<Token> tokens = lexer.tokenize();
            
            if (tokens.isEmpty() || tokens.get(0).type() == TokenType.EOF) {
                return CellType.STRING;
            }
            
            Token firstToken = tokens.get(0);
            TokenType tokenType = firstToken.type();
            
            // Map token types to cell types
            switch (tokenType) {
                case BOOLEAN:
                    return CellType.BOOLEAN;
                case NUMBER:
                    // Check if it's a float or integer
                    if (value.contains(".")) {
                        return CellType.FLOAT;
                    } else {
                        return CellType.NUMBER;
                    }
                case DURATION_LITERAL:
                    return CellType.DURATION;
                case TIMESTAMP_LITERAL:
                    return CellType.TIMESTAMP;
                case MEMORY_SIZE_LITERAL:
                    return CellType.MEMORY_SIZE;
                case RATE_UNIT:
                    return CellType.RATE;
                case STRING:
                    return CellType.STRING;
                case LBRACKET:
                    // Array detection - if starts with [
                    return CellType.ARRAY;
                default:
                    // Fallback to manual detection for complex cases
                    return inferColumnTypeManually(value);
            }
        } catch (Exception e) {
            // If lexer fails, fallback to manual detection
            return inferColumnTypeManually(value);
        }
    }
    
    /**
     * Fallback manual type inference for cases where lexer doesn't work
     */
    private static CellType inferColumnTypeManually(String value) {
        String trimmed = value.trim();
        
        // Check for arrays
        if (trimmed.startsWith("[") && trimmed.endsWith("]")) {
            return CellType.ARRAY;
        }
        
        // Check for boolean
        if (trimmed.equalsIgnoreCase("true") || trimmed.equalsIgnoreCase("false")) {
            return CellType.BOOLEAN;
        }
        
        // Check for duration literals (e.g., "1000ms", "5s", "2h")
        if (Pattern.matches("\\d+(\\.\\d+)?(ms|ns|us|min|m|s|h|d)", trimmed)) {
            return CellType.DURATION;
        }
        
        // Check for memory size literals (e.g., "1024B", "5KB", "2GB")
        if (Pattern.matches("\\d+(\\.\\d+)?(B|KB|MB|GB|TB)", trimmed)) {
            return CellType.MEMORY_SIZE;
        }
        
        // Check for timestamp (ISO format)
        if (Pattern.matches("\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}(\\.\\d{3})?Z?", trimmed)) {
            return CellType.TIMESTAMP;
        }
        
        // Check for numbers
        try {
            if (trimmed.contains(".")) {
                Double.parseDouble(trimmed);
                return CellType.FLOAT;
            } else {
                Long.parseLong(trimmed);
                return CellType.NUMBER;
            }
        } catch (NumberFormatException e) {
            // Not a number, continue checking
        }
        
        // Default to string
        return CellType.STRING;
    }
    
    /**
     * Parse a value according to its expected type using lexer for accurate parsing
     */
    public static Object parseValueByType(String value, CellType type) {
        if (value == null || value.equalsIgnoreCase("null")) {
            return null;
        }
        
        String trimmed = value.trim();
        
        switch (type) {
            case NULL:
                return null;
                
            case BOOLEAN:
                return Boolean.parseBoolean(trimmed);
                
            case NUMBER:
                try {
                    return Long.parseLong(trimmed);
                } catch (NumberFormatException e) {
                    // Fallback to double for large numbers
                    return Double.parseDouble(trimmed);
                }
                
            case FLOAT:
                return Double.parseDouble(trimmed);
                
            case DURATION:
                return parseDurationValue(trimmed);
                
            case TIMESTAMP:
                return parseTimestampValue(trimmed);
                
            case MEMORY_SIZE:
                return parseMemorySizeValue(trimmed);
                
            case RATE:
                return parseRateValue(trimmed);
                
            case ARRAY:
                return parseArrayValue(trimmed);
                
            case STRING:
            default:
                // Remove quotes if present
                if (trimmed.startsWith("\"") && trimmed.endsWith("\"")) {
                    return trimmed.substring(1, trimmed.length() - 1);
                }
                if (trimmed.startsWith("'") && trimmed.endsWith("'")) {
                    return trimmed.substring(1, trimmed.length() - 1);
                }
                return trimmed;
        }
    }
    
    /**
     * Parse duration values (e.g., "1000ms", "5s", "2h")
     */
    private static Object parseDurationValue(String value) {
        // Extract number and unit
        if (value.endsWith("ms")) {
            return Long.parseLong(value.substring(0, value.length() - 2));
        } else if (value.endsWith("ns")) {
            return Long.parseLong(value.substring(0, value.length() - 2)) / 1_000_000; // Convert to ms
        } else if (value.endsWith("us")) {
            return Long.parseLong(value.substring(0, value.length() - 2)) / 1_000; // Convert to ms
        } else if (value.endsWith("s")) {
            return Long.parseLong(value.substring(0, value.length() - 1)) * 1_000; // Convert to ms
        } else if (value.endsWith("min") || value.endsWith("m")) {
            String numPart = value.endsWith("min") ? value.substring(0, value.length() - 3) : value.substring(0, value.length() - 1);
            return Long.parseLong(numPart) * 60_000; // Convert to ms
        } else if (value.endsWith("h")) {
            return Long.parseLong(value.substring(0, value.length() - 1)) * 3_600_000; // Convert to ms
        } else if (value.endsWith("d")) {
            return Long.parseLong(value.substring(0, value.length() - 1)) * 86_400_000; // Convert to ms
        } else {
            // Assume it's already in milliseconds
            return Long.parseLong(value);
        }
    }
    
    /**
     * Parse timestamp values (ISO format or epoch)
     */
    private static Object parseTimestampValue(String value) {
        // For simplicity, return as string for now - could be enhanced with proper date parsing
        if (value.matches("\\d+")) {
            return Long.parseLong(value); // Epoch timestamp
        }
        return value; // ISO format string
    }
    
    /**
     * Parse memory size values (e.g., "1024B", "5KB", "2GB")
     */
    private static Object parseMemorySizeValue(String value) {
        if (value.endsWith("B")) {
            return Long.parseLong(value.substring(0, value.length() - 1));
        } else if (value.endsWith("KB")) {
            return Long.parseLong(value.substring(0, value.length() - 2)) * 1_024;
        } else if (value.endsWith("MB")) {
            return Long.parseLong(value.substring(0, value.length() - 2)) * 1_048_576;
        } else if (value.endsWith("GB")) {
            return Long.parseLong(value.substring(0, value.length() - 2)) * 1_073_741_824;
        } else if (value.endsWith("TB")) {
            return Long.parseLong(value.substring(0, value.length() - 2)) * 1_099_511_627_776L;
        } else {
            // Assume it's already in bytes
            return Long.parseLong(value);
        }
    }
    
    /**
     * Parse rate values (e.g., "per second", "/s")
     */
    private static Object parseRateValue(String value) {
        // For now, return as string - could be enhanced with proper rate parsing
        return value;
    }
    
    /**
     * Parse array values (e.g., "[1,2,3]", "[INFO,WARN,ERROR]")
     */
    private static Object parseArrayValue(String value) {
        if (!value.startsWith("[") || !value.endsWith("]")) {
            return value; // Not a proper array format
        }
        
        String content = value.substring(1, value.length() - 1).trim();
        if (content.isEmpty()) {
            return Collections.emptyList();
        }
        
        // Split by comma and parse each element
        String[] elements = content.split(",");
        List<Object> parsedElements = new ArrayList<>();
        
        for (String element : elements) {
            String trimmedElement = element.trim();
            // Try to infer type of each element and parse accordingly
            CellType elementType = inferColumnType(trimmedElement);
            Object parsedElement = parseValueByType(trimmedElement, elementType);
            parsedElements.add(parsedElement);
        }
        
        return parsedElements;
    }
    
    
    /**
     * Parse value from string with string type specification - supports all cell types
     */
    public static Object parseValue(String valueStr, String expectedType) {
        if (valueStr == null || valueStr.equalsIgnoreCase("null")) {
            return null;
        }
        
        switch (expectedType.toUpperCase()) {
            case "NULL":
                return null;
                
            case "STRING":
                // Remove quotes if present
                if (valueStr.startsWith("\"") && valueStr.endsWith("\"")) {
                    return valueStr.substring(1, valueStr.length() - 1);
                }
                if (valueStr.startsWith("'") && valueStr.endsWith("'")) {
                    return valueStr.substring(1, valueStr.length() - 1);
                }
                return valueStr;
                
            case "NUMBER":
                try {
                    return Long.parseLong(valueStr);
                } catch (NumberFormatException e) {
                    return Double.parseDouble(valueStr);
                }
                
            case "FLOAT":
                return Double.parseDouble(valueStr);
                
            case "BOOLEAN":
                return Boolean.parseBoolean(valueStr);
                
            case "TIMESTAMP":
                return parseTimestampValue(valueStr);
                
            case "DURATION":
                return parseDurationValue(valueStr);
                
            case "MEMORY_SIZE":
                return parseMemorySizeValue(valueStr);
                
            case "RATE":
                return parseRateValue(valueStr);
                
            case "ARRAY":
                return parseArrayValue(valueStr);
                
            default:
                return valueStr;
        }
    }
    
    /**
     * Parse value from string with CellType specification
     */
    public static Object parseValue(String valueStr, CellType expectedType) {
        return parseValue(valueStr, expectedType.name());
    }
    
    /**
     * Parse column type from string name
     */
    public static CellType parseColumnType(String typeStr) {
        switch (typeStr.toUpperCase()) {
            case "STRING": return CellType.STRING;
            case "NUMBER": return CellType.NUMBER;
            case "FLOAT": return CellType.FLOAT;
            case "BOOLEAN": return CellType.BOOLEAN;
            case "TIMESTAMP": return CellType.TIMESTAMP;
            case "DURATION": return CellType.DURATION;
            case "MEMORY_SIZE": return CellType.MEMORY_SIZE;
            case "RATE": return CellType.RATE;
            case "ARRAY": return CellType.ARRAY;
            case "NULL": return CellType.NULL;
            default: 
                throw new IllegalArgumentException("Unknown column type: " + typeStr);
        }
    }
    
    /**
     * Infer type from a string value and return as string name (for backward compatibility)
     */
    public static String inferType(String value) {
        return inferColumnType(value).name();
    }
    
    /**
     * Get all supported cell types
     */
    public static CellType[] getSupportedCellTypes() {
        return CellType.values();
    }
    
    /**
     * Check if a string represents a valid cell type
     */
    public static boolean isValidCellType(String typeStr) {
        try {
            parseColumnType(typeStr);
            return true;
        } catch (IllegalArgumentException e) {
            return false;
        }
    }
    
    /**
     * Convert a value to string representation suitable for table specs
     */
    public static String valueToTableSpec(Object value, CellType type) {
        if (value == null) {
            return "null";
        }
        
        switch (type) {
            case STRING:
                return value.toString();
            case ARRAY:
                if (value instanceof List) {
                    List<?> list = (List<?>) value;
                    StringBuilder sb = new StringBuilder("[");
                    for (int i = 0; i < list.size(); i++) {
                        if (i > 0) sb.append(",");
                        sb.append(list.get(i));
                    }
                    sb.append("]");
                    return sb.toString();
                }
                return value.toString();
            case DURATION:
                if (value instanceof Number) {
                    return value + "ms";
                }
                return value.toString();
            case MEMORY_SIZE:
                if (value instanceof Number) {
                    return value + "B";
                }
                return value.toString();
            default:
                return value.toString();
        }
    }
}
