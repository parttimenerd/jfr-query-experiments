package me.bechberger.jfr.extended.plan.exception;

/**
 * Exception for data access and availability errors.
 * 
 * This exception covers errors related to:
 * - Table not found
 * - Column not found  
 * - Data source unavailable
 * - File not found or unreadable
 * - Permission denied errors
 * 
 * @author JFR Query Plan Architecture
 * @since 2.0
 */
public class DataAccessException extends Exception {
    
    /** The resource that could not be accessed */
    private final String resource;
    
    /** The type of access that was attempted */
    private final AccessType accessType;
    
    /** Alternative suggestions for the failed access */
    private final String[] alternatives;
    
    /**
     * Types of data access operations.
     */
    public enum AccessType {
        TABLE_LOOKUP("table lookup"),
        COLUMN_ACCESS("column access");
        
        private final String description;
        
        AccessType(String description) {
            this.description = description;
        }
        
        public String getDescription() {
            return description;
        }
    }
    
    /**
     * Creates a data access exception for resource not found.
     */
    public DataAccessException(String message, String resource, AccessType accessType, Throwable cause) {
        super(message, cause);
        this.resource = resource;
        this.accessType = accessType;
        this.alternatives = null;
    }
    
    /**
     * Creates a data access exception without cause.
     */
    public DataAccessException(String message, String resource, AccessType accessType) {
        super(message);
        this.resource = resource;
        this.accessType = accessType;
        this.alternatives = null;
    }
    
    /**
     * Creates a data access exception with alternatives.
     */
    public DataAccessException(String message, String resource, AccessType accessType, String[] alternatives) {
        super(message);
        this.resource = resource;
        this.accessType = accessType;
        this.alternatives = alternatives;
    }
    
    /**
     * Creates a data access exception with alternatives and cause.
     */
    public DataAccessException(String message, String resource, AccessType accessType, String[] alternatives, Throwable cause) {
        super(message, cause);
        this.resource = resource;
        this.accessType = accessType;
        this.alternatives = alternatives;
    }
    
    /**
     * Get the resource that could not be accessed.
     */
    public String getResource() {
        return resource;
    }
    
    /**
     * Get the type of access that was attempted.
     */
    public AccessType getAccessType() {
        return accessType;
    }
    
    /**
     * Get alternative suggestions for the failed access.
     */
    public String[] getAlternatives() {
        return alternatives;
    }
    
    /**
     * Check if alternatives are available.
     */
    public boolean hasAlternatives() {
        return alternatives != null && alternatives.length > 0;
    }
    
    /**
     * Get a detailed error message.
     */
    public String getDetailedMessage() {
        StringBuilder sb = new StringBuilder(getMessage());
        
        if (resource != null) {
            sb.append(" - Resource: ").append(resource);
        }
        
        if (accessType != null) {
            sb.append(" - Access type: ").append(accessType.getDescription());
        }
        
        if (hasAlternatives()) {
            sb.append(" - Available alternatives: ");
            for (int i = 0; i < alternatives.length; i++) {
                if (i > 0) sb.append(", ");
                sb.append(alternatives[i]);
            }
        }
        
        return sb.toString();
    }
}
