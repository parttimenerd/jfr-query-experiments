package me.bechberger.jfr.extended.engine.exception;

import me.bechberger.jfr.extended.ast.ASTNode;

/**
 * Exception thrown when encountering features that are not yet implemented.
 * 
 * This exception provides specific information about the unimplemented feature,
 * its planned implementation status, and alternative approaches if available.
 * 
 * @author Query Engine Team
 * @since 1.0
 */
public class FeatureNotImplementedException extends QueryExecutionException {
    
    private final String featureName;
    private final ImplementationStatus status;
    
    /**
     * Enum defining different implementation statuses
     */
    public enum ImplementationStatus {
        PLANNED("Feature is planned for future implementation"),
        IN_PROGRESS("Feature is currently being implemented"),
        EXPERIMENTAL("Feature is experimental and may change"),
        DEPRECATED("Feature is deprecated and will not be implemented"),
        REQUIRES_DESIGN("Feature requires additional design work");
        
        private final String description;
        
        ImplementationStatus(String description) {
            this.description = description;
        }
        
        public String getDescription() {
            return description;
        }
    }
    
    /**
     * Creates a new FeatureNotImplementedException.
     * 
     * @param featureName The name of the unimplemented feature
     * @param status The implementation status
     * @param message The error message
     * @param errorNode The AST node where the error occurred (can be null)
     */
    public FeatureNotImplementedException(String featureName, ImplementationStatus status,
                                        String message, ASTNode errorNode) {
        super(
            message,
            errorNode,
            buildContext(featureName, status),
            buildUserHint(featureName, status),
            null
        );
        this.featureName = featureName;
        this.status = status;
    }
    
    /**
     * Factory method for planned features.
     */
    public static FeatureNotImplementedException forPlannedFeature(String featureName, ASTNode errorNode) {
        return new FeatureNotImplementedException(
            featureName,
            ImplementationStatus.PLANNED,
            String.format("Feature '%s' is not yet implemented", featureName),
            errorNode
        );
    }
    
    /**
     * Factory method for features in progress.
     */
    public static FeatureNotImplementedException forInProgressFeature(String featureName, 
                                                                    String workAroundHint,
                                                                    ASTNode errorNode) {
        return new FeatureNotImplementedException(
            featureName,
            ImplementationStatus.IN_PROGRESS,
            String.format("Feature '%s' is currently being implemented. %s", featureName, workAroundHint),
            errorNode
        );
    }
    
    /**
     * Factory method for experimental features.
     */
    public static FeatureNotImplementedException forExperimentalFeature(String featureName, 
                                                                       String experimentalNote,
                                                                       ASTNode errorNode) {
        return new FeatureNotImplementedException(
            featureName,
            ImplementationStatus.EXPERIMENTAL,
            String.format("Feature '%s' is experimental: %s", featureName, experimentalNote),
            errorNode
        );
    }
    
    /**
     * Factory method for deprecated features.
     */
    public static FeatureNotImplementedException forDeprecatedFeature(String featureName, 
                                                                     String alternative,
                                                                     ASTNode errorNode) {
        return new FeatureNotImplementedException(
            featureName,
            ImplementationStatus.DEPRECATED,
            String.format("Feature '%s' is deprecated and will not be implemented. Use %s instead", 
                featureName, alternative),
            errorNode
        );
    }
    
    private static String buildContext(String featureName, ImplementationStatus status) {
        return String.format("Unimplemented feature: %s - %s", featureName, status.getDescription());
    }
    
    private static String buildUserHint(String featureName, ImplementationStatus status) {
        return switch (status) {
            case PLANNED -> String.format(
                "Feature '%s' is planned for a future release. Consider using alternative approaches or wait for implementation.", 
                featureName
            );
            case IN_PROGRESS -> String.format(
                "Feature '%s' is being actively developed. Check for updates in newer versions.", 
                featureName
            );
            case EXPERIMENTAL -> String.format(
                "Feature '%s' is experimental and may change in future versions. Use with caution.", 
                featureName
            );
            case DEPRECATED -> String.format(
                "Feature '%s' is deprecated. Migrate to supported alternatives.", 
                featureName
            );
            case REQUIRES_DESIGN -> String.format(
                "Feature '%s' requires additional design work. Consider contributing to the design process.", 
                featureName
            );
        };
    }
    
    public String getFeatureName() {
        return featureName;
    }
    
    public ImplementationStatus getStatus() {
        return status;
    }
}
