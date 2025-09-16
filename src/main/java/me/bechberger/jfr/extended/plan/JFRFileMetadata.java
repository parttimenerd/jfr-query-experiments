package me.bechberger.jfr.extended.plan;

import java.util.List;
import java.util.Set;

/**
 * Simplified metadata for JFR files containing only event types.
 * 
 * This class stores the minimal metadata needed for query planning,
 * focusing only on the available event types without counts or timing information.
 * 
 * @author JFR Query Plan Architecture
 * @since 2.0
 */
public record JFRFileMetadata(Set<String> eventTypes) {
    
    /**
     * Creates metadata with a list of event types.
     */
    public static JFRFileMetadata of(List<String> eventTypes) {
        return new JFRFileMetadata(Set.copyOf(eventTypes));
    }
    
    /**
     * Creates metadata with a set of event types.
     */
    public static JFRFileMetadata of(Set<String> eventTypes) {
        return new JFRFileMetadata(Set.copyOf(eventTypes));
    }
    
    /**
     * Check if an event type is available.
     */
    public boolean hasEventType(String eventType) {
        return eventTypes.contains(eventType);
    }
    
    /**
     * Get the number of available event types.
     */
    public int getEventTypeCount() {
        return eventTypes.size();
    }
}
