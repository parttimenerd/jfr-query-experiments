package me.bechberger.jfr.extended.engine.framework;

import java.util.*;

/**
 * Execution context for managing query execution state
 */
public class ExecutionContext {
    private final boolean debugMode;
    private final Map<String, Object> variables = new HashMap<>();
    
    public ExecutionContext(boolean debugMode) {
        this.debugMode = debugMode;
    }
    
    public boolean isDebugMode() {
        return debugMode;
    }
    
    public void setVariable(String name, Object value) {
        variables.put(name, value);
    }
    
    public Object getVariable(String name) {
        return variables.get(name);
    }
    
    public void clear() {
        variables.clear();
    }
}
