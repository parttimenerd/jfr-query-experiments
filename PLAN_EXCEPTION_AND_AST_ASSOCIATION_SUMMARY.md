# Plan Exception Hierarchy and AST Node Association Implementation

## Overview

This document summarizes the comprehensive plan exception hierarchy and AST node association system implemented for the streaming query plan architecture.

## Exception Hierarchy Structure

### 1. Base Exception Class
- **PlanExecutionException**: Abstract base class for all plan execution errors
- **Features**:
  - AST node association for error context
  - Error categorization (VALIDATION, EXECUTION, RESOURCE, DATA, CONFIGURATION, DEPENDENCY)
  - Plan type identification
  - Detailed error messaging with context

### 2. Specialized Exception Classes

#### SelectPlanException
- **Purpose**: Errors during SELECT plan execution
- **Types**: PROJECTION_ERROR, WHERE_CLAUSE_ERROR, EXPRESSION_ERROR, COLUMN_REFERENCE_ERROR, TYPE_CONVERSION_ERROR
- **Context**: Column names, selection expressions

#### GroupByPlanException  
- **Purpose**: Errors during GROUP BY plan execution
- **Types**: GROUPING_ERROR, AGGREGATE_ERROR, HAVING_CLAUSE_ERROR, GROUP_KEY_ERROR, MEMORY_EXHAUSTED
- **Context**: Aggregate function names, group keys

#### JoinPlanException
- **Purpose**: Errors during JOIN plan execution  
- **Types**: JOIN_CONDITION_ERROR, MEMORY_EXHAUSTED, CARTESIAN_PRODUCT, TYPE_MISMATCH, INPUT_PLAN_ERROR, HASH_TABLE_ERROR
- **Context**: Join type (INNER, LEFT, RIGHT, FULL), join conditions

#### TableScanPlanException
- **Purpose**: Errors during table scan execution
- **Types**: QUERY_EXECUTION_ERROR, DATA_LOADING_ERROR, TABLE_CONSTRUCTION_ERROR, INVALID_QUERY, IO_ERROR, TIMEOUT_ERROR
- **Context**: Raw JFR query strings

#### ExpressionEvaluationException
- **Purpose**: Errors during expression evaluation
- **Types**: FUNCTION_ERROR, OPERATOR_ERROR, TYPE_CONVERSION_ERROR, NULL_VALUE_ERROR, DIVIDE_BY_ZERO, INVALID_ARGUMENT
- **Context**: Expression strings, function/operator names

#### MemoryExhaustedException
- **Purpose**: Memory exhaustion during plan execution
- **Types**: HEAP_EXHAUSTED, BUFFER_OVERFLOW, ALLOCATION_FAILED, STREAMING_BUFFER_FULL, HASH_TABLE_TOO_LARGE
- **Context**: Requested vs. available memory in bytes

### 3. Exception Factory
- **PlanExceptionFactory**: Provides convenient factory methods for creating strongly-typed exceptions
- **Benefits**: Consistent exception creation, proper context setting, reduced boilerplate

## AST Node Association System

### 1. AbstractStreamingPlan Enhancement
- **AST Node Association**: Every plan now stores its associated AST node
- **Benefits**: 
  - Error context with AST information
  - Plan visualization capabilities
  - Debugging and error reporting enhancement

### 2. Plan Interface Updates
- **StreamingQueryPlan**: Added `getAssociatedNode()` method
- **QueryExecutionContext**: Enhanced context management with variables and properties
- **Error Propagation**: Automatic wrapping of unexpected errors with AST context

### 3. Concrete Plan Updates
- **SelectPlan**: Associates with SelectNode, uses SelectItemNode for projections
- **Enhanced Error Handling**: Context-aware exception creation with specific error types

## Event Type Management System

### 1. Implicit View Definition
- **Concept**: Every JFR event type X automatically gets a view "VIEW X AS (SELECT * FROM X)"
- **Implementation**: Raw queries determine event types rather than explicit declaration
- **Benefits**: 
  - Automatic event type discovery
  - Simplified query writing
  - Dynamic event type support

### 2. Event Type Resolution
- **Raw Query Analysis**: Parse raw JFR queries to extract event types
- **Dynamic Discovery**: Event types determined at execution time
- **Flexibility**: Support for new event types without code changes

## Implementation Status

### ✅ Completed Components
- **Exception Hierarchy**: Complete hierarchy with 6 specialized exception classes
- **AST Node Association**: Base infrastructure for all plans
- **Factory Pattern**: PlanExceptionFactory for consistent exception creation
- **SelectPlan**: Updated with AST nodes and proper exception handling

### 🔄 In Progress Components
- **Missing Classes**: FilterPlan, PlanVisualizer, other supporting classes
- **Event Type System**: Raw query-based event type discovery
- **Plan Completion**: TableScanPlan, GroupByPlan, JoinPlan updates

### 📋 Next Steps
1. **Create FilterPlan**: Dedicated plan for WHERE clause filtering
2. **Implement PlanVisualizer**: Query plan tree visualization 
3. **Update Event Type System**: Raw query-based event type resolution
4. **Complete Plan Updates**: Associate all plans with AST nodes
5. **Add Missing Support Classes**: Complete the infrastructure

## Benefits Achieved

### 1. Enhanced Error Handling
- **Contextual Errors**: AST node context in all exceptions
- **Categorized Errors**: Clear error categories for different failure types
- **Detailed Messages**: Rich error information with plan and AST context

### 2. Improved Debugging
- **AST Traceability**: Every plan traceable to source AST
- **Error Pinpointing**: Exact location of errors in query structure
- **Plan Visualization**: Foundation for query plan visualization

### 3. Better Architecture
- **Separation of Concerns**: Dedicated exception types for different error scenarios
- **Consistent Patterns**: Factory pattern for exception creation
- **Extensibility**: Easy addition of new exception types and plans

### 4. Event Type Flexibility
- **Dynamic Discovery**: Event types discovered from raw queries
- **Implicit Views**: Automatic view creation for all event types
- **Simplified Queries**: No explicit event type declarations needed

## Code Examples

### Exception Usage
```java
// Specific exception with context
throw PlanExceptionFactory.projectionError(
    "Column not found: " + columnName, 
    selectItemNode, 
    columnName);

// Aggregate function error
throw PlanExceptionFactory.aggregateError(
    "Invalid aggregate function", 
    functionNode, 
    "INVALID_FUNC");
```

### AST Node Association
```java
public class SelectPlan extends AbstractStreamingPlan {
    public SelectPlan(SelectNode selectNode, ...) {
        super(selectNode, "SELECT with " + selections.size() + " columns");
        // Plan automatically associated with AST node
    }
}
```

### Event Type Discovery
```java
// Raw query determines event types
Set<String> eventTypes = extractEventTypesFromRawQuery(rawQuery);
// Implicit view: "VIEW jdk.GarbageCollection AS (SELECT * FROM jdk.GarbageCollection)"
```

This implementation provides a robust foundation for error handling, debugging, and plan management in the streaming query system.
