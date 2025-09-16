# Query Plan Package Organization

This document describes the enhanced package hierarchy for the JFR Query Plan infrastructure.

## Package Structure

```
me.bechberger.jfr.extended.plan/
├── core/                           # Core interfaces and base classes
│   ├── StreamingQueryPlan.java     # Main plan interface
│   ├── AbstractStreamingPlan.java  # Base implementation with AST association
│   └── QueryExecutionContext.java  # Execution context management
├── nodes/                          # Concrete plan implementations
│   ├── TableScanPlan.java         # Raw JFR query execution
│   ├── FilterPlan.java            # WHERE clause filtering
│   ├── SelectPlan.java            # SELECT projection and basic operations
│   ├── GroupByPlan.java           # GROUP BY aggregation
│   ├── JoinPlan.java              # JOIN operations
│   ├── OrderByPlan.java           # ORDER BY sorting (placeholder)
│   ├── LimitPlan.java             # LIMIT/OFFSET operations (placeholder)
│   ├── HavingPlan.java            # HAVING clause filtering (placeholder)
│   └── DistinctPlan.java          # DISTINCT deduplication (placeholder)
├── exception/                      # Exception hierarchy
│   ├── QueryPlanException.java    # Base exception with categorization
│   ├── PlanExecutionException.java # Original plan exception
│   ├── TypeMismatchException.java  # Type-related errors
│   ├── DataException.java         # Data quality/integrity errors
│   ├── ResourceException.java     # Memory/resource exhaustion
│   ├── ConfigurationException.java # Config/setup errors
│   ├── SyntaxException.java       # Syntax/semantic errors
│   ├── PlanExceptionFactory.java  # Original exception factory
│   └── EnhancedPlanExceptionFactory.java # Enhanced factory
├── factory/                        # Plan creation factories
│   └── QueryPlanFactory.java      # Centralized plan creation
├── evaluator/                      # Expression and function evaluation
│   ├── SimpleFunctionEvaluator.java
│   └── AggregateEvaluator.java
├── converter/                      # Data conversion utilities
│   └── JfrTableConverter.java
├── visitor/                        # Plan visitor patterns
│   └── PlanVisualizer.java
└── [legacy files]                 # Existing files (to be organized)
    ├── QueryPlanExecutionException.java # Enhanced with plan context
    ├── EventRow.java
    ├── MemoryUsageStats.java
    └── ...
```

## Key Improvements

### 1. Enhanced Exception System

#### Exception Hierarchy
- **QueryPlanException**: Base class with error categorization and severity levels
- **6 Specialized Exception Types**: TypeMismatch, Data, Resource, Configuration, Syntax errors
- **Error Categories**: SYNTAX_ERROR, TYPE_ERROR, RUNTIME_ERROR, RESOURCE_ERROR, DATA_ERROR, CONFIGURATION_ERROR, NETWORK_ERROR, SECURITY_ERROR
- **Severity Levels**: INFO, WARNING, ERROR, FATAL
- **Recovery Suggestions**: Automatic suggestions for fixing errors based on context

#### Enhanced Context
- **AST Node Association**: Every exception includes related AST node for precise error location
- **Plan Context**: Exceptions include the failing query plan for better debugging
- **Execution Phase Tracking**: Errors are categorized by execution phase (PLANNING, EXECUTION, etc.)
- **Detailed Error Reports**: Rich error reports with plan visualization and recovery suggestions

### 2. Comprehensive Plan Infrastructure

#### Completed Plans
- **TableScanPlan**: Raw JFR query execution with implicit view creation
- **FilterPlan**: WHERE clause filtering with boolean expression evaluation
- **SelectPlan**: Projection with column aliasing and result construction
- **GroupByPlan**: Aggregation with HAVING clause support
- **JoinPlan**: Full JOIN support (INNER, LEFT, RIGHT, FULL OUTER)

#### Placeholder Plans (Ready for Implementation)
- **OrderByPlan**: Structure for ORDER BY operations with multi-field sorting
- **LimitPlan**: Structure for LIMIT/OFFSET operations with efficient streaming
- **HavingPlan**: Structure for HAVING clause filtering after GROUP BY
- **DistinctPlan**: Structure for DISTINCT deduplication

### 3. Plan Creation and Management

#### Factory Pattern
- **QueryPlanFactory**: Centralized plan creation with proper error handling
- **Type-Safe Construction**: Proper AST node association for every plan
- **Error Handling**: Enhanced exceptions during plan creation

#### Visualization and Debugging
- **PlanVisualizer**: Enhanced with AST context and cost estimates
- **Plan Tree Traversal**: Proper input plan getter methods for tree visualization
- **Execution Context**: Rich context management for plan execution

## Feature Parity Status

### Completed (100%)
- ✅ Exception hierarchy with AST association
- ✅ Core plan infrastructure (TableScan, Filter, Select, GroupBy, Join)
- ✅ Plan visualization with AST context
- ✅ Raw query event type discovery
- ✅ Enhanced error reporting with recovery suggestions

### Placeholder Implementation (25%)
- 🔄 ORDER BY operations (structure complete, sorting logic needed)
- 🔄 LIMIT/OFFSET operations (structure complete, stream limiting needed)
- 🔄 HAVING clause filtering (structure complete, condition evaluation needed)
- 🔄 DISTINCT deduplication (structure complete, hash-based deduplication needed)

### Missing (0%)
- ❌ Subquery support (SubqueryPlan)
- ❌ View definition support (ViewDefinitionPlan)
- ❌ Variable assignment support (AssignmentPlan)
- ❌ Advanced function evaluation (complex expressions)

## Next Steps

1. **Implement Placeholder Plans**: Complete the 4 placeholder plans with actual functionality
2. **Add Missing Plans**: Create SubqueryPlan, ViewDefinitionPlan, AssignmentPlan
3. **Enhanced Expression Evaluation**: Complete integration with existing evaluators
4. **Performance Optimization**: Add cost-based optimization and memory management
5. **Comprehensive Testing**: Create test suites for all plan types and combinations

This infrastructure provides a solid foundation for achieving complete feature parity with QueryEvaluator while offering enhanced error handling and debugging capabilities.
