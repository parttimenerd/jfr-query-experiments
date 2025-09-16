# JFR Query Plan Architecture - Implementation Roadmap

## Executive Summary

This roadmap tracks the implementation of a streaming query plan architecture that provides an alternative to QueryEvaluator with streaming capabilities for memory efficiency. The architecture focuses on implementing missing core classes and establishing a working compilation baseline.

## Current Implementation Status

| Component | Status | Completion |
|-----------|--------|------------|
| Core Interfaces | **IMPLEMENTED** | 95% |
| Plan Implementations | **PARTIAL** | 60% |
| Missing Infrastructure | **IN PROGRESS** | 30% |
| Compilation Status | **FAILING** | 10% |
| Testing Framework | **NOT STARTED** | 0% |

organize in packages and test

## **COMPLETED PHASES**

  - **AdvancedQueryPlanOptimizer** with 4 optimization levels (NONE → EXPERIMENTAL)
  - **Memory-Aware Join Algorithm Selection** (hash/merge/nested loop based on data size)
  - **Streaming Aggregate Optimization** with incremental computation for large GROUP BY
  - **Enhanced Error Context** with AST position tracking and recovery suggestions
  - **Filter Reordering & Predicate Pushdown** for optimal query execution
  - **Advanced Cost Estimation** with detailed algorithm-specific calculations
  - **ASCII Art Visualization** with optimization steps and decision trees
  - **Comprehensive Reporting** with before/after optimization analysis
  - **Configurable Logging** with detailed diagnostic information

### **Phase 0: Foundation & Exception System** ✅ **COMPLETED**
- **Enhanced Exception Hierarchy**: 6 specialized exception classes with AST association
- **Package Organization**: Proper separation into nodes/, exception/, factory/, optimizer/
- **Plan Infrastructure**: Core streaming query plan architecture with proper interfaces

## 🔄 **ACTIVE PHASES**

### **Phase 1: Core Plan Implementation** (Priority: **URGENT**)
- **Status**: 🔄 **IN PROGRESS** - 25% Complete
- **Timeline**: **THIS WEEK** (Critical Path)
- **Remaining Tasks**:
  
  **🎯 OrderByPlan Implementation** (Est: 2-3 days)
  ```java
  // Implementation Strategy:
  - Comparator-based multi-field sorting with ASC/DESC support
  - Memory-efficient streaming sort using Java 8 Stream.sorted()
  - Integration with expression evaluation for complex sort expressions
  - Performance optimization for large datasets (> 10K rows)
  ```
  
  **🎯 LimitPlan Implementation** (Est: 1 day)
  ```java
  // Implementation Strategy:
  - Stream.skip() and Stream.limit() for efficient row limiting
  - Integration with OrderByPlan for top-N query optimization
  - Memory optimization for large OFFSET values using lazy evaluation
  ```
  
  **🎯 HavingPlan Implementation** (Est: 2 days)
  ```java
  // Implementation Strategy:
  - Post-aggregation filtering using existing ExpressionEvaluator
  - Proper handling of aggregate function references in HAVING conditions
  - Integration with GroupByPlan for seamless aggregate → filter pipeline
  ```
  
  **🎯 DistinctPlan Implementation** (Est: 1-2 days)
  ```java
  // Implementation Strategy:
  - Hash-based deduplication using ConcurrentHashMap for thread safety
  - Custom EventRow.hashCode() and equals() for proper comparison
  - Streaming deduplication to avoid memory overflow on large datasets
  ```

### **Phase 2: Advanced Query Features** (Priority: **HIGH**)
- **Status**: ❌ **NOT STARTED** - 0% Complete  
- **Timeline**: 2-3 weeks after Phase 1
- **Components**:
  - **SubqueryPlan**: Nested query execution with proper scope management
  - **ViewDefinitionPlan**: Materialized views with dependency tracking
  - **AssignmentPlan**: Variable storage and scope management
  - **Advanced Join Optimizations**: Complex join conditions and memory management

### **Phase 3: Expression Enhancement** (Priority: **MEDIUM**)
- **Status**: 🔄 **PARTIAL** - 75% Complete
- **Timeline**: 1-2 weeks
- **Remaining**: CASE/WHEN expressions, array operations, nested expression evaluation

### **Phase 5: Testing & Validation** (Priority: **HIGH**)
- **Status**: ❌ **NOT STARTED** - 0% Complete
- **Timeline**: 1-2 weeks (parallel with other phases)
- **Critical**: Performance benchmarks vs QueryEvaluator, comprehensive integration tests


## Critical Missing Classes

The following classes need to be implemented to achieve successful compilation:

### Core Result Classes
- **QueryResult.java** - Result wrapper for query execution (success/error states)
- **MemoryUsageStats.java** - Memory usage tracking for streaming operations  
- **JFRErrorContext.java** - Enhanced error context with AST and execution details

### Parser and AST Integration
- **Parser.java** - Main query parser (renamed from QueryParser)
- **QueryNodeVisitor.java** - Visitor interface for AST traversal
- **QueryNode.java** - Base AST node interface

### Plan Infrastructure
- **PlanVisualizer.java** - Query plan visualization and debugging
- **QueryExecutionContext.java** - Execution context with resource management
- **EventRow.java** - Row representation for streaming operations

## Current Architecture Status

### Implemented Components
- StreamingQueryPlan interface with execute() and explain() methods
- AbstractStreamingPlan base class with AST association
- Multiple plan implementations (FilterPlan, GroupByPlan, SelectPlan, JoinPlan)
- Exception hierarchy with enhanced error context
- Package structure with core and specialized plan types

### Package Organization
```
me.bechberger.jfr.extended.plan/
├── StreamingQueryPlan.java           ✓ IMPLEMENTED
├── AbstractStreamingPlan.java        ✓ IMPLEMENTED  
├── QueryPlanExecutor.java            ✓ IMPLEMENTED
├── QueryExecutionContext.java        ❌ MISSING
├── EventRow.java                     ❌ MISSING
├── QueryResult.java                  ❌ MISSING
├── core/                            ✓ REFERENCE IMPLEMENTATION
├── nodes/                           ✓ SPECIALIZED PLANS
├── exception/                       ✓ ERROR HANDLING
└── memory/                          ❌ MISSING COMPONENTS
```

## Immediate Implementation Priorities

### Phase 1: Missing Infrastructure Classes (Critical)

**Objective**: Achieve successful compilation by implementing missing classes

1. **QueryResult.java** - Success/error result wrapper with JfrTable payload
2. **MemoryUsageStats.java** - Memory tracking for streaming operations  
3. **JFRErrorContext.java** - Enhanced error context with AST positioning
4. **Parser.java** - Rename and fix QueryParser class compilation issues

### Phase 2: Plan Execution Context (Essential)

**Objective**: Establish working execution environment

1. **QueryExecutionContext.java** - Execution context with resource management
2. **EventRow.java** - Row representation for streaming data processing
3. **JfrTableConverter.java** - Conversion between tables and streaming formats

### Phase 3: Package Consolidation (Cleanup)

**Objective**: Resolve duplicate implementations and package conflicts

1. Remove duplicate files between `plan` and `plan.core` packages
2. Establish single source of truth for each class
3. Fix import statements and package references
4. Ensure consistent AST node type usage

## Current Compilation Blockers

### Missing Classes (Immediate Action Required)
- `QueryResult` - Referenced in QueryPlanExecutor.executeMultiStatement()
- `MemoryUsageStats` - Referenced in QueryPlanExecutor.getLastExecutionStats()
- `JFRErrorContext` - Referenced in QueryPlanExecutor error handling
- `QueryNodeVisitor` - Referenced in plan conversion and AST traversal
- `QueryNode` - Base interface for AST nodes

### Package Import Issues
- Multiple files reference `StreamingQueryPlan` from wrong package
- AST node type mismatches (BinaryOperatorNode vs BinaryExpressionNode)
- Inconsistent package structure between plan and plan.core

## Next Steps

1. **Implement QueryResult class** (highest priority - blocks compilation)
2. **Create missing infrastructure classes** (MemoryUsageStats, JFRErrorContext)
3. **Consolidate package structure** (remove duplicates, fix imports)
4. **Establish compilation baseline** (maven compile success)
5. **Begin systematic testing** (unit tests for core functionality)

## Current Focus

The immediate goal is to implement the missing classes that are preventing compilation, starting with `QueryResult.java` which is blocking the `QueryPlanExecutor` class. Once these critical infrastructure classes are in place, we can focus on package consolidation and establishing a working compilation baseline.

## Query Optimization Strategy

**📋 OPTIMIZATION APPROACH: NO ADVANCED OPTIMIZATION PLANNED**

The streaming query plan architecture will focus on **functional correctness** and **basic execution** rather than advanced optimization techniques. This decision is based on:

### Rationale for No Optimization
- **Primary Goal**: Establish working streaming architecture as QueryEvaluator alternative
- **Complexity Management**: Avoid premature optimization that could introduce bugs
- **Resource Focus**: Concentrate development effort on core functionality and reliability
- **Incremental Approach**: Optimization can be added later once baseline is stable

### Current Execution Strategy
- **Fallback Architecture**: Use existing QueryEvaluator for actual query execution
- **Streaming Interface**: Provide streaming API without internal streaming optimization
- **Simple Plan Selection**: Direct mapping from AST to plan without cost-based optimization
- **Linear Execution**: Execute plans in straightforward pipeline without reordering

### Future Optimization Considerations (Deferred)
- Cost-based query optimization
- Join reordering and algorithm selection
- Predicate pushdown and filter optimization
- Memory-aware execution strategies
- Parallel execution and streaming optimizations

**This approach ensures rapid development of a working baseline while maintaining architectural flexibility for future optimization work.**
