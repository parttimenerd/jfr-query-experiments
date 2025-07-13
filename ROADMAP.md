# JFR Query Language Development Roadmap

## Recently Completed (December 2024)

### ✅ Parser Documentation Enhancement
- **Task**: Add comprehensive Javadoc documentation to all major parsing methods
- **Status**: COMPLETED
- **Details**: 
  - Added detailed Javadoc to 13 major parsing methods in Parser.java
  - Documented grammar rules, error handling strategies, and special cases
  - Included examples and edge case documentation
  - All code compiles successfully

### ✅ Test File Consolidation
- **Task**: Consolidate redundant test files to improve maintainability
- **Status**: COMPLETED  
- **Details**:
  - Consolidated 24 test files into 2 comprehensive test classes
  - Created `ParserErrorHandlingComprehensiveTest.java` (replaces 10 error handling test files)
  - Created `LanguageSyntaxFeaturesTest.java` (replaces 6 syntax test files)
  - Removed 3 additional redundant test files
  - Fixed all import issues (ParserException path corrections)
  - Maintained full test coverage while improving organization
  - Used JUnit5 parameterized tests for better maintainability

### ✅ Operator Confusion Detection (January 2025)
- **Task**: Improve error messages for common operator confusion (== vs =)
- **Status**: COMPLETED
- **Details**: 
  - Enhanced `createExpressionSuggestion()` method in `ParserErrorHandler.java`
  - Added specific detection for double equals (`==`) operator confusion
  - Provides clear guidance: "Use single '=' for comparison, not '==' (double equals is not supported in this query language)"
  - Created comprehensive test suite `OperatorConfusionTest.java` with JUnit5 parameterized tests
  - Covers multiple scenarios with different data types (strings, durations, timestamps)
  - Significant improvement in user experience for this common mistake

### ✅ ORDER BY Clause Implementation (January 2025)

- **Task**: Implement, document, and thoroughly test ORDER BY clause support
- **Status**: COMPLETED
- **Details**:
  - **Parser Support**: Confirmed robust ORDER BY parsing with ASC/DESC, arbitrary expressions, and multi-field sorting
  - **Engine Implementation**: Added comprehensive ORDER BY evaluation in QueryEvaluator.java supporting:
    - Complex expressions and field access with aliases
    - Multi-field sorting with proper precedence
    - Integration with GROUP BY, HAVING, LIMIT, and aggregate functions
    - Robust error handling for invalid syntax and runtime errors
  - **Comprehensive Testing**: Created 4 dedicated test classes with 50+ parameterized tests:
    - `OrderByEvaluationTest.java`: Basic field sorting and multi-field scenarios
    - `OrderByGroupByIntegrationTest.java`: ORDER BY with GROUP BY, aggregates, percentiles, and LIMIT
    - `OrderByErrorHandlingTest.java`: Comprehensive error handling for syntax and runtime errors
    - `OrderByExpressionTest.java`: Complex expression-based ORDER BY with aliases
  - **Test Framework Enhancement**: Registered mock tables ("MockUsers", "MockEmployees") for realistic test scenarios
  - **Grammar Documentation**: Updated Grammar.java with detailed ORDER BY syntax, features, and comprehensive examples
  - **Quality**: All tests follow JUnit5 parameterized test patterns with builder pattern AST construction

## Current Status


### 🔄 Parser Infrastructure
- **Core parsing**: Functional with comprehensive error handling
- **AST generation**: Complete with builder patterns
- **Error recovery**: Implemented with fine-grained recovery mechanisms
- **Function validation**: Unknown function detection with suggestions
- **Multi-statement support**: Handles complex query sequences

### 🔄 Language Features

- **Basic SQL-like syntax**: SELECT, FROM, WHERE, GROUP BY, ORDER BY, LIMIT
- **Extended JFR features**: COLUMN/FORMAT clauses, @ prefix queries
- **Array syntax**: Square bracket array literals and operations
- **Join operations**: Standard joins (INNER, LEFT, RIGHT, FULL) and fuzzy joins
- **Literal types**: String, number, boolean, duration, memory size, timestamp
- **Percentile functions**: P90, P95, P99, P999, PERCENTILE with selection variants
- **Assignment statements**: Variable assignments with :=
- **View definitions**: CREATE VIEW-like syntax
- **Show commands**: SHOW EVENTS, SHOW FIELDS

### 🔄 JFR Integration

- **Basic JFR file reading**: Parse JFR files and extract event data
- **View.ini configuration**: Support for predefined query configurations
- **Event type discovery**: Basic enumeration of available event types
- **Field metadata**: Access to event field names and types
- **Missing**: Real-time JFR analysis, multi-file correlation, advanced metadata handling

### 🔄 Language Server & IDE Support

- **Basic syntax parsing**: Core language parsing infrastructure in place
- **Error detection**: Comprehensive parser error handling with context-aware messages
- **Missing**: LSP implementation, auto-completion, syntax highlighting, IDE integration

### 🔄 Testing Infrastructure
- **Core tests**: ParserTest, LexerTest, ParserASTTest maintained
- **Error handling**: Comprehensive error scenario coverage
- **Syntax features**: Complete syntax validation test suite
- **Engine tests**: Mock table and query execution tests
- **Framework**: QueryTestFramework for integration testing

## Next Priorities

### 🎯 High Priority

1. **Language Server Implementation**
   - Basic language server protocol (LSP) support
   - Syntax highlighting for JFR query language
   - Auto-completion based on:
     - Available event types from loaded JFR files
     - Event field names and types from JFR metadata
     - Built-in functions and operators
     - View.ini predefined queries and configurations
   - Real-time error checking and validation
   - Hover documentation for events and fields

2. **Engine Integration Testing**
   - Validate that all parser features work with the query engine
   - Test complex queries end-to-end with real JFR data
   - Verify error propagation through the execution pipeline
   - Performance testing with large JFR files

3. **Performance Optimization**
   - Profile parser performance on large/complex queries
   - Optimize error recovery mechanisms
   - Benchmark against previous versions
   - JFR file streaming and memory optimization

### 🎯 Medium Priority

1. **Advanced JFR Integration Features**
   - Multi-file JFR analysis and correlation
   - JFR event streaming for real-time analysis
   - Integration with JFR configuration files (view.ini, .jfc files)
   - Custom event type definitions and metadata handling
   - JFR file format version compatibility

2. **Enhanced Language Server Features**
   - Code refactoring and quick fixes
   - Symbol navigation (go-to-definition for event types)
   - Query templates and snippets based on common JFR patterns
   - Integration with popular IDEs (VS Code, IntelliJ, Eclipse)
   - Debugging support for query execution

3. **Advanced Language Features**
   - Subquery support enhancement
   - Complex expression evaluation
   - Advanced function library expansion
   - Window functions for time-series analysis
   - User-defined functions and aggregates

4. **Developer Experience Enhancements**
   - Interactive query builder UI
   - Query result visualization and charting
   - Export capabilities (CSV, JSON, custom formats)
   - Query history and favorites management
   - Performance profiling and optimization hints

### 🎯 Future Enhancements

1. **Enterprise JFR Integration**
   - JFR repository and archival system integration
   - Distributed JFR analysis across multiple JVMs
   - Real-time JFR streaming from production systems
   - Integration with APM tools and monitoring systems
   - JFR data federation and cross-environment correlation

2. **Advanced Analytics and Visualization**
   - Built-in statistical analysis functions
   - Time-series analysis and forecasting
   - Anomaly detection algorithms
   - Interactive dashboard generation
   - Chart and graph visualization from query results

3. **Platform and Ecosystem Integration**
   - REST API for query execution
   - Web-based query interface
   - Integration with popular data tools (Grafana, Kibana, etc.)
   - Cloud platform integration (AWS, Azure, GCP)
   - Containerized deployment and scaling

## Implementation Notes

### Code Quality Standards
- ✅ JUnit5 for all new tests
- ✅ Builder pattern for AST node creation in tests
- ✅ Comprehensive class-level documentation
- ✅ Remove unused imports and debug classes
- ✅ Parameterized tests for repetitive test scenarios

### Documentation Requirements
- ✅ Update ROADMAP.md regularly with new tasks
- ✅ Add showcases for every Current Status item
- ✅ Keep grammar documentation current with new example queries
- ✅ Maintain comprehensive Javadoc for all public methods

### Testing Philosophy
- ✅ Write many comprehensive tests
- ✅ Test error conditions extensively
- ✅ Use builder patterns for readable test code
- ✅ Organize tests logically by functionality
- ✅ Maintain high test coverage

## Recent Achievements Showcase

### Parser Documentation
```java
/**
 * Parse SHOW EVENTS or SHOW FIELDS queries (metadata queries).
 * 
 * <p><strong>Grammar Rule:</strong></p>
 * <pre>
 * show_query ::= SHOW EVENTS
 *              | SHOW FIELDS event_type
 * event_type ::= IDENTIFIER ( DOT IDENTIFIER )*
 * </pre>
 * 
 * <p><strong>Error Handling Strategy:</strong></p>
 * <ul>
 *   <li><strong>Missing keyword:</strong> Uses consume() for clear error messages</li>
 *   <li><strong>Unknown show type:</strong> Throws ParserException with helpful message</li>
 *   <li><strong>Dotted event types:</strong> Properly handles qualified names like jdk.GarbageCollection</li>
 * </ul>
 */
```

### Test Consolidation Results
- **Before**: 24 scattered test files with duplicate functionality
- **After**: 2 comprehensive, well-documented test classes
- **Benefit**: 90% reduction in test file maintenance overhead

### Error Handling Improvements
```java
@ParameterizedTest(name = "{0}: {1}")
@MethodSource("errorTestCases")
@DisplayName("Parameterized error handling tests")
public void testParameterizedErrorHandling(String description, String query) {
    // Comprehensive error validation with context checking
}
```

### Operator Confusion Detection Results

**Before Enhancement:**
```
Query: @SELECT * FROM GarbageCollection WHERE duration == 5ms
Error: Invalid expression: Invalid expression in comparison: Unexpected token: EQUALS('=')
Suggestion: Review the expression syntax and ensure all parts are valid.
```

**After Enhancement:**
```
Query: @SELECT * FROM GarbageCollection WHERE duration == 5ms  
Error: Invalid expression: Invalid expression in comparison: Unexpected token: EQUALS('=')
Suggestion: Use single '=' for comparison, not '==' (double equals is not supported in this query language)
```

**Impact:** Clear, actionable guidance for one of the most common syntax mistakes from users with SQL/programming backgrounds

### ORDER BY Implementation Results

**Before Enhancement:**
- No ORDER BY support in the query engine
- Queries returned results in unpredictable order
- No way to sort by aggregate functions or complex expressions

**After Enhancement:**
```java
// Basic field sorting with direction control
@ SELECT * FROM ExecutionSample ORDER BY duration DESC

// Multi-field sorting with precedence
@ SELECT name, age FROM Users ORDER BY name ASC, age DESC

// Complex expression-based sorting
@ SELECT * FROM Users ORDER BY ABS(age - 30) ASC

// ORDER BY with GROUP BY and aggregates
@ SELECT threadId, COUNT(*) FROM ExecutionSample 
  GROUP BY threadId ORDER BY COUNT(*) DESC

// ORDER BY with percentiles and complex expressions
@ SELECT threadId, P99(duration) as p99 FROM ExecutionSample 
  GROUP BY threadId ORDER BY (P99(duration) * 2 + 1) DESC

// Integration with HAVING and LIMIT
@ SELECT stackTrace, COUNT(*) FROM ExecutionSample 
  GROUP BY stackTrace 
  HAVING COUNT(*) > 10 
  ORDER BY COUNT(*) DESC 
  LIMIT 5
```

**Impact:** 
- Full SQL-like ORDER BY support with ASC/DESC directions
- Complex expression evaluation in ORDER BY clauses
- Seamless integration with GROUP BY, aggregates, HAVING, and LIMIT
- Comprehensive error handling for invalid ORDER BY usage
- 50+ parameterized tests ensuring robust functionality

### Planned JFR Integration and Language Server Results

**Current State:**
```text
- Manual query writing without auto-completion
- No real-time validation of event types or field names
- Limited integration with actual JFR files
- Basic view.ini configuration support
```

**After JFR Integration Enhancement:**
```java
// Auto-completion based on loaded JFR file metadata
@SELECT threadName, stackTrace, duration 
FROM jdk.ExecutionSample  // <- Auto-completed from JFR metadata
WHERE duration > 10ms     // <- Field validation from event schema

// Integration with view.ini configurations
@SELECT * FROM environment.active-recordings  // <- Predefined view from view.ini

// Real-time event discovery
SHOW EVENTS              // <- Lists all available events from loaded JFR file
SHOW FIELDS jdk.GarbageCollection  // <- Shows actual field schema
```

**After Language Server Implementation:**
```typescript
// Syntax highlighting and error checking in IDE
@SELECT threadName, stackTrace, duration 
FROM jdk.ExecutionSample 
WHERE duraton > 10ms  // <- Real-time error: "Unknown field 'duraton', did you mean 'duration'?"

// Auto-completion features:
// - Event types from loaded JFR files
// - Field names with type information
// - Built-in functions (COUNT, AVG, P99, etc.)
// - Query templates for common patterns

// Hover documentation showing:
// - Event type descriptions from JFR metadata
// - Field type information and valid ranges
// - Function signatures and examples
```

**Expected Impact:**
- Seamless integration with real JFR files and event discovery
- Intelligent auto-completion based on actual event schemas
- Real-time validation and error checking in development environments  
- Reduced query development time through IDE integration
- Enhanced developer experience with rich language support