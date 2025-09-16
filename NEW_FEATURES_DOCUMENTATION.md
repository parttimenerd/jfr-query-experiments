# New Features Documentation

This document describes the newly implemented features in the JFR Query system: HAVING clauses, FUZZY JOINs, and variable assignments in WHERE clauses.

## HAVING Clauses

HAVING clauses allow filtering of grouped results based on aggregate conditions.

### Basic Syntax
```sql
@SELECT column1, AGG_FUNCTION(column2) 
FROM table 
GROUP BY column1 
HAVING condition
```

### Examples

#### Simple HAVING with COUNT
```sql
@SELECT cause, COUNT(*) as count
FROM GarbageCollection 
GROUP BY cause 
HAVING COUNT(*) > 1
```

#### Complex HAVING with Multiple Conditions
```sql
@SELECT cause, COUNT(*) as count, AVG(duration) as avg_duration
FROM GarbageCollection 
GROUP BY cause 
HAVING COUNT(*) > 1 AND AVG(duration) > 50
```

#### HAVING with Aggregate Aliases
```sql
@SELECT cause, COUNT(*) as count
FROM GarbageCollection 
GROUP BY cause 
HAVING count > 1
```

### Supported Aggregate Functions in HAVING
- COUNT(*), COUNT(column)
- SUM(column)
- AVG(column)
- MIN(column)
- MAX(column)
- COLLECT(column)

### HAVING vs WHERE
- **WHERE**: Filters rows before grouping
- **HAVING**: Filters groups after aggregation

```sql
-- WHERE filters individual rows
@SELECT cause, COUNT(*) 
FROM GarbageCollection 
WHERE duration > 50 
GROUP BY cause

-- HAVING filters groups based on aggregates
@SELECT cause, COUNT(*) 
FROM GarbageCollection 
GROUP BY cause 
HAVING COUNT(*) > 1
```

## FUZZY JOINs

FUZZY JOINs allow joining tables based on proximity in time or value rather than exact matches.

### Basic Syntax
```sql
@SELECT columns
FROM table1
FUZZY JOIN table2 ON condition
[TOLERANCE duration]
[DIRECTION NEAREST|PREVIOUS|AFTER]
```

### Join Types

#### NEAREST (Default)
Finds the closest match in time:
```sql
@SELECT gc.cause, exec.threadName
FROM GarbageCollection gc
FUZZY JOIN ExecutionSample exec ON gc.startTime = exec.startTime
TOLERANCE 1000ms
DIRECTION NEAREST
```

#### PREVIOUS
Finds the most recent match before the target time:
```sql
@SELECT gc.cause, exec.threadName
FROM GarbageCollection gc
FUZZY JOIN ExecutionSample exec ON gc.startTime = exec.startTime
TOLERANCE 5000ms
DIRECTION PREVIOUS
```

#### AFTER
Finds the earliest match after the target time:
```sql
@SELECT gc.cause, exec.threadName
FROM GarbageCollection gc
FUZZY JOIN ExecutionSample exec ON gc.startTime = exec.startTime
TOLERANCE 2000ms
DIRECTION AFTER
```

### Tolerance Specifications
- **Milliseconds**: `1000ms`, `500ms`
- **Seconds**: `5s`, `10s`
- **Minutes**: `1m`, `30m`
- **Hours**: `1h`, `2h`

### Complex FUZZY JOIN Examples

#### Multiple FUZZY JOINs
```sql
@SELECT gc.cause, exec.threadName, alloc.objectClass
FROM GarbageCollection gc
FUZZY JOIN ExecutionSample exec ON gc.startTime = exec.startTime
TOLERANCE 1000ms DIRECTION NEAREST
FUZZY JOIN Allocation alloc ON gc.startTime = alloc.startTime
TOLERANCE 500ms DIRECTION PREVIOUS
```

#### FUZZY JOIN with Additional Conditions
```sql
@SELECT gc.cause, exec.threadName
FROM GarbageCollection gc
FUZZY JOIN ExecutionSample exec ON gc.startTime = exec.startTime
TOLERANCE 1000ms
WHERE gc.duration > 50
AND exec.threadName = 'main'
```

## Variable Assignment in WHERE Clauses

Variables can be declared and used within WHERE clauses for complex filtering logic.

### Basic Syntax
```sql
@SELECT columns
FROM table
WHERE var expression := value AND condition
```

### Simple Variable Assignment
```sql
@SELECT cause, duration
FROM GarbageCollection
WHERE var threshold := 100 AND duration > threshold
```

### Complex Variable Assignment
```sql
@SELECT cause, duration
FROM GarbageCollection
WHERE var avg_duration := (@SELECT AVG(duration) FROM GarbageCollection) 
AND duration > avg_duration * 1.5
```

### Multiple Variable Assignments
```sql
@SELECT cause, duration, memoryUsed
FROM GarbageCollection
WHERE var min_duration := 50 
AND var max_memory := 1000000
AND duration > min_duration 
AND memoryUsed < max_memory
```

### Variable Assignment with Subqueries
```sql
@SELECT threadName, stackTrace
FROM ExecutionSample
WHERE var main_thread := 'main'
AND var recent_time := (@SELECT MAX(startTime) FROM ExecutionSample)
AND threadName = main_thread
AND startTime > recent_time - 5000ms
```

## Combining Features

### HAVING with FUZZY JOIN
```sql
@SELECT gc.cause, COUNT(*) as count, AVG(exec.duration) as avg_exec_duration
FROM GarbageCollection gc
FUZZY JOIN ExecutionSample exec ON gc.startTime = exec.startTime
TOLERANCE 1000ms
GROUP BY gc.cause
HAVING COUNT(*) > 1 AND AVG(exec.duration) > 50
```

### FUZZY JOIN with Variable Assignment
```sql
@SELECT gc.cause, exec.threadName
FROM GarbageCollection gc
FUZZY JOIN ExecutionSample exec ON gc.startTime = exec.startTime
TOLERANCE 1000ms
WHERE var target_thread := 'main'
AND exec.threadName = target_thread
AND gc.duration > 50
```

### All Features Combined
```sql
@SELECT gc.cause, COUNT(*) as count, AVG(exec.duration) as avg_exec_duration
FROM GarbageCollection gc
FUZZY JOIN ExecutionSample exec ON gc.startTime = exec.startTime
TOLERANCE 1000ms DIRECTION NEAREST
WHERE var min_gc_duration := 50
AND gc.duration > min_gc_duration
GROUP BY gc.cause
HAVING COUNT(*) > 1 AND AVG(exec.duration) > 25
ORDER BY count DESC
```

## Performance Considerations

### HAVING Clause Performance
- HAVING conditions are evaluated after grouping and aggregation
- Use WHERE conditions when possible to reduce the dataset before grouping
- Complex HAVING conditions may require multiple passes through grouped data

### FUZZY JOIN Performance
- FUZZY JOINs require temporal indexing for optimal performance
- Smaller tolerance values generally perform better
- NEAREST direction may be slower than PREVIOUS/AFTER for large datasets
- Consider using WHERE conditions to reduce dataset size before fuzzy joining

### Variable Assignment Performance
- Simple variable assignments have minimal overhead
- Subquery variable assignments are evaluated once per query
- Complex expressions in variable assignments may impact performance
- Variables are scoped to the WHERE clause where they're defined

## Error Handling

### HAVING Clause Errors
- Invalid aggregate functions in HAVING conditions
- References to non-grouped columns in HAVING
- Type mismatches in HAVING conditions

### FUZZY JOIN Errors
- Invalid tolerance specifications
- Unsupported join directions
- Type mismatches in join conditions
- Missing temporal columns for time-based joins

### Variable Assignment Errors
- Invalid variable names or syntax
- Type mismatches in variable assignments
- Circular variable dependencies
- Variables used outside their scope

## Testing

All new features are comprehensively tested in `NewFeaturesTest.java`:

- **HavingClauseTests**: Tests for various HAVING conditions and scenarios
- **FuzzyJoinTests**: Tests for all FUZZY JOIN types and configurations
- **VariableAssignmentTests**: Tests for variable declarations and usage
- **CombinedFeaturesTests**: Tests for feature combinations
- **ErrorHandlingTests**: Tests for error conditions and edge cases

Run the tests using:
```bash
mvn test -Dtest=NewFeaturesTest
```

## Migration Guide

### From WHERE to HAVING
If you're filtering on aggregated values, move the condition from WHERE to HAVING:

```sql
-- Before (incorrect)
@SELECT cause, COUNT(*) as count
FROM GarbageCollection 
WHERE COUNT(*) > 1  -- This won't work
GROUP BY cause

-- After (correct)
@SELECT cause, COUNT(*) as count
FROM GarbageCollection 
GROUP BY cause 
HAVING COUNT(*) > 1
```

### From Exact JOINs to FUZZY JOINs
For time-based correlations, consider using FUZZY JOINs:

```sql
-- Before (exact match, may miss correlations)
@SELECT gc.cause, exec.threadName
FROM GarbageCollection gc
JOIN ExecutionSample exec ON gc.startTime = exec.startTime

-- After (fuzzy match, captures nearby events)
@SELECT gc.cause, exec.threadName
FROM GarbageCollection gc
FUZZY JOIN ExecutionSample exec ON gc.startTime = exec.startTime
TOLERANCE 1000ms DIRECTION NEAREST
```

### Adding Variables for Complex Logic
Replace repeated complex expressions with variables:

```sql
-- Before (repeated complex expression)
@SELECT cause, duration
FROM GarbageCollection
WHERE duration > (@SELECT AVG(duration) FROM GarbageCollection) * 1.5
AND memoryUsed > (@SELECT AVG(duration) FROM GarbageCollection) * 1000

-- After (using variables)
@SELECT cause, duration
FROM GarbageCollection
WHERE var avg_duration := (@SELECT AVG(duration) FROM GarbageCollection)
AND duration > avg_duration * 1.5
AND memoryUsed > avg_duration * 1000
```
