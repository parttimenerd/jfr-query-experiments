# MockTableBuilder Fix Summary

## Problem
The MockTableBuilder's fluent API had parsing issues when working with:
- ARRAY column types 
- Timestamp parsing from ISO format strings (e.g., "2023-01-01T10:00:00Z")

## Root Cause
1. **Missing ARRAY case**: The `convertToCellValue` method in `TableBuilder.java` was missing a case for `CellType.ARRAY`
2. **Timestamp parsing**: The timestamp parsing logic only handled epoch milliseconds, not ISO format strings

## Fix Applied
Updated `/src/test/java/me/bechberger/jfr/extended/engine/framework/TableBuilder.java`:

1. **Added ARRAY support**:
   ```java
   case ARRAY -> {
       if (value instanceof List<?> list) {
           List<CellValue> cellValues = new ArrayList<>();
           for (Object element : list) {
               cellValues.add(CellValue.of(element));
           }
           yield new CellValue.ArrayValue(cellValues);
       } else if (value instanceof Object[] array) {
           List<CellValue> cellValues = new ArrayList<>();
           for (Object element : array) {
               cellValues.add(CellValue.of(element));
           }
           yield new CellValue.ArrayValue(cellValues);
       } else {
           yield new CellValue.ArrayValue(Collections.singletonList(CellValue.of(value)));
       }
   }
   ```

2. **Enhanced timestamp parsing**:
   ```java
   case TIMESTAMP -> {
       if (value instanceof java.time.Instant instant) {
           yield new CellValue.TimestampValue(instant);
       } else {
           String timestampStr = value.toString();
           try {
               // Try to parse as ISO date string first
               if (timestampStr.contains("T") || timestampStr.contains("Z")) {
                   yield new CellValue.TimestampValue(java.time.Instant.parse(timestampStr));
               } else {
                   // Try to parse as epoch milliseconds
                   yield new CellValue.TimestampValue(java.time.Instant.ofEpochMilli(Long.parseLong(timestampStr)));
               }
           } catch (Exception e) {
               // If both fail, try to parse as number
               yield new CellValue.TimestampValue(java.time.Instant.ofEpochMilli(value instanceof Number n ? n.longValue() : Long.parseLong(timestampStr)));
           }
       }
   }
   ```

## Verification
Created `MockTableBuilderFixTest.java` to verify:
- ✅ Timestamp parsing with ISO format strings
- ✅ Array column support
- ✅ All column types working correctly
- ✅ Both fluent API and preferred `createTable` method work
- ✅ Basic query execution with `executeAndExpectTable`

## Status
The MockTableBuilder fix is complete and working. The fluent API now properly supports:
- All column types including arrays and timestamps
- ISO format timestamp parsing
- Proper integration with the QueryTestFramework

The comprehensive test suite revealed that some advanced features like `GROUP BY` and `HAVING` clauses may have implementation issues in the current query engine, but the MockTableBuilder itself is now fully functional.
