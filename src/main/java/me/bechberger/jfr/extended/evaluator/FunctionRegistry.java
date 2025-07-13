package me.bechberger.jfr.extended.evaluator;

import me.bechberger.jfr.extended.TokenType;
import me.bechberger.jfr.extended.table.CellValue;
import java.util.*;

/**
 * Central registry for all supported functions in the JFR query language.
 * Contains both function definitions and evaluation logic for all function types.
 * 
 * This registry has been moved to the evaluator package and uses static function
 * implementations from dedicated function classes.
 */
public class FunctionRegistry {
    
    private static final FunctionRegistry INSTANCE = new FunctionRegistry();
    
    private final Map<String, FunctionDefinition> functions;
    private final Map<TokenType, FunctionDefinition> tokenToFunction;
    
    private FunctionRegistry() {
        this.functions = new HashMap<>();
        this.tokenToFunction = new HashMap<>();
        initializeFunctions();
    }
    
    public static FunctionRegistry getInstance() {
        return INSTANCE;
    }
    
    // ===== FUNCTION EVALUATION METHODS =====
    
    /**
     * Evaluates a function with given arguments and context
     */
    public CellValue evaluateFunction(String functionName, List<CellValue> arguments, AggregateFunctions.EvaluationContext context) {
        FunctionDefinition function = functions.get(functionName.toUpperCase());
        if (function == null) {
            throw new IllegalArgumentException("Unknown function: " + functionName);
        }
        
        return switch (function) {
            case AggregateFunctionDefinition aggFunc -> aggFunc.apply(context, arguments.toArray(new CellValue[0]));
            case SimpleFunctionDefinition simpleFunc -> simpleFunc.apply(arguments.toArray(new CellValue[0]));
        };
    }
    
    // ===== HELPER METHODS FROM ABSTRACT FUNCTION EVALUATOR =====
    
    /**
     * Validates the number of arguments for a function
     */
    protected void validateArgumentCount(String functionName, List<CellValue> arguments, int expectedCount) {
        if (arguments.size() != expectedCount) {
            throw new IllegalArgumentException(
                functionName + " function requires exactly " + expectedCount + 
                " argument" + (expectedCount == 1 ? "" : "s") + 
                ", but got " + arguments.size()
            );
        }
    }
    
    /**
     * Validates the minimum number of arguments for a function
     */
    protected void validateMinArgumentCount(String functionName, List<CellValue> arguments, int minCount) {
        if (arguments.size() < minCount) {
            throw new IllegalArgumentException(
                functionName + " function requires at least " + minCount + 
                " argument" + (minCount == 1 ? "" : "s") + 
                ", but got " + arguments.size()
            );
        }
    }
    
    // ===== REGISTRATION AND UTILITY METHODS =====
    
    /**
     * Register a function definition
     */
    private void register(FunctionDefinition definition) {
        functions.put(definition.name(), definition);
        if (definition.token() != null) {
            tokenToFunction.put(definition.token(), definition);
        }
    }
    
    /**
     * Check if a function with the given name exists
     */
    public boolean isFunction(String name) {
        return functions.containsKey(name.toUpperCase());
    }
    
    /**
     * Check if a function with the given token exists
     */
    public boolean isFunction(TokenType token) {
        return tokenToFunction.containsKey(token);
    }
    
    /**
     * Get all function definitions
     */
    public java.util.Collection<FunctionDefinition> getAllFunctions() {
        return functions.values();
    }
  
    
    /**
     * Get functions by type
     */
    public List<FunctionDefinition> getFunctionsByType(FunctionType type) {
        return functions.values().stream()
                .filter(def -> def.type() == type)
                .collect(java.util.stream.Collectors.toList());
    }
    
    /**
     * Get function definition by name
     */
    public FunctionDefinition getFunction(String name) {
        return functions.get(name.toUpperCase());
    }
    
    /**
     * Get function definition by token
     */
    public FunctionDefinition getFunction(TokenType token) {
        return tokenToFunction.get(token);
    }
    
    /**
     * Get all supported tokens
     */
    public Set<TokenType> getSupportedTokens() {
        return Collections.unmodifiableSet(tokenToFunction.keySet());
    }
    
    /**
     * Get all function names
     */
    public Set<String> getFunctionNames() {
        return Collections.unmodifiableSet(functions.keySet());
    }
    
    /**
     * Generates a function signature string for documentation
     */
    private String generateSignature(String functionName, List<ParameterDefinition> parameters) {
        StringBuilder sig = new StringBuilder();
        sig.append(functionName).append("(");
        
        for (int i = 0; i < parameters.size(); i++) {
            ParameterDefinition param = parameters.get(i);
            if (param.optional()) {
                sig.append("[");
            }
            sig.append(param.name());
            if (param.variadic()) {
                sig.append("...");
            }
            if (param.optional()) {
                sig.append("]");
            }
            if (i < parameters.size() - 1) {
                sig.append(", ");
            }
        }
        sig.append(")");
        return sig.toString();
    }

    /**
     * Initialize all supported functions
     */
    private void initializeFunctions() {
        // ==============================================
        // AGGREGATE FUNCTIONS
        // ==============================================
        registerAggregateFunctions();
        
        // ==============================================
        // SIMPLE FUNCTIONS
        // ==============================================
        registerDataAccessFunctions();
        registerMathematicalFunctions();
        registerStringFunctions();
        registerDateTimeFunctions();
        registerConditionalFunctions();
            }
    
    /**
     * Register all aggregate functions
     */
    private void registerAggregateFunctions() {
        // Basic aggregate functions
        register(createAggregateFunction("AVG", null, FunctionType.AGGREGATE,
                "Calculate average of numeric values",
                List.of(new ParameterDefinition("field", ParameterType.FIELD, "Field to average")),
                ReturnType.SAME_AS_INPUT,
                "AVG(duration), AVG(allocatedBytes)",
                AggregateFunctions::evaluateAvg
        ));
        register(createAggregateFunction("SUM", null, FunctionType.AGGREGATE,
                "Calculate sum of numeric values",
                List.of(new ParameterDefinition("field", ParameterType.FIELD, "Field to sum")),
                ReturnType.SAME_AS_INPUT,
                "SUM(duration), SUM(allocatedBytes)",
                AggregateFunctions::evaluateSum
        ));
        
        // Mathematical aggregate functions (for fields)
        register(createAggregateFunction("MIN", null, FunctionType.AGGREGATE,
                "Find minimum value in a field across records",
                List.of(new ParameterDefinition("field", ParameterType.FIELD, "Field to find minimum")),
                ReturnType.SAME_AS_INPUT,
                "MIN(duration), MIN(allocatedBytes)",
                AggregateFunctions::evaluateMin
        ));
        register(createAggregateFunction("MAX", null, FunctionType.AGGREGATE,
                "Find maximum value in a field across records",
                List.of(new ParameterDefinition("field", ParameterType.FIELD, "Field to find maximum")),
                ReturnType.SAME_AS_INPUT,
                "MAX(duration), MAX(allocatedBytes)",
                AggregateFunctions::evaluateMax
        ));
        
        // Continue with other aggregate functions...
        register(createAggregateFunction("COUNT", null, FunctionType.AGGREGATE,
                "Count number of records or non-null values",
                List.of(ParameterDefinition.optional("field", ParameterType.FIELD, "Field to count (optional, * for all records)")),
                ReturnType.NUMBER,
                "COUNT(*), COUNT(duration)",
                AggregateFunctions::evaluateCount
        ));
        
        // Percentile functions
        register(createAggregateFunction("PERCENTILE", TokenType.PERCENTILE, FunctionType.AGGREGATE,
                "Calculate arbitrary percentile with custom expression",
                List.of(
                    new ParameterDefinition("percentile", ParameterType.PERCENTILE, "Percentile value (0-100)"),
                    new ParameterDefinition("field", ParameterType.FIELD, "Field to analyze")
                ),
                ReturnType.SAME_AS_INPUT,
                "PERCENTILE(90, duration), PERCENTILE(95, allocatedBytes)",
                AggregateFunctions::evaluatePercentile
        ));
        
        // Add specific percentile functions
        register(createAggregateFunction("P99", TokenType.P99, FunctionType.AGGREGATE,
                "Calculate 99th percentile",
                List.of(new ParameterDefinition("field", ParameterType.FIELD, "Field to analyze")),
                ReturnType.SAME_AS_INPUT,
                "P99(duration)",
                AggregateFunctions::evaluateP99
        ));
        
        register(createAggregateFunction("P95", TokenType.P95, FunctionType.AGGREGATE,
                "Calculate 95th percentile",
                List.of(new ParameterDefinition("field", ParameterType.FIELD, "Field to analyze")),
                ReturnType.SAME_AS_INPUT,
                "P95(duration)",
                AggregateFunctions::evaluateP95
        ));
        
        register(createAggregateFunction("P90", TokenType.P90, FunctionType.AGGREGATE,
                "Calculate 90th percentile",
                List.of(new ParameterDefinition("field", ParameterType.FIELD, "Field to analyze")),
                ReturnType.SAME_AS_INPUT,
                "P90(duration)",
                AggregateFunctions::evaluateP90
        ));
        
        register(createAggregateFunction("P50", null, FunctionType.AGGREGATE,
                "Calculate 50th percentile (median)",
                List.of(new ParameterDefinition("field", ParameterType.FIELD, "Field to analyze")),
                ReturnType.SAME_AS_INPUT,
                "P50(duration)",
                AggregateFunctions::evaluateP50
        ));
        
        register(createAggregateFunction("P999", TokenType.P999, FunctionType.AGGREGATE,
                "Calculate 99.9th percentile",
                List.of(new ParameterDefinition("field", ParameterType.FIELD, "Field to analyze")),
                ReturnType.SAME_AS_INPUT,
                "P999(duration)",
                AggregateFunctions::evaluateP999
        ));
        
        // Add statistical functions
        register(createAggregateFunction("STDDEV", null, FunctionType.AGGREGATE,
                "Calculate standard deviation",
                List.of(new ParameterDefinition("field", ParameterType.FIELD, "Field to analyze")),
                ReturnType.SAME_AS_INPUT,
                "STDDEV(duration)",
                AggregateFunctions::evaluateStddev
        ));
        
        register(createAggregateFunction("VARIANCE", null, FunctionType.AGGREGATE,
                "Calculate variance",
                List.of(new ParameterDefinition("field", ParameterType.FIELD, "Field to analyze")),
                ReturnType.SAME_AS_INPUT,
                "VARIANCE(duration)",
                AggregateFunctions::evaluateVariance
        ));
        
        // Add median function
        register(createAggregateFunction("MEDIAN", null, FunctionType.AGGREGATE,
                "Calculate median (50th percentile)",
                List.of(new ParameterDefinition("field", ParameterType.FIELD, "Field to analyze")),
                ReturnType.SAME_AS_INPUT,
                "MEDIAN(duration)",
                AggregateFunctions::evaluateP50
        ));
    }
    
    /**
     * Register all data access functions
     */
    private void registerDataAccessFunctions() {
        register(createAggregateFunction("FIRST", null, FunctionType.DATA_ACCESS,
                "Get first value from a field",
                List.of(new ParameterDefinition("field", ParameterType.FIELD, "Field to get first value from")),
                ReturnType.SAME_AS_INPUT,
                "FIRST(duration)",
                DataAccessFunctions::evaluateFirst
        ));
        
        register(createAggregateFunction("LAST", null, FunctionType.DATA_ACCESS,
                "Get last value from a field",
                List.of(new ParameterDefinition("field", ParameterType.FIELD, "Field to get last value from")),
                ReturnType.SAME_AS_INPUT,
                "LAST(duration)",
                DataAccessFunctions::evaluateLast
        ));
        
        register(createAggregateFunction("LAST_BATCH", null, FunctionType.DATA_ACCESS,
                "Get last N values from a field",
                List.of(
                    new ParameterDefinition("field", ParameterType.FIELD, "Field to get values from"),
                    new ParameterDefinition("count", ParameterType.NUMBER, "Number of values to get")
                ),
                ReturnType.ARRAY,
                "LAST_BATCH(duration, 5)",
                DataAccessFunctions::evaluateLastBatch
        ));
        
        register(createAggregateFunction("UNIQUE", null, FunctionType.DATA_ACCESS,
                "Get unique values from a field",
                List.of(new ParameterDefinition("field", ParameterType.FIELD, "Field to get unique values from")),
                ReturnType.ARRAY,
                "UNIQUE(eventType)",
                DataAccessFunctions::evaluateUnique
        ));
        
        register(createAggregateFunction("LIST", null, FunctionType.DATA_ACCESS,
                "Get all values from a field as a list",
                List.of(new ParameterDefinition("field", ParameterType.FIELD, "Field to get all values from")),
                ReturnType.ARRAY,
                "LIST(duration)",
                DataAccessFunctions::evaluateList
        ));
        
        register(createAggregateFunction("DIFF", null, FunctionType.DATA_ACCESS,
                "Calculate differences between consecutive values",
                List.of(new ParameterDefinition("field", ParameterType.FIELD, "Field to calculate differences for")),
                ReturnType.ARRAY,
                "DIFF(memoryUsed)",
                DataAccessFunctions::evaluateDiff
        ));
        
        register(createAggregateFunction("HEAD", null, FunctionType.DATA_ACCESS,
                "Get first N values from a field",
                List.of(
                    new ParameterDefinition("field", ParameterType.FIELD, "Field to get values from"),
                    new ParameterDefinition("count", ParameterType.NUMBER, "Number of values to get")
                ),
                ReturnType.ARRAY,
                "HEAD(duration, 10)",
                DataAccessFunctions::evaluateHead
        ));
        
        register(createAggregateFunction("TAIL", null, FunctionType.DATA_ACCESS,
                "Get last N values from a field",
                List.of(
                    new ParameterDefinition("field", ParameterType.FIELD, "Field to get values from"),
                    new ParameterDefinition("count", ParameterType.NUMBER, "Number of values to get")
                ),
                ReturnType.ARRAY,
                "TAIL(duration, 5)",
                DataAccessFunctions::evaluateTail
        ));
        
        register(createAggregateFunction("SLICE", null, FunctionType.DATA_ACCESS,
                "Get slice of values from a field",
                List.of(
                    new ParameterDefinition("field", ParameterType.FIELD, "Field to get values from"),
                    new ParameterDefinition("start", ParameterType.NUMBER, "Start index"),
                    new ParameterDefinition("end", ParameterType.NUMBER, "End index")
                ),
                ReturnType.ARRAY,
                "SLICE(duration, 2, 8)",
                DataAccessFunctions::evaluateSlice
        ));
        
        // GC-related functions
        register(createAggregateFunction("BEFORE_GC", null, FunctionType.DATA_ACCESS,
                "Get GC event ID that occurred before given timestamp",
                List.of(new ParameterDefinition("timestamp", ParameterType.TIMESTAMP, "Timestamp to search from")),
                ReturnType.ANY,
                "BEFORE_GC(startTime)",
                DataAccessFunctions::evaluateBeforeGc
        ));
        
        register(createAggregateFunction("AFTER_GC", null, FunctionType.DATA_ACCESS,
                "Get GC event ID that occurred after given timestamp",
                List.of(new ParameterDefinition("timestamp", ParameterType.TIMESTAMP, "Timestamp to search from")),
                ReturnType.ANY,
                "AFTER_GC(startTime)",
                DataAccessFunctions::evaluateAfterGc
        ));
        
        register(createAggregateFunction("CLOSEST_GC", null, FunctionType.DATA_ACCESS,
                "Get GC event ID closest to given timestamp",
                List.of(new ParameterDefinition("timestamp", ParameterType.TIMESTAMP, "Timestamp to search from")),
                ReturnType.ANY,
                "CLOSEST_GC(startTime)",
                DataAccessFunctions::evaluateClosestGc
        ));
        
        // Percentile selection functions
        register(createAggregateFunction("PERCENTILE_SELECT", TokenType.PERCENTILE_SELECT, FunctionType.DATA_ACCESS,
                "Select values at specific percentile",
                List.of(
                    new ParameterDefinition("percentile", ParameterType.NUMBER, "Percentile value (0-100)"),
                    new ParameterDefinition("table", ParameterType.STRING, "Table name"),
                    new ParameterDefinition("idField", ParameterType.STRING, "ID field name"),
                    new ParameterDefinition("valueField", ParameterType.FIELD, "Value field to analyze")
                ),
                ReturnType.ARRAY,
                "PERCENTILE_SELECT(90, Events, id, latency)",
                (context, args) -> PercentileSelectionFunctions.evaluatePercentileSelect(args, context)
        ));
        
        // Specific percentile selection functions
        register(createAggregateFunction("P90SELECT", TokenType.P90SELECT, FunctionType.DATA_ACCESS,
                "Select values at 90th percentile",
                List.of(
                    new ParameterDefinition("table", ParameterType.STRING, "Table name"),
                    new ParameterDefinition("idField", ParameterType.STRING, "ID field name"),
                    new ParameterDefinition("valueField", ParameterType.FIELD, "Value field to analyze")
                ),
                ReturnType.ARRAY,
                "P90SELECT(Events, id, latency)",
                (context, args) -> PercentileSelectionFunctions.evaluateP90Select(args, context)
        ));
        
        register(createAggregateFunction("P95SELECT", TokenType.P95SELECT, FunctionType.DATA_ACCESS,
                "Select values at 95th percentile",
                List.of(
                    new ParameterDefinition("table", ParameterType.STRING, "Table name"),
                    new ParameterDefinition("idField", ParameterType.STRING, "ID field name"),
                    new ParameterDefinition("valueField", ParameterType.FIELD, "Value field to analyze")
                ),
                ReturnType.ARRAY,
                "P95SELECT(Events, id, latency)",
                (context, args) -> PercentileSelectionFunctions.evaluateP95Select(args, context)
        ));
        
        register(createAggregateFunction("P99SELECT", TokenType.P99SELECT, FunctionType.DATA_ACCESS,
                "Select values at 99th percentile",
                List.of(
                    new ParameterDefinition("table", ParameterType.STRING, "Table name"),
                    new ParameterDefinition("idField", ParameterType.STRING, "ID field name"),
                    new ParameterDefinition("valueField", ParameterType.FIELD, "Value field to analyze")
                ),
                ReturnType.ARRAY,
                "P99SELECT(Events, id, latency)",
                (context, args) -> PercentileSelectionFunctions.evaluateP99Select(args, context)
        ));
        
        register(createAggregateFunction("P999SELECT", TokenType.P999SELECT, FunctionType.DATA_ACCESS,
                "Select values at 99.9th percentile",
                List.of(
                    new ParameterDefinition("table", ParameterType.STRING, "Table name"),
                    new ParameterDefinition("idField", ParameterType.STRING, "ID field name"),
                    new ParameterDefinition("valueField", ParameterType.FIELD, "Value field to analyze")
                ),
                ReturnType.ARRAY,
                "P999SELECT(Events, id, latency)",
                (context, args) -> PercentileSelectionFunctions.evaluateP999Select(args, context)
        ));
    }
    
    /**
     * Register all mathematical functions
     */
    private void registerMathematicalFunctions() {
        register(createSimpleFunction("ABS", null, FunctionType.MATHEMATICAL,
                "Calculate absolute value",
                List.of(new ParameterDefinition("value", ParameterType.NUMBER, "Value to calculate absolute for")),
                ReturnType.NUMBER,
                "ABS(-5)",
                MathematicalFunctions::evaluateAbs
        ));
        
        register(createSimpleFunction("CEIL", null, FunctionType.MATHEMATICAL,
                "Round up to nearest integer",
                List.of(new ParameterDefinition("value", ParameterType.NUMBER, "Value to round up")),
                ReturnType.NUMBER,
                "CEIL(3.2)",
                MathematicalFunctions::evaluateCeil
        ));
        
        register(createSimpleFunction("FLOOR", null, FunctionType.MATHEMATICAL,
                "Round down to nearest integer",
                List.of(new ParameterDefinition("value", ParameterType.NUMBER, "Value to round down")),
                ReturnType.NUMBER,
                "FLOOR(3.8)",
                MathematicalFunctions::evaluateFloor
        ));
        
        register(createSimpleFunction("ROUND", null, FunctionType.MATHEMATICAL,
                "Round to nearest integer",
                List.of(new ParameterDefinition("value", ParameterType.NUMBER, "Value to round")),
                ReturnType.NUMBER,
                "ROUND(3.5)",
                MathematicalFunctions::evaluateRound
        ));
        
        register(createSimpleFunction("SQRT", null, FunctionType.MATHEMATICAL,
                "Calculate square root",
                List.of(new ParameterDefinition("value", ParameterType.NUMBER, "Value to calculate square root for")),
                ReturnType.NUMBER,
                "SQRT(16)",
                MathematicalFunctions::evaluateSqrt
        ));
        
        register(createSimpleFunction("POW", null, FunctionType.MATHEMATICAL,
                "Calculate power",
                List.of(
                    new ParameterDefinition("base", ParameterType.NUMBER, "Base value"),
                    new ParameterDefinition("exponent", ParameterType.NUMBER, "Exponent value")
                ),
                ReturnType.NUMBER,
                "POW(2, 3)",
                MathematicalFunctions::evaluatePow
        ));
        
        register(createSimpleFunction("MOD", null, FunctionType.MATHEMATICAL,
                "Calculate modulo",
                List.of(
                    new ParameterDefinition("dividend", ParameterType.NUMBER, "Dividend value"),
                    new ParameterDefinition("divisor", ParameterType.NUMBER, "Divisor value")
                ),
                ReturnType.NUMBER,
                "MOD(10, 3)",
                MathematicalFunctions::evaluateMod
        ));
        
        register(createSimpleFunction("LOG", null, FunctionType.MATHEMATICAL,
                "Calculate natural logarithm",
                List.of(new ParameterDefinition("value", ParameterType.NUMBER, "Value to calculate logarithm for")),
                ReturnType.NUMBER,
                "LOG(10)",
                MathematicalFunctions::evaluateLog
        ));
        
        register(createSimpleFunction("LOG10", null, FunctionType.MATHEMATICAL,
                "Calculate base-10 logarithm",
                List.of(new ParameterDefinition("value", ParameterType.NUMBER, "Value to calculate logarithm for")),
                ReturnType.NUMBER,
                "LOG10(100)",
                MathematicalFunctions::evaluateLog10
        ));
        
        register(createSimpleFunction("EXP", null, FunctionType.MATHEMATICAL,
                "Calculate exponential (e^x)",
                List.of(new ParameterDefinition("value", ParameterType.NUMBER, "Exponent value")),
                ReturnType.NUMBER,
                "EXP(2)",
                MathematicalFunctions::evaluateExp
        ));
        
        register(createSimpleFunction("SIN", null, FunctionType.MATHEMATICAL,
                "Calculate sine",
                List.of(new ParameterDefinition("angle", ParameterType.NUMBER, "Angle in radians")),
                ReturnType.NUMBER,
                "SIN(1.57)",
                MathematicalFunctions::evaluateSin
        ));
        
        register(createSimpleFunction("COS", null, FunctionType.MATHEMATICAL,
                "Calculate cosine",
                List.of(new ParameterDefinition("angle", ParameterType.NUMBER, "Angle in radians")),
                ReturnType.NUMBER,
                "COS(0)",
                MathematicalFunctions::evaluateCos
        ));
        
        register(createSimpleFunction("TAN", null, FunctionType.MATHEMATICAL,
                "Calculate tangent",
                List.of(new ParameterDefinition("angle", ParameterType.NUMBER, "Angle in radians")),
                ReturnType.NUMBER,
                "TAN(0.785)",
                MathematicalFunctions::evaluateTan
        ));
        
        // Functions for multiple values
        register(createSimpleFunction("MIN_VALUES", null, FunctionType.MATHEMATICAL,
                "Find minimum value among multiple arguments",
                List.of(ParameterDefinition.variadic("values", ParameterType.NUMBER, "Values to compare")),
                ReturnType.SAME_AS_INPUT,
                "MIN_VALUES(1, 2, 3)",
                MathematicalFunctions::evaluateMinMultiple
        ));
        
        register(createSimpleFunction("MAX_VALUES", null, FunctionType.MATHEMATICAL,
                "Find maximum value among multiple arguments",
                List.of(ParameterDefinition.variadic("values", ParameterType.NUMBER, "Values to compare")),
                ReturnType.SAME_AS_INPUT,
                "MAX_VALUES(1, 2, 3)",
                MathematicalFunctions::evaluateMaxMultiple
        ));
    }
    
    /**
     * Register all string functions
     */
    private void registerStringFunctions() {
        register(createSimpleFunction("UPPER", null, FunctionType.STRING,
                "Convert to uppercase",
                List.of(new ParameterDefinition("string", ParameterType.STRING, "Input string")),
                ReturnType.STRING,
                "UPPER('foo')",
                StringFunctions::evaluateUpper
        ));
        
        register(createSimpleFunction("LOWER", null, FunctionType.STRING,
                "Convert to lowercase",
                List.of(new ParameterDefinition("string", ParameterType.STRING, "Input string")),
                ReturnType.STRING,
                "LOWER('FOO')",
                StringFunctions::evaluateLower
        ));
        
        register(createSimpleFunction("LENGTH", null, FunctionType.STRING,
                "Get length of string",
                List.of(new ParameterDefinition("string", ParameterType.STRING, "Input string")),
                ReturnType.NUMBER,
                "LENGTH('hello')",
                StringFunctions::evaluateLength
        ));
        
        register(createSimpleFunction("TRIM", null, FunctionType.STRING,
                "Remove whitespace from string",
                List.of(new ParameterDefinition("string", ParameterType.STRING, "Input string")),
                ReturnType.STRING,
                "TRIM('  hello  ')",
                StringFunctions::evaluateTrim
        ));
        
        register(createSimpleFunction("SUBSTRING", null, FunctionType.STRING,
                "Extract substring",
                List.of(
                    new ParameterDefinition("string", ParameterType.STRING, "Input string"),
                    new ParameterDefinition("start", ParameterType.NUMBER, "Start position"),
                    ParameterDefinition.optional("length", ParameterType.NUMBER, "Length (optional)")
                ),
                ReturnType.STRING,
                "SUBSTRING('hello', 1, 3)",
                StringFunctions::evaluateSubstring
        ));
        
        register(createSimpleFunction("CONCAT", null, FunctionType.STRING,
                "Concatenate strings",
                List.of(ParameterDefinition.variadic("strings", ParameterType.STRING, "Strings to concatenate")),
                ReturnType.STRING,
                "CONCAT('hello', ' ', 'world')",
                StringFunctions::evaluateConcat
        ));
        
        register(createSimpleFunction("REPLACE", null, FunctionType.STRING,
                "Replace substring",
                List.of(
                    new ParameterDefinition("string", ParameterType.STRING, "Input string"),
                    new ParameterDefinition("target", ParameterType.STRING, "String to replace"),
                    new ParameterDefinition("replacement", ParameterType.STRING, "Replacement string")
                ),
                ReturnType.STRING,
                "REPLACE('hello world', 'world', 'JFR')",
                StringFunctions::evaluateReplace
        ));
    }
    
    /**
     * Register all date/time functions
     */
    private void registerDateTimeFunctions() {
        register(createSimpleFunction("NOW", null, FunctionType.DATE_TIME,
                "Get current timestamp",
                List.of(),
                ReturnType.TIMESTAMP,
                "NOW()",
                DateTimeFunctions::evaluateNow
        ));
        
        register(createSimpleFunction("YEAR", null, FunctionType.DATE_TIME,
                "Extract year from timestamp",
                List.of(new ParameterDefinition("timestamp", ParameterType.TIMESTAMP, "Input timestamp")),
                ReturnType.NUMBER,
                "YEAR(startTime)",
                DateTimeFunctions::evaluateYear
        ));
        
        register(createSimpleFunction("MONTH", null, FunctionType.DATE_TIME,
                "Extract month from timestamp",
                List.of(new ParameterDefinition("timestamp", ParameterType.TIMESTAMP, "Input timestamp")),
                ReturnType.NUMBER,
                "MONTH(startTime)",
                DateTimeFunctions::evaluateMonth
        ));
        
        register(createSimpleFunction("DAY", null, FunctionType.DATE_TIME,
                "Extract day from timestamp",
                List.of(new ParameterDefinition("timestamp", ParameterType.TIMESTAMP, "Input timestamp")),
                ReturnType.NUMBER,
                "DAY(startTime)",
                DateTimeFunctions::evaluateDay
        ));
        
        register(createSimpleFunction("HOUR", null, FunctionType.DATE_TIME,
                "Extract hour from timestamp",
                List.of(new ParameterDefinition("timestamp", ParameterType.TIMESTAMP, "Input timestamp")),
                ReturnType.NUMBER,
                "HOUR(startTime)",
                DateTimeFunctions::evaluateHour
        ));
        
        register(createSimpleFunction("MINUTE", null, FunctionType.DATE_TIME,
                "Extract minute from timestamp",
                List.of(new ParameterDefinition("timestamp", ParameterType.TIMESTAMP, "Input timestamp")),
                ReturnType.NUMBER,
                "MINUTE(startTime)",
                DateTimeFunctions::evaluateMinute
        ));
        
        register(createSimpleFunction("SECOND", null, FunctionType.DATE_TIME,
                "Extract second from timestamp",
                List.of(new ParameterDefinition("timestamp", ParameterType.TIMESTAMP, "Input timestamp")),
                ReturnType.NUMBER,
                "SECOND(startTime)",
                DateTimeFunctions::evaluateSecond
        ));
        
        register(createSimpleFunction("DATE_DIFF", null, FunctionType.DATE_TIME,
                "Calculate difference between timestamps",
                List.of(
                    new ParameterDefinition("unit", ParameterType.STRING, "Time unit (SECONDS, MINUTES, HOURS, DAYS)"),
                    new ParameterDefinition("start", ParameterType.TIMESTAMP, "Start timestamp"),
                    new ParameterDefinition("end", ParameterType.TIMESTAMP, "End timestamp")
                ),
                ReturnType.NUMBER,
                "DATE_DIFF('HOURS', startTime, endTime)",
                DateTimeFunctions::evaluateDateDiff
        ));
        
        register(createSimpleFunction("DATE_ADD", null, FunctionType.DATE_TIME,
                "Add duration to timestamp",
                List.of(
                    new ParameterDefinition("timestamp", ParameterType.TIMESTAMP, "Input timestamp"),
                    new ParameterDefinition("amount", ParameterType.NUMBER, "Amount to add"),
                    new ParameterDefinition("unit", ParameterType.STRING, "Time unit (SECONDS, MINUTES, HOURS, DAYS)")
                ),
                ReturnType.TIMESTAMP,
                "DATE_ADD(startTime, 1, 'HOURS')",
                DateTimeFunctions::evaluateDateAdd
        ));
    }
    
    /**
     * Register all conditional functions
     */
    private void registerConditionalFunctions() {
        register(createSimpleFunction("IF", null, FunctionType.CONDITIONAL,
                "Conditional evaluation",
                List.of(
                    new ParameterDefinition("condition", ParameterType.BOOLEAN, "Condition to test"),
                    new ParameterDefinition("trueValue", ParameterType.ANY, "Value if true"),
                    new ParameterDefinition("falseValue", ParameterType.ANY, "Value if false")
                ),
                ReturnType.ANY,
                "IF(value > 10, 'high', 'low')",
                args -> ConditionalFunctions.evaluateIf(args)
        ));
        
        register(createSimpleFunction("CASE", null, FunctionType.CONDITIONAL,
                "Multi-way conditional evaluation",
                List.of(ParameterDefinition.variadic("conditions_values", ParameterType.ANY, "Alternating conditions and values, with default value as last argument")),
                ReturnType.ANY,
                "CASE(value > 10, 'high', value > 5, 'medium', 'low')",
                args -> ConditionalFunctions.evaluateCase(args)
        ));
        
        register(createSimpleFunction("COALESCE", null, FunctionType.CONDITIONAL,
                "Return first non-null value",
                List.of(ParameterDefinition.variadic("values", ParameterType.ANY, "Values to check for non-null")),
                ReturnType.ANY,
                "COALESCE(field1, field2, 'default')",
                args -> ConditionalFunctions.evaluateCoalesce(args)
        ));
        
        register(createSimpleFunction("NULLIF", null, FunctionType.CONDITIONAL,
                "Return null if values are equal",
                List.of(
                    new ParameterDefinition("value1", ParameterType.ANY, "First value to compare"),
                    new ParameterDefinition("value2", ParameterType.ANY, "Second value to compare")
                ),
                ReturnType.ANY,
                "NULLIF(value, 0)",
                args -> ConditionalFunctions.evaluateNullIf(args)
        ));
        
        register(createSimpleFunction("ISNULL", null, FunctionType.CONDITIONAL,
                "Check if value is null",
                List.of(new ParameterDefinition("value", ParameterType.ANY, "Value to check for null")),
                ReturnType.BOOLEAN,
                "ISNULL(field)",
                args -> ConditionalFunctions.evaluateIsNull(args)
        ));
        
        register(createSimpleFunction("ISNOTNULL", null, FunctionType.CONDITIONAL,
                "Check if value is not null",
                List.of(new ParameterDefinition("value", ParameterType.ANY, "Value to check for not null")),
                ReturnType.BOOLEAN,
                "ISNOTNULL(field)",
                args -> ConditionalFunctions.evaluateIsNotNull(args)
        ));
        
        register(createSimpleFunction("GREATEST", null, FunctionType.CONDITIONAL,
                "Return the greatest value among arguments",
                List.of(ParameterDefinition.variadic("values", ParameterType.ANY, "Values to compare")),
                ReturnType.SAME_AS_INPUT,
                "GREATEST(value1, value2, value3)",
                args -> ConditionalFunctions.evaluateGreatest(args)
        ));
        
        register(createSimpleFunction("LEAST", null, FunctionType.CONDITIONAL,
                "Return the smallest value among arguments",
                List.of(ParameterDefinition.variadic("values", ParameterType.ANY, "Values to compare")),
                ReturnType.SAME_AS_INPUT,
                "LEAST(value1, value2, value3)",
                args -> ConditionalFunctions.evaluateLeast(args)
        ));
    }
    
    // ===== FUNCTION DEFINITION INTERFACES =====
    
    /**
     * Sealed interface for all function definitions
     */
    public sealed interface FunctionDefinition permits AggregateFunctionDefinition, SimpleFunctionDefinition {
        String name();
        TokenType token();
        FunctionType type();
        String description();
        List<ParameterDefinition> parameters();
        ReturnType returnType();
        String signature();
        String examples();
    }

    /**
     * Definition for aggregate functions (e.g., AVG, COUNT)
     */
    public record AggregateFunctionDefinition(
            String name,
            TokenType token,
            FunctionType type,
            String description,
            List<ParameterDefinition> parameters,
            ReturnType returnType,
            String signature,
            String examples,
            AggregateFunctionImplementation implementation
    ) implements FunctionDefinition {
        public CellValue apply(AggregateFunctions.EvaluationContext context, CellValue... args) {
            return implementation.apply(context, List.of(args));
        }
    }

    /**
     * Definition for simple (non-aggregate) functions
     */
    public record SimpleFunctionDefinition(
            String name,
            TokenType token,
            FunctionType type,
            String description,
            List<ParameterDefinition> parameters,
            ReturnType returnType,
            String signature,
            String examples,
            FunctionImplementation implementation
    ) implements FunctionDefinition {
        public CellValue apply(CellValue... args) {
            return implementation.apply(List.of(args));
        }
    }

    /**
     * FunctionalInterface for aggregate function implementations
     */
    @FunctionalInterface
    public interface AggregateFunctionImplementation {
        CellValue apply(AggregateFunctions.EvaluationContext context, List<CellValue> args);
    }
    
    /**
     * Functional interface for function implementations
     */
    @FunctionalInterface
    public interface FunctionImplementation {
        CellValue apply(List<CellValue> args);
    }
    
    /**
     * Definition of a function parameter
     */
    public record ParameterDefinition(
            String name,
            ParameterType type,
            boolean optional,
            boolean variadic,
            String description
    ) {
        // Convenience constructor for required parameters
        public ParameterDefinition(String name, ParameterType type, String description) {
            this(name, type, false, false, description);
        }
        
        // Convenience constructor for optional parameters
        public static ParameterDefinition optional(String name, ParameterType type, String description) {
            return new ParameterDefinition(name, type, true, false, description);
        }
        
        // Convenience constructor for variadic parameters
        public static ParameterDefinition variadic(String name, ParameterType type, String description) {
            return new ParameterDefinition(name, type, false, true, description);
        }
    }
    
    /**
     * Types of function parameters
     */
    public enum ParameterType {
        FIELD,          // Field name or expression
        NUMBER,         // Numeric value
        STRING,         // String literal
        TIMESTAMP,      // Timestamp value
        ARRAY,          // Array of values
        EXPRESSION,     // General expression
        PERCENTILE,     // Percentile value (0-100)
        BOOLEAN,        // Boolean value
        ANY             // Any type
    }
    
    /**
     * Types of function return values
     */
    public enum ReturnType {
        NUMBER,         // Numeric value
        STRING,         // String value
        TIMESTAMP,      // Timestamp value
        BOOLEAN,        // Boolean value
        ARRAY,          // Array of values
        FIELD,          // Field reference
        ANY,            // Any type
        VOID,           // No return value
        SAME_AS_INPUT   // Same type as the input parameter
    }
    
    /**
     * Types of functions
     */
    public enum FunctionType {
        AGGREGATE,      // Functions that aggregate multiple values (AVG, COUNT, etc.)
        DATA_ACCESS,    // Functions that access/transform data (FIRST, LAST, etc.)
        MATHEMATICAL,   // Mathematical functions (ABS, SQRT, etc.)
        STRING,         // String manipulation functions
        DATE_TIME,      // Date/time functions
        CONDITIONAL     // Conditional functions (IF, CASE, etc.)
    }
    
    /**
     * Helper method to create a SimpleFunctionDefinition with automatic signature generation
     */
    private SimpleFunctionDefinition createSimpleFunction(String name, TokenType token, FunctionType type,
            String description, List<ParameterDefinition> parameters, ReturnType returnType,
            String examples, FunctionImplementation implementation) {
        return new SimpleFunctionDefinition(name, token, type, description, parameters, returnType,
                generateSignature(name, parameters), examples, implementation);
    }
    
    /**
     * Helper method to create an AggregateFunctionDefinition with automatic signature generation
     */
    @SuppressWarnings("unused")
    private AggregateFunctionDefinition createAggregateFunction(String name, TokenType token, FunctionType type,
            String description, List<ParameterDefinition> parameters, ReturnType returnType,
            String examples, AggregateFunctionImplementation implementation) {
        return new AggregateFunctionDefinition(name, token, type, description, parameters, returnType,
                generateSignature(name, parameters), examples, implementation);
    }
}
