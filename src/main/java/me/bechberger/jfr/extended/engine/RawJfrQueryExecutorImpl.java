package me.bechberger.jfr.extended.engine;

import jdk.jfr.EventType;
import jdk.jfr.consumer.RecordedEvent;
import jdk.jfr.consumer.RecordingFile;
import jdk.jfr.consumer.EventStream;
import me.bechberger.jfr.extended.ast.ASTNodes.RawJfrQueryNode;
import me.bechberger.jfr.extended.table.JfrTable;
import me.bechberger.jfr.extended.table.StandardJfrTable;
import me.bechberger.jfr.extended.table.CellValue;
import me.bechberger.jfr.extended.table.CellType;
import me.bechberger.jfr.extended.table.SingleCellTable;
import me.bechberger.jfr.extended.engine.exception.QueryExecutorException;
import me.bechberger.jfr.extended.engine.exception.SourceTableException;
import me.bechberger.jfr.query.Configuration;
import me.bechberger.jfr.query.QueryPrinter;
import me.bechberger.jfr.util.Output;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Consumer;

/**
 * Implementation of RawJfrQueryExecutor that executes raw JFR queries using a cached EventStream.
 * This class uses RecordingFile to load and cache all events, then provides a custom EventStream
 * implementation for efficient re-execution of queries without re-parsing the JFR file.
 */
public class RawJfrQueryExecutorImpl implements RawJfrQueryExecutor {
    
    private final Path jfrFilePath;
    private final AsyncLineProcessor lineProcessor;
    private CachedEventStream cachedEventStream;
    private List<EventType> eventTypes;
    
    /**
     * Constructor with JFR file path
     */
    public RawJfrQueryExecutorImpl(Path jfrFilePath) {
        this.jfrFilePath = jfrFilePath;
        this.lineProcessor = new AsyncLineProcessor();
        this.cachedEventStream = null;
        this.eventTypes = new ArrayList<>();
    }
    
    /**
     * Get available event types from the JFR file
     */
    public List<jdk.jfr.EventType> getEventTypes() throws IOException {
        ensureCachedEventStream();
        return new ArrayList<>(eventTypes);
    }
    
    @Override
    public JfrTable execute(RawJfrQueryNode queryNode) throws Exception {
        String query = queryNode.rawQuery();
        
        try {
            // Ensure we have cached the events
            ensureCachedEventStream();
            
            // Execute query using AsyncBuffer for streaming results
            AsyncBuffer buffer = new AsyncBuffer();
            CompletableFuture<JfrTable> resultFuture = new CompletableFuture<>();
            
            // Start async parsing of results
            buffer.setLineConsumer(line -> {
                try {
                    lineProcessor.processLine(line);
                } catch (Exception e) {
                    resultFuture.completeExceptionally(
                        SourceTableException.initializationFailure(
                            "JFR query results", 
                            "Error processing query result line: " + e.getMessage(), 
                            queryNode
                        )
                    );
                }
            });
            
            // When parsing is complete, build the final table
            lineProcessor.setCompletionHandler(() -> {
                try {
                    JfrTable result = lineProcessor.buildTable();
                    resultFuture.complete(result);
                } catch (Exception e) {
                    resultFuture.completeExceptionally(
                        SourceTableException.initializationFailure(
                            "JFR query results", 
                            "Error building result table: " + e.getMessage(), 
                            queryNode
                        )
                    );
                }
            });
            
            // Execute the JFR query using our cached event stream
            executeJfrQuery(query, buffer);
            
            // Wait for parsing to complete and return result
            return resultFuture.get();
        } catch (IOException e) {
            throw SourceTableException.dataSourceFailure(
                "JFR file: " + jfrFilePath, 
                "Cannot access JFR file: " + e.getMessage(), 
                queryNode
            );
        } catch (Exception e) {
            if (e instanceof SourceTableException) {
                throw e; // Re-throw our specific exceptions
            }
            throw QueryExecutorException.forExecutionFailure(
                "RawJfrQueryExecutor", 
                "Failed to execute JFR query: " + e.getMessage(), 
                queryNode, e
            );
        }
    }
    
    /**
     * Ensure the cached event stream is initialized
     */
    private void ensureCachedEventStream() throws IOException {
        if (cachedEventStream == null) {
            List<RecordedEvent> allEvents = new ArrayList<>();
            Set<EventType> eventTypeSet = new HashSet<>();
            
            // Load all events from the JFR file using RecordingFile
            try (RecordingFile recordingFile = new RecordingFile(jfrFilePath)) {
                while (recordingFile.hasMoreEvents()) {
                    RecordedEvent event = recordingFile.readEvent();
                    allEvents.add(event);
                    eventTypeSet.add(event.getEventType());
                }
            } catch (IOException e) {
                throw SourceTableException.dataSourceFailure(
                    jfrFilePath.toString(), 
                    "Cannot read JFR file: " + e.getMessage(), 
                    null
                );
            } catch (Exception e) {
                throw SourceTableException.initializationFailure(
                    jfrFilePath.toString(), 
                    "Failed to process JFR events: " + e.getMessage(), 
                    null
                );
            }
            
            // Store event types for later use
            this.eventTypes = new ArrayList<>(eventTypeSet);
            
            // Create cached event stream
            this.cachedEventStream = new CachedEventStream(allEvents, eventTypes);
        }
    }
    
    /**
     * Execute JFR query using the cached EventStream
     */
    private void executeJfrQuery(String query, AsyncBuffer buffer) throws Exception {
        Output.BufferedPrinter printer = new Output.BufferedPrinter(new PrintStream(buffer.getOutputStream()));
        
        // Initialize configuration
        Configuration configuration = new Configuration();
        configuration.width = 200; // Default width
        configuration.output = printer;
        
        // Use our cached event stream instead of opening the file again
        try {
            QueryPrinter queryPrinter = new QueryPrinter(configuration, cachedEventStream);
            queryPrinter.execute(query);
            printer.flush();
            buffer.close(); // Signal completion
        } catch (Exception e) {
            buffer.close();
            throw QueryExecutorException.forExecutionFailure(
                "QueryPrinter", 
                "Failed to execute raw JFR query: " + e.getMessage(), 
                null, e
            );
        }
    }
    
    /**
     * Custom EventStream implementation that provides cached events
     */
    private static class CachedEventStream implements EventStream {
        private final List<RecordedEvent> events;
        private final Map<String, Consumer<RecordedEvent>> eventActions = new HashMap<>();
        private Consumer<Throwable> errorHandler;
        private Runnable closeHandler;
        private boolean started = false;
        private boolean closed = false;
        
        public CachedEventStream(List<RecordedEvent> events, List<EventType> eventTypes) {
            this.events = new ArrayList<>(events);
            // We don't need to store eventTypes since we don't use them
        }
        
        @Override
        public void onEvent(Consumer<RecordedEvent> action) {
            if (started) {
                throw QueryExecutorException.forConfigurationError(
                    "EventStream", 
                    "Cannot register event handler after stream has started", 
                    null, null
                );
            }
            // Register action for all events
            eventActions.put("*", action);
        }
        
        @Override
        public void onEvent(String eventName, Consumer<RecordedEvent> action) {
            if (started) {
                throw QueryExecutorException.forConfigurationError(
                    "EventStream", 
                    "Cannot register event handler after stream has started", 
                    null, null
                );
            }
            eventActions.put(eventName, action);
        }
        
        @Override
        public void onError(Consumer<Throwable> errorHandler) {
            this.errorHandler = errorHandler;
        }
        
        @Override
        public void onClose(Runnable closeHandler) {
            this.closeHandler = closeHandler;
        }
        
        @Override
        public void start() {
            if (started) {
                return;
            }
            if (closed) {
                throw QueryExecutorException.forConfigurationError(
                    "EventStream", 
                    "Cannot start event stream that has been closed", 
                    null, null
                );
            }
            
            started = true;
            
            try {
                // Process all cached events
                for (RecordedEvent event : events) {
                    if (closed) {
                        break;
                    }
                    
                    // Check for specific event handler
                    Consumer<RecordedEvent> specificHandler = eventActions.get(event.getEventType().getName());
                    if (specificHandler != null) {
                        specificHandler.accept(event);
                    }
                    
                    // Check for generic event handler
                    Consumer<RecordedEvent> genericHandler = eventActions.get("*");
                    if (genericHandler != null) {
                        genericHandler.accept(event);
                    }
                }
                
                // Call close handler when done
                if (closeHandler != null && !closed) {
                    closeHandler.run();
                }
            } catch (Exception e) {
                if (errorHandler != null) {
                    errorHandler.accept(e);
                } else {
                    throw QueryExecutorException.forExecutionFailure(
                        "EventStream", 
                        "Error processing cached events: " + e.getMessage(), 
                        null, e
                    );
                }
            }
        }
        
        @Override
        public void startAsync() {
            CompletableFuture.runAsync(this::start);
        }
        
        @Override
        public void awaitTermination() throws InterruptedException {
            // Since we process events synchronously, this is a no-op
        }
        
        @Override
        public void awaitTermination(java.time.Duration timeout) throws InterruptedException {
            // Since we process events synchronously, this is a no-op
        }
        
        @Override
        public void setStartTime(java.time.Instant startTime) {
            // For simplicity, we don't filter by time in the cached implementation
        }
        
        @Override
        public void setEndTime(java.time.Instant endTime) {
            // For simplicity, we don't filter by time in the cached implementation
        }
        
        @Override
        public void setOrdered(boolean ordered) {
            // Events are already ordered in our cache
        }
        
        @Override
        public void setReuse(boolean reuse) {
            // Not applicable for cached implementation
        }
        
        @Override
        public boolean remove(Object action) {
            // Simple implementation - remove from actions map
            if (action instanceof Consumer) {
                return eventActions.values().remove(action);
            }
            return false;
        }
        
        @Override
        public void onFlush(Runnable flushHandler) {
            // Not applicable for cached implementation - events are already loaded
        }
        
        @Override
        public void close() {
            closed = true;
            if (closeHandler != null) {
                closeHandler.run();
            }
        }
    }
    
    /**
     * Custom buffer implementation that processes lines asynchronously
     */
    private static class AsyncBuffer extends ByteArrayOutputStream {
        private Consumer<String> lineConsumer;
        private final StringBuilder currentLine = new StringBuilder();
        
        public void setLineConsumer(Consumer<String> lineConsumer) {
            this.lineConsumer = lineConsumer;
        }
        
        @Override
        public synchronized void write(int b) {
            super.write(b);
            
            char ch = (char) b;
            if (ch == '\n') {
                // Complete line found
                String line = currentLine.toString();
                currentLine.setLength(0); // Clear for next line
                
                if (lineConsumer != null && !line.trim().isEmpty()) {
                    // Process line asynchronously
                    CompletableFuture.runAsync(() -> lineConsumer.accept(line));
                }
            } else if (ch != '\r') { // Ignore carriage returns
                currentLine.append(ch);
            }
        }
        
        @Override
        public synchronized void write(byte[] b, int off, int len) {
            for (int i = off; i < off + len; i++) {
                write(b[i]);
            }
        }
        
        public void close() {
            // Process any remaining content
            if (currentLine.length() > 0 && lineConsumer != null) {
                String line = currentLine.toString();
                if (!line.trim().isEmpty()) {
                    lineConsumer.accept(line);
                }
            }
            // Buffer is now closed
        }
        
        public PrintStream getOutputStream() {
            return new PrintStream(this);
        }
    }
    
    /**
     * Processes lines asynchronously and builds the final JfrTable
     */
    private static class AsyncLineProcessor {
        private final ConcurrentLinkedQueue<String> lines = new ConcurrentLinkedQueue<>();
        private Runnable completionHandler;
        private volatile boolean headerProcessed = false;
        private List<String> headers = new ArrayList<>();
        
        public void processLine(String line) {
            lines.offer(line);
            
            // Process immediately if it's a header line
            if (!headerProcessed && isHeaderLine(line)) {
                processHeaderLine(line);
            }
        }
        
        public void setCompletionHandler(Runnable completionHandler) {
            this.completionHandler = completionHandler;
            // If we're already done processing, call immediately
            checkCompletion();
        }
        
        private void checkCompletion() {
            // Simple completion check - in a real implementation,
            // this would be more sophisticated
            if (completionHandler != null) {
                CompletableFuture.runAsync(completionHandler);
            }
        }
        
        private boolean isHeaderLine(String line) {
            // Detect header by looking for typical column separators
            return line.contains("|") || line.matches(".*\\s+.*\\s+.*");
        }
        
        private void processHeaderLine(String line) {
            if (headerProcessed) return;
            
            // Extract headers - simplified implementation
            // In a full implementation, this would use the same logic as main.js
            if (line.contains("|")) {
                String[] parts = line.split("\\|");
                for (String part : parts) {
                    String header = part.trim();
                    if (!header.isEmpty()) {
                        headers.add(header);
                    }
                }
            } else {
                // Space-separated headers
                String[] parts = line.split("\\s+");
                for (String part : parts) {
                    if (!part.trim().isEmpty()) {
                        headers.add(part.trim());
                    }
                }
            }
            
            headerProcessed = true;
        }
        
        public JfrTable buildTable() {
            List<String> allLines = new ArrayList<>(lines);
            
            if (allLines.isEmpty()) {
                // Return empty table using optimized SingleCellTable
                return SingleCellTable.of("result", "");
            }
            
            // Parse table structure like main.js does
            return parseTabularData(String.join("\n", allLines));
        }
        
        /**
         * Parse tabular data similar to main.js parseTabularData function
         */
        private JfrTable parseTabularData(String data) {
            String[] lines = data.split("\n");
            String headerLine = "";
            String separatorLine = "";
            List<String> dataLines = new ArrayList<>();
            int separatorIndex = -1;
            
            // Find the separator line (like main.js logic)
            for (int i = 0; i < lines.length; i++) {
                String line = lines[i].trim();
                if (line.matches("^-+(\s+-+)+$")) {
                    separatorIndex = i;
                    separatorLine = line;
                    if (i > 0) {
                        headerLine = lines[i - 1];
                    }
                    for (int j = i + 1; j < lines.length; j++) {
                        if (!lines[j].trim().isEmpty()) {
                            dataLines.add(lines[j]);
                        }
                    }
                    break;
                }
            }
            
            if (separatorIndex == -1 || headerLine.isEmpty()) {
                // No valid table format, create simple single-column result
                List<JfrTable.Column> columns = List.of(new JfrTable.Column("output", CellType.STRING));
                JfrTable result = new StandardJfrTable(columns);
                for (String line : lines) {
                    if (!line.trim().isEmpty()) {
                        result.addRow(new JfrTable.Row(List.of(new CellValue.StringValue(line.trim()))));
                    }
                }
                return result;
            }
            
            // Parse column positions (like main.js)
            List<ColumnPosition> columnPositions = parseColumnPositions(separatorLine);
            
            // Parse headers
            List<String> headers = new ArrayList<>();
            for (ColumnPosition col : columnPositions) {
                String header = extractColumn(headerLine, col).trim();
                headers.add(header.isEmpty() ? "column" + headers.size() : header);
            }
            
            // Analyze data to determine column types
            List<CellType> columnTypes = new ArrayList<>();
            for (int colIndex = 0; colIndex < headers.size(); colIndex++) {
                columnTypes.add(detectColumnType(dataLines, columnPositions.get(colIndex)));
            }
            
            // Create JfrTable columns with detected types
            List<JfrTable.Column> columns = new ArrayList<>();
            for (int i = 0; i < headers.size(); i++) {
                columns.add(new JfrTable.Column(headers.get(i), columnTypes.get(i)));
            }
            
            JfrTable result = new StandardJfrTable(columns);
            
            // Parse data rows with proper type conversion
            for (String line : dataLines) {
                List<CellValue> cells = new ArrayList<>();
                for (int colIndex = 0; colIndex < columnPositions.size(); colIndex++) {
                    ColumnPosition col = columnPositions.get(colIndex);
                    String cellValue = extractColumn(line, col).trim();
                    CellType expectedType = columnTypes.get(colIndex);
                    cells.add(parseValueWithType(cellValue, expectedType));
                }
                result.addRow(new JfrTable.Row(cells));
            }
            
            return result;
        }
        
        private List<ColumnPosition> parseColumnPositions(String separatorLine) {
            List<ColumnPosition> positions = new ArrayList<>();
            int currentStart = -1;
            boolean inColumn = false;
            
            for (int i = 0; i < separatorLine.length(); i++) {
                char ch = separatorLine.charAt(i);
                if (ch == '-') {
                    if (!inColumn) {
                        inColumn = true;
                        currentStart = i;
                    }
                } else if (ch == ' ') {
                    if (inColumn) {
                        positions.add(new ColumnPosition(currentStart, i - 1));
                        inColumn = false;
                    }
                }
            }
            
            if (inColumn) {
                positions.add(new ColumnPosition(currentStart, separatorLine.length() - 1));
            }
            
            return positions;
        }
        
        private String extractColumn(String line, ColumnPosition col) {
            int start = Math.max(0, col.start);
            int end = Math.min(line.length(), col.end + 1);
            if (start >= line.length()) {
                return "";
            }
            return line.substring(start, end);
        }
        
        private static class ColumnPosition {
            final int start;
            final int end;
            
            ColumnPosition(int start, int end) {
                this.start = start;
                this.end = end;
            }
        }
        
        /**
         * Detect column type by analyzing sample values (similar to main.js logic)
         */
        private CellType detectColumnType(List<String> dataLines, ColumnPosition col) {
            int sampleSize = Math.min(10, dataLines.size());
            int numericCount = 0;
            int durationCount = 0;
            int memoryCount = 0;
            int timestampCount = 0;
            int booleanCount = 0;
            
            for (int i = 0; i < sampleSize; i++) {
                String line = dataLines.get(i);
                String value = extractColumn(line, col).trim();
                
                if (value.isEmpty() || value.equals("-")) {
                    continue;
                }
                
                // Check for boolean values
                if (value.equalsIgnoreCase("true") || value.equalsIgnoreCase("false")) {
                    booleanCount++;
                    continue;
                }
                
                // Check for timestamp (ISO format or epoch-like numbers)
                if (isTimestamp(value)) {
                    timestampCount++;
                    continue;
                }
                
                // Check for duration (ns, μs, ms, s, m, h, d)
                if (isDuration(value)) {
                    durationCount++;
                    continue;
                }
                
                // Check for memory size (B, KB, MB, GB, etc.)
                if (isMemorySize(value)) {
                    memoryCount++;
                    continue;
                }
                
                // Check for numeric value
                if (isNumeric(value)) {
                    numericCount++;
                }
            }
            
            // Determine type based on majority
            int totalSamples = sampleSize;
            if (booleanCount > totalSamples / 2) return CellType.BOOLEAN;
            if (timestampCount > totalSamples / 2) return CellType.TIMESTAMP;
            if (durationCount > totalSamples / 2) return CellType.DURATION;
            if (memoryCount > totalSamples / 2) return CellType.MEMORY_SIZE;
            if (numericCount > totalSamples / 2) return CellType.NUMBER;
            
            // Default to string
            return CellType.STRING;
        }
        
        /**
         * Parse value with proper type conversion (similar to main.js parseValueWithUnit)
         */
        private CellValue parseValueWithType(String value, CellType expectedType) {
            if (value == null || value.trim().isEmpty() || value.equals("-")) {
                return new CellValue.NullValue();
            }
            
            value = value.trim();
            
            try {
                switch (expectedType) {
                    case BOOLEAN:
                        return new CellValue.BooleanValue(Boolean.parseBoolean(value));
                        
                    case NUMBER:
                        // Handle comma-separated numbers and pure numbers
                        String cleanNumber = value.replace(",", "");
                        if (cleanNumber.matches("^-?\\d+$")) {
                            return new CellValue.NumberValue(Long.parseLong(cleanNumber));
                        } else if (cleanNumber.matches("^-?\\d*\\.\\d+$")) {
                            return new CellValue.FloatValue(Double.parseDouble(cleanNumber));
                        }
                        break;
                        
                    case FLOAT:
                        String cleanFloat = value.replace(",", "");
                        return new CellValue.FloatValue(Double.parseDouble(cleanFloat));
                        
                    case DURATION:
                        return parseDurationValue(value);
                        
                    case MEMORY_SIZE:
                        return parseMemorySizeValue(value);
                        
                    case TIMESTAMP:
                        return parseTimestampValue(value);
                        
                    case STRING:
                    default:
                        return new CellValue.StringValue(value);
                }
            } catch (Exception e) {
                // If parsing fails, return as string
                return new CellValue.StringValue(value);
            }
            
            return new CellValue.StringValue(value);
        }
        
        private boolean isNumeric(String value) {
            if (value == null || value.isEmpty()) return false;
            
            // Remove commas for comma-separated numbers
            String cleanValue = value.replace(",", "");
            
            try {
                Double.parseDouble(cleanValue);
                return true;
            } catch (NumberFormatException e) {
                return false;
            }
        }
        
        private boolean isDuration(String value) {
            return value.matches("^\\d+(?:[.,]\\d+)?\\s*(ns|μs|ms|s|m|h|d)$");
        }
        
        private boolean isMemorySize(String value) {
            return value.matches("^\\d+(?:[.,]\\d+)?\\s*(B|KB|MB|GB|TB|CiB|MiB|GiB|TiB)$");
        }
        
        private boolean isTimestamp(String value) {
            // ISO format
            if (value.matches("^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}")) {
                return true;
            }
            
            // Large number that could be epoch timestamp
            try {
                long num = Long.parseLong(value.replace(",", ""));
                // Check if it's in reasonable timestamp range (after 2000, before 2100)
                return num > 946684800000L && num < 4102444800000L;
            } catch (NumberFormatException e) {
                return false;
            }
        }
        
        private CellValue parseDurationValue(String value) {
            java.util.regex.Pattern pattern = java.util.regex.Pattern.compile("^(\\d+(?:[.,]\\d+)?)\\s*(ns|μs|ms|s|m|h|d)$");
            java.util.regex.Matcher matcher = pattern.matcher(value);
            
            if (!matcher.matches()) {
                return new CellValue.StringValue(value);
            }
            
            double amount = Double.parseDouble(matcher.group(1).replace(",", "."));
            String unit = matcher.group(2);
            
            // Convert to nanoseconds
            long nanoseconds = switch (unit) {
                case "ns" -> (long) amount;
                case "μs" -> (long) (amount * 1_000);
                case "ms" -> (long) (amount * 1_000_000);
                case "s" -> (long) (amount * 1_000_000_000);
                case "m" -> (long) (amount * 60_000_000_000L);
                case "h" -> (long) (amount * 3_600_000_000_000L);
                case "d" -> (long) (amount * 86_400_000_000_000L);
                default -> (long) amount;
            };
            
            return new CellValue.DurationValue(java.time.Duration.ofNanos(nanoseconds));
        }
        
        private CellValue parseMemorySizeValue(String value) {
            java.util.regex.Pattern pattern = java.util.regex.Pattern.compile("^(\\d+(?:[.,]\\d+)?)\\s*(B|KB|MB|GB|TB|CiB|MiB|GiB|TiB)$");
            java.util.regex.Matcher matcher = pattern.matcher(value);
            
            if (!matcher.matches()) {
                return new CellValue.StringValue(value);
            }
            
            double amount = Double.parseDouble(matcher.group(1).replace(",", "."));
            String unit = matcher.group(2);
            
            // Convert to bytes
            long bytes = switch (unit) {
                case "B" -> (long) amount;
                case "KB" -> (long) (amount * 1_000);
                case "MB" -> (long) (amount * 1_000_000);
                case "GB" -> (long) (amount * 1_000_000_000);
                case "TB" -> (long) (amount * 1_000_000_000_000L);
                case "CiB" -> (long) (amount * 1_024);
                case "MiB" -> (long) (amount * 1_048_576);
                case "GiB" -> (long) (amount * 1_073_741_824);
                case "TiB" -> (long) (amount * 1_099_511_627_776L);
                default -> (long) amount;
            };
            
            return new CellValue.MemorySizeValue(bytes);
        }
        
        private CellValue parseTimestampValue(String value) {
            try {
                // Try ISO format first
                if (value.matches("^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}")) {
                    java.time.Instant instant = java.time.Instant.parse(value.endsWith("Z") ? value : value + "Z");
                    return new CellValue.TimestampValue(instant);
                }
                
                // Try epoch milliseconds
                long epochMillis = Long.parseLong(value.replace(",", ""));
                if (epochMillis > 946684800000L && epochMillis < 4102444800000L) {
                    return new CellValue.TimestampValue(java.time.Instant.ofEpochMilli(epochMillis));
                }
                
                // Try epoch nanoseconds (JFR timestamps)
                if (epochMillis > 946684800000000000L) {
                    return new CellValue.TimestampValue(java.time.Instant.ofEpochSecond(0, epochMillis));
                }
                
            } catch (Exception e) {
                // Fall back to string
            }
            
            return new CellValue.StringValue(value);
        }
    }
}
