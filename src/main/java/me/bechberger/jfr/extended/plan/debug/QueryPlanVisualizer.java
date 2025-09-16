package me.bechberger.jfr.extended.plan.debug;

import me.bechberger.jfr.extended.ast.ASTNodes.*;
import me.bechberger.jfr.extended.ast.ASTNode;
import me.bechberger.jfr.extended.plan.plans.*;
import me.bechberger.jfr.extended.plan.QueryExecutionContext;
import me.bechberger.jfr.extended.plan.QueryResult;
import me.bechberger.jfr.extended.table.JfrTable;
import me.bechberger.jfr.extended.table.CellValue;
import me.bechberger.jfr.extended.engine.exception.FunctionArgumentException;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Comprehensive query plan visualizer for debugging query execution.
 * Shows detailed execution flow, data transformations, and intermediate results.
 * 
 * @author JFR Query Engine Team
 * @since 2.0
 */
public class QueryPlanVisualizer {
    
    private final StringBuilder output = new StringBuilder();
    private int indentLevel = 0;
    private final Map<StreamingQueryPlan, String> planIds = new HashMap<>();
    private int planCounter = 0;
    private int maxRowsToDisplay = 5;
    
    /**
     * Visualize a complete query plan execution with detailed debugging information.
     */
    public String visualizeExecution(StreamingQueryPlan plan, QueryExecutionContext context) {
        output.setLength(0);
        indentLevel = 0;
        planIds.clear();
        planCounter = 0;
        
        appendLine("🔍 QUERY PLAN EXECUTION VISUALIZATION");
        appendLine("══════════════════════════════════════");
        appendLine("");
        
        // Show context state before execution
        visualizeExecutionContext("INITIAL CONTEXT", context);
        appendLine("");
        
        // Visualize the plan structure
        appendLine("📋 PLAN STRUCTURE:");
        appendLine("─────────────────");
        visualizePlanStructure(plan);
        appendLine("");
        
        // Execute and trace the plan
        appendLine("⚡ PLAN EXECUTION TRACE:");
        appendLine("─────────────────────");
        try {
            QueryResult result = executeWithTracing(plan, context);
            appendLine("");
            
            // Show final result
            visualizeQueryResult("FINAL RESULT", result);
            appendLine("");
            
            // Show context state after execution
            visualizeExecutionContext("FINAL CONTEXT", context);
            
        } catch (Exception e) {
            appendLine("❌ EXECUTION FAILED:");
            appendLine("Error: " + e.getMessage());
            if (e.getCause() != null) {
                appendLine("Cause: " + e.getCause().getMessage());
            }
            e.printStackTrace();
        }
        
        return output.toString();
    }
    
    /**
     * Visualize the hierarchical structure of the query plan.
     */
    private void visualizePlanStructure(StreamingQueryPlan plan) {
        String planId = assignPlanId(plan);
        
        appendLine("├─ " + planId + ": " + formatPlanName(plan));
        indent();
        
        // Show plan-specific details
        appendLine("│  Type: " + plan.getClass().getSimpleName());
        appendLine("│  Explanation: " + plan.explain());
        appendLine("│  Supports Streaming: " + plan.supportsStreaming());
        
        // Show AST node information
        if (plan.getSourceNode() != null) {
            appendLine("│  AST Node: " + plan.getSourceNode().getClass().getSimpleName());
            appendLine("│  Pretty Print: " + formatSourceNodePretty(plan.getSourceNode()));
            visualizeASTNode(plan.getSourceNode());
        }
        
        // Show detailed plan attributes via reflection
        appendLine("│  Plan Attributes:");
        indent();
        visualizePlanAttributes(plan);
        outdent();
        
        // Show plan-specific configuration
        visualizePlanSpecificDetails(plan);
        
        outdent();
    }
    
    /**
     * Show plan-specific configuration and state.
     */
    private void visualizePlanSpecificDetails(StreamingQueryPlan plan) {
        switch (plan) {
            case FilterPlan filterPlan -> {
                appendLine("│  Filter Expression: " + formatExpression(getFilterExpression(filterPlan)));
                if (getSourcePlan(filterPlan) != null) {
                    appendLine("│  Source Plan:");
                    indent();
                    visualizePlanStructure(getSourcePlan(filterPlan));
                    outdent();
                }
            }
            case ProjectionPlan projectionPlan -> {
                appendLine("│  Projection Details: Available via reflection");
                if (getSourcePlan(projectionPlan) != null) {
                    appendLine("│  Source Plan:");
                    indent();
                    visualizePlanStructure(getSourcePlan(projectionPlan));
                    outdent();
                }
            }
            case ScanPlan scanPlan -> {
                appendLine("│  Table/Source: " + getScanSource(scanPlan));
                appendLine("│  Alias: " + getScanAlias(scanPlan));
            }
            case RawQueryPlan rawQueryPlan -> {
                appendLine("│  Raw Query: " + getRawQuery(rawQueryPlan));
            }
            case GlobalVariableAssignmentPlan globalVarPlan -> {
                appendLine("│  Variable: " + getGlobalVariableName(globalVarPlan));
                appendLine("│  Expression: " + formatExpression(getGlobalVariableExpression(globalVarPlan)));
            }
            default -> {
                // For other plan types, show basic information
                appendLine("│  Plan-specific details: " + plan.toString());
            }
        }
    }
    
    /**
     * Execute the plan while tracing each step.
     */
    private QueryResult executeWithTracing(StreamingQueryPlan plan, QueryExecutionContext context) throws Exception {
        String planId = getPlanId(plan);
        
        appendLine("🚀 Executing " + planId + " (" + formatPlanName(plan) + ")");
        indent();
        
        // Show input context
        appendLine("📥 Input Context:");
        indent();
        appendLine("Variables: " + getVariableCount(context));
        for (String varName : getVariableNames(context)) {
            Object value = context.getVariable(varName);
            appendLine("  " + varName + " = " + formatValue(value));
        }
        outdent();
        
        // Execute the plan
        long startTime = System.nanoTime();
        QueryResult result = plan.execute(context);
        long executionTime = System.nanoTime() - startTime;
        
        // Show execution results
        appendLine("⏱️  Execution time: " + (executionTime / 1_000_000.0) + " ms");
        appendLine("📤 Output Result:");
        indent();
        visualizeQueryResult("", result);
        outdent();
        
        outdent();
        return result;
    }
    
    /**
     * Visualize the execution context state.
     */
    private void visualizeExecutionContext(String title, QueryExecutionContext context) {
        if (!title.isEmpty()) {
            appendLine("🔧 " + title + ":");
            appendLine("─".repeat(title.length() + 3));
        }
        
        indent();
        appendLine("📊 Variables (" + getVariableCount(context) + "):");
        indent();
        for (String varName : getVariableNames(context)) {
            Object value = context.getVariable(varName);
            appendLine(varName + " = " + formatValue(value));
        }
        outdent();
        
        appendLine("💾 Lazy Variables (" + getLazyVariableCount(context) + "):");
        indent();
        for (String varName : getLazyVariableNames(context)) {
            appendLine(varName + " = <lazy>");
        }
        outdent();
        
        outdent();
    }
    
    /**
     * Visualize a query result with detailed table information.
     */
    private void visualizeQueryResult(String title, QueryResult result) {
        if (!title.isEmpty()) {
            appendLine(title + ":");
        }
        
        indent();
        appendLine("Success: " + result.isSuccess());
        
        if (result.isSuccess() && result.getTable() != null) {
            JfrTable table = result.getTable();
            appendLine("📊 Table Details:");
            indent();
            appendLine("Rows: " + table.getRowCount());
            appendLine("Columns: " + table.getColumnCount());
            
            // Show column information
            appendLine("Column Schema:");
            for (int c = 0; c < table.getColumnCount(); c++) {
                var column = table.getColumns().get(c);
                appendLine("  [" + c + "] " + column.name() + " (" + column.type() + ")");
            }
            
            // Show data in ASCII table format
            int maxRows = Math.min(this.maxRowsToDisplay, table.getRowCount());
            if (maxRows > 0) {
                appendLine("Data Table:");
                String asciiTable = formatTableAsAscii(table, maxRows);
                appendLine(asciiTable);
            } else {
                appendLine("(empty table)");
            }
            outdent();
        } else if (!result.isSuccess()) {
            appendLine("❌ Error: " + (result.getError() != null ? result.getError().getMessage() : "Unknown error"));
            if (result.getError() != null && result.getError().getCause() != null) {
                appendLine("   Cause: " + result.getError().getCause().getMessage());
            }
        } else {
            appendLine("📊 Table: null");
        }
        outdent();
    }
    
    /**
     * Visualize AST node information.
     */
    private void visualizeASTNode(ASTNode node) {
        if (node == null) return;
        
        switch (node) {
            case SelectNode select -> {
                appendLine("│    SELECT fields: " + select.items().size());
                appendLine("│    SELECT ALL: " + select.isSelectAll());
            }
            case IdentifierNode identifier -> {
                appendLine("│    Identifier: " + identifier.name());
            }
            case LiteralNode literal -> {
                appendLine("│    Literal: " + formatValue(literal.value()));
            }
            case FieldAccessNode field -> {
                appendLine("│    Field: " + field.field());
                appendLine("│    Qualifier: " + field.qualifier());
            }
            case BinaryExpressionNode binary -> {
                appendLine("│    Operator: " + binary.operator());
                appendLine("│    Left: " + formatExpression(binary.left()));
                appendLine("│    Right: " + formatExpression(binary.right()));
            }
            default -> {
                appendLine("│    AST: " + node.getClass().getSimpleName());
            }
        }
    }
    
    /**
     * Format an expression for display.
     */
    private String formatExpression(ExpressionNode expr) {
        if (expr == null) return "null";
        
        return switch (expr) {
            case IdentifierNode id -> id.name();
            case LiteralNode lit -> formatValue(lit.value());
            case FieldAccessNode field -> 
                (field.qualifier() != null ? field.qualifier() + "." : "") + field.field();
            case BinaryExpressionNode binary -> 
                "(" + formatExpression(binary.left()) + " " + binary.operator() + " " + formatExpression(binary.right()) + ")";
            case FunctionCallNode func ->
                func.functionName() + "(" + 
                func.arguments().stream()
                    .map(this::formatExpression)
                    .collect(Collectors.joining(", ")) + ")";
            default -> expr.getClass().getSimpleName() + "{}";
        };
    }
    
    /**
     * Format a value for display with type information.
     */
    private String formatValue(Object value) {
        if (value == null) return "null";
        if (value instanceof String) return "\"" + value + "\"";
        if (value instanceof CellValue cellValue) {
            return formatValue(cellValue.getValue()) + " (CellValue)";
        }
        return value.toString() + " (" + value.getClass().getSimpleName() + ")";
    }
    
    /**
     * Helper methods to extract plan details using reflection or accessor methods.
     */
    private String assignPlanId(StreamingQueryPlan plan) {
        String id = "P" + (++planCounter);
        planIds.put(plan, id);
        return id;
    }
    
    private String getPlanId(StreamingQueryPlan plan) {
        return planIds.getOrDefault(plan, "P?");
    }
    
    private String formatPlanName(StreamingQueryPlan plan) {
        return plan.getClass().getSimpleName().replace("Plan", "");
    }
    
    // Helper methods to extract context information
    private int getVariableCount(QueryExecutionContext context) {
        try {
            var field = context.getClass().getDeclaredField("variables");
            field.setAccessible(true);
            Map<?, ?> variables = (Map<?, ?>) field.get(context);
            return variables.size();
        } catch (Exception e) {
            return 0;
        }
    }
    
    private Set<String> getVariableNames(QueryExecutionContext context) {
        try {
            var field = context.getClass().getDeclaredField("variables");
            field.setAccessible(true);
            @SuppressWarnings("unchecked")
            Map<String, ?> variables = (Map<String, ?>) field.get(context);
            return variables.keySet();
        } catch (Exception e) {
            return Set.of();
        }
    }
    
    // Helper methods to extract plan-specific information
    // These use reflection to access private fields
    
    private ExpressionNode getFilterExpression(FilterPlan plan) {
        try {
            var field = plan.getClass().getDeclaredField("whereCondition");
            field.setAccessible(true);
            return (ExpressionNode) field.get(plan);
        } catch (Exception e) {
            return null;
        }
    }
    
    private StreamingQueryPlan getSourcePlan(StreamingQueryPlan plan) {
        try {
            var field = plan.getClass().getDeclaredField("sourcePlan");
            field.setAccessible(true);
            return (StreamingQueryPlan) field.get(plan);
        } catch (Exception e) {
            return null;
        }
    }
    
    private String getScanSource(ScanPlan plan) {
        try {
            var field = plan.getClass().getDeclaredField("sourceNode");
            field.setAccessible(true);
            SourceNode sourceNode = (SourceNode) field.get(plan);
            return sourceNode != null ? sourceNode.toString() : "unknown";
        } catch (Exception e) {
            return "unknown";
        }
    }
    
    private String getScanAlias(ScanPlan plan) {
        try {
            var field = plan.getClass().getDeclaredField("alias");
            field.setAccessible(true);
            return (String) field.get(plan);
        } catch (Exception e) {
            return "none";
        }
    }
    
    private String getRawQuery(RawQueryPlan plan) {
        try {
            var field = plan.getClass().getDeclaredField("rawQuery");
            field.setAccessible(true);
            return (String) field.get(plan);
        } catch (Exception e) {
            return "unknown";
        }
    }
    
    private String getGlobalVariableName(GlobalVariableAssignmentPlan plan) {
        try {
            var field = plan.getClass().getDeclaredField("globalVarNode");
            field.setAccessible(true);
            GlobalVariableAssignmentNode node = (GlobalVariableAssignmentNode) field.get(plan);
            return node.variable();
        } catch (Exception e) {
            return "unknown";
        }
    }
    
    private ExpressionNode getGlobalVariableExpression(GlobalVariableAssignmentPlan plan) {
        try {
            var field = plan.getClass().getDeclaredField("globalVarNode");
            field.setAccessible(true);
            GlobalVariableAssignmentNode node = (GlobalVariableAssignmentNode) field.get(plan);
            return node.expression();
        } catch (Exception e) {
            return null;
        }
    }
    
    private int getLazyVariableCount(QueryExecutionContext context) {
        try {
            var field = context.getClass().getDeclaredField("lazyVariables");
            field.setAccessible(true);
            Map<?, ?> lazyVariables = (Map<?, ?>) field.get(context);
            return lazyVariables.size();
        } catch (Exception e) {
            return 0;
        }
    }
    
    private Set<String> getLazyVariableNames(QueryExecutionContext context) {
        try {
            var field = context.getClass().getDeclaredField("lazyVariables");
            field.setAccessible(true);
            @SuppressWarnings("unchecked")
            Map<String, ?> lazyVariables = (Map<String, ?>) field.get(context);
            return lazyVariables.keySet();
        } catch (Exception e) {
            return Set.of();
        }
    }
    
    /**
     * Set the maximum number of rows to display for each intermediate table.
     * @param maxRows the maximum rows to display (must be > 0)
     * @throws FunctionArgumentException if maxRows <= 0
     */
    public void setMaxRowsToDisplay(int maxRows) {
        if (maxRows <= 0) {
            throw new FunctionArgumentException(
                "setMaxRowsToDisplay", 0,
                "positive integer", "non-positive integer", maxRows,
                FunctionArgumentException.ArgumentErrorType.OUT_OF_RANGE, null);
        }
        this.maxRowsToDisplay = maxRows;
    }
    
    /**
     * Get the current maximum number of rows to display for each intermediate table.
     * @return the maximum rows to display
     */
    public int getMaxRowsToDisplay() {
        return maxRowsToDisplay;
    }
    
    /**
     * Visualize a complete query plan execution with configurable row display limit.
     * @param plan the query plan to execute
     * @param context the execution context
     * @param maxRowsPerTable maximum rows to display per intermediate table
     * @return formatted visualization string
     */
    public String visualizeExecution(StreamingQueryPlan plan, QueryExecutionContext context, int maxRowsPerTable) {
        int originalMaxRows = this.maxRowsToDisplay;
        this.maxRowsToDisplay = maxRowsPerTable;
        
        try {
            output.setLength(0);
            indentLevel = 0;
            planIds.clear();
            planCounter = 0;
            
            appendLine("🔍 QUERY PLAN EXECUTION VISUALIZATION (Max " + maxRowsPerTable + " rows per table)");
            appendLine("══════════════════════════════════════════════════════════════════════");
            appendLine("");
            
            // Show context state before execution
            visualizeExecutionContext("INITIAL CONTEXT", context);
            appendLine("");
            
            // Visualize the plan structure
            appendLine("📋 PLAN STRUCTURE:");
            appendLine("─────────────────");
            visualizePlanStructure(plan);
            appendLine("");
            
            // Execute and trace the plan
            appendLine("⚡ PLAN EXECUTION TRACE:");
            appendLine("─────────────────────");
            try {
                QueryResult result = executeWithTracing(plan, context);
                appendLine("");
                
                // Show final result
                visualizeQueryResult("FINAL RESULT", result);
                appendLine("");
                
                // Show context state after execution
                visualizeExecutionContext("FINAL CONTEXT", context);
                
            } catch (Exception e) {
                appendLine("❌ EXECUTION FAILED:");
                appendLine("Error: " + e.getMessage());
                if (e.getCause() != null) {
                    appendLine("Cause: " + e.getCause().getMessage());
                }
                e.printStackTrace();
            }
            
            return output.toString();
        } finally {
            // Restore original max rows setting
            this.maxRowsToDisplay = originalMaxRows;
        }
    }
    
    /**
     * Format AST node in a readable way
     */
    private String formatSourceNodePretty(ASTNode node) {
        if (node == null) return "null";
        
        // Try to extract meaningful information from the AST node
        String nodeType = node.getClass().getSimpleName();
        
        try {
            // Use reflection to get useful fields
            var fields = node.getClass().getDeclaredFields();
            StringBuilder info = new StringBuilder();
            
            for (var field : fields) {
                if (field.getName().equals("location")) continue; // Skip location info
                
                field.setAccessible(true);
                Object value = field.get(node);
                
                if (value != null) {
                    if (info.length() > 0) info.append(", ");
                    
                    if (value instanceof String) {
                        info.append(field.getName()).append("=\"").append(value).append("\"");
                    } else if (value instanceof List<?> list) {
                        info.append(field.getName()).append("=[").append(list.size()).append(" items]");
                    } else if (value instanceof ASTNode) {
                        info.append(field.getName()).append("=").append(value.getClass().getSimpleName());
                    } else {
                        info.append(field.getName()).append("=").append(value);
                    }
                }
            }
            
            return nodeType + "(" + info.toString() + ")";
        } catch (Exception e) {
            return nodeType + "(reflection failed: " + e.getMessage() + ")";
        }
    }
    
    /**
     * Visualize plan attributes using reflection
     */
    private void visualizePlanAttributes(StreamingQueryPlan plan) {
        try {
            var fields = plan.getClass().getDeclaredFields();
            
            for (var field : fields) {
                field.setAccessible(true);
                Object value = field.get(plan);
                
                String fieldName = field.getName();
                String valueStr = formatAttributeValue(value);
                
                appendLine("│  " + fieldName + ": " + valueStr);
            }
        } catch (Exception e) {
            appendLine("│  (reflection failed: " + e.getMessage() + ")");
        }
    }
    
    /**
     * Format attribute value for display
     */
    private String formatAttributeValue(Object value) {
        if (value == null) return "null";
        
        if (value instanceof String str) {
            return "\"" + (str.length() > 50 ? str.substring(0, 47) + "..." : str) + "\"";
        } else if (value instanceof List<?> list) {
            return "[" + list.size() + " items]";
        } else if (value instanceof ASTNode node) {
            return node.getClass().getSimpleName();
        } else if (value instanceof StreamingQueryPlan subPlan) {
            return subPlan.getClass().getSimpleName() + " (sub-plan)";
        } else {
            String str = value.toString();
            return str.length() > 100 ? str.substring(0, 97) + "..." : str;
        }
    }
    
    /**
     * Format a table as ASCII art with proper column alignment
     */
    private String formatTableAsAscii(JfrTable table, int maxRows) {
        if (table.getRowCount() == 0) {
            return "    (empty table)";
        }
        
        StringBuilder ascii = new StringBuilder();
        List<JfrTable.Column> columns = table.getColumns();
        
        // Calculate column widths
        int[] widths = new int[columns.size()];
        for (int c = 0; c < columns.size(); c++) {
            widths[c] = Math.max(columns.get(c).name().length(), 8); // minimum 8 chars
        }
        
        // Check data widths (up to maxRows)
        int rowsToCheck = Math.min(maxRows, table.getRowCount());
        for (int r = 0; r < rowsToCheck; r++) {
            for (int c = 0; c < columns.size(); c++) {
                String cellStr = formatCellForTable(table, r, c);
                widths[c] = Math.max(widths[c], cellStr.length());
            }
        }
        
        // Create header separator
        String separator = "    +";
        for (int width : widths) {
            separator += "-".repeat(width + 2) + "+";
        }
        
        // Header
        ascii.append(separator).append("\n");
        ascii.append("    |");
        for (int c = 0; c < columns.size(); c++) {
            ascii.append(String.format(" %-" + widths[c] + "s ", columns.get(c).name()));
            ascii.append("|");
        }
        ascii.append("\n").append(separator).append("\n");
        
        // Data rows
        for (int r = 0; r < rowsToCheck; r++) {
            ascii.append("    |");
            for (int c = 0; c < columns.size(); c++) {
                String cellStr = formatCellForTable(table, r, c);
                ascii.append(String.format(" %-" + widths[c] + "s ", cellStr));
                ascii.append("|");
            }
            ascii.append("\n");
        }
        ascii.append(separator);
        
        if (table.getRowCount() > maxRows) {
            ascii.append("\n    ... (").append(table.getRowCount() - maxRows).append(" more rows)");
        }
        
        return ascii.toString();
    }
    
    /**
     * Format a cell value for display in ASCII table
     */
    private String formatCellForTable(JfrTable table, int row, int col) {
        try {
            var cell = table.getCell(row, col);
            if (cell instanceof CellValue.NullValue) {
                return "NULL";
            } else if (cell instanceof CellValue.StringValue str) {
                String value = str.value();
                return value.length() > 20 ? value.substring(0, 17) + "..." : value;
            } else if (cell instanceof CellValue.NumberValue num) {
                return String.valueOf(num.value());
            } else if (cell instanceof CellValue.NumberValue flt) {
                return String.format("%.2f", flt.value());
            } else if (cell instanceof CellValue.BooleanValue bool) {
                return String.valueOf(bool.value());
            } else if (cell instanceof CellValue.ArrayValue arr) {
                return "[" + arr.elements().size() + " items]";
            } else {
                return cell.toString();
            }
        } catch (Exception e) {
            return "ERROR";
        }
    }
    
    private void appendLine(String line) {
        output.append("  ".repeat(indentLevel)).append(line).append("\n");
    }
    
    private void indent() {
        indentLevel++;
    }
    
    private void outdent() {
        if (indentLevel > 0) indentLevel--;
    }
}
