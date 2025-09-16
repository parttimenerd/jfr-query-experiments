package me.bechberger.jfr.extended.plan;

import java.util.List;

/**
 * ASCII art visualizer for query plan execution flow.
 * 
 * This visualizer creates diagrams showing how data flows through the streaming
 * plan architecture, making it easy to understand the pipeline composition
 * and execution results.
 * 
 * @author JFR Query Plan Architecture
 * @since 2.0
 */
public class QueryPlanVisualizer {
    
    // ASCII Box Drawing Characters
    private static final String HORIZONTAL_LINE = "─";
    private static final String VERTICAL_LINE = "│";
    private static final String TOP_LEFT = "┌";
    private static final String TOP_RIGHT = "┐";
    private static final String BOTTOM_LEFT = "└";
    private static final String BOTTOM_RIGHT = "┘";
    private static final String ARROW_DOWN = "↓";
    private static final String ARROW_RIGHT = "→";
    private static final String T_DOWN = "┬";
    private static final String T_UP = "┴";
    private static final String T_RIGHT = "├";
    private static final String T_LEFT = "┤";
    private static final String CROSS = "┼";
    
    // Flow characters
    private static final String FLOW_PIPE = "║";
    private static final String FLOW_CORNER_TOP_LEFT = "╔";
    private static final String FLOW_CORNER_TOP_RIGHT = "╗";
    private static final String FLOW_CORNER_BOTTOM_LEFT = "╚";
    private static final String FLOW_CORNER_BOTTOM_RIGHT = "╝";
    private static final String FLOW_HORIZONTAL = "═";
    
    private static boolean useAsciiArt = true;
    
    /**
     * Enable or disable ASCII art for all visualizations.
     */
    public static void setAsciiArtMode(boolean enabled) {
        useAsciiArt = enabled;
    }
    
    /**
     * Check if ASCII art mode is enabled.
     */
    public static boolean isAsciiArtEnabled() {
        return useAsciiArt;
    }
    
    /**
     * Visualizes the complete query plan execution pipeline.
     */
    public static void visualizePipeline(String queryDescription, 
                                       List<PlanStep> steps, 
                                       ExecutionMetrics metrics) {
        System.out.println();
        if (useAsciiArt) {
            printHeader("QUERY PLAN EXECUTION PIPELINE");
            System.out.println();
            
            // Show query description in a fancy box
            printQueryBox(queryDescription);
            System.out.println();
            
            // Show execution flow with ASCII boxes
            printExecutionFlowWithBoxes(steps);
            System.out.println();
            
            // Show metrics summary
            printMetrics(metrics);
        } else {
            printSimpleHeader("QUERY PLAN EXECUTION PIPELINE");
            System.out.println("Query: " + queryDescription);
            System.out.println();
            
            // Show simple execution flow
            printSimpleExecutionFlow(steps);
            System.out.println();
            
            // Show simple metrics
            printSimpleMetrics(metrics);
        }
        System.out.println();
    }
    
    /**
     * Creates a visual representation of data flow between plans.
     */
    public static void visualizeDataFlow(List<PlanStep> steps) {
        System.out.println();
        if (useAsciiArt) {
            printHeader("DATA FLOW VISUALIZATION");
            System.out.println();
            
            for (int i = 0; i < steps.size(); i++) {
                PlanStep step = steps.get(i);
                
                // Plan box
                printPlanBox(step);
                
                // Data flow arrow (except for last step)
                if (i < steps.size() - 1) {
                    printDataFlowArrow(step);
                }
            }
        } else {
            printSimpleHeader("DATA FLOW VISUALIZATION");
            System.out.println();
            
            for (int i = 0; i < steps.size(); i++) {
                PlanStep step = steps.get(i);
                System.out.printf("%s -> %s (%d rows)%n", 
                    step.inputDescription, step.outputDescription, step.rowCount);
                if (i < steps.size() - 1) {
                    System.out.println("  |");
                    System.out.println("  v");
                }
            }
        }
        System.out.println();
    }
    
    /**
     * Creates an architecture overview diagram.
     */
    public static void visualizeArchitecture() {
        System.out.println();
        if (useAsciiArt) {
            printHeader("STREAMING PLAN ARCHITECTURE");
            System.out.println();
            
            System.out.println("┌─────────────────────────────────────────────────────────────┐");
            System.out.println("│                    QUERY EXECUTION FLOW                    │");
            System.out.println("├─────────────────────────────────────────────────────────────┤");
            System.out.println("│                                                             │");
            System.out.println("│  ┌──────────┐    ┌──────────┐    ┌──────────┐             │");
            System.out.println("│  │   JFR    │ → │   SCAN   │ → │  FILTER  │ → Results   │");
            System.out.println("│  │  Events  │    │   PLAN   │    │   PLAN   │             │");
            System.out.println("│  └──────────┘    └──────────┘    └──────────┘             │");
            System.out.println("│                                                             │");
            System.out.println("│  • ScanPlan: Creates JFR event tables (no mocking)         │");
            System.out.println("│  • FilterPlan: Applies WHERE conditions on streaming data  │");
            System.out.println("│  • Data flows through QueryExecutionContext               │");
            System.out.println("│  • Alias resolution and variable management               │");
            System.out.println("│                                                             │");
            System.out.println("└─────────────────────────────────────────────────────────────┘");
        } else {
            printSimpleHeader("STREAMING PLAN ARCHITECTURE");
            System.out.println();
            System.out.println("Query Execution Flow:");
            System.out.println("  JFR Events -> ScanPlan -> FilterPlan -> Results");
            System.out.println();
            System.out.println("Key Components:");
            System.out.println("  - ScanPlan: Creates JFR event tables (no mocking)");
            System.out.println("  - FilterPlan: Applies WHERE conditions on streaming data");
            System.out.println("  - Data flows through QueryExecutionContext");
            System.out.println("  - Alias resolution and variable management");
        }
        System.out.println();
    }
    
    public static void printHeader(String title) {
        int width = 65;
        int padding = (width - title.length() - 2) / 2;
        
        System.out.println("╔" + HORIZONTAL_LINE.repeat(width - 2) + "╗");
        System.out.println("║" + " ".repeat(padding) + title + " ".repeat(width - title.length() - padding - 2) + "║");
        System.out.println("╚" + HORIZONTAL_LINE.repeat(width - 2) + "╝");
    }
    
    public static void printQueryBox(String query) {
        int width = Math.max(50, query.length() + 10); // Increased minimum padding
        int spacesNeeded = width - query.length() - 9; // "QUERY: " is 8 chars + 1 space
        
        // Ensure we never have negative spaces
        if (spacesNeeded < 0) {
            width = query.length() + 10;
            spacesNeeded = 1;
        }
        
        System.out.println(TOP_LEFT + HORIZONTAL_LINE.repeat(width - 2) + TOP_RIGHT);
        System.out.println(VERTICAL_LINE + " QUERY: " + query + " ".repeat(spacesNeeded) + VERTICAL_LINE);
        System.out.println(BOTTOM_LEFT + HORIZONTAL_LINE.repeat(width - 2) + BOTTOM_RIGHT);
    }
    
    /**
     * Enhanced execution flow with proper ASCII boxes and flow indicators.
     */
    private static void printExecutionFlowWithBoxes(List<PlanStep> steps) {
        System.out.println("EXECUTION FLOW:");
        System.out.println();
        
        for (int i = 0; i < steps.size(); i++) {
            PlanStep step = steps.get(i);
            
            // Create a detailed flow box for each step
            printDetailedStepBox(step, i + 1);
            
            // Flow arrow to next step (except for last)
            if (i < steps.size() - 1) {
                printFlowArrow();
            }
        }
    }
    
    /**
     * Simple text-based execution flow without ASCII art.
     */
    private static void printSimpleExecutionFlow(List<PlanStep> steps) {
        System.out.println("Execution Flow:");
        
        for (int i = 0; i < steps.size(); i++) {
            PlanStep step = steps.get(i);
            String status = step.success ? "SUCCESS" : "FAILED";
            
            System.out.printf("%d. %s [%s]%n", i + 1, step.planType, status);
            System.out.printf("   Input:  %s%n", step.inputDescription);
            System.out.printf("   Output: %s%n", step.outputDescription);
            System.out.printf("   Rows:   %d%n", step.rowCount);
            
            if (i < steps.size() - 1) {
                System.out.println("   |");
                System.out.println("   v");
            }
        }
        System.out.println();
    }
    
    /**
     * Prints a detailed step box with flow indicators.
     */
    private static void printDetailedStepBox(PlanStep step, int stepNumber) {
        String title = String.format("Step %d: %s", stepNumber, step.planType);
        String status = step.success ? "✓ SUCCESS" : "✗ FAILED";
        
        int width = Math.max(60, Math.max(title.length(), step.inputDescription.length()) + 8);
        
        // Top border with title
        System.out.println(FLOW_CORNER_TOP_LEFT + FLOW_HORIZONTAL.repeat(width - 2) + FLOW_CORNER_TOP_RIGHT);
        System.out.println(FLOW_PIPE + " " + title + " ".repeat(width - title.length() - 3) + FLOW_PIPE);
        System.out.println(T_RIGHT + HORIZONTAL_LINE.repeat(width - 2) + T_LEFT);
        
        // Content
        System.out.println(FLOW_PIPE + " Status: " + status + " ".repeat(width - status.length() - 10) + FLOW_PIPE);
        System.out.println(FLOW_PIPE + " Input:  " + step.inputDescription + " ".repeat(width - step.inputDescription.length() - 10) + FLOW_PIPE);
        System.out.println(FLOW_PIPE + " Output: " + step.outputDescription + " ".repeat(width - step.outputDescription.length() - 10) + FLOW_PIPE);
        
        // Only show row count if it's non-negative
        if (step.rowCount >= 0) {
            System.out.println(FLOW_PIPE + " Rows:   " + step.rowCount + " ".repeat(width - String.valueOf(step.rowCount).length() - 10) + FLOW_PIPE);
        }
        
        // Bottom border
        System.out.println(FLOW_CORNER_BOTTOM_LEFT + FLOW_HORIZONTAL.repeat(width - 2) + FLOW_CORNER_BOTTOM_RIGHT);
    }
    
    /**
     * Prints a flow arrow between steps.
     */
    private static void printFlowArrow() {
        System.out.println("                     " + FLOW_PIPE);
        System.out.println("                     " + ARROW_DOWN);
        System.out.println("                     " + FLOW_PIPE);
        System.out.println();
    }
    
    /**
     * Simple header without ASCII art.
     */
    private static void printSimpleHeader(String title) {
        System.out.println("=== " + title + " ===");
    }
    
    /**
     * Simple metrics without ASCII art.
     */
    private static void printSimpleMetrics(ExecutionMetrics metrics) {
        System.out.println("Execution Metrics:");
        System.out.printf("- Total Steps: %d%n", metrics.totalSteps);
        System.out.printf("- Successful: %d%n", metrics.successfulSteps);
        System.out.printf("- Failed: %d%n", metrics.failedSteps);
        System.out.printf("- Total Rows Processed: %d%n", metrics.totalRowsProcessed);
        System.out.printf("- Final Result Rows: %d%n", metrics.finalResultRows);
        if (metrics.totalRowsProcessed > 0) {
            double efficiency = (double) metrics.finalResultRows / metrics.totalRowsProcessed * 100;
            System.out.printf("- Filter Efficiency: %.1f%%%n", efficiency);
        }
    }
    private static void printPlanBox(PlanStep step) {
        System.out.println("┌─────────────────────┐");
        System.out.printf("│ %-19s │%n", step.planType);
        System.out.println("├─────────────────────┤");
        System.out.printf("│ Status: %-11s │%n", step.success ? "SUCCESS" : "FAILED");
        if (step.rowCount >= 0) {
            System.out.printf("│ Rows: %-13d │%n", step.rowCount);
        }
        System.out.println("└─────────────────────┘");
    }
    
    private static void printDataFlowArrow(PlanStep step) {
        System.out.println("          │");
        System.out.printf("          │ Data: %s%n", step.outputDescription);
        System.out.println("          ↓");
    }
    
    private static void printMetrics(ExecutionMetrics metrics) {
        System.out.println("EXECUTION METRICS:");
        System.out.println();
        
        System.out.println("┌─────────────────────────────────────────┐");
        System.out.println("│              PERFORMANCE                │");
        System.out.println("├─────────────────────────────────────────┤");
        System.out.printf("│ Total Steps: %-26d │%n", metrics.totalSteps);
        System.out.printf("│ Successful: %-27d │%n", metrics.successfulSteps);
        System.out.printf("│ Failed: %-31d │%n", metrics.failedSteps);
        System.out.printf("│ Total Rows Processed: %-17d │%n", metrics.totalRowsProcessed);
        System.out.printf("│ Final Result Rows: %-20d │%n", metrics.finalResultRows);
        System.out.println("└─────────────────────────────────────────┘");
    }
    
    /**
     * Creates a plan step visualization for the demo.
     */
    public static PlanStep createScanStep(boolean success, int rowCount) {
        return new PlanStep(
            "ScanPlan",
            "JFR Event Stream",
            success ? "JFR Event Table (" + rowCount + " rows)" : "Error",
            success,
            rowCount
        );
    }
    
    public static PlanStep createFilterStep(boolean success, int inputRows, int outputRows) {
        return new PlanStep(
            "FilterPlan",
            "JFR Event Table (" + inputRows + " rows)",
            success ? "Filtered Table (" + outputRows + " rows)" : "Error",
            success,
            outputRows
        );
    }
    
    /**
     * Represents a single step in the query execution pipeline.
     */
    public static class PlanStep {
        public final String planType;
        public final String inputDescription;
        public final String outputDescription;
        public final boolean success;
        public final int rowCount;
        
        public PlanStep(String planType, String inputDescription, String outputDescription, 
                       boolean success, int rowCount) {
            this.planType = planType;
            this.inputDescription = inputDescription;
            this.outputDescription = outputDescription;
            this.success = success;
            this.rowCount = rowCount;
        }
    }
    
    /**
     * Execution metrics for the pipeline.
     */
    public static class ExecutionMetrics {
        public final int totalSteps;
        public final int successfulSteps;
        public final int failedSteps;
        public final int totalRowsProcessed;
        public final int finalResultRows;
        
        public ExecutionMetrics(int totalSteps, int successfulSteps, int failedSteps,
                              int totalRowsProcessed, int finalResultRows) {
            this.totalSteps = totalSteps;
            this.successfulSteps = successfulSteps;
            this.failedSteps = failedSteps;
            this.totalRowsProcessed = totalRowsProcessed;
            this.finalResultRows = finalResultRows;
        }
        
        public static ExecutionMetrics fromSteps(List<PlanStep> steps) {
            int total = steps.size();
            int successful = (int) steps.stream().mapToInt(s -> s.success ? 1 : 0).sum();
            int failed = total - successful;
            int totalRows = steps.stream().mapToInt(s -> Math.max(0, s.rowCount)).sum();
            int finalRows = steps.isEmpty() ? 0 : Math.max(0, steps.get(steps.size() - 1).rowCount);
            
            return new ExecutionMetrics(total, successful, failed, totalRows, finalRows);
        }
    }
}
