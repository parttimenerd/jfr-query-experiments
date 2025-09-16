package me.bechberger.jfr.extended.plan.visualizer;

import me.bechberger.jfr.extended.plan.plans.StreamingQueryPlan;
import me.bechberger.jfr.extended.plan.plans.AbstractStreamingPlan;
import me.bechberger.jfr.extended.plan.plans.ScanPlan;
import me.bechberger.jfr.extended.plan.plans.FilterPlan;
import me.bechberger.jfr.extended.plan.plans.ProjectionPlan;
import me.bechberger.jfr.extended.plan.plans.AggregationPlan;
import me.bechberger.jfr.extended.plan.plans.SortPlan;
import me.bechberger.jfr.extended.plan.plans.JoinPlan;
import me.bechberger.jfr.extended.plan.plans.HavingPlan;

import java.util.*;

/**
 * Query Plan Visualizer for debugging and explaining JFR query execution plans.
 * 
 * Provides multiple visualization formats:
 * - Simple tree view
 * - Verbose detailed view with inputs/outputs
 * - ASCII art visualization
 * - Performance metrics view
 * 
 * @author JFR Query Plan Architecture
 * @since 3.0
 */
public class QueryPlanVisualizer {

    private static final String BRANCH = "├─";
    private static final String LAST_BRANCH = "└─";
    private static final String VERTICAL = "│ ";
    private static final String SPACE = "  ";
    
    /**
     * Generate a simple tree visualization of the query plan
     */
    public static String visualizeSimple(StreamingQueryPlan plan) {
        StringBuilder sb = new StringBuilder();
        sb.append("Query Plan:\n");
        visualizeTreeNode(plan, "", true, sb);
        return sb.toString();
    }
    
    /**
     * Generate a verbose visualization with inputs, outputs, and execution details
     */
    public static String visualizeVerbose(StreamingQueryPlan plan) {
        StringBuilder sb = new StringBuilder();
        sb.append("┌─ VERBOSE QUERY PLAN VISUALIZATION ─┐\n");
        sb.append("│                                    │\n");
        sb.append("└────────────────────────────────────┘\n\n");
        
        PlanAnalyzer analyzer = new PlanAnalyzer();
        PlanStats stats = analyzer.analyze(plan);
        
        sb.append("📊 PLAN STATISTICS:\n");
        sb.append("   • Total Plans: ").append(stats.totalPlans).append("\n");
        sb.append("   • Max Depth: ").append(stats.maxDepth).append("\n");
        sb.append("   • Plan Types: ").append(stats.planTypes).append("\n");
        sb.append("   • Estimated Cost: ").append(stats.estimatedCost).append("\n\n");
        
        sb.append("🔍 EXECUTION FLOW:\n");
        visualizeVerboseNode(plan, "", true, sb, 0);
        
        sb.append("\n🎯 OPTIMIZATION OPPORTUNITIES:\n");
        List<String> suggestions = analyzer.getOptimizationSuggestions(plan);
        if (suggestions.isEmpty()) {
            sb.append("   ✅ No obvious optimization opportunities found.\n");
        } else {
            for (String suggestion : suggestions) {
                sb.append("   💡 ").append(suggestion).append("\n");
            }
        }
        
        return sb.toString();
    }
    
    /**
     * Generate an ASCII art visualization that's fun to read
     */
    public static String visualizeAsciiArt(StreamingQueryPlan plan) {
        StringBuilder sb = new StringBuilder();
        
        sb.append("╔══════════════════════════════════════╗\n");
        sb.append("║      🚀 JFR QUERY EXECUTION PLAN    ║\n");
        sb.append("╚══════════════════════════════════════╝\n\n");
        
        sb.append("     Data flows from bottom to top     \n");
        sb.append("              ↑ ↑ ↑ ↑ ↑                \n\n");
        
        List<StreamingQueryPlan> planLevels = collectPlanLevels(plan);
        
        for (int level = planLevels.size() - 1; level >= 0; level--) {
            StreamingQueryPlan currentPlan = planLevels.get(level);
            
            // Draw the plan box
            String planName = getPlanName(currentPlan);
            String planDetails = getPlanDetails(currentPlan);
            
            sb.append("┌─").append("─".repeat(Math.max(20, planName.length() + 2))).append("─┐\n");
            sb.append("│ ").append(padRight(planName, Math.max(20, planName.length() + 2))).append(" │\n");
            
            if (!planDetails.isEmpty()) {
                for (String detail : planDetails.split("\n")) {
                    if (!detail.trim().isEmpty()) {
                        sb.append("│ ").append(padRight(detail, Math.max(20, planName.length() + 2))).append(" │\n");
                    }
                }
            }
            
            sb.append("└─").append("─".repeat(Math.max(20, planName.length() + 2))).append("─┘\n");
            
            if (level > 0) {
                sb.append("     ↑ ↑ ↑ ↑ ↑ ↑ ↑ ↑ ↑ ↑ ↑       \n");
            }
        }
        
        sb.append("\n🎯 RESULT: Final output to user\n");
        
        return sb.toString();
    }
    
    /**
     * Generate performance-focused visualization
     */
    public static String visualizePerformance(StreamingQueryPlan plan) {
        StringBuilder sb = new StringBuilder();
        
        sb.append("⚡ PERFORMANCE ANALYSIS\n");
        sb.append("═".repeat(50)).append("\n\n");
        
        PlanAnalyzer analyzer = new PlanAnalyzer();
        Map<String, PerformanceMetrics> metrics = analyzer.analyzePerformance(plan);
        
        sb.append("📈 PLAN PERFORMANCE METRICS:\n\n");
        
        for (Map.Entry<String, PerformanceMetrics> entry : metrics.entrySet()) {
            PerformanceMetrics metric = entry.getValue();
            sb.append("  ").append(entry.getKey()).append(":\n");
            sb.append("    • Expected Complexity: ").append(metric.complexity).append("\n");
            sb.append("    • Memory Usage: ").append(metric.memoryUsage).append("\n");
            sb.append("    • Streaming Support: ").append(metric.streamingSupport ? "✅" : "❌").append("\n");
            sb.append("    • Bottleneck Risk: ").append(metric.bottleneckRisk).append("\n\n");
        }
        
        List<String> bottlenecks = analyzer.identifyBottlenecks(plan);
        if (!bottlenecks.isEmpty()) {
            sb.append("🚨 POTENTIAL BOTTLENECKS:\n");
            for (String bottleneck : bottlenecks) {
                sb.append("  ⚠️  ").append(bottleneck).append("\n");
            }
        }
        
        return sb.toString();
    }
    
    // Helper methods for tree visualization
    private static void visualizeTreeNode(StreamingQueryPlan plan, String prefix, boolean isLast, StringBuilder sb) {
        String connector = isLast ? LAST_BRANCH : BRANCH;
        sb.append(prefix).append(connector).append(" ");
        
        String planName = getPlanName(plan);
        String planSummary = getPlanSummary(plan);
        
        sb.append(planName);
        if (!planSummary.isEmpty()) {
            sb.append(" (").append(planSummary).append(")");
        }
        sb.append("\n");
        
        List<StreamingQueryPlan> children = getChildPlans(plan);
        String newPrefix = prefix + (isLast ? SPACE : VERTICAL);
        
        for (int i = 0; i < children.size(); i++) {
            boolean isChildLast = (i == children.size() - 1);
            visualizeTreeNode(children.get(i), newPrefix, isChildLast, sb);
        }
    }
    
    private static void visualizeVerboseNode(StreamingQueryPlan plan, String prefix, boolean isLast, StringBuilder sb, int depth) {
        String connector = isLast ? LAST_BRANCH : BRANCH;
        sb.append(prefix).append(connector).append(" ");
        
        String planName = getPlanName(plan);
        String planIcon = getPlanIcon(plan);
        
        sb.append(planIcon).append(" ").append(planName).append("\n");
        
        String newPrefix = prefix + (isLast ? SPACE : VERTICAL);
        
        // Add detailed information
        String details = getPlanDetails(plan);
        if (!details.isEmpty()) {
            for (String line : details.split("\n")) {
                if (!line.trim().isEmpty()) {
                    sb.append(newPrefix).append("   ").append(line).append("\n");
                }
            }
        }
        
        // Add input/output schema information
        SchemaInfo schema = getSchemaInfo(plan);
        if (schema != null) {
            sb.append(newPrefix).append("   📥 INPUT: ").append(schema.inputSchema).append("\n");
            sb.append(newPrefix).append("   📤 OUTPUT: ").append(schema.outputSchema).append("\n");
        }
        
        // Add performance characteristics
        String perfInfo = getPerformanceInfo(plan);
        if (!perfInfo.isEmpty()) {
            sb.append(newPrefix).append("   ⚡ PERFORMANCE: ").append(perfInfo).append("\n");
        }
        
        sb.append(newPrefix).append("\n");
        
        List<StreamingQueryPlan> children = getChildPlans(plan);
        for (int i = 0; i < children.size(); i++) {
            boolean isChildLast = (i == children.size() - 1);
            visualizeVerboseNode(children.get(i), newPrefix, isChildLast, sb, depth + 1);
        }
    }
    
    // Helper methods to extract plan information
    private static String getPlanName(StreamingQueryPlan plan) {
        return plan.getClass().getSimpleName();
    }
    
    private static String getPlanIcon(StreamingQueryPlan plan) {
        return switch (plan.getClass().getSimpleName()) {
            case "ScanPlan" -> "🔍";
            case "FilterPlan" -> "🎯";
            case "ProjectionPlan" -> "📋";
            case "AggregationPlan" -> "📊";
            case "SortPlan" -> "📈";
            case "JoinPlan", "HashJoinPlan", "NestedLoopJoinPlan" -> "🔗";
            case "FuzzyJoinPlan" -> "🔀";
            case "HavingPlan" -> "✅";
            case "GlobalVariableAssignmentPlan" -> "📝";
            case "RawQueryPlan" -> "⚙️";
            case "ShowEventsPlan", "ShowFieldsPlan" -> "📄";
            case "HelpPlan", "HelpFunctionPlan", "HelpGrammarPlan" -> "❓";
            default -> "📦";
        };
    }
    
    private static String getPlanSummary(StreamingQueryPlan plan) {
        return switch (plan) {
            case ScanPlan scan -> "table: " + getTableName(scan);
            case FilterPlan filter -> "condition: " + getConditionSummary(filter);
            case ProjectionPlan projection -> "columns: " + getProjectionSummary(projection);
            case AggregationPlan aggregation -> "aggregates: " + getAggregationSummary(aggregation);
            case SortPlan sort -> "order: " + getSortSummary(sort);
            case JoinPlan join -> "join: " + getJoinSummary(join);
            case HavingPlan having -> "having: " + getHavingSummary(having);
            default -> "";
        };
    }
    
    private static String getPlanDetails(StreamingQueryPlan plan) {
        StringBuilder details = new StringBuilder();
        
        switch (plan) {
            case ScanPlan scan -> {
                details.append("Table: ").append(getTableName(scan)).append("\n");
                details.append("Scan Type: ").append(getScanType(scan)).append("\n");
                details.append("Estimated Rows: ").append(getEstimatedRows(scan));
            }
            case FilterPlan filter -> {
                details.append("Condition: ").append(getConditionDetails(filter)).append("\n");
                details.append("Selectivity: ").append(getFilterSelectivity(filter)).append("\n");
                details.append("Pushdown: ").append(canPushDown(filter) ? "Yes" : "No");
            }
            case ProjectionPlan projection -> {
                details.append("Columns: ").append(getProjectionDetails(projection)).append("\n");
                details.append("Expressions: ").append(getExpressionCount(projection)).append("\n");
                details.append("Streaming: ").append(isStreamingProjection(projection) ? "Yes" : "No");
            }
            case AggregationPlan aggregation -> {
                details.append("Aggregates: ").append(getAggregationDetails(aggregation)).append("\n");
                details.append("Group By: ").append(getGroupByDetails(aggregation)).append("\n");
                details.append("Memory Usage: ").append(getAggregationMemoryUsage(aggregation));
            }
            case SortPlan sort -> {
                details.append("Order By: ").append(getSortDetails(sort)).append("\n");
                details.append("Sort Algorithm: ").append(getSortAlgorithm(sort)).append("\n");
                details.append("In-Memory: ").append(isInMemorySort(sort) ? "Yes" : "No");
            }
            default -> details.append("No detailed information available");
        }
        
        return details.toString();
    }
    
    private static List<StreamingQueryPlan> getChildPlans(StreamingQueryPlan plan) {
        List<StreamingQueryPlan> children = new ArrayList<>();
        
        // Use reflection or specific methods to get child plans
        if (plan instanceof AbstractStreamingPlan abstractPlan) {
            // Try to get child plans through common patterns
            try {
                var fields = abstractPlan.getClass().getDeclaredFields();
                for (var field : fields) {
                    field.setAccessible(true);
                    Object value = field.get(abstractPlan);
                    if (value instanceof StreamingQueryPlan childPlan) {
                        children.add(childPlan);
                    } else if (value instanceof List<?> list) {
                        for (Object item : list) {
                            if (item instanceof StreamingQueryPlan childPlan) {
                                children.add(childPlan);
                            }
                        }
                    }
                }
            } catch (Exception e) {
                // Silently ignore reflection errors
            }
        }
        
        return children;
    }
    
    private static List<StreamingQueryPlan> collectPlanLevels(StreamingQueryPlan plan) {
        List<StreamingQueryPlan> levels = new ArrayList<>();
        collectPlanLevelsRecursive(plan, levels, new HashSet<>());
        return levels;
    }
    
    private static void collectPlanLevelsRecursive(StreamingQueryPlan plan, List<StreamingQueryPlan> levels, Set<StreamingQueryPlan> visited) {
        if (visited.contains(plan)) return;
        visited.add(plan);
        
        levels.add(plan);
        
        for (StreamingQueryPlan child : getChildPlans(plan)) {
            collectPlanLevelsRecursive(child, levels, visited);
        }
    }
    
    private static String padRight(String str, int length) {
        return String.format("%-" + length + "s", str);
    }
    
    // Schema information helper
    private static class SchemaInfo {
        final String inputSchema;
        final String outputSchema;
        
        SchemaInfo(String inputSchema, String outputSchema) {
            this.inputSchema = inputSchema;
            this.outputSchema = outputSchema;
        }
    }
    
    private static SchemaInfo getSchemaInfo(StreamingQueryPlan plan) {
        // This would need actual schema information from the plan
        // For now, return placeholder information
        return new SchemaInfo("dynamic", "dynamic");
    }
    
    private static String getPerformanceInfo(StreamingQueryPlan plan) {
        return switch (plan.getClass().getSimpleName()) {
            case "ScanPlan" -> "Sequential scan, O(n)";
            case "FilterPlan" -> "Linear filter, O(n)";
            case "ProjectionPlan" -> "Column projection, O(n)";
            case "AggregationPlan" -> "Hash aggregation, O(n)";
            case "SortPlan" -> "External sort, O(n log n)";
            case "HashJoinPlan" -> "Hash join, O(n+m)";
            case "NestedLoopJoinPlan" -> "Nested loop, O(n*m)";
            default -> "Unknown complexity";
        };
    }
    
    // Placeholder methods for extracting plan-specific information
    // These would need to be implemented based on actual plan interfaces
    private static String getTableName(ScanPlan scan) { return "Events"; }
    private static String getScanType(ScanPlan scan) { return "Full Table Scan"; }
    private static String getEstimatedRows(ScanPlan scan) { return "Unknown"; }
    private static String getConditionSummary(FilterPlan filter) { return "complex"; }
    private static String getConditionDetails(FilterPlan filter) { return "Complex condition"; }
    private static String getFilterSelectivity(FilterPlan filter) { return "Unknown"; }
    private static boolean canPushDown(FilterPlan filter) { return false; }
    private static String getProjectionSummary(ProjectionPlan projection) { return "multiple"; }
    private static String getProjectionDetails(ProjectionPlan projection) { return "Multiple columns"; }
    private static String getExpressionCount(ProjectionPlan projection) { return "Unknown"; }
    private static boolean isStreamingProjection(ProjectionPlan projection) { return true; }
    private static String getAggregationSummary(AggregationPlan aggregation) { return "multiple"; }
    private static String getAggregationDetails(AggregationPlan aggregation) { return "Multiple aggregates"; }
    private static String getGroupByDetails(AggregationPlan aggregation) { return "None"; }
    private static String getAggregationMemoryUsage(AggregationPlan aggregation) { return "High"; }
    private static String getSortSummary(SortPlan sort) { return "multiple"; }
    private static String getSortDetails(SortPlan sort) { return "Multiple columns"; }
    private static String getSortAlgorithm(SortPlan sort) { return "External Sort"; }
    private static boolean isInMemorySort(SortPlan sort) { return false; }
    private static String getJoinSummary(JoinPlan join) { return "equi-join"; }
    private static String getHavingSummary(HavingPlan having) { return "complex"; }
    
    // Plan analyzer for statistics and optimization suggestions
    private static class PlanAnalyzer {
        
        PlanStats analyze(StreamingQueryPlan plan) {
            PlanStats stats = new PlanStats();
            analyzeRecursive(plan, stats, new HashSet<>(), 0);
            return stats;
        }
        
        private void analyzeRecursive(StreamingQueryPlan plan, PlanStats stats, Set<StreamingQueryPlan> visited, int depth) {
            if (visited.contains(plan)) return;
            visited.add(plan);
            
            stats.totalPlans++;
            stats.maxDepth = Math.max(stats.maxDepth, depth);
            stats.planTypes.add(plan.getClass().getSimpleName());
            stats.estimatedCost += getEstimatedCost(plan);
            
            for (StreamingQueryPlan child : getChildPlans(plan)) {
                analyzeRecursive(child, stats, visited, depth + 1);
            }
        }
        
        private int getEstimatedCost(StreamingQueryPlan plan) {
            return switch (plan.getClass().getSimpleName()) {
                case "ScanPlan" -> 10;
                case "FilterPlan" -> 5;
                case "ProjectionPlan" -> 3;
                case "AggregationPlan" -> 15;
                case "SortPlan" -> 20;
                case "HashJoinPlan" -> 25;
                case "NestedLoopJoinPlan" -> 50;
                default -> 5;
            };
        }
        
        List<String> getOptimizationSuggestions(StreamingQueryPlan plan) {
            List<String> suggestions = new ArrayList<>();
            
            // Add specific optimization suggestions based on plan analysis
            if (containsPlanType(plan, "NestedLoopJoinPlan")) {
                suggestions.add("Consider using HashJoin instead of NestedLoop join for better performance");
            }
            
            if (containsPlanType(plan, "SortPlan") && containsPlanType(plan, "AggregationPlan")) {
                suggestions.add("Consider combining sort and aggregation operations");
            }
            
            if (getDepth(plan) > 5) {
                suggestions.add("Query plan depth is high, consider query restructuring");
            }
            
            return suggestions;
        }
        
        Map<String, PerformanceMetrics> analyzePerformance(StreamingQueryPlan plan) {
            Map<String, PerformanceMetrics> metrics = new HashMap<>();
            analyzePerformanceRecursive(plan, metrics, new HashSet<>());
            return metrics;
        }
        
        private void analyzePerformanceRecursive(StreamingQueryPlan plan, Map<String, PerformanceMetrics> metrics, Set<StreamingQueryPlan> visited) {
            if (visited.contains(plan)) return;
            visited.add(plan);
            
            String planName = plan.getClass().getSimpleName();
            PerformanceMetrics metric = createPerformanceMetric(plan);
            metrics.put(planName, metric);
            
            for (StreamingQueryPlan child : getChildPlans(plan)) {
                analyzePerformanceRecursive(child, metrics, visited);
            }
        }
        
        private PerformanceMetrics createPerformanceMetric(StreamingQueryPlan plan) {
            String complexity = getPerformanceInfo(plan);
            String memoryUsage = getMemoryUsage(plan);
            boolean streamingSupport = plan.supportsStreaming();
            String bottleneckRisk = getBottleneckRisk(plan);
            
            return new PerformanceMetrics(complexity, memoryUsage, streamingSupport, bottleneckRisk);
        }
        
        private String getMemoryUsage(StreamingQueryPlan plan) {
            return switch (plan.getClass().getSimpleName()) {
                case "ScanPlan" -> "Low";
                case "FilterPlan" -> "Low";
                case "ProjectionPlan" -> "Low";
                case "AggregationPlan" -> "High";
                case "SortPlan" -> "High";
                case "HashJoinPlan" -> "Medium";
                case "NestedLoopJoinPlan" -> "Low";
                default -> "Unknown";
            };
        }
        
        private String getBottleneckRisk(StreamingQueryPlan plan) {
            return switch (plan.getClass().getSimpleName()) {
                case "ScanPlan" -> "Low";
                case "FilterPlan" -> "Low";
                case "ProjectionPlan" -> "Low";
                case "AggregationPlan" -> "Medium";
                case "SortPlan" -> "High";
                case "HashJoinPlan" -> "Medium";
                case "NestedLoopJoinPlan" -> "High";
                default -> "Unknown";
            };
        }
        
        List<String> identifyBottlenecks(StreamingQueryPlan plan) {
            List<String> bottlenecks = new ArrayList<>();
            
            if (containsPlanType(plan, "SortPlan")) {
                bottlenecks.add("Sort operation may require significant memory and processing time");
            }
            
            if (containsPlanType(plan, "NestedLoopJoinPlan")) {
                bottlenecks.add("Nested loop join has O(n*m) complexity and may be slow for large datasets");
            }
            
            if (containsPlanType(plan, "AggregationPlan")) {
                bottlenecks.add("Aggregation operations may require materializing intermediate results");
            }
            
            return bottlenecks;
        }
        
        private boolean containsPlanType(StreamingQueryPlan plan, String planType) {
            return containsPlanTypeRecursive(plan, planType, new HashSet<>());
        }
        
        private boolean containsPlanTypeRecursive(StreamingQueryPlan plan, String planType, Set<StreamingQueryPlan> visited) {
            if (visited.contains(plan)) return false;
            visited.add(plan);
            
            if (plan.getClass().getSimpleName().equals(planType)) {
                return true;
            }
            
            for (StreamingQueryPlan child : getChildPlans(plan)) {
                if (containsPlanTypeRecursive(child, planType, visited)) {
                    return true;
                }
            }
            
            return false;
        }
        
        private int getDepth(StreamingQueryPlan plan) {
            return getDepthRecursive(plan, new HashSet<>());
        }
        
        private int getDepthRecursive(StreamingQueryPlan plan, Set<StreamingQueryPlan> visited) {
            if (visited.contains(plan)) return 0;
            visited.add(plan);
            
            int maxChildDepth = 0;
            for (StreamingQueryPlan child : getChildPlans(plan)) {
                maxChildDepth = Math.max(maxChildDepth, getDepthRecursive(child, visited));
            }
            
            return maxChildDepth + 1;
        }
    }
    
    // Support classes for analysis
    private static class PlanStats {
        int totalPlans = 0;
        int maxDepth = 0;
        Set<String> planTypes = new HashSet<>();
        int estimatedCost = 0;
    }
    
    private static class PerformanceMetrics {
        final String complexity;
        final String memoryUsage;
        final boolean streamingSupport;
        final String bottleneckRisk;
        
        PerformanceMetrics(String complexity, String memoryUsage, boolean streamingSupport, String bottleneckRisk) {
            this.complexity = complexity;
            this.memoryUsage = memoryUsage;
            this.streamingSupport = streamingSupport;
            this.bottleneckRisk = bottleneckRisk;
        }
    }
}
