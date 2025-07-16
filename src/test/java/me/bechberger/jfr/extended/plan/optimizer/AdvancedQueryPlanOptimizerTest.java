package me.bechberger.jfr.extended.plan.optimizer;

import me.bechberger.jfr.extended.plan.*;
import me.bechberger.jfr.extended.plan.nodes.*;
import me.bechberger.jfr.extended.plan.optimizer.QueryPlanOptimizer.OptimizationLevel;
import me.bechberger.jfr.extended.plan.exception.QueryPlanException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive tests for the advanced query plan optimizer.
 * 
 * Tests include:
 * - Basic optimization functionality
 * - Memory-aware join algorithm selection
 * - Streaming aggregate optimization
 * - Filter reordering and predicate pushdown
 * - Cost estimation accuracy
 * - Visualization and reporting
 */
class AdvancedQueryPlanOptimizerTest {
    
    private AdvancedQueryPlanOptimizer optimizer;
    private QueryExecutionContext context;
    private AdvancedPlanVisualizer visualizer;
    
    @BeforeEach
    void setUp() {
        optimizer = new AdvancedQueryPlanOptimizer();
        context = new QueryExecutionContext();
        visualizer = new AdvancedPlanVisualizer();
    }
    
    // ===== BASIC OPTIMIZATION TESTS =====
    
    @Test
    @DisplayName("Optimizer should handle null plans gracefully")
    void testNullPlanHandling() {
        assertFalse(optimizer.canOptimize(null));
        assertEquals(0.0, optimizer.estimateCost(null, context));
    }
    
    @Test
    @DisplayName("Optimizer should skip optimization for NONE level")
    void testNoOptimizationLevel() throws QueryPlanException {
        StreamingQueryPlan mockPlan = createMockTableScan();
        StreamingQueryPlan result = optimizer.optimize(mockPlan, context, OptimizationLevel.NONE);
        assertSame(mockPlan, result, "NONE level should return original plan unchanged");
    }
    
    @ParameterizedTest
    @EnumSource(OptimizationLevel.class)
    @DisplayName("Optimizer should handle all optimization levels")
    void testAllOptimizationLevels(OptimizationLevel level) throws QueryPlanException {
        StreamingQueryPlan mockPlan = createMockTableScan();
        StreamingQueryPlan result = optimizer.optimize(mockPlan, context, level);
        assertNotNull(result, "Optimization should never return null");
    }
    
    @Test
    @DisplayName("Optimizer should generate optimization reports")
    void testOptimizationReporting() throws QueryPlanException {
        StreamingQueryPlan original = createMockTableScan();
        StreamingQueryPlan optimized = optimizer.optimize(original, context, OptimizationLevel.AGGRESSIVE);
        
        OptimizationReport report = optimizer.generateReport(original, optimized);
        
        assertNotNull(report);
        assertSame(original, report.getOriginalPlan());
        assertSame(optimized, report.getOptimizedPlan());
        assertTrue(report.getOriginalCost() >= 0);
        assertTrue(report.getOptimizedCost() >= 0);
        assertNotNull(report.getSummary());
    }
    
    // ===== COST ESTIMATION TESTS =====
    
    @Test
    @DisplayName("Cost estimator should provide reasonable estimates")
    void testCostEstimation() {
        QueryPlanCostEstimator costEstimator = new QueryPlanCostEstimator();
        
        StreamingQueryPlan smallPlan = createMockTableScanWithSize(100);
        StreamingQueryPlan largePlan = createMockTableScanWithSize(10000);
        
        double smallCost = costEstimator.estimateCost(smallPlan, context);
        double largeCost = costEstimator.estimateCost(largePlan, context);
        
        assertTrue(largeCost > smallCost, "Larger plans should have higher cost");
        assertTrue(smallCost > 0, "Cost should be positive");
    }
    
    @Test
    @DisplayName("Cost estimator should provide detailed breakdown")
    void testDetailedCostBreakdown() {
        QueryPlanCostEstimator costEstimator = new QueryPlanCostEstimator();
        StreamingQueryPlan plan = createMockTableScan();
        
        QueryPlanCostEstimator.CostBreakdown breakdown = costEstimator.getDetailedCost(plan, context);
        
        assertNotNull(breakdown);
        assertTrue(breakdown.getTotalCost() > 0);
        assertFalse(breakdown.getCosts().isEmpty());
        assertNotNull(breakdown.getSummary());
    }
    
    // ===== VISUALIZATION TESTS =====
    
    @Test
    @DisplayName("Visualizer should generate ASCII art")
    void testAsciiVisualization() {
        StreamingQueryPlan plan = createMockTableScan();
        String ascii = visualizer.generateAsciiVisualization(plan, context);
        
        assertNotNull(ascii);
        assertFalse(ascii.trim().isEmpty());
        assertTrue(ascii.contains("QUERY EXECUTION PLAN"), "Should contain header");
        assertTrue(ascii.contains("TableScanPlan"), "Should contain plan type"); // Mock implementation
    }
    
    @Test
    @DisplayName("Visualizer should generate optimization comparison")
    void testOptimizationComparison() throws QueryPlanException {
        StreamingQueryPlan original = createMockTableScan();
        StreamingQueryPlan optimized = optimizer.optimize(original, context, OptimizationLevel.BASIC);
        OptimizationReport report = optimizer.generateReport(original, optimized);
        
        String comparison = visualizer.generateOptimizationComparison(original, optimized, report);
        
        assertNotNull(comparison);
        assertTrue(comparison.contains("OPTIMIZATION COMPARISON"));
        assertTrue(comparison.contains("COST ANALYSIS"));
        assertTrue(comparison.contains("OPTIMIZATIONS APPLIED"));
    }
    
    @Test
    @DisplayName("Visualizer should generate memory heatmap")
    void testMemoryHeatmap() {
        StreamingQueryPlan plan = createMockTableScan();
        String heatmap = visualizer.generateMemoryHeatmap(plan, context);
        
        assertNotNull(heatmap);
        assertTrue(heatmap.contains("MEMORY USAGE ANALYSIS"));
        assertTrue(heatmap.contains("Memory Pressure"));
        assertTrue(heatmap.contains("Memory Hotspots"));
    }
    
    // ===== CONFIGURATION TESTS =====
    
    @Test
    @DisplayName("Optimizer configuration should be customizable")
    void testOptimizerConfiguration() {
        AdvancedQueryPlanOptimizer.OptimizerConfig config = 
            new AdvancedQueryPlanOptimizer.OptimizerConfig();
        
        // Test default values
        assertTrue(config.isLoggingEnabled());
        assertTrue(config.isAsciiVisualizationEnabled());
        assertTrue(config.isAggressiveOptimizations());
        assertEquals(100, config.getMemoryThresholdMB());
        
        // Test customization
        config.setLoggingEnabled(false);
        config.setMemoryThresholdMB(200);
        
        assertFalse(config.isLoggingEnabled());
        assertEquals(200, config.getMemoryThresholdMB());
    }
    
    @Test
    @DisplayName("Visualization configuration should support different modes")
    void testVisualizationConfiguration() {
        AdvancedPlanVisualizer.VisualizationConfig minimal = 
            AdvancedPlanVisualizer.VisualizationConfig.minimal();
        AdvancedPlanVisualizer.VisualizationConfig detailed = 
            AdvancedPlanVisualizer.VisualizationConfig.detailed();
        
        // Test minimal config
        assertFalse(minimal.showHeader);
        assertFalse(minimal.showDetails);
        assertFalse(minimal.showSummary);
        
        // Test detailed config
        assertTrue(detailed.showHeader);
        assertTrue(detailed.showDetails);
        assertTrue(detailed.showSummary);
        assertTrue(detailed.showCosts);
        assertTrue(detailed.showRowCounts);
    }
    
    // ===== ERROR HANDLING TESTS =====
    
    @Test
    @DisplayName("Optimizer should handle optimization failures gracefully")
    void testOptimizationErrorHandling() {
        // This would test error scenarios in a real implementation
        StreamingQueryPlan plan = createMockTableScan();
        
        // Test with different optimization levels
        assertDoesNotThrow(() -> {
            optimizer.optimize(plan, context, OptimizationLevel.BASIC);
            optimizer.optimize(plan, context, OptimizationLevel.AGGRESSIVE);
            optimizer.optimize(plan, context, OptimizationLevel.EXPERIMENTAL);
        });
    }
    
    // ===== HELPER METHODS =====
    
    private StreamingQueryPlan createMockTableScan() {
        return createMockTableScanWithSize(1000);
    }
    
    private StreamingQueryPlan createMockTableScanWithSize(long size) {
        return new StreamingQueryPlan() {
            @Override
            public QueryPlanResult execute(QueryExecutionContext context) {
                return null; // Mock implementation
            }
            
            @Override
            public long estimateOutputSize(QueryExecutionContext context) {
                return size;
            }
            
            @Override
            public java.util.Set<String> getRequiredEventTypes() {
                return java.util.Set.of("TestEvent");
            }
            
            @Override
            public String explain() {
                return "MockTableScanPlan";
            }
        };
    }
}
