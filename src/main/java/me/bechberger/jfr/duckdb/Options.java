package me.bechberger.jfr.duckdb;

import picocli.CommandLine;

/**
 *
 */
public class Options {

    public enum ExcludableItems {
        /**
         * Just have a package string
         */
        PACKAGE_HIERARCHY,
    }

    @CommandLine.Option(names = {"-e", "--exclude"}, description = "Exclude certain items from the import, possible values: ${COMPLETION-CANDIDATES}")
    private ExcludableItems[] exclude = new ExcludableItems[0];

    @CommandLine.Option(names = {"-s", "--stacktrace-depth"}, description = "Maximum stack trace depth to import (default: ${DEFAULT-VALUE})", defaultValue = "10")
    private int maxStackTraceDepth = 10;

    @CommandLine.Option(names = {"--complex-descriptors"}, description = "Use package names in descriptors", defaultValue = "false")
    private boolean complexDecorators = false;

    private boolean useExamplesForLLM = false;

    public boolean isExcluded(ExcludableItems item) {
        for (ExcludableItems ex : exclude) {
            if (ex == item) {
                return true;
            }
        }
        return false;
    }

    public Options() {
    }

    public Options(ExcludableItems... exclude) {
        this.exclude = exclude;
    }

    public int getMaxStackTraceDepth() {
        return maxStackTraceDepth;
    }

    public boolean useComplexDecorators() {
        return complexDecorators;
    }

    public boolean useExamplesForLLM() {
        return useExamplesForLLM;
    }

    public void setUseExamplesForLLM(boolean useExamplesForLLM) {
        this.useExamplesForLLM = useExamplesForLLM;
    }
}