package me.bechberger.jfr.duckdb;

import picocli.CommandLine;

/**
 *
 */
public class Options {

    public enum ExcludableItems {
        /**
         * Exclude all stack traces, usually not needed for GC analysis
         */
        STACK_TRACES,
        CLASS_LOADERS,
        MODULES,
        /**
         * Just have a package string
         */
        PACKAGE_HIERARCHY,
        THREAD_GROUPS,
    }

    @CommandLine.Option(names = {"-e", "--exclude"}, description = "Exclude certain items from the import, possible values: ${COMPLETION-CANDIDATES}")
    private ExcludableItems[] exclude = new ExcludableItems[0];

    @CommandLine.Option(names = "--exclude-all", description = "Exclude all optional items")
    private boolean excludeAll = false;

    @CommandLine.Option(names = "--exclude-all-but-stacktraces", description = "Exclude all optional items except stack traces")
    private boolean excludeAllButStackTraces = false;

    @CommandLine.Option(names = "--limit-depth", description = "Limit stack traces to a maximum depth of the given value (default: ${DEFAULT-VALUE})")
    public int limitStackTraceDepth = 100;

    @CommandLine.Option(names = "--limit-from-bottom", description = "When limiting stack traces, limit from the bottom (the oldest frames) (default: ${DEFAULT-VALUE})")
    public boolean limitFromTop = false;

    public boolean isExcluded(ExcludableItems item) {
        if (excludeAll) {
            return true;
        }
        if (excludeAllButStackTraces && item != ExcludableItems.STACK_TRACES) {
            return true;
        }
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

    public static Options excludeAll() {
        return new Options(ExcludableItems.values());
    }

    public static Options excludeAllButStackTraces() {
        Options options = new Options();
        options.excludeAllButStackTraces = true;
        return options;
    }
}