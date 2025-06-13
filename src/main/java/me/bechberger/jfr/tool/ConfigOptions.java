package me.bechberger.jfr.tool;

import me.bechberger.jfr.query.Configuration;
import picocli.CommandLine;

public class ConfigOptions {
    @CommandLine.Option(names = "--verbose", description = "Display additional information about the query execution")
     boolean verbose = false;

    @CommandLine.Option(names = "--width", description = "Maximum number of horizontal characters")
     Integer width = null;

    @CommandLine.Option(names = "--truncate", description = "Truncate mode (BEGINNING or END)")
     Configuration.Truncate truncate = null;

    @CommandLine.Option(names = "--cell-height", description = "Maximum height for cells")
     Integer cellHeight = null;

    @CommandLine.Option(names = "--maxage", description = "Length of time for the query to span, in (s)econds, (m)inutes, (h)ours, or (d)ays, e.g. 60m, or 0 for no limit")
     String maxAge = null;

    public void init(Configuration configuration) {
        configuration.verbose = verbose;
        configuration.width = width != null ? width : Configuration.PREFERRED_WIDTH;
        configuration.truncate = truncate != null ? truncate : Configuration.Truncate.END;
        configuration.cellHeight = cellHeight != null ? cellHeight : 1; // Default to 1 row height
    }
}
