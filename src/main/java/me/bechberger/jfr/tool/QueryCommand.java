/*
 * Copyright (c) 2023, Oracle and/or its affiliates. All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This code is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License version 2 only, as
 * published by the Free Software Foundation.  Oracle designates this
 * particular file as subject to the "Classpath" exception as provided
 * by Oracle in the LICENSE file that accompanied this code.
 *
 * This code is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * version 2 for more details (a copy is included in the LICENSE file that
 * accompanied this code).
 *
 * You should have received a copy of the GNU General Public License version
 * 2 along with this work; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Please contact Oracle, 500 Oracle Parkway, Redwood Shores, CA 94065 USA
 * or visit www.oracle.com if you need additional information or have any
 * questions.
 */

package me.bechberger.jfr.tool;

import jdk.jfr.consumer.EventStream;
import me.bechberger.jfr.query.Configuration;
import me.bechberger.jfr.query.Configuration.Truncate;
import me.bechberger.jfr.query.QueryPrinter;
import me.bechberger.jfr.query.ViewPrinter;
import me.bechberger.jfr.util.Output.BufferedPrinter;
import me.bechberger.jfr.util.UserDataException;
import me.bechberger.jfr.util.UserSyntaxException;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.Callable;

@Command(
        name = "query",
        mixinStandardHelpOptions = true,
        version = "0.1",
        description = "Execute JFR queries against recording files"
)
public final class QueryCommand implements Callable<Integer>, Footerable {

    @Option(names = "--verbose", description = "Display additional information about the query execution")
    private boolean verbose = false;

    @Option(names = "--width", description = "Maximum number of horizontal characters")
    private Integer width = null;

    @Option(names = "--truncate", description = "Truncate mode (BEGINNING or END)")
    private Truncate truncate = null;

    @Option(names = "--cell-height", description = "Maximum height for cells")
    private Integer cellHeight = null;

    @Option(names = "--maxage", description = "Length of time for the query to span, in (s)econds, (m)inutes, (h)ours, or (d)ays, e.g. 60m, or 0 for no limit")
    private String maxAge = null;

    @Option(names = "--maxsize", description = "Maximum size for the query to span, in (M)B or (G)B, e.g. 500M, or 0 for no limit")
    private String maxSize = null;

    @Parameters(index = "0", description = "The view or query to execute")
    private String view;

    @Parameters(index = "1", description = "The JFR file to analyze")
    private Path file;

    @Override
    public String footer() {
        return """
            Examples:
              $ jcmd <pid> JFR.query "types"

              $ jcmd <pid> JFR.query "SHOW FIELDS ObjectAllocationSample"

              $ jcmd <pid> JFR.query "SELECT * FROM ObjectAllocationSample"
                            verbose=true maxsize=10M

              $ jcmd <pid> JFR.query "SELECT pid, path FROM SystemProcess"
                            width=100

              $ jcmd <pid> JFR.query "SELECT stackTrace.topFrame AS T, SUM(weight)
                            FROM ObjectAllocationSample GROUP BY T"
                            maxage=100s

              $ jcmd <pid> JFR.query "CAPTION 'Method', 'Percentage'
                            FORMAT default, normalized;width:10
                            SELECT stackTrace.topFrame AS T, COUNT(*) AS C
                            GROUP BY T
                            FROM ExecutionSample ORDER BY C DESC"

              $ jcmd <pid> JFR.query "CAPTION 'Start', 'GC ID', 'Heap Before GC',
                            'Heap After GC', 'Longest Pause'
                            SELECT G.startTime, G.gcId, B.heapUsed,
                                   A.heapUsed, longestPause
                            FROM GarbageCollection AS G,
                                 GCHeapSummary AS B,
                                 GCHeapSummary AS A
                            WHERE B.when = 'Before GC' AND A.when = 'After GC'
                            GROUP BY gcId
                            ORDER BY G.startTime"

            """ + QueryPrinter.getGrammarText();
    }

    @Override
    public Integer call() throws UserSyntaxException, UserDataException {
        try {
            Configuration configuration = new Configuration();
            BufferedPrinter printer = new BufferedPrinter(System.out);
            configuration.output = printer;

            // Set configuration options
            if (verbose) {
                configuration.verbose = true;
            }

            if (truncate != null) {
                configuration.truncate = truncate;
            }

            if (cellHeight != null) {
                configuration.cellHeight = cellHeight;
            }

            if (width != null) {
                configuration.width = width;
            }

            try (EventStream stream = EventStream.openFile(file)) {
                QueryPrinter vp = new QueryPrinter(configuration, stream);
                vp.execute(view);
                printer.flush();
                return 0;
            } catch (IOException ioe) {
                System.err.println("Could not read file: " + file + ": " + ioe.getMessage());
                return 1;
            }
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            return 1;
        }
    }
}