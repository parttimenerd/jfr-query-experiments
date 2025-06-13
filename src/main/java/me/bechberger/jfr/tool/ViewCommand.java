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
import me.bechberger.jfr.query.ViewPrinter;
import me.bechberger.jfr.util.Output.BufferedPrinter;
import me.bechberger.jfr.util.UserDataException;
import me.bechberger.jfr.util.UserSyntaxException;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.Callable;

@Command(
        name = "view",
        mixinStandardHelpOptions = true,
        version = "0.1",
        description = "Display event values in a recording file (.jfr) in predefined views"
)
public final class ViewCommand implements Callable<Integer>, Footerable {
    @CommandLine.Mixin
    private ConfigOptions configOptions;

    @Parameters(index = "0", description = "Name of the view or event type to display")
    private String view;

    @Parameters(index = "1", description = "Location of the recording file (.jfr)")
    private Path file;

    @Override
    public Integer call() throws UserSyntaxException, UserDataException {
        try {
            Configuration configuration = new Configuration();
            BufferedPrinter printer = new BufferedPrinter(System.out);
            configuration.output = printer;
            configOptions.init(configuration);
            try (EventStream stream = EventStream.openFile(file)) {
                ViewPrinter vp = new ViewPrinter(configuration, stream);
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

    @Override
    public String footer() {
        return """
                Available views:
                %s
                The <view> parameter can be an event type name. Use 'all-views' to display all views
                or 'all-events' to display all events.
                Examples:
                  $ jfr view gc recording.jfr

                  $ jfr view --width 160 hot-methods recording.jfr

                  $ jfr view --verbose allocation-by-class recording.jfr

                  $ jfr view contention-by-site recording.jfr

                  $ jfr view jdk.GarbageCollection recording.jfr

                  $ jfr view --cell-height 10 ThreadStart recording.jfr

                  $ jfr view --truncate beginning SystemProcess recording.jfr
                """.formatted(ViewPrinter.getAvailableViews());
    }
}