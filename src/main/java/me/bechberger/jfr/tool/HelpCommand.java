/*
 * Copyright (c) 2016, 2023, Oracle and/or its affiliates. All rights reserved.
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

import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

import java.util.concurrent.Callable;

@Command(
        name = "help",
        aliases = {"--help", "-h", "-?"},
        mixinStandardHelpOptions = true,
        version = "0.1",
        description = "Display all available commands, or help about a specific command",
        footer = "Examples:%n" +
                "  $ jfr help%n" +
                "  $ jfr help view%n" +
                "  $ jfr help query"
)
public final class HelpCommand implements Callable<Integer> {

    @Parameters(index = "0", arity = "0..1", description = "The name of the command to get help for")
    private String commandName;

    @Override
    public Integer call() {
        try {
            if (commandName == null || commandName.isEmpty()) {
                // Display help for all available commands
                // This would need to be adapted to work with the picocli command structure
                System.out.println("Available commands:");
                // We would list all commands here in a picocli-based implementation
                return 0;
            }

            // Show help for specific command
            // In a picocli application, we would retrieve the command and show its help
            System.out.println("Help for command '" + commandName + "':");
            // Code to display command-specific help would go here
            return 0;

        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            return 1;
        }
    }
}