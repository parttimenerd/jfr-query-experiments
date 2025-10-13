package me.bechberger.jfr.duckdb;

import java.nio.file.Path;

public enum JFRFile {
    /** Small default.jfc recording */
    DEFAULT("default.jfr"),
    /** Recording from a container */
    CONTAINER("container.jfr"),
    /** Recording with lots of enabled events and a class modifying agent on Linux */
    METAL("metal.jfr");
    private final Path path;

    JFRFile(String fileName) {
        this.path = Path.of("jfr_files").resolve(fileName);
    }

    public Path getPath() {
        return path;
    }
}
