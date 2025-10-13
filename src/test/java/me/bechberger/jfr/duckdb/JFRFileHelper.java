package me.bechberger.jfr.duckdb;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.duckdb.DuckDBConnection;

class JFRFileHelper {
    private final JFRViewResultCache jfrViewResultCache;
    private final Path dbFile;
    private final List<DuckDBConnection> openConnections = new ArrayList<>();

    public JFRFileHelper(JFRFile jfrFile) throws IOException, SQLException {
        if (!Files.exists(jfrFile.getPath())) {
            throw new IOException("Test file not found: " + jfrFile.getPath().toAbsolutePath());
        }

        // Create cached database file next to the JFR file
        String jfrFileName = jfrFile.getPath().getFileName().toString();
        String dbFileName = jfrFileName.replaceAll("\\.[^.]+$", ".db");
        this.dbFile = jfrFile.getPath().getParent().resolve(dbFileName);

        // Check if we can use cached database or need full import
        if (shouldUseCachedDatabase()) {
            // Database exists and is newer than source files, just add macros and views
            try (DuckDBConnection connection =
                    (DuckDBConnection)
                            java.sql.DriverManager.getConnection(
                                    "jdbc:duckdb:" + dbFile.toAbsolutePath())) {
                BasicParallelImporter.addMacrosAndViews(connection);
            }
        } else {
            // Need full import
            Files.deleteIfExists(dbFile);
            Options options = new Options();
            options.setComplexDescriptors(true); // Use complex descriptors for tests
            BasicParallelImporter.createFile(jfrFile.getPath(), dbFile.toAbsolutePath(), options);
        }

        this.jfrViewResultCache = new JFRViewResultCache(jfrFile.getPath());
    }

    private boolean shouldUseCachedDatabase() throws IOException {
        if (!Files.exists(dbFile)) {
            return false;
        }

        long dbModTime = Files.getLastModifiedTime(dbFile).toMillis();

        // Check all non-test source files, excluding MacroCollection, ViewCollection, and *Command
        // files
        Path srcDir = Paths.get("src/main/java");
        if (!Files.exists(srcDir)) {
            return true; // No source directory, use cached version
        }

        try (Stream<Path> files = Files.walk(srcDir)) {
            return files.filter(Files::isRegularFile)
                    .filter(path -> path.toString().endsWith(".java"))
                    .filter(path -> !path.getFileName().toString().equals("MacroCollection.java"))
                    .filter(path -> !path.getFileName().toString().equals("ViewCollection.java"))
                    .filter(path -> !path.getFileName().toString().endsWith("Command.java"))
                    .allMatch(
                            path -> {
                                try {
                                    return Files.getLastModifiedTime(path).toMillis() < dbModTime;
                                } catch (IOException e) {
                                    return false; // If we can't read the file time, consider it
                                    // newer
                                }
                            });
        }
    }

    public String executeJFR(String view) throws IOException, InterruptedException {
        return jfrViewResultCache.execute(view);
    }

    public void cleanup() {
        for (DuckDBConnection con : openConnections) {
            try {
                con.close();
            } catch (SQLException e) {
                // ignore
            }
        }
        openConnections.clear();
    }

    public DuckDBConnection openConnection() throws SQLException {
        DuckDBConnection con =
                (DuckDBConnection)
                        java.sql.DriverManager.getConnection(
                                "jdbc:duckdb:" + dbFile.toAbsolutePath());
        openConnections.add(con);
        return con;
    }
}
