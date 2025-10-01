package me.bechberger.jfr.duckdb;

import me.bechberger.jfr.duckdb.definitions.View;
import me.bechberger.jfr.duckdb.definitions.ViewCollection;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

/**
 * Simple test-side cache for results of `jfr view`.
 * Keys are: absolute jfr path + "|" + view name
 */
public class JFRViewResultCache {
    private final Path jfrFile;
    private final Path cachePath = Path.of(".jfr.cache");
    private final Properties props = new Properties();

    public JFRViewResultCache(Path jfrFile) {
        this.jfrFile = jfrFile;
        if (Files.exists(cachePath)) {
            try (FileInputStream in = new FileInputStream(cachePath.toFile())) {
                props.load(in);
            } catch (IOException e) {
                throw new RuntimeException("Error loading cache file: " + cachePath.toAbsolutePath(), e);
            }
        }
    }

    public String execute(String viewName) throws IOException, InterruptedException {
        View view = ViewCollection.getView(viewName);
        if (view == null) {
            throw new IllegalArgumentException("View not found: " + viewName);
        }

        String key = jfrFile.toAbsolutePath() + "|" + viewName;
        String cached = props.getProperty(key);
        if (cached != null) {
            return cached;
        }

        System.out.println("Executing jfr view for the first time, caching result: jfr view " + viewName + " " + jfrFile.toAbsolutePath());

        // Use ProcessBuilder for better process management
        ProcessBuilder processBuilder = new ProcessBuilder(
                "jfr", "view", view.relatedJFRView(), jfrFile.toAbsolutePath().toString()
        );

        Process process = processBuilder.start();

        // Read stdout while the process is running
        StringBuilder outputBuilder = new StringBuilder();
        try (var reader = process.inputReader()) {
            String line;
            while ((line = reader.readLine()) != null) {
                outputBuilder.append(line).append("\n");
            }
        }

        int exit = process.waitFor();
        if (exit != 0) {
            // Also read stderr for error information
            String errorOutput;
            try (var errorReader = process.errorReader()) {
                errorOutput = errorReader.lines()
                        .reduce("", (a, b) -> a + b + "\n");
            }
            throw new IOException("Error executing jfr view command, exit code: " + exit +
                    ", stderr: " + errorOutput);
        }

        String result = outputBuilder.toString();

        props.setProperty(key, result);
        try (FileOutputStream out = new FileOutputStream(cachePath.toFile())) {
            props.store(out, "JFR view cache: key = absolute jfr path | view name");
        }

        return result;
    }
}