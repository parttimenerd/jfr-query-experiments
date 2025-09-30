package me.bechberger.jfr.duckdb;

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
        ViewCollection.View view = ViewCollection.getView(viewName);
        if (view == null) {
            throw new IllegalArgumentException("View not found: " + viewName);
        }

        String key = jfrFile.toAbsolutePath() + "|" + viewName;
        String cached = props.getProperty(key);
        if (cached != null) {
            return cached;
        }

        Process process = Runtime.getRuntime().exec(new String[]{
                "jfr", "view", view.relatedJFRView(), jfrFile.toAbsolutePath().toString()
        });
        int exit = process.waitFor();
        if (exit != 0) {
            throw new IOException("Error executing jfr view command, exit code: " + exit);
        }
        String result = new String(process.getInputStream().readAllBytes());

        props.setProperty(key, result);
        try (FileOutputStream out = new FileOutputStream(cachePath.toFile())) {
            props.store(out, "JFR view cache: key = absolute jfr path | view name");
        }

        return result;
    }
}