package me.bechberger.jfr.tool;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import jdk.jfr.consumer.EventStream;
import me.bechberger.jfr.query.Configuration;
import me.bechberger.jfr.query.QueryPrinter;
import me.bechberger.jfr.util.Output.BufferedPrinter;

import picocli.CommandLine;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.StringWriter;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;

@CommandLine.Command(
        name = "web",
        mixinStandardHelpOptions = true,
        description = "Start a web server to query JFR files"
)
public class WebCommand implements Callable<Integer>, Footerable {

    @CommandLine.Mixin
    private ConfigOptions configOptions;

    @CommandLine.Option(names = "--port", description = "Port to run the web server on (default: 8080)")
    private int port = 8080;

    @CommandLine.Option(names = "--host", description = "Host to bind the web server to (default: localhost)")
    private String host = "localhost";

    @CommandLine.Parameters(index = "0", description = "The JFR file to serve")
    private Path file;

    @Override
    public Integer call() {
        try {

            // Validate file exists
            if (!file.toFile().exists()) {
                System.err.println("Error: File not found: " + file);
                return 1;
            }

            // Start HTTP server
            HttpServer server = HttpServer.create(new InetSocketAddress(host, port), 0);

            // Add query endpoint
            server.createContext("/query", exchange -> handleQueryRequest(exchange, file));

            // Add root endpoint with simple UI
            server.createContext("/", exchange -> {
                exchange.getResponseHeaders().set("Content-Type", "text/html");
                sendResponse(exchange, 200, getSimpleUI());
            });

            server.start();
            System.out.printf("Web server started at http://%s:%d%n", host, port);
            System.out.println("Press Ctrl+C to stop");

            // Keep the application running
            Thread.currentThread().join();

            return 0;
        } catch (Exception e) {
            System.err.println("Error starting server: " + e.getMessage());
            return 1;
        }
    }

    private void handleQueryRequest(HttpExchange exchange, Path file) throws IOException {
        try {
            // Parse query parameters
            Map<String, String> params = parseQueryParameters(exchange.getRequestURI());
            String query = params.get("q");

            if (query == null || query.isBlank()) {
                sendResponse(exchange, 400, "Missing query parameter 'q'");
                return;
            }

            if (query.equals("grammar")) {
                sendResponse(exchange, 200, QueryPrinter.getGrammarText());
                return;
            }
            if (query.equals("views")) {
                // read the views from views.ini in the resources folder
                StringWriter writer = new StringWriter();
                try (var stream = this.getClass().getResourceAsStream("/view.ini")) {
                    if (stream == null) {
                        sendResponse(exchange, 500, "Views file not found");
                        return;
                    }
                    writer.write(new String(stream.readAllBytes(), StandardCharsets.UTF_8));
                } catch (IOException e) {
                    sendResponse(exchange, 500, "Error reading views file: " + e.getMessage());
                    return;
                }
                exchange.getResponseHeaders().set("Content-Type", "text/plain");
                sendResponse(exchange, 200, writer.toString());
                return;
            }

            // Execute query
            ByteArrayOutputStream buffer = new ByteArrayOutputStream();
            BufferedPrinter printer = new BufferedPrinter(new PrintStream(buffer));
            // Initialize configuration
            Configuration configuration = new Configuration();
            configOptions.init(configuration);
            configuration.width = configOptions.width != null ? configOptions.width : 200;
            configuration.output = printer;

            try (EventStream stream = EventStream.openFile(file)) {
                QueryPrinter queryPrinter = new QueryPrinter(configuration, stream);
                queryPrinter.execute(query);
                printer.flush();
                exchange.getResponseHeaders().set("Content-Type", "text/plain");
                sendResponse(exchange, 200, buffer.toString());
            } catch (Exception e) {
                sendResponse(exchange, 400, "Query error: " + e.getMessage());
            }
        } catch (Exception e) {
            sendResponse(exchange, 500, "Server error: " + e.getMessage());
        }
    }

    private Map<String, String> parseQueryParameters(URI uri) {
        Map<String, String> params = new HashMap<>();
        String query = uri.getQuery();

        if (query != null && !query.isEmpty()) {
            String[] pairs = query.split("&");
            for (String pair : pairs) {
                int idx = pair.indexOf("=");
                if (idx > 0) {
                    String key = URLDecoder.decode(pair.substring(0, idx), StandardCharsets.UTF_8);
                    String value = URLDecoder.decode(pair.substring(idx + 1), StandardCharsets.UTF_8);
                    params.put(key, value);
                }
            }
        }

        return params;
    }

    private void sendResponse(HttpExchange exchange, int statusCode, String response) throws IOException {
        var bytes = response.getBytes();
        exchange.sendResponseHeaders(statusCode, bytes.length);
        try (var os = exchange.getResponseBody()) {
            os.write(bytes);
        }
    }

    private String getSimpleUI() {
        // load from resources file webui.html
        var resource = this.getClass().getResource("/webui.html");
        if (resource == null) {
            return "<html><body><h1>Error: Web UI not found</h1></body></html>";
        }
        try (var stream = resource.openStream()) {
            return new String(stream.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String footer() {
        return """
                Examples:
                  $ jfr web recording.jfr
                  $ jfr web --port 9090 recording.jfr
                  $ jfr web --host 0.0.0.0 --port 8000 recording.jfr
                """;
    }
}