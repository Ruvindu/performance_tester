package com.dfn.performance_tester;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@SpringBootApplication
public class PerformanceTesterApplication {
    private static final System.Logger LOGGER = System.getLogger(PerformanceTesterApplication.class.getName());
    private static final Scanner INPUT = new Scanner(System.in);

    @Value("${performance.test.template-path:src/main/resources/performance-test.properties}")
    private String templatePath;

    @Value("${performance.test.results-directory:src/main/resources}")
    private String resultsDirectoryPath;

    public static void main(String[] args) {
        SpringApplication.run(PerformanceTesterApplication.class, args);
    }

    @Bean
    CommandLineRunner runPerformanceTest() {
        return args -> {
            Config config = Config.load(Path.of(templatePath));
            do {
                int requestCount = ask("Request count", config.requestBurstCount);
                int ratePerSecond = ask("Rate per second", config.ratePerSecond);
                execute(config, requestCount, ratePerSecond, Path.of(resultsDirectoryPath));
                System.out.print("Run another round? [y/N]: ");
                System.out.flush();
            } while (INPUT.hasNextLine() && INPUT.nextLine().trim().equalsIgnoreCase("y"));
        };
    }

    private static int ask(String label, int defaultValue) {
        System.out.printf("%s [%d]: ", label, defaultValue);
        System.out.flush();
        try {
            String input = INPUT.nextLine().trim();
            if (input.isEmpty()) return defaultValue;
            int value = Integer.parseInt(input);
            if (value < 1) throw new IllegalArgumentException("Value must be at least 1");
            return value;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(label + " must be a positive integer", e);
        }
    }

    private static void execute(Config config, int total, int rate, Path resultsDirectory) throws Exception {
        HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
        List<HttpRequest> preparedRequests = new ArrayList<>(total);
        for (int i = 0; i < total; i++) {
            HttpRequest.Builder builder = HttpRequest.newBuilder(URI.create(config.endpoint))
                    .timeout(Duration.ofSeconds(30));
            config.headers.forEach(builder::header);
            HttpRequest.BodyPublisher body = config.body.isEmpty()
                    ? HttpRequest.BodyPublishers.noBody()
                    : HttpRequest.BodyPublishers.ofString(config.body);
            preparedRequests.add(builder.POST(body).build());
        }

        long startNanos = System.nanoTime();
        AtomicInteger pending = new AtomicInteger(total);
        Map<Long, Counts> perSecond = new ConcurrentSkipListMap<>();
        ScheduledExecutorService launcher = Executors.newSingleThreadScheduledExecutor();
        for (int i = 0; i < total; i++) {
            // Space requests uniformly: e.g. 1,000 requests/second means one launch every 1 ms.
            long delayNanos = (long) (((double) i * TimeUnit.SECONDS.toNanos(1)) / rate);
            HttpRequest request = preparedRequests.get(i);
            launcher.schedule(() -> {
                long sentAtNanos = System.nanoTime();
                long sentSecond = Math.max(0, TimeUnit.NANOSECONDS.toSeconds(sentAtNanos - startNanos));
                Counts sentCounts = perSecond.computeIfAbsent(sentSecond, ignored -> new Counts());
                synchronized (sentCounts) { sentCounts.sent++; }
                try {
                    client.sendAsync(request, HttpResponse.BodyHandlers.discarding()).whenComplete((response, error) -> {
                        long completedAtNanos = System.nanoTime();
                        long second = Math.max(0, TimeUnit.NANOSECONDS.toSeconds(completedAtNanos - startNanos));
                        long latencyNanos = Math.max(0, completedAtNanos - sentAtNanos);
                        Counts counts = perSecond.computeIfAbsent(second, ignored -> new Counts());
                        synchronized (counts) {
                            counts.completed++;
                            counts.latencyNanos += latencyNanos;
                            counts.maxLatencyNanos = Math.max(counts.maxLatencyNanos, latencyNanos);
                            if (error == null && response.statusCode() >= 200 && response.statusCode() < 300) {
                                counts.success++;
                                counts.statuses.merge(response.statusCode(), 1, Integer::sum);
                            } else {
                                counts.failed++;
                                if (error == null) counts.statuses.merge(response.statusCode(), 1, Integer::sum);
                                else counts.transportErrors++;
                            }
                        }
                        pending.decrementAndGet();
                    });
                } catch (RuntimeException e) {
                    synchronized (sentCounts) {
                        sentCounts.completed++;
                        sentCounts.failed++;
                        sentCounts.transportErrors++;
                    }
                    pending.decrementAndGet();
                }
            }, delayNanos, TimeUnit.NANOSECONDS);
        }
        launcher.shutdown();

        long nextSecondToPrint = 0;
        while (pending.get() > 0) {
            long elapsedSecond = TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - startNanos);
            if (elapsedSecond >= nextSecondToPrint + 1) {
                printSecond(perSecond, nextSecondToPrint++);
            } else {
                TimeUnit.MILLISECONDS.sleep(50);
            }
        }
        long finalSecond = TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - startNanos);
        while (nextSecondToPrint <= finalSecond) printSecond(perSecond, nextSecondToPrint++);
        Counts totalCounts = summarize(perSecond);
        LOGGER.log(System.Logger.Level.INFO,
                "Round complete: requested={0}, sent={1}, completed={2}, success={3}, failed={4}, success_rate={5}%, avg_latency={6} ms, max_latency={7} ms, transport_errors={8}, http_statuses={9}",
                total, totalCounts.sent, totalCounts.completed, totalCounts.success, totalCounts.failed,
                percent(totalCounts.success, totalCounts.completed),
                millis(totalCounts.latencyNanos, totalCounts.completed), millis(totalCounts.maxLatencyNanos),
                totalCounts.transportErrors, totalCounts.statuses);
        Instant completedAt = Instant.now();
        Path resultFile = writeCsv(resultsDirectory, completedAt, config.endpoint, total, rate, perSecond);
        System.out.println("Results saved to " + resultFile.toAbsolutePath());
    }

    private static void printSecond(Map<Long, Counts> perSecond, long second) {
        Counts counts = perSecond.get(second);
        int success = 0;
        int failed = 0;
        int sent = 0;
        int completed = 0;
        int transportErrors = 0;
        long latencyNanos = 0;
        long maxLatencyNanos = 0;
        Map<Integer, Integer> statuses = Map.of();
        if (counts != null) {
            synchronized (counts) {
                success = counts.success;
                failed = counts.failed;
                sent = counts.sent;
                completed = counts.completed;
                transportErrors = counts.transportErrors;
                latencyNanos = counts.latencyNanos;
                maxLatencyNanos = counts.maxLatencyNanos;
                statuses = new TreeMap<>(counts.statuses);
            }
        }
        double avgLatency = millis(latencyNanos, completed);
        double maxLatency = millis(maxLatencyNanos);
        LOGGER.log(System.Logger.Level.INFO,
                "Second {0}: sent={1}, completed={2}, success={3}, failed={4}, success_rate={5}%, completed_throughput={2} req/s, avg_latency={6} ms, max_latency={7} ms, transport_errors={8}, http_statuses={9}",
                second + 1, sent, completed, success, failed, percent(success, completed), avgLatency, maxLatency,
                transportErrors, statuses);
    }

    private static Counts summarize(Map<Long, Counts> perSecond) {
        Counts total = new Counts();
        for (Counts counts : perSecond.values()) {
            synchronized (counts) {
                total.success += counts.success;
                total.failed += counts.failed;
                total.sent += counts.sent;
                total.completed += counts.completed;
                total.latencyNanos += counts.latencyNanos;
                total.maxLatencyNanos = Math.max(total.maxLatencyNanos, counts.maxLatencyNanos);
                total.transportErrors += counts.transportErrors;
                counts.statuses.forEach((status, amount) -> total.statuses.merge(status, amount, Integer::sum));
            }
        }
        return total;
    }

    private static double percent(int part, int whole) {
        return whole == 0 ? 0 : Math.round(part * 10000.0 / whole) / 100.0;
    }

    private static double millis(long nanos) { return nanos / 1_000_000.0; }

    private static double millis(long nanos, int count) { return count == 0 ? 0 : millis(nanos) / count; }

    private static Path writeCsv(Path resultsDirectory, Instant completedAt, String endpoint, int total, int rate,
                                 Map<Long, Counts> perSecond) throws IOException {
        Files.createDirectories(resultsDirectory);
        String timestamp = DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss-SSS")
                .withZone(java.time.ZoneOffset.UTC).format(completedAt);
        Path resultFile = resultsDirectory.resolve("performance-results-" + timestamp + "-"
                + java.util.UUID.randomUUID().toString().substring(0, 8) + ".csv");
        try (var writer = Files.newBufferedWriter(resultFile, StandardCharsets.UTF_8,
                java.nio.file.StandardOpenOption.CREATE_NEW, java.nio.file.StandardOpenOption.WRITE)) {
            writer.write("completed_at,endpoint,request_burst_count,rate_per_second,second,sent,completed,success,failed,success_rate_percent,avg_latency_ms,max_latency_ms,transport_errors,http_statuses\n");
            String safeEndpoint = endpoint.replace("\"", "\"\"");
            for (Map.Entry<Long, Counts> entry : perSecond.entrySet()) {
                Counts c = entry.getValue();
                synchronized (c) {
                writer.write(String.format("%s,\"%s\",%d,%d,%d,%d,%d,%d,%d,%.2f,%.2f,%.2f,%d,\"%s\"%n",
                        DateTimeFormatter.ISO_INSTANT.format(completedAt), safeEndpoint, total, rate,
                        entry.getKey() + 1, c.sent, c.completed, c.success, c.failed, percent(c.success, c.completed),
                        millis(c.latencyNanos, c.completed), millis(c.maxLatencyNanos), c.transportErrors,
                        c.statuses.toString().replace("\"", "\"\"")));
                }
            }
        }
        return resultFile;
    }

    private static final class Counts {
        int sent;
        int completed;
        int success;
        int failed;
        int transportErrors;
        long latencyNanos;
        long maxLatencyNanos;
        final Map<Integer, Integer> statuses = new TreeMap<>();
    }

    private static final class Config {
        final String endpoint;
        final String body;
        final Map<String, String> headers;
        final int requestBurstCount;
        final int ratePerSecond;

        private Config(String endpoint, String body, Map<String, String> headers, int requestBurstCount, int ratePerSecond) {
            this.endpoint = endpoint;
            this.body = body;
            this.headers = headers;
            this.requestBurstCount = requestBurstCount;
            this.ratePerSecond = ratePerSecond;
        }

        static Config load(Path path) throws IOException {
            var properties = new java.util.Properties();
            try (var in = Files.newInputStream(path)) { properties.load(in); }
            String endpoint = required(properties, "endpoint");
            String body = properties.getProperty("request.body", "");
            Map<String, String> headers = new java.util.LinkedHashMap<>();
            for (String key : properties.stringPropertyNames()) {
                if (key.startsWith("request.header."))
                    headers.put(key.substring("request.header.".length()), properties.getProperty(key).trim());
            }
            return new Config(endpoint, body, headers,
                    positive(properties, "request.burst-count"), positive(properties, "request.rate-per-second"));
        }

        private static String required(java.util.Properties p, String key) {
            String value = p.getProperty(key);
            if (value == null || value.isBlank()) throw new IllegalArgumentException("Missing template setting: " + key);
            return value.trim();
        }

        private static int positive(java.util.Properties p, String key) {
            int value = Integer.parseInt(required(p, key));
            if (value < 1) throw new IllegalArgumentException(key + " must be at least 1");
            return value;
        }
    }
}
