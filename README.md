# REST API Performance Tester

Configure `src/main/resources/performance-test.properties` before running. Set `endpoint`, `request.body`, and any `request.header.<name>` entries. Requests are sent using HTTP POST. `request.burst-count` is the total number of requests; `request.rate-per-second` is the pacing rate (the maximum number scheduled in each second).

The template and results folder locations are configurable in `src/main/resources/application.properties` with `performance.test.template-path` and `performance.test.results-directory`.

Run with `./mvnw spring-boot:run` (Windows: `mvnw.cmd spring-boot:run`). The application prompts for the burst count and rate before every round. Press Enter to use each template default. Requests are paced evenly across the second (for example, 1,000 per second is scheduled about one millisecond apart). Live logs distinguish requests sent during each second from responses completed during that second. After each round it creates a new uniquely named CSV file in `src/main/resources` (for example, `performance-results-20261029-120000-000-a1b2c3d4.csv`). Each file contains one row per second of the run. It then asks whether to run another round; if you answer yes, it prompts again for the count and rate.

HTTP 2xx responses count as successful; non-2xx responses and transport errors count as failed. Live logs include per-second throughput, success rate, average and maximum response latency, transport errors, and HTTP status counts. A round summary reports the same totals. Each test run produces its own CSV file with per-second metrics and leaves previous result files unchanged.
