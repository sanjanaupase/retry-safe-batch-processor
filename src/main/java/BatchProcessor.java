import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

public class BatchProcessor {
    static final int MAX_RETRIES = 3;
    static final long BASE_BACKOFF_MS = 100;
    static final Path DONE = Path.of("done.log");

    record Row(String sellerId, String sku, String price) {}

    static class TransientException extends Exception {
        TransientException(String m) { super(m); }
    }

    static final Set<String> done = ConcurrentHashMap.newKeySet();
    static BufferedWriter doneWriter;
    static final AtomicInteger accepted = new AtomicInteger(), skipped = new AtomicInteger(),
                               failed = new AtomicInteger(), retried = new AtomicInteger();

    public static void main(String[] args) throws Exception {
        doneWriter = Files.newBufferedWriter(DONE, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        if (Files.exists(DONE)) done.addAll(Files.readAllLines(DONE));

        List<Row> rows = Files.readAllLines(Path.of(args[0])).stream().skip(1)
            .map(l -> l.split(",", -1)).filter(p -> p.length >= 3)
            .map(p -> new Row(p[0].trim(), p[1].trim(), p[2].trim())).toList();

        ExecutorService pool = Executors.newFixedThreadPool(8);
        List<String> failures = Collections.synchronizedList(new ArrayList<>());

        for (Row r : rows) {
            pool.submit(() -> {
                String key = r.sellerId() + "|" + r.sku();
                if (!done.add(key)) { skipped.incrementAndGet(); return; }
                try {
                    withRetry(() -> { process(r); return null; });
                    markDone(key);
                    accepted.incrementAndGet();
                } catch (Exception e) {
                    done.remove(key);
                    failed.incrementAndGet();
                    failures.add(r.sellerId() + "," + r.sku() + "," + r.price() + "," + e.getMessage());
                }
            });
        }

        pool.shutdown();
        pool.awaitTermination(1, TimeUnit.HOURS);
        doneWriter.close();
        Files.write(Path.of("failed.csv"), failures);
        System.out.printf("accepted=%d skipped=%d failed=%d retried=%d%n",
            accepted.get(), skipped.get(), failed.get(), retried.get());
    }

    // Permanent errors fail immediately; transient errors retry with exponential backoff.
    static <T> T withRetry(Callable<T> op) throws Exception {
        for (int attempt = 0; ; attempt++) {
            try {
                return op.call();
            } catch (TransientException e) {
                if (attempt >= MAX_RETRIES) throw e;
                retried.incrementAndGet();
                Thread.sleep(BASE_BACKOFF_MS << attempt);
            }
        }
    }

    // Stand-in for the real downstream write. Replace with your upsert call.
    static void process(Row r) throws Exception {
        if (Double.parseDouble(r.price()) <= 0) throw new IllegalArgumentException("price<=0");
        if (ThreadLocalRandom.current().nextInt(1000) == 0) throw new TransientException("simulated timeout");
    }

    static synchronized void markDone(String key) throws IOException {
        doneWriter.write(key);
        doneWriter.newLine();
    }
}