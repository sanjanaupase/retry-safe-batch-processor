import org.junit.jupiter.api.*;
import java.util.concurrent.atomic.*;
import static org.junit.jupiter.api.Assertions.*;

class BatchProcessorTest {

    @Test
    void retriesTransientThenSucceeds() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        String result = BatchProcessor.withRetry(() -> {
            if (calls.incrementAndGet() < 3) throw new BatchProcessor.TransientException("timeout");
            return "ok";
        });
        assertEquals("ok", result);
        assertEquals(3, calls.get());
    }

    @Test
    void givesUpAfterMaxRetries() {
        AtomicInteger calls = new AtomicInteger();
        assertThrows(BatchProcessor.TransientException.class, () ->
            BatchProcessor.withRetry(() -> {
                calls.incrementAndGet();
                throw new BatchProcessor.TransientException("timeout");
            }));
        assertEquals(BatchProcessor.MAX_RETRIES + 1, calls.get());
    }

    @Test
    void permanentErrorIsNotRetried() {
        AtomicInteger calls = new AtomicInteger();
        assertThrows(IllegalArgumentException.class, () ->
            BatchProcessor.withRetry(() -> {
                calls.incrementAndGet();
                throw new IllegalArgumentException("price<=0");
            }));
        assertEquals(1, calls.get());
    }

    @Test
    void processRejectsNonPositivePrice() {
        var row = new BatchProcessor.Row("S1", "SKU-1", "0");
        assertThrows(IllegalArgumentException.class, () -> BatchProcessor.process(row));
    }
}
