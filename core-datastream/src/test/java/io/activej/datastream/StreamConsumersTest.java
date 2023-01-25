package io.activej.datastream;

import io.activej.promise.Promise;
import io.activej.test.rules.EventloopRule;
import org.junit.ClassRule;
import org.junit.Test;

import static io.activej.promise.TestUtils.awaitException;
import static org.junit.Assert.assertEquals;

public class StreamConsumersTest {

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@Test
	public void withAcknowledge() {
		StreamSupplier<Integer> supplier = StreamSupplier.of(1, 2, 3);
		StreamConsumer<Integer> failingConsumer = StreamConsumer_ToList.<Integer>create()
				.withAcknowledgement(ack -> ack
						.then(($, e) -> Promise.ofException(new Exception("Test"))));

		Exception exception = awaitException(supplier.streamTo(failingConsumer));
		assertEquals("Test", exception.getMessage());
	}
}
