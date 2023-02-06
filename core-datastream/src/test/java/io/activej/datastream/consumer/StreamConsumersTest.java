package io.activej.datastream.consumer;

import io.activej.datastream.supplier.StreamSupplier;
import io.activej.datastream.supplier.StreamSuppliers;
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
		StreamSupplier<Integer> supplier = StreamSuppliers.ofValues(1, 2, 3);
		StreamConsumer<Integer> failingConsumer = ToListStreamConsumer.<Integer>create()
				.withAcknowledgement(ack -> ack
						.then(($, e) -> Promise.ofException(new Exception("Test"))));

		Exception exception = awaitException(supplier.streamTo(failingConsumer));
		assertEquals("Test", exception.getMessage());
	}
}
