package io.activej.datastream.stats;

import io.activej.datastream.consumer.StreamConsumers;
import io.activej.datastream.supplier.StreamSuppliers;
import io.activej.promise.Promise;
import io.activej.test.rules.EventloopRule;
import org.junit.ClassRule;
import org.junit.Test;

import static io.activej.datastream.TestStreamTransformers.decorate;
import static io.activej.promise.TestUtils.await;
import static io.activej.promise.TestUtils.awaitException;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

public class StreamStatsForwarderTest {

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@Test
	public void testDetailedStats() {
		DetailedStreamStats<Integer> stats = StreamStats.<Integer>detailedBuilder()
				.withSizeCounter(number -> number)
				.build();

		await(StreamSuppliers.ofValues(1, 2, 3, 4, 5)
				.transformWith(stats)
				.streamTo(StreamConsumers.skip()));

		assertEquals(5, stats.getCount());
		//noinspection DataFlowIssue
		assertEquals(15L, stats.getTotalSize().longValue());
	}

	@Test
	public void testOnErrorStats() {
		BasicStreamStats<Integer> stats = StreamStats.basic();
		Exception exception = new Exception("Test");

		Exception e = awaitException(StreamSuppliers.ofValues(1, 2, 3, 4, 5)
				.transformWith(stats)
				.streamTo(StreamConsumers.<Integer>skip()
						.transformWith(decorate(promise ->
								promise.then(item ->
										item == 4 ?
												Promise.ofException(exception) :
												Promise.of(item)))))
		);

		assertSame(exception, e);
		assertEquals(1, stats.getError().getTotal());
	}
}
