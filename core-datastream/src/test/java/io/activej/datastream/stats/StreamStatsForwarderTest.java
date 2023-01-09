package io.activej.datastream.stats;

import io.activej.datastream.StreamConsumer;
import io.activej.datastream.StreamSupplier;
import io.activej.reactor.Reactor;
import io.activej.test.rules.EventloopRule;
import org.junit.ClassRule;
import org.junit.Test;

import static io.activej.promise.TestUtils.await;
import static org.junit.Assert.assertEquals;

public class StreamStatsForwarderTest {

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@Test
	public void testDetailedStats() {
		StreamStatsDetailed<Integer> stats = StreamStats.detailed(Reactor.getCurrentReactor(), number -> number);

		await(StreamSupplier.of(1, 2, 3, 4, 5)
				.transformWith(stats)
				.streamTo(StreamConsumer.skip()));

		assertEquals(5, stats.getCount());
		//noinspection DataFlowIssue
		assertEquals(15L, stats.getTotalSize().longValue());
	}
}
