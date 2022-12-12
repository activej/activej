package io.activej.csp.queue;

import io.activej.csp.ChannelSupplier;
import io.activej.test.rules.EventloopRule;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.List;

import static io.activej.promise.TestUtils.await;
import static org.junit.Assert.assertEquals;

public class ChannelBufferTest {

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@Test
	public void testBufferStreaming() {
		ChannelBuffer<Integer> buffer = new ChannelBuffer<>(3);
		ChannelSupplier.of(1, 2, 3, 4, 5).streamTo(buffer.getConsumer());
		List<Integer> list = await(buffer.getSupplier().toList());

		assertEquals(List.of(1, 2, 3, 4, 5), list);
	}
}
