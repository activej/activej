package io.activej.csp.process.transformer.impl;

import io.activej.bytebuf.ByteBuf;
import io.activej.bytebuf.ByteBufStrings;
import io.activej.csp.consumer.ChannelConsumers;
import io.activej.csp.supplier.ChannelSupplier;
import io.activej.csp.supplier.ChannelSuppliers;
import io.activej.test.rules.ByteBufRule;
import io.activej.test.rules.EventloopRule;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.stream.Stream;

import static io.activej.promise.TestUtils.await;

public class ByteChunkerTest {

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@Test
	public void testForStackOverflow() {
		ByteChunker byteChunker = new ByteChunker(10_000, 10_000);
		ChannelSupplier<ByteBuf> supplier = ChannelSuppliers.ofStream(Stream.generate(() -> ByteBufStrings.wrapAscii("a")).limit(10_000))
				.transformWith(byteChunker);
		await(supplier.streamTo(ChannelConsumers.ofConsumer(ByteBuf::recycle)));
	}
}
