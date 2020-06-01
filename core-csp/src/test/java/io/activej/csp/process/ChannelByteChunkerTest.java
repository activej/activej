package io.activej.csp.process;

import io.activej.bytebuf.ByteBuf;
import io.activej.bytebuf.ByteBufStrings;
import io.activej.common.MemSize;
import io.activej.csp.ChannelConsumer;
import io.activej.csp.ChannelSupplier;
import io.activej.test.rules.ByteBufRule;
import io.activej.test.rules.EventloopRule;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.stream.Stream;

import static io.activej.promise.TestUtils.await;

public class ChannelByteChunkerTest {

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@Test
	public void testForStackOverflow() {
		ChannelByteChunker channelByteChunker = ChannelByteChunker.create(MemSize.of(10_000), MemSize.of(10_000));
		ChannelSupplier<ByteBuf> supplier = ChannelSupplier.ofStream(Stream.generate(() -> ByteBufStrings.wrapAscii("a")).limit(10_000))
				.transformWith(channelByteChunker);
		await(supplier.streamTo(ChannelConsumer.ofConsumer(ByteBuf::recycle)));
	}
}
