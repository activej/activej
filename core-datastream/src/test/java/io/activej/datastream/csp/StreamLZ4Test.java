package io.activej.datastream.csp;

import io.activej.bytebuf.ByteBuf;
import io.activej.bytebuf.ByteBufQueue;
import io.activej.common.MemSize;
import io.activej.csp.ChannelSupplier;
import io.activej.csp.process.ChannelByteChunker;
import io.activej.csp.process.ChannelLZ4Compressor;
import io.activej.csp.process.ChannelLZ4Decompressor;
import io.activej.test.rules.ByteBufRule;
import io.activej.test.rules.EventloopRule;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;

import static io.activej.promise.TestUtils.await;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertArrayEquals;

public final class StreamLZ4Test {

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@Test
	public void test() {
		//[START EXAMPLE]
		int buffersCount = 100;

		List<ByteBuf> buffers = IntStream.range(0, buffersCount).mapToObj($ -> createRandomByteBuf()).collect(toList());
		byte[] expected = buffers.stream().map(ByteBuf::slice).collect(ByteBufQueue.collector()).asArray();

		ChannelSupplier<ByteBuf> supplier = ChannelSupplier.ofList(buffers)
				.transformWith(ChannelByteChunker.create(MemSize.of(64), MemSize.of(128)))
				.transformWith(ChannelLZ4Compressor.createFastCompressor())
				.transformWith(ChannelByteChunker.create(MemSize.of(64), MemSize.of(128)))
				.transformWith(ChannelLZ4Decompressor.create());

		ByteBuf collected = await(supplier.toCollector(ByteBufQueue.collector()));
		assertArrayEquals(expected, collected.asArray());
		//[END EXAMPLE]
	}

	@Test
	public void testLz4Fast() {
		doTest(ChannelLZ4Compressor.createFastCompressor());
	}

	@Test
	public void testLz4High() {
		doTest(ChannelLZ4Compressor.createHighCompressor());
	}

	@Test
	public void testLz4High10() {
		doTest(ChannelLZ4Compressor.createHighCompressor(10));
	}

	private void doTest(ChannelLZ4Compressor compressor) {
		byte[] data = "1".getBytes();

		ChannelSupplier<ByteBuf> supplier = ChannelSupplier.of(ByteBuf.wrapForReading(data))
				.transformWith(compressor)
				.transformWith(ChannelLZ4Decompressor.create());

		ByteBuf collected = await(supplier.toCollector(ByteBufQueue.collector()));
		assertArrayEquals(data, collected.asArray());
	}

	private static ByteBuf createRandomByteBuf() {
		ThreadLocalRandom random = ThreadLocalRandom.current();
		int offset = random.nextInt(10);
		int tail = random.nextInt(10);
		int len = random.nextInt(100);
		byte[] array = new byte[offset + len + tail];
		random.nextBytes(array);
		return ByteBuf.wrap(array, offset, offset + len);
	}
}
