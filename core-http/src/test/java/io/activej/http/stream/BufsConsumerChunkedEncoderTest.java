package io.activej.http.stream;

import io.activej.bytebuf.ByteBuf;
import io.activej.bytebuf.ByteBufPool;
import io.activej.csp.supplier.ChannelSuppliers;
import io.activej.http.TestUtils.AssertingConsumer;
import io.activej.test.rules.ByteBufRule;
import io.activej.test.rules.EventloopRule;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static io.activej.promise.TestUtils.await;

public final class BufsConsumerChunkedEncoderTest {

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	private final List<ByteBuf> list = new ArrayList<>();
	private final AssertingConsumer consumer = new AssertingConsumer();
	private final BufsConsumerChunkedEncoder chunkedEncoder = BufsConsumerChunkedEncoder.create();

	@Before
	public void setUp() {
		consumer.reset();
		chunkedEncoder.getOutput().set(consumer);
	}

	@Test
	public void testEncoderSingleBuf() {
		byte[] chunkData = new byte[100];
		ThreadLocalRandom.current().nextBytes(chunkData);
		ByteBuf buf = ByteBufPool.allocate(chunkData.length);
		ByteBuf expected = ByteBufPool.allocate(200);
		buf.put(chunkData);
		list.add(buf);
		expected.put((Integer.toHexString(100) + "\r\n").getBytes());
		expected.put(chunkData);
		expected.put("\r\n0\r\n\r\n".getBytes());
		consumer.setExpectedBuf(expected);
		doTest();
	}

	@Test
	public void testEncodeWithEmptyBuf() {
		byte[] chunkData = new byte[100];
		ThreadLocalRandom.current().nextBytes(chunkData);
		ByteBuf buf = ByteBufPool.allocate(chunkData.length);
		buf.put(chunkData);
		ByteBuf expected = ByteBufPool.allocate(200);
		ByteBuf empty = ByteBufPool.allocate(100);
		list.add(buf);
		list.add(empty);
		expected.put((Integer.toHexString(100) + "\r\n").getBytes());
		expected.put(chunkData);
		expected.put("\r\n0\r\n\r\n".getBytes());
		consumer.setExpectedBuf(expected);

		doTest();
	}

	private void doTest() {
		chunkedEncoder.getInput().set(ChannelSuppliers.ofList(list));
		await(chunkedEncoder.getProcessCompletion());
	}
}
