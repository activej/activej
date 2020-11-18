package io.activej.http.stream;

import io.activej.async.process.AsyncProcess;
import io.activej.bytebuf.ByteBuf;
import io.activej.bytebuf.ByteBufPool;
import io.activej.http.TestUtils.AssertingConsumer;
import io.activej.promise.Promises;
import io.activej.test.rules.ByteBufRule;
import io.activej.test.rules.EventloopRule;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.concurrent.ThreadLocalRandom;

import static io.activej.csp.ChannelSupplier.ofList;
import static io.activej.http.TestUtils.chunkedByByte;
import static io.activej.promise.TestUtils.await;
import static org.junit.Assert.assertTrue;

public final class BufsConsumerIntegrationTest {

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	private final AssertingConsumer consumer = new AssertingConsumer();
	private final ArrayList<ByteBuf> list = new ArrayList<>();
	private final BufsConsumerChunkedEncoder chunkedEncoder = BufsConsumerChunkedEncoder.create();
	private final BufsConsumerChunkedDecoder chunkedDecoder = BufsConsumerChunkedDecoder.create();
	private final BufsConsumerGzipDeflater gzipDeflater = BufsConsumerGzipDeflater.create();
	private final BufsConsumerGzipInflater gzipInflater = BufsConsumerGzipInflater.create();

	@Before
	public void setUp() {
		consumer.reset();
		chunkedEncoder.getOutput().set(chunkedDecoder.getInput().getConsumer());
		chunkedDecoder.getOutput().set(consumer);
		gzipDeflater.getOutput().set(gzipInflater.getInput().getConsumer());
		gzipInflater.getOutput().set(consumer);
	}

	@Test
	public void testEncodeDecodeSingleBuf() {
		writeSingleBuf();
		chunkedEncoder.getInput().set(chunkedByByte(ofList(list)));
		doTest(chunkedEncoder, chunkedDecoder);
	}

	@Test
	public void testEncodeDecodeMultipleBufs() {
		writeMultipleBufs();
		chunkedEncoder.getInput().set(ofList(list));
		doTest(chunkedEncoder, chunkedDecoder);
	}

	@Test
	public void testGzipGunzipSingleBuf() {
		writeSingleBuf();
		gzipDeflater.getInput().set(chunkedByByte(ofList(list)));
		doTest(gzipInflater, gzipDeflater);
	}

	@Test
	public void testGzipGunzipMultipleBufs() {
		writeMultipleBufs();
		gzipDeflater.getInput().set(ofList(list));
		doTest(gzipInflater, gzipDeflater);
	}

	private void writeSingleBuf() {
		byte[] data = new byte[1000];
		ThreadLocalRandom.current().nextBytes(data);
		consumer.setExpectedByteArray(data);
		ByteBuf buf = ByteBufPool.allocate(data.length);
		buf.put(data);
		list.add(buf);
	}

	private void writeMultipleBufs() {
		byte[] data = new byte[100_000];
		ThreadLocalRandom.current().nextBytes(data);
		ByteBuf toBeSplit = ByteBufPool.allocate(data.length);
		ByteBuf expected = ByteBufPool.allocate(data.length);
		toBeSplit.put(data);
		expected.put(data);
		consumer.setExpectedBuf(expected);
		while (toBeSplit.readRemaining() != 0) {
			int part = Math.min(ThreadLocalRandom.current().nextInt(100) + 100, toBeSplit.readRemaining());
			ByteBuf slice = toBeSplit.slice(part);
			toBeSplit.moveHead(part);
			list.add(slice);
		}
		toBeSplit.recycle();
	}

	private void doTest(AsyncProcess process1, AsyncProcess process2) {
		await(Promises.all(process1.getProcessCompletion(), process2.getProcessCompletion()));
		assertTrue(consumer.executed);
	}
}
