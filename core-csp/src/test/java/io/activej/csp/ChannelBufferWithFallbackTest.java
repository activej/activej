package io.activej.csp;

import io.activej.bytebuf.ByteBuf;
import io.activej.common.ref.Ref;
import io.activej.common.ref.RefInt;
import io.activej.csp.queue.ChannelBufferWithFallback;
import io.activej.csp.queue.ChannelFileBuffer;
import io.activej.csp.queue.ChannelZeroBuffer;
import io.activej.promise.Promise;
import io.activej.test.rules.ByteBufRule;
import io.activej.test.rules.EventloopRule;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static io.activej.bytebuf.ByteBufStrings.wrapAscii;
import static io.activej.promise.TestUtils.await;
import static java.nio.charset.StandardCharsets.US_ASCII;
import static org.junit.Assert.*;

public final class ChannelBufferWithFallbackTest {

	@Rule
	public ByteBufRule byteBufRule = new ByteBufRule();

	@Rule
	public EventloopRule eventloopRule = new EventloopRule();

	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	private ExecutorService executor;

	@Before
	public void setUp() {
		executor = Executors.newSingleThreadExecutor();
	}

	@After
	public void tearDown() {
		executor.shutdownNow();
	}

	private ChannelBufferWithFallback<ByteBuf> createBufferedQueue(RefInt secondaryConter) {
		return new ChannelBufferWithFallback<>(
				new ChannelZeroBuffer<>(),
				() -> {
					secondaryConter.value++;
					try {
						return ChannelFileBuffer.create(executor, temporaryFolder.newFile().toPath());
					} catch (IOException e) {
						throw new AssertionError(e);
					}
				});
	}

	@Test
	public void bufferedQueue() {
		RefInt counter = new RefInt(0);
		ChannelBufferWithFallback<ByteBuf> q = createBufferedQueue(counter);

		assertTrue(q.isExhausted());
		assertFalse(q.isSaturated());

		await(q.put(wrapAscii("hello world, this is a byte buffer\n")));
		await(q.put(wrapAscii("second: hello world, this is a byte buffer")));

		assertFalse(q.isExhausted());
		assertFalse(q.isSaturated());

		String taken = await(q.take()).asString(US_ASCII);

		assertTrue(q.isExhausted());
		assertFalse(q.isSaturated());

		await(q.put(wrapAscii("another input\n")));
		await(q.put(wrapAscii("second buffer")));
		await(q.put(wrapAscii(" and a third one")));

		String taken2 = await(q.take()).asString(US_ASCII);

		assertEquals("hello world, this is a byte buffer\nsecond: hello world, this is a byte buffer", taken);
		assertEquals("another input\nsecond buffer and a third one", taken2);

		// should be 1 and not 2, because we've exhausted the secondary buffer
		// but then we are putting items in - primary queue were full and still is full at this point
		assertEquals(1, counter.value);

		q.close();
	}

	@Test
	public void bufferedQueueTakeWait() {
		RefInt counter = new RefInt(0);
		ChannelBufferWithFallback<ByteBuf> cfq = createBufferedQueue(counter);

		Ref<ByteBuf> ref = new Ref<>();

		cfq.take().whenResult(ref::set);

		Promise<Void> fastPut = cfq.put(wrapAscii("this is a string for testing"));
		assertTrue(fastPut.isComplete());

		ByteBuf res = ref.get();
		assertNotNull(res);
		assertEquals("this is a string for testing", res.asString(US_ASCII));

		assertEquals(0, counter.value);
		cfq.put(wrapAscii("this should not be appended because the take will be completed immediately"));
		assertEquals(1, counter.value);

		cfq.close();
	}
}
