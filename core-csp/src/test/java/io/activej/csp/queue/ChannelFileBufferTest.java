package io.activej.csp.queue;

import io.activej.bytebuf.ByteBuf;
import io.activej.common.ref.Ref;
import io.activej.promise.Promise;
import io.activej.test.rules.ByteBufRule;
import io.activej.test.rules.EventloopRule;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static io.activej.bytebuf.ByteBufStrings.wrapAscii;
import static io.activej.promise.TestUtils.await;
import static java.nio.charset.StandardCharsets.US_ASCII;
import static org.junit.Assert.*;

public final class ChannelFileBufferTest {

	@ClassRule
	public static ByteBufRule byteBufRule = new ByteBufRule();

	@ClassRule
	public static EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static TemporaryFolder temporaryFolder = new TemporaryFolder();

	private ExecutorService executor;

	@Before
	public void setUp() {
		executor = Executors.newSingleThreadExecutor();
	}

	@After
	public void tearDown() {
		executor.shutdownNow();
	}

	@Test
	public void basicFileQueue() throws IOException {
		ChannelFileBuffer cfq = await(ChannelFileBuffer.create(executor, temporaryFolder.newFile().toPath()));

		assertTrue(cfq.isExhausted());
		assertFalse(cfq.isSaturated());

		await(cfq.put(wrapAscii("hello world, this is a byte buffer\n")));
		await(cfq.put(wrapAscii("second: hello world, this is a byte buffer")));

		assertFalse(cfq.isExhausted());
		assertFalse(cfq.isSaturated());

		String taken = await(cfq.take()).asString(US_ASCII);

		assertTrue(cfq.isExhausted());
		assertFalse(cfq.isSaturated());

		assertEquals("hello world, this is a byte buffer\nsecond: hello world, this is a byte buffer", taken);

		cfq.close();
	}

	@Test
	public void fileQueueTakeWait() throws IOException {
		ChannelFileBuffer cfq = await(ChannelFileBuffer.create(executor, temporaryFolder.newFile().toPath()));

		Ref<ByteBuf> ref = new Ref<>();

		cfq.take().whenResult(ref::set);

		Promise<Void> fastPut = cfq.put(wrapAscii("this is a string for testing"));
		assertTrue(fastPut.isComplete());
		ByteBuf res = ref.get();
		assertNotNull(res);
		assertEquals("this is a string for testing", res.asString(US_ASCII));

		await(cfq.put(wrapAscii("this should not be appended because the take will be completed immediately")));

		cfq.close();
	}
}
