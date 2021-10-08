package io.activej.csp;

import io.activej.bytebuf.ByteBuf;
import io.activej.bytebuf.ByteBufPool;
import io.activej.bytebuf.ByteBufs;
import io.activej.eventloop.Eventloop;
import io.activej.promise.Promise;
import io.activej.test.rules.ByteBufRule;
import io.activej.test.rules.EventloopRule;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.InputStream;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import static io.activej.common.MemSize.kilobytes;
import static io.activej.csp.ChannelSuppliers.channelSupplierAsInputStream;
import static io.activej.csp.ChannelSuppliers.inputStreamAsChannelSupplier;
import static io.activej.promise.TestUtils.await;
import static io.activej.promise.TestUtils.awaitException;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.asList;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.*;

public class ChannelSupplierTest {
	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@Test
	public void testToCollector() {
		ChannelSupplier<ByteBuf> supplier = ChannelSupplier.ofList(asList(
				ByteBuf.wrapForReading("Test1".getBytes(UTF_8)),
				ByteBuf.wrapForReading("Test2".getBytes(UTF_8)),
				ByteBuf.wrapForReading("Test3".getBytes(UTF_8)),
				ByteBuf.wrapForReading("Test4".getBytes(UTF_8)),
				ByteBuf.wrapForReading("Test5".getBytes(UTF_8)),
				ByteBuf.wrapForReading("Test6".getBytes(UTF_8))
		));

		ByteBuf resultBuf = await(supplier.toCollector(ByteBufs.collector()));
		assertEquals("Test1Test2Test3Test4Test5Test6", resultBuf.asString(UTF_8));
	}

	@Test
	public void testToCollectorWithException() {
		ByteBuf value = ByteBufPool.allocate(100);
		value.put("Test".getBytes(UTF_8));
		Exception exception = new Exception("Test Exception");
		ChannelSupplier<ByteBuf> supplier = ChannelSuppliers.concat(
				ChannelSupplier.of(value),
				ChannelSupplier.ofException(exception)
		);

		Exception e = awaitException(supplier.toCollector(ByteBufs.collector()));
		assertSame(exception, e);
	}

	@Test
	public void testToCollectorMaxSize() {
		ByteBuf byteBuf1 = ByteBuf.wrapForReading("T".getBytes(UTF_8));
		ByteBuf byteBuf2 = ByteBuf.wrapForReading("Te".getBytes(UTF_8));
		ByteBuf byteBuf3 = ByteBuf.wrapForReading("Tes".getBytes(UTF_8));
		ByteBuf byteBuf4 = ByteBuf.wrapForReading("Test".getBytes(UTF_8));

		await(ChannelSupplier.of(byteBuf1).toCollector(ByteBufs.collector(2)));
		await(ChannelSupplier.of(byteBuf2).toCollector(ByteBufs.collector(2)));
		Exception e1 = awaitException(ChannelSupplier.of(byteBuf3).toCollector(ByteBufs.collector(2)));
		assertThat(e1.getMessage(), containsString("Size of ByteBufs exceeds maximum size of 2 bytes"));
		Exception e2 = awaitException(ChannelSupplier.of(byteBuf4).toCollector(ByteBufs.collector(2)));
		assertThat(e2.getMessage(), containsString("Size of ByteBufs exceeds maximum size of 2 bytes"));
	}

	@Test
	public void testOfInputStream() {
		int expectedSize = 10000;
		InputStream inputStream = new InputStream() {
			int count = 0;

			@Override
			public int read() {
				if (++count > expectedSize) return -1;
				return 0;
			}
		};

		ChannelSupplier<ByteBuf> channel = inputStreamAsChannelSupplier(newSingleThreadExecutor(), kilobytes(16), inputStream);
		List<ByteBuf> byteBufList = await(channel.toCollector(Collectors.toList()));
		int readSize = 0;
		for (ByteBuf buf : byteBufList) {
			readSize += buf.readRemaining();
			buf.recycle();
		}
		assertEquals(readSize, expectedSize);
	}

	@Test
	public void testOfEmptyInputStream() {
		InputStream inputStream = new InputStream() {
			@Override
			public int read() {
				return -1;
			}
		};

		ChannelSupplier<ByteBuf> channel = inputStreamAsChannelSupplier(newSingleThreadExecutor(), kilobytes(16), inputStream);
		List<ByteBuf> byteBufList = await(channel.toCollector(Collectors.toList()));
		int readSize = 0;
		for (ByteBuf buf : byteBufList) {
			readSize += buf.readRemaining();
			buf.recycle();
		}
		assertEquals(0, readSize);
	}

	@Test
	public void testAsInputStream() {
		ChannelSupplier<ByteBuf> channelSupplier = ChannelSupplier.of(
				ByteBuf.wrapForReading("Hello".getBytes()),
				ByteBuf.wrapForReading("World".getBytes()));

		Eventloop currentEventloop = Eventloop.getCurrentEventloop();
		await(Promise.ofBlocking(Executors.newSingleThreadExecutor(),
				() -> {
					InputStream inputStream = channelSupplierAsInputStream(currentEventloop, channelSupplier);
					int b;
					ByteBuf buf = ByteBufPool.allocate(100);
					while ((b = inputStream.read()) != -1) {
						buf.writeByte((byte) b);
					}
					assertEquals("HelloWorld", buf.asString(UTF_8));
				}));
	}

	@Test
	public void testEmptyInputStream() {
		ChannelSupplier<ByteBuf> channelSupplier = ChannelSupplier.of(ByteBuf.empty(), ByteBuf.empty());

		Eventloop currentEventloop = Eventloop.getCurrentEventloop();
		await(Promise.ofBlocking(Executors.newSingleThreadExecutor(),
				() -> {
					InputStream inputStream = channelSupplierAsInputStream(currentEventloop, channelSupplier);
					int b;
					ByteBuf buf = ByteBufPool.allocate(100);
					while ((b = inputStream.read()) != -1) {
						buf.writeByte((byte) b);
					}
					assertTrue(buf.asString(UTF_8).isEmpty());
				}));
	}
}
