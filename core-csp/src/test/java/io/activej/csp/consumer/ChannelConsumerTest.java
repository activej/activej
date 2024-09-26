package io.activej.csp.consumer;

import io.activej.bytebuf.ByteBuf;
import io.activej.bytebuf.ByteBufPool;
import io.activej.eventloop.Eventloop;
import io.activej.promise.Promise;
import io.activej.reactor.Reactor;
import io.activej.test.ExpectedException;
import io.activej.test.rules.ByteBufRule;
import io.activej.test.rules.EventloopRule;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import static io.activej.common.exception.FatalErrorHandlers.rethrow;
import static io.activej.csp.binary.Utils.channelConsumerAsOutputStream;
import static io.activej.csp.consumer.ChannelConsumers.ofOutputStream;
import static io.activej.promise.TestUtils.await;
import static io.activej.promise.TestUtils.awaitException;
import static io.activej.reactor.Reactor.executeWithReactor;
import static io.activej.reactor.Reactor.getCurrentReactor;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static org.junit.Assert.*;

public class ChannelConsumerTest {
	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@Test
	public void testToOutputStream() {
		ByteBuf buf = ByteBufPool.allocate(100);
		OutputStream outputStream = new OutputStream() {
			@Override
			public void write(int i) {
				buf.writeByte((byte) i);
			}
		};

		ChannelConsumer<ByteBuf> channelConsumer = ofOutputStream(newSingleThreadExecutor(), outputStream);
		await(channelConsumer.acceptAll(
			ByteBuf.wrapForReading("Hello".getBytes()),
			ByteBuf.wrapForReading("World".getBytes())));

		assertEquals("HelloWorld", buf.asString(Charset.defaultCharset()));
	}

	@Test
	public void testToOutputStreamEmpty() {
		ByteBuf buf = ByteBufPool.allocate(100);
		OutputStream outputStream = new OutputStream() {
			@Override
			public void write(int i) {
				buf.writeByte((byte) i);
			}
		};

		ChannelConsumer<ByteBuf> channelConsumer = ofOutputStream(newSingleThreadExecutor(), outputStream);
		await(channelConsumer.acceptAll(ByteBuf.empty(), ByteBuf.empty()));

		assertTrue(buf.asString(UTF_8).isEmpty());
	}

	@Test
	public void testToOutputStreamException() {
		IOException exception = new IOException("Some exception");
		OutputStream outputStream = new OutputStream() {
			@Override
			public void write(int i) throws IOException {
				throw exception;
			}
		};

		ChannelConsumer<ByteBuf> channelConsumer = ofOutputStream(newSingleThreadExecutor(), outputStream);
		Exception exception2 = awaitException(channelConsumer.acceptAll(ByteBuf.empty(), ByteBuf.wrapForReading("Hello".getBytes())));
		assertSame(exception, exception2);
	}

	@Test
	public void testAsOutputStream() {
		int expectedSize = 1000;

		ByteBuf result = ByteBuf.wrapForWriting(new byte[expectedSize]);

		ChannelConsumer<ByteBuf> channelConsumer = ChannelConsumers.ofAsyncConsumer(
			buf -> {
				result.put(buf);
				buf.recycle();
				return Promise.complete();
			});

		Reactor reactor = getCurrentReactor();
		await(Promise.ofBlocking(newSingleThreadExecutor(),
			() -> {
				try (OutputStream outputStream = channelConsumerAsOutputStream(reactor, channelConsumer)) {
					for (int i = 0; i < expectedSize; i++) {
						outputStream.write(i);
					}
				}
			}));

		for (int i = 0; i < expectedSize; i++) {
			assertEquals((byte) i, result.array()[i]);
		}
	}

	@Test
	public void testAsOutputStreamEmpty() {
		int expectedSize = 0;

		ChannelConsumer<ByteBuf> channelConsumer = ChannelConsumers.ofAsyncConsumer(value -> {
			assertEquals(expectedSize, value.readRemaining());
			value.recycle();
			return Promise.complete();
		});

		Reactor reactor = getCurrentReactor();
		await(Promise.ofBlocking(newSingleThreadExecutor(), () -> {
			//noinspection EmptyTryBlock
			try (OutputStream ignored = channelConsumerAsOutputStream(reactor, channelConsumer)) {
			}
		}));
	}

	@Test
	public void testAsOutputStreamException() {
		ChannelConsumer<ByteBuf> channelConsumer = ChannelConsumers.ofAsyncConsumer(value -> {
			value.recycle();
			return Promise.ofException(new RuntimeException());
		});

		Reactor reactor = getCurrentReactor();
		await(Promise.ofBlocking(newSingleThreadExecutor(), () -> {
			assertThrows(RuntimeException.class, () -> {
				try (OutputStream outputStream = channelConsumerAsOutputStream(reactor, channelConsumer)) {
					outputStream.write(0);
				}
			});
		}));
	}

	@Test
	public void testOfAnotherReactor() {
		Eventloop anotherReactor = Eventloop
			.builder()
			.withFatalErrorHandler(rethrow())
			.build();
		List<Integer> expectedList = List.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
		List<Integer> actualList = new ArrayList<>();
		ChannelConsumer<Integer> anotherEventloopConsumer = executeWithReactor(anotherReactor, () -> ChannelConsumers.ofConsumer(actualList::add));
		ChannelConsumer<Integer> consumer = ChannelConsumers.ofAnotherReactor(anotherReactor, anotherEventloopConsumer);

		startAnotherEventloop(anotherReactor);
		await(consumer.acceptAll(expectedList));
		stopAnotherEventloop(anotherReactor);

		assertEquals(expectedList, actualList);
	}

	@Test
	public void testOfAnotherReactorException() {
		Eventloop anotherReactor = Eventloop.builder()
			.withFatalErrorHandler(rethrow())
			.build();
		ExpectedException expectedException = new ExpectedException();
		List<Integer> list = new ArrayList<>();
		ChannelConsumer<Integer> anotherEventloopConsumer = executeWithReactor(anotherReactor, () -> ChannelConsumers.ofConsumer(list::add));
		ChannelConsumer<Integer> consumer = ChannelConsumers.ofAnotherReactor(anotherReactor, anotherEventloopConsumer);

		startAnotherEventloop(anotherReactor);
		Exception exception = awaitException(consumer.accept(1)
			.then(() -> consumer.accept(2))
			.whenComplete(() -> consumer.closeEx(expectedException))
			.then(() -> consumer.accept(3)));
		stopAnotherEventloop(anotherReactor);

		assertSame(expectedException, exception);
		assertEquals(List.of(1, 2), list);
	}

	private void startAnotherEventloop(Eventloop anotherEventloop) {
		anotherEventloop.keepAlive(true);
		new Thread(anotherEventloop, "another").start();
	}

	private void stopAnotherEventloop(Eventloop anotherEventloop) {
		anotherEventloop.execute(() -> anotherEventloop.keepAlive(false));
	}

}
