package io.activej.csp.consumer;

import io.activej.bytebuf.ByteBuf;
import io.activej.bytebuf.ByteBufs;
import io.activej.csp.supplier.ChannelSuppliers;
import io.activej.eventloop.Eventloop;
import io.activej.test.rules.ByteBufRule;
import org.junit.*;
import org.junit.rules.TemporaryFolder;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static io.activej.bytebuf.ByteBufStrings.wrapUtf8;
import static io.activej.common.exception.FatalErrorHandlers.rethrow;
import static io.activej.csp.binary.Utils.channelConsumerAsOutputStream;
import static io.activej.csp.consumer.ChannelConsumers.ofOutputStream;
import static io.activej.promise.TestUtils.await;
import static io.activej.promise.TestUtils.awaitException;
import static io.activej.reactor.Reactor.executeWithReactor;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.*;

public class ChannelConsumersIOTest {

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@Rule
	public final TemporaryFolder temporaryFolder = new TemporaryFolder();

	public static final byte[] DATA = "Hello world".getBytes(UTF_8);

	private ExecutorService executor;
	private Path file;

	@Before
	public void setUp() throws Exception {
		executor = Executors.newSingleThreadExecutor();
		file = temporaryFolder.newFile().toPath();
		Files.write(file, DATA);
	}

	@After
	public void tearDown() throws Exception {
		executor.shutdown();
	}

	@Test
	public void outputStreamAsChannelConsumerTest() throws IOException {
		Eventloop.builder()
			.withCurrentThread()
			.withFatalErrorHandler(rethrow())
			.build();
		ChannelConsumer<ByteBuf> consumer;
		try (OutputStream os = outputStream()) {
			consumer = ofOutputStream(executor, os);

			ByteBuf value = ByteBuf.wrapForReading(DATA);
			await(ChannelSuppliers.ofValue(value).streamTo(consumer));
		}
		assertArrayEquals(DATA, Files.readAllBytes(file));

		Exception exception = awaitException(consumer.accept(wrapUtf8("error")));
		assertThat(exception, instanceOf(IOException.class));
		assertEquals("Stream Closed", exception.getMessage());
	}

	@Test
	public void outputStreamAsChannelConsumerCloseTest() throws IOException {
		Eventloop.builder()
			.withCurrentThread()
			.withFatalErrorHandler(rethrow())
			.build();
		try (OutputStream os = outputStream()) {
			os.close();
			ChannelConsumer<ByteBuf> consumer = ofOutputStream(executor, os);

			IOException exception = awaitException(consumer.accept(ByteBuf.wrapForReading(new byte[]{1})));
			IOException e = assertThrows(IOException.class, () -> os.write(1));
			assertEquals(e.getMessage(), exception.getMessage());
		}
	}

	@Test
	public void channelConsumerAsOutputStreamTest() throws IOException, InterruptedException {
		Eventloop eventloop = Eventloop.builder()
			.withFatalErrorHandler(rethrow())
			.build();
		eventloop.keepAlive(true);
		Thread eventloopThread = new Thread(eventloop);
		eventloopThread.start();

		List<ByteBuf> expected = List.of(wrapUtf8("Hello"), wrapUtf8("World"));
		List<ByteBuf> bufs = new ArrayList<>();

		ChannelConsumer<ByteBuf> consumer = executeWithReactor(eventloop, () -> ChannelConsumers.ofConsumer(bufs::add));

		try (
			OutputStream outputStream = outputStream();
			OutputStream channelConsumerAsOutputStream = channelConsumerAsOutputStream(eventloop, consumer)
		) {
			for (ByteBuf byteBuf : expected) {
				outputStream.write(byteBuf.getArray());
				channelConsumerAsOutputStream.write(byteBuf.asArray());
			}
		}

		byte[] actual = bufs.stream().collect(ByteBufs.collector()).asArray();
		assertArrayEquals(actual, Files.readAllBytes(file));

		eventloop.keepAlive(false);
		eventloopThread.join();
	}

	@Test
	public void channelConsumerAsOutputStreamCloseTest() throws IOException, InterruptedException {
		Eventloop eventloop = Eventloop.builder()
			.withFatalErrorHandler(rethrow())
			.build();
		eventloop.keepAlive(true);
		Thread eventloopThread = new Thread(eventloop);
		eventloopThread.start();

		ChannelConsumer<ByteBuf> consumer = executeWithReactor(eventloop, () -> ChannelConsumers.ofConsumer($ -> fail()));

		try (
			OutputStream outputStream = outputStream();
			OutputStream channelConsumerAsOutputStream = channelConsumerAsOutputStream(eventloop, consumer)
		) {
			outputStream.close();
			channelConsumerAsOutputStream.close();

			IOException exception1 = assertThrows(IOException.class, () -> outputStream.write(1));
			IOException exception2 = assertThrows(IOException.class, () -> channelConsumerAsOutputStream.write(1));

			assertEquals(exception1.getMessage(), exception2.getMessage());
		}

		eventloop.keepAlive(false);
		eventloopThread.join();

	}

	private OutputStream outputStream() {
		try {
			return new FileOutputStream(file.toFile());
		} catch (FileNotFoundException e) {
			throw new AssertionError(e);
		}
	}
}
