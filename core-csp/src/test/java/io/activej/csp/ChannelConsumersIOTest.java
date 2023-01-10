package io.activej.csp;

import io.activej.bytebuf.ByteBuf;
import io.activej.bytebuf.ByteBufs;
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
import static io.activej.common.exception.FatalErrorHandler.rethrow;
import static io.activej.csp.ChannelConsumers.channelConsumerAsOutputStream;
import static io.activej.csp.ChannelConsumers.outputStreamAsChannelConsumer;
import static io.activej.promise.TestUtils.await;
import static io.activej.promise.TestUtils.awaitException;
import static io.activej.reactor.Reactor.executeWithReactor;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
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
		Eventloop.create().withCurrentThread().withFatalErrorHandler(rethrow());
		ChannelConsumer<ByteBuf> consumer;
		try (OutputStream os = outputStream()) {
			consumer = outputStreamAsChannelConsumer(executor, os);

			await(ChannelSupplier.of(ByteBuf.wrapForReading(DATA)).streamTo(consumer));
		}
		assertArrayEquals(DATA, Files.readAllBytes(file));

		Exception exception = awaitException(consumer.accept(wrapUtf8("error")));
		assertThat(exception, instanceOf(IOException.class));
		assertEquals("Stream Closed", exception.getMessage());
	}

	@Test
	public void outputStreamAsChannelConsumerCloseTest() throws IOException {
		Eventloop.create().withCurrentThread().withFatalErrorHandler(rethrow());
		try (OutputStream os = outputStream()) {
			os.close();
			ChannelConsumer<ByteBuf> consumer = outputStreamAsChannelConsumer(executor, os);

			IOException exception = awaitException(consumer.accept(ByteBuf.wrapForReading(new byte[]{1})));
			try {
				os.write(1);
				fail();
			} catch (IOException e) {
				assertEquals(e.getMessage(), exception.getMessage());
			}
		}
	}

	@Test
	public void channelConsumerAsOutputStreamTest() throws IOException, InterruptedException {
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrow());
		eventloop.keepAlive(true);
		Thread eventloopThread = new Thread(eventloop);
		eventloopThread.start();

		List<ByteBuf> expected = List.of(wrapUtf8("Hello"), wrapUtf8("World"));
		List<ByteBuf> bufs = new ArrayList<>();

		ChannelConsumer<ByteBuf> consumer = executeWithReactor(eventloop, () -> ChannelConsumer.ofConsumer(bufs::add));

		try (OutputStream outputStream = outputStream();
			 OutputStream channelConsumerAsOutputStream = channelConsumerAsOutputStream(eventloop, consumer)) {
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
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrow());
		eventloop.keepAlive(true);
		Thread eventloopThread = new Thread(eventloop);
		eventloopThread.start();

		ChannelConsumer<ByteBuf> consumer = executeWithReactor(eventloop, () -> ChannelConsumer.ofConsumer($ -> fail()));

		try (OutputStream outputStream = outputStream();
			 OutputStream channelConsumerAsOutputStream = channelConsumerAsOutputStream(eventloop, consumer)) {
			outputStream.close();
			channelConsumerAsOutputStream.close();

			IOException exception1 = null;
			try {
				outputStream.write(1);
				fail();
			} catch (IOException e) {
				exception1 = e;
			}

			IOException exception2 = null;
			try {
				channelConsumerAsOutputStream.write(1);
				fail();
			} catch (IOException e) {
				exception2 = e;
			}

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
