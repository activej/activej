package io.activej.csp.supplier;

import io.activej.bytebuf.ByteBuf;
import io.activej.bytebuf.ByteBufs;
import io.activej.eventloop.Eventloop;
import io.activej.test.rules.ByteBufRule;
import org.junit.*;
import org.junit.rules.TemporaryFolder;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static io.activej.bytebuf.ByteBuf.wrapForReading;
import static io.activej.common.exception.FatalErrorHandlers.rethrow;
import static io.activej.csp.binary.Utils.channelSupplierAsInputStream;
import static io.activej.csp.supplier.ChannelSuppliers.ofInputStream;
import static io.activej.promise.TestUtils.await;
import static io.activej.promise.TestUtils.awaitException;
import static io.activej.reactor.Reactor.executeWithReactor;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.*;

public class ChannelSuppliersIOTest {

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
	public void inputStreamAsChannelSupplierTest() throws IOException {
		Eventloop.builder()
			.withCurrentThread()
			.withFatalErrorHandler(rethrow())
			.build();
		ChannelSupplier<ByteBuf> supplier;
		try (InputStream is = inputStream()) {
			supplier = ofInputStream(executor, is);

			ByteBuf buf = await(supplier.toCollector(ByteBufs.collector()));
			assertArrayEquals(DATA, buf.asArray());
		}

		Exception exception = awaitException(supplier.get());
		assertThat(exception, instanceOf(IOException.class));
		assertEquals("Stream Closed", exception.getMessage());
	}

	@Test
	public void inputStreamAsChannelSupplierCloseTest() throws IOException {
		Eventloop.builder()
			.withCurrentThread()
			.withFatalErrorHandler(rethrow())
			.build();
		try (InputStream is = inputStream()) {
			is.close();
			ChannelSupplier<ByteBuf> supplier = ofInputStream(executor, is);

			IOException exception = awaitException(supplier.get());
			IOException e = assertThrows(IOException.class, is::read);

			assertEquals(e.getMessage(), exception.getMessage());
		}
	}

	@Test
	public void channelSupplierAsInputStreamTest() throws IOException, InterruptedException {
		ByteBuf byteBuf = wrapForReading(DATA);

		Eventloop eventloop = Eventloop.builder()
			.withFatalErrorHandler(rethrow())
			.build();
		eventloop.keepAlive(true);
		Thread eventloopThread = new Thread(eventloop);
		eventloopThread.start();

		ChannelSupplier<ByteBuf> supplier = executeWithReactor(eventloop, () -> ChannelSuppliers.ofValue(byteBuf));

		try (
			InputStream inputStream = inputStream();
			InputStream channelSupplierAsInputStream = channelSupplierAsInputStream(eventloop, supplier)
		) {
			while (true) {
				int read1 = inputStream.read();
				int read2 = channelSupplierAsInputStream.read();

				assertEquals(read1, read2);

				if (read1 == -1) {
					read1 = inputStream.read();
					read2 = channelSupplierAsInputStream.read();

					assertEquals(read1, read2);
					assertEquals(-1, read1);
					break;
				}
			}
		}

		eventloop.keepAlive(false);
		eventloopThread.join();
	}

	@SuppressWarnings("ResultOfMethodCallIgnored")
	@Test
	public void channelSupplierAsInputStreamCloseTest() throws IOException, InterruptedException {
		ByteBuf byteBuf = wrapForReading(DATA);

		Eventloop eventloop = Eventloop
			.builder()
			.withFatalErrorHandler(rethrow())
			.build();
		eventloop.keepAlive(true);
		Thread eventloopThread = new Thread(eventloop);
		eventloopThread.start();

		ChannelSupplier<ByteBuf> supplier = executeWithReactor(eventloop, () -> ChannelSuppliers.ofValue(byteBuf));

		try (
			InputStream inputStream = inputStream();
			InputStream channelSupplierAsInputStream = channelSupplierAsInputStream(eventloop, supplier)
		) {
			inputStream.close();
			channelSupplierAsInputStream.close();

			IOException exception1 = assertThrows(IOException.class, inputStream::read);
			IOException exception2 = assertThrows(IOException.class, channelSupplierAsInputStream::read);

			assertEquals(exception1.getMessage(), exception2.getMessage());
		}

		eventloop.keepAlive(false);
		eventloopThread.join();
	}

	private InputStream inputStream() {
		try {
			return new FileInputStream(file.toFile());
		} catch (FileNotFoundException e) {
			throw new AssertionError(e);
		}
	}
}
