package io.activej.fs;

import io.activej.bytebuf.ByteBuf;
import io.activej.bytebuf.ByteBufQueue;
import io.activej.csp.ChannelConsumer;
import io.activej.csp.ChannelSupplier;
import io.activej.csp.file.ChannelFileWriter;
import io.activej.eventloop.Eventloop;
import io.activej.fs.exception.scalar.IllegalOffsetException;
import io.activej.fs.exception.scalar.MalformedGlobException;
import io.activej.fs.tcp.ActiveFsServer;
import io.activej.fs.tcp.RemoteActiveFs;
import io.activej.test.rules.ByteBufRule;
import io.activej.test.rules.EventloopRule;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;

import static io.activej.fs.Utils.initTempDir;
import static io.activej.promise.TestUtils.await;
import static io.activej.promise.TestUtils.awaitException;
import static io.activej.test.TestUtils.getFreePort;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.*;

public final class TestPartialRemoteFs {
	private static final int PORT = getFreePort();
	private static final String FILE = "file.txt";
	private static final byte[] CONTENT = "test content of the file".getBytes(UTF_8);

	private static final InetSocketAddress ADDRESS = new InetSocketAddress("localhost", PORT);

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@Rule
	public final TemporaryFolder tempFolder = new TemporaryFolder();

	private ActiveFsServer server;
	private RemoteActiveFs client;

	private Path serverStorage;
	private Path clientStorage;

	@Before
	public void setup() throws IOException {
		Executor executor = Executors.newSingleThreadExecutor();

		serverStorage = tempFolder.newFolder().toPath();
		clientStorage = tempFolder.newFolder().toPath();
		LocalActiveFs localFs = LocalActiveFs.create(Eventloop.getCurrentEventloop(), executor, serverStorage);
		initTempDir(serverStorage);
		server = ActiveFsServer.create(Eventloop.getCurrentEventloop(), localFs).withListenAddress(ADDRESS);
		server.listen();
		client = RemoteActiveFs.create(Eventloop.getCurrentEventloop(), ADDRESS);

		Files.write(serverStorage.resolve(FILE), CONTENT);
	}

	@Test
	public void justDownload() throws IOException {
		await(ChannelSupplier.ofPromise(client.download(FILE))
				.streamTo(ChannelFileWriter.open(newCachedThreadPool(), clientStorage.resolve(FILE)))
				.whenComplete(server::close));

		assertArrayEquals(CONTENT, Files.readAllBytes(clientStorage.resolve(FILE)));
	}

	@Test
	public void ensuredUpload() throws IOException {
		byte[] data = new byte[10 * (1 << 20)]; // 10 mb
		ThreadLocalRandom.current().nextBytes(data);

		ChannelSupplier<ByteBuf> supplier = ChannelSupplier.of(ByteBuf.wrapForReading(data));
		ChannelConsumer<ByteBuf> consumer = ChannelConsumer.ofPromise(client.upload("test_big_file.bin", data.length));

		await(supplier.streamTo(consumer)
				.whenComplete(server::close));

		assertArrayEquals(data, Files.readAllBytes(serverStorage.resolve("test_big_file.bin")));
	}

	@Test
	public void downloadPrefix() throws IOException {
		await(ChannelSupplier.ofPromise(client.download(FILE, 0, 12))
				.streamTo(ChannelFileWriter.open(newCachedThreadPool(), clientStorage.resolve(FILE)))
				.whenComplete(server::close));

		assertArrayEquals("test content".getBytes(UTF_8), Files.readAllBytes(clientStorage.resolve(FILE)));
	}

	@Test
	public void downloadSuffix() throws IOException {
		await(ChannelSupplier.ofPromise(client.download(FILE, 13, Long.MAX_VALUE))
				.streamTo(ChannelFileWriter.open(newCachedThreadPool(), clientStorage.resolve(FILE)))
				.whenComplete(server::close));

		assertArrayEquals("of the file".getBytes(UTF_8), Files.readAllBytes(clientStorage.resolve(FILE)));
	}

	@Test
	public void downloadPart() throws IOException {
		await(ChannelSupplier.ofPromise(client.download(FILE, 5, 10))
				.streamTo(ChannelFileWriter.open(newCachedThreadPool(), clientStorage.resolve(FILE)))
				.whenComplete(server::close));

		assertArrayEquals("content of".getBytes(UTF_8), Files.readAllBytes(clientStorage.resolve(FILE)));
	}

	@Test
	public void downloadOverSuffix() {
		int offset = 13;
		ByteBuf result = await(ChannelSupplier.ofPromise(client.download(FILE, offset, 123))
				.toCollector(ByteBufQueue.collector())
				.whenComplete(server::close));

		assertEquals(new String(CONTENT, offset, CONTENT.length - offset, UTF_8), result.asString(UTF_8));
	}

	@Test
	public void downloadOver() {
		Throwable exception = awaitException(ChannelSupplier.ofPromise(client.download(FILE, 123, 123))
				.toCollector(ByteBufQueue.collector())
				.whenComplete(server::close));

		assertThat(exception, instanceOf(IllegalOffsetException.class));
	}

	@Test
	public void malformedGlob() {
		Throwable exception = awaitException(client.list("[")
				.whenComplete(server::close));

		assertThat(exception, instanceOf(MalformedGlobException.class));
	}
}
