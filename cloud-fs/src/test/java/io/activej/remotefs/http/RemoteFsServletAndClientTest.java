package io.activej.remotefs.http;

import io.activej.bytebuf.ByteBuf;
import io.activej.bytebuf.ByteBufQueue;
import io.activej.csp.ChannelSupplier;
import io.activej.http.AsyncServlet;
import io.activej.http.StubHttpClient;
import io.activej.remotefs.FileMetadata;
import io.activej.remotefs.FsClient;
import io.activej.remotefs.LocalFsClient;
import io.activej.test.rules.ByteBufRule;
import io.activej.test.rules.EventloopRule;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static io.activej.bytebuf.ByteBufStrings.wrapUtf8;
import static io.activej.common.collection.CollectionUtils.set;
import static io.activej.eventloop.Eventloop.getCurrentEventloop;
import static io.activej.promise.TestUtils.await;
import static io.activej.promise.TestUtils.awaitException;
import static io.activej.remotefs.FsClient.BAD_PATH;
import static io.activej.remotefs.FsClient.FILE_NOT_FOUND;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static java.util.stream.Collectors.toSet;
import static org.junit.Assert.*;

public final class RemoteFsServletAndClientTest {
	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@Rule
	public final TemporaryFolder tmpFolder = new TemporaryFolder();

	private static final Set<String> initialFiles = set(
			"file",
			"file2",
			"directory/subdir/file3.txt",
			"directory/file.txt",
			"directory2/file2.txt"
	);

	private Path storage;

	private FsClient client;

	@Before
	public void setUp() throws Exception {
		storage = tmpFolder.newFolder("storage").toPath();

		AsyncServlet servlet = RemoteFsServlet.create(LocalFsClient.create(getCurrentEventloop(), newSingleThreadExecutor(), storage));
		client = HttpFsClient.create("http://localhost", StubHttpClient.of(servlet));

		initializeDirs();
	}

	@Test
	public void list() {
		List<FileMetadata> metadata = await(client.list("**"));
		Set<String> actual = metadata.stream().map(FileMetadata::getName).collect(toSet());
		assertEquals(initialFiles, actual);
	}

	@Test
	public void upload() throws IOException {
		String content = "Test data";
		String fileName = "newDir/newFile";
		await(ChannelSupplier.of(wrapUtf8(content)).streamTo(client.upload(fileName)));
		List<String> strings = Files.readAllLines(storage.resolve(fileName));

		assertEquals(1, strings.size());
		assertEquals(content, strings.get(0));
	}

	@Test
	public void uploadIllegalPath() {
		Throwable exception = awaitException(ChannelSupplier.of(wrapUtf8("test")).streamTo(client.upload("../outside")));
		assertSame(BAD_PATH, exception);
	}

	@Test
	public void download() throws IOException {
		String fileName = "directory/subdir/file3.txt";
		ChannelSupplier<ByteBuf> supplier = await(client.download(fileName));
		ByteBuf result = await(supplier.toCollector(ByteBufQueue.collector()));
		byte[] expected = Files.readAllBytes(storage.resolve(fileName));

		assertArrayEquals(expected, result.asArray());
	}

	@Test
	public void downloadNonExistent() {
		Throwable exception = awaitException(client.download("nonExistent"));
		assertSame(FILE_NOT_FOUND, exception);
	}

	@Test
	public void info() {

	}

	private void clearDirectory(Path dir) throws IOException {
		for (Iterator<Path> iterator = Files.list(dir).iterator(); iterator.hasNext(); ) {
			Path file = iterator.next();
			if (Files.isDirectory(file))
				clearDirectory(file);
			Files.delete(file);
		}
	}

	private void initializeDirs() {
		try {
			clearDirectory(storage);

			for (String path : initialFiles) {
				Path file = this.storage.resolve(path);
				Files.createDirectories(file.getParent());
				Files.write(file, String.format("This is contents of file %s", file).getBytes(UTF_8), CREATE, TRUNCATE_EXISTING);
			}
		} catch (IOException e) {
			throw new AssertionError(e);
		}
	}
}
