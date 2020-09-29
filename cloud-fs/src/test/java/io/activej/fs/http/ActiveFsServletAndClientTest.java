package io.activej.fs.http;

import io.activej.bytebuf.ByteBuf;
import io.activej.bytebuf.ByteBufQueue;
import io.activej.common.exception.ExpectedException;
import io.activej.csp.ChannelConsumer;
import io.activej.csp.ChannelSupplier;
import io.activej.csp.ChannelSuppliers;
import io.activej.fs.ActiveFs;
import io.activej.fs.FileMetadata;
import io.activej.fs.LocalActiveFs;
import io.activej.fs.exception.scalar.FileNotFoundException;
import io.activej.fs.exception.scalar.ForbiddenPathException;
import io.activej.http.AsyncServlet;
import io.activej.http.StubHttpClient;
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
import java.util.List;
import java.util.Map;
import java.util.Set;

import static io.activej.bytebuf.ByteBufStrings.wrapUtf8;
import static io.activej.common.collection.CollectionUtils.set;
import static io.activej.eventloop.Eventloop.getCurrentEventloop;
import static io.activej.fs.Utils.initTempDir;
import static io.activej.fs.util.RemoteFsUtils.UNEXPECTED_DATA;
import static io.activej.fs.util.RemoteFsUtils.UNEXPECTED_END_OF_STREAM;
import static io.activej.promise.TestUtils.await;
import static io.activej.promise.TestUtils.awaitException;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.*;

public final class ActiveFsServletAndClientTest {
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

	private ActiveFs fs;

	@Before
	public void setUp() throws Exception {
		storage = tmpFolder.newFolder("storage").toPath();

		initTempDir(storage);
		AsyncServlet servlet = ActiveFsServlet.create(LocalActiveFs.create(getCurrentEventloop(), newSingleThreadExecutor(), storage));
		fs = HttpActiveFs.create("http://localhost", StubHttpClient.of(servlet));

		initializeDirs();
	}

	@Test
	public void list() {
		Map<String, FileMetadata> metadata = await(fs.list("**"));
		assertEquals(initialFiles, metadata.keySet());
	}

	@Test
	public void upload() throws IOException {
		String content = "Test data";
		String fileName = "newDir/newFile";
		await(ChannelSupplier.of(wrapUtf8(content)).streamTo(fs.upload(fileName)));
		List<String> strings = Files.readAllLines(storage.resolve(fileName));

		assertEquals(1, strings.size());
		assertEquals(content, strings.get(0));
	}

	@Test
	public void uploadIncompleteFile() {
		String filename = "incomplete.txt";
		Path path = storage.resolve(filename);
		assertFalse(Files.exists(path));

		ExpectedException expectedException = new ExpectedException();
		ChannelConsumer<ByteBuf> consumer = await(fs.upload(filename));

		Throwable exception = awaitException(ChannelSuppliers.concat(
				ChannelSupplier.of(wrapUtf8("some"), wrapUtf8("test"), wrapUtf8("data")),
				ChannelSupplier.ofException(expectedException))
				.streamTo(consumer));

		assertSame(expectedException, exception);

		assertFalse(Files.exists(path));
	}

	@Test
	public void uploadLessThanSpecified() {
		String filename = "incomplete.txt";
		Path path = storage.resolve(filename);
		assertFalse(Files.exists(path));

		ChannelConsumer<ByteBuf> consumer = await(fs.upload(filename, 10));

		Throwable exception = awaitException(ChannelSupplier.of(wrapUtf8("data")).streamTo(consumer));

		assertSame(UNEXPECTED_END_OF_STREAM, exception);

		assertFalse(Files.exists(path));
	}

	@Test
	public void uploadMoreThanSpecified() {
		String filename = "incomplete.txt";
		Path path = storage.resolve(filename);
		assertFalse(Files.exists(path));

		ChannelConsumer<ByteBuf> consumer = await(fs.upload(filename, 10));

		Throwable exception = awaitException(ChannelSupplier.of(wrapUtf8("data data data data")).streamTo(consumer));

		assertSame(UNEXPECTED_DATA, exception);

		assertFalse(Files.exists(path));
	}

	@Test
	public void uploadIllegalPath() {
		Throwable exception = awaitException(ChannelSupplier.of(wrapUtf8("test")).streamTo(fs.upload("../outside")));
		assertThat(exception, instanceOf(ForbiddenPathException.class));
	}

	@Test
	public void download() throws IOException {
		String fileName = "directory/subdir/file3.txt";
		ChannelSupplier<ByteBuf> supplier = await(fs.download(fileName));
		ByteBuf result = await(supplier.toCollector(ByteBufQueue.collector()));
		byte[] expected = Files.readAllBytes(storage.resolve(fileName));

		assertArrayEquals(expected, result.asArray());
	}

	@Test
	public void downloadNonExistent() {
		Throwable exception = awaitException(fs.download("nonExistent"));
		assertThat(exception, instanceOf(FileNotFoundException.class));
	}

	private void initializeDirs() {
		try {
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
