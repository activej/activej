package io.activej.fs;

import io.activej.bytebuf.ByteBuf;
import io.activej.bytebuf.ByteBufQueue;
import io.activej.common.tuple.Tuple2;
import io.activej.csp.ChannelConsumer;
import io.activej.csp.ChannelSupplier;
import io.activej.csp.ChannelSuppliers;
import io.activej.csp.file.ChannelFileWriter;
import io.activej.eventloop.Eventloop;
import io.activej.fs.exception.FsException;
import io.activej.fs.exception.FsIOException;
import io.activej.fs.exception.scalar.ForbiddenPathException;
import io.activej.fs.tcp.ActiveFsServer;
import io.activej.fs.tcp.RemoteActiveFs;
import io.activej.promise.Promise;
import io.activej.promise.Promises;
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
import java.util.*;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;

import static io.activej.bytebuf.ByteBufStrings.wrapUtf8;
import static io.activej.common.collection.CollectionUtils.set;
import static io.activej.fs.Utils.initTempDir;
import static io.activej.fs.util.RemoteFsUtils.UNEXPECTED_DATA;
import static io.activej.fs.util.RemoteFsUtils.UNEXPECTED_END_OF_STREAM;
import static io.activej.promise.TestUtils.await;
import static io.activej.promise.TestUtils.awaitException;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.*;

public final class FsIntegrationTest {
	private static final InetSocketAddress address = new InetSocketAddress("localhost", 5560);
	private static final byte[] BIG_FILE = new byte[2 * 1024 * 1024]; // 2 MB
	private static final byte[] CONTENT = "content".getBytes(UTF_8);

	static {
		ThreadLocalRandom.current().nextBytes(BIG_FILE);
	}

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@Rule
	public final TemporaryFolder temporaryFolder = new TemporaryFolder();

	private Path storage;
	private ActiveFsServer server;
	private ActiveFs fs;

	@Before
	public void setup() throws IOException {
		Executor executor = newCachedThreadPool();

		storage = temporaryFolder.newFolder("server_storage").toPath();
		initTempDir(storage);
		LocalActiveFs localFs = LocalActiveFs.create(Eventloop.getCurrentEventloop(), executor, storage);
		server = ActiveFsServer.create(Eventloop.getCurrentEventloop(), localFs).withListenAddress(address);
		server.listen();
		fs = RemoteActiveFs.create(Eventloop.getCurrentEventloop(), address);
	}

	@Test
	public void testUpload() throws IOException {
		String resultFile = "file_uploaded.txt";

		await(upload(resultFile, CONTENT)
				.whenComplete(server::close));

		assertArrayEquals(CONTENT, Files.readAllBytes(storage.resolve(resultFile)));
	}

	@Test
	public void testUploadCompletesCorrectly() {
		String resultFile = "file_uploaded.txt";

		byte[] bytes = await(fs.upload(resultFile)
				.then(ChannelSupplier.of(ByteBuf.wrapForReading(CONTENT))::streamTo)
				.map($ -> {
					try {
						return Files.readAllBytes(storage.resolve(resultFile));
					} catch (IOException e) {
						throw new AssertionError(e);
					}
				})
				.whenComplete(server::close));

		assertArrayEquals(CONTENT, bytes);
	}

	@Test
	public void uploadLessThanSpecified() {
		String filename = "incomplete.txt";
		Path path = storage.resolve(filename);
		assertFalse(Files.exists(path));

		Throwable exception = awaitException(ChannelSupplier.of(wrapUtf8("data"))
				.streamTo(fs.upload(filename, 10))
				.whenComplete(server::close));

		assertSame(UNEXPECTED_END_OF_STREAM, exception);

		assertFalse(Files.exists(path));
	}

	@Test
	public void uploadMoreThanSpecified() {
		String filename = "incomplete.txt";
		Path path = storage.resolve(filename);
		assertFalse(Files.exists(path));

		Throwable exception = awaitException(ChannelSupplier.of(wrapUtf8("data data data data"))
				.streamTo(fs.upload(filename, 10))
				.whenComplete(server::close));

		assertSame(UNEXPECTED_DATA, exception);

		assertFalse(Files.exists(path));
	}

	@Test
	public void testUploadMultiple() throws IOException {
		int files = 10;

		await(Promises.all(IntStream.range(0, 10)
				.mapToObj(i -> ChannelSupplier.of(ByteBuf.wrapForReading(CONTENT))
						.streamTo(ChannelConsumer.ofPromise(fs.upload("file" + i, CONTENT.length)))))
				.whenComplete(server::close));

		for (int i = 0; i < files; i++) {
			assertArrayEquals(CONTENT, Files.readAllBytes(storage.resolve("file" + i)));
		}
	}

	@Test
	public void testUploadBigFile() throws IOException {
		String resultFile = "big file_uploaded.txt";

		await(upload(resultFile, BIG_FILE)
				.whenComplete(server::close));

		assertArrayEquals(BIG_FILE, Files.readAllBytes(storage.resolve(resultFile)));
	}

	@Test
	public void testUploadLong() throws IOException {
		String resultFile = "this/is/not/empty/directory/2/file2_uploaded.txt";

		await(upload(resultFile, CONTENT)
				.whenComplete(server::close));

		assertArrayEquals(CONTENT, Files.readAllBytes(storage.resolve(resultFile)));
	}

	@Test
	public void testUploadServerFail() {
		Throwable exception = awaitException(upload("../../nonlocal/../file.txt", CONTENT)
				.whenComplete(server::close));

		assertThat(exception, instanceOf(ForbiddenPathException.class));
	}

	@Test
	public void testOnClientExceptionWhileUploading() {
		String resultFile = "upload_with_exceptions.txt";

		ChannelSupplier<ByteBuf> supplier = ChannelSuppliers.concat(
				ChannelSupplier.of(wrapUtf8("Test1"), wrapUtf8(" Test2"), wrapUtf8(" Test3")).async(),
				ChannelSupplier.of(ByteBuf.wrapForReading(BIG_FILE)),
				ChannelSupplier.ofException(new FsIOException(FsIntegrationTest.class, "Test exception")),
				ChannelSupplier.of(wrapUtf8("Test4")));

		Throwable exception = awaitException(supplier.streamTo(ChannelConsumer.ofPromise(fs.upload(resultFile, Long.MAX_VALUE)))
				.whenComplete(server::close));

		assertThat(exception, instanceOf(FsException.class));
		assertThat(exception.getMessage(), containsString("Test exception"));

		assertFalse(Files.exists(storage.resolve(resultFile)));
	}

	private Promise<ByteBuf> download(String file) {
		return fs.download(file)
				.then(supplier -> supplier.toCollector(ByteBufQueue.collector()))
				.whenComplete(server::close);
	}

	@Test
	public void testDownload() throws Exception {
		String file = "file1_downloaded.txt";
		Files.write(storage.resolve(file), CONTENT);

		ByteBuf result = await(download(file));

		assertArrayEquals(CONTENT, result.asArray());
	}

	@Test
	public void testDownloadLong() throws Exception {
		String file = "this/is/not/empty/directory/file.txt";
		Files.createDirectories(storage.resolve("this/is/not/empty/directory"));
		Files.write(storage.resolve(file), CONTENT);

		ByteBuf result = await(download(file));

		assertArrayEquals(CONTENT, result.asArray());
	}

	@Test
	public void testDownloadNotExist() {
		String file = "file_not_exist_downloaded.txt";
		Throwable exception = awaitException(ChannelSupplier.ofPromise(fs.download(file))
				.streamTo(ChannelConsumer.of($ -> Promise.complete()))
				.whenComplete(server::close));

		assertThat(exception, instanceOf(FsException.class));
		assertThat(exception.getMessage(), containsString("File not found"));
	}

	@Test
	public void testManySimultaneousDownloads() throws IOException {
		String file = "some_file.txt";
		Files.write(storage.resolve(file), CONTENT);

		List<Promise<Void>> tasks = new ArrayList<>();

		Executor executor = newCachedThreadPool();
		for (int i = 0; i < 10; i++) {
			tasks.add(ChannelSupplier.ofPromise(fs.download(file))
					.streamTo(ChannelFileWriter.open(executor, storage.resolve("file" + i))));
		}

		await(Promises.all(tasks)
				.whenComplete(server::close));

		for (int i = 0; i < tasks.size(); i++) {
			assertArrayEquals(CONTENT, Files.readAllBytes(storage.resolve("file" + i)));
		}
	}

	@Test
	public void testDeleteFile() throws Exception {
		String file = "file.txt";
		Files.write(storage.resolve(file), CONTENT);

		await(fs.delete(file)
				.whenComplete(server::close));

		assertFalse(Files.exists(storage.resolve(file)));
	}

	@Test
	public void testDeleteMissingFile() {
		String file = "no_file.txt";

		FileMetadata metadata = await(fs.delete(file)
				.then(() -> fs.info(file))
				.whenComplete(server::close));
		assertNull(metadata);
	}

	@Test
	public void testFileList() throws Exception {
		Set<String> expected = set(
				"this/is/not/empty/directory/file1.txt",
				"file1.txt",
				"first file.txt"
		);

		Files.createDirectories(storage.resolve("this/is/not/empty/directory/"));
		for (String filename : expected) {
			Files.write(storage.resolve(filename), CONTENT);
		}

		Map<String, FileMetadata> metadataMap = await(fs.list("**")
				.whenComplete(server::close));

		assertEquals(expected, metadataMap.keySet());
	}

	@Test
	public void testSubdirectoryClient() throws IOException {
		Files.createDirectories(storage.resolve("this/is/not/empty/directory/"));
		Files.createDirectories(storage.resolve("subdirectory1/"));
		Files.createDirectories(storage.resolve("subdirectory2/subsubdirectory"));
		Files.write(storage.resolve("this/is/not/empty/directory/file1.txt"), CONTENT);
		Files.write(storage.resolve("this/is/not/empty/directory/file1.txt"), CONTENT);
		Files.write(storage.resolve("file1.txt"), CONTENT);
		Files.write(storage.resolve("first file.txt"), CONTENT);
		Files.write(storage.resolve("subdirectory1/file1.txt"), CONTENT);
		Files.write(storage.resolve("subdirectory1/first file.txt"), CONTENT);
		Files.write(storage.resolve("subdirectory2/file1.txt"), CONTENT);
		Files.write(storage.resolve("subdirectory2/first file.txt"), CONTENT);
		Files.write(storage.resolve("subdirectory2/subsubdirectory/file1.txt"), CONTENT);
		Files.write(storage.resolve("subdirectory2/subsubdirectory/first file.txt"), CONTENT);

		Set<String> expected1 = new HashSet<>();
		expected1.add("file1.txt");
		expected1.add("first file.txt");

		Set<String> expected2 = new HashSet<>(expected1);
		expected2.add("subsubdirectory/file1.txt");
		expected2.add("subsubdirectory/first file.txt");

		Tuple2<Map<String, FileMetadata>, Map<String, FileMetadata>> tuple = await(
				Promises.toTuple(ActiveFsAdapters.subdirectory(fs, "subdirectory1").list("**"), ActiveFsAdapters.subdirectory(fs, "subdirectory2").list("**"))
						.whenComplete(server::close)
		);

		assertEquals(expected1, tuple.getValue1().keySet());
		assertEquals(expected2, tuple.getValue2().keySet());
	}

	@Test
	public void testAppend() throws IOException {
		String filename = "file.txt";
		int offset = 3;
		String contentString = new String(CONTENT, UTF_8);
		String toAppend = "appended";
		String appended = contentString.substring(offset) + toAppend;
		Files.write(storage.resolve(filename), CONTENT);

		String result = await(ChannelSupplier.of(wrapUtf8(appended))
				.streamTo(fs.append(filename, offset))
				.then(() -> fs.download(filename))
				.then(supplier -> supplier.toCollector(ByteBufQueue.collector())
						.map(byteBuf -> byteBuf.asString(UTF_8)))
				.whenComplete(server::close));

		assertEquals(contentString + toAppend, result);
	}

	private Promise<Void> upload(String resultFile, byte[] bytes) {
		return fs.upload(resultFile, bytes.length)
				.then(ChannelSupplier.of(ByteBuf.wrapForReading(bytes))::streamTo);
	}
}
