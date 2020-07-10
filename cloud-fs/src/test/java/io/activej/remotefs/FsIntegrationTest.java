package io.activej.remotefs;

import io.activej.bytebuf.ByteBuf;
import io.activej.bytebuf.ByteBufQueue;
import io.activej.common.exception.StacklessException;
import io.activej.common.tuple.Tuple2;
import io.activej.csp.ChannelConsumer;
import io.activej.csp.ChannelSupplier;
import io.activej.csp.ChannelSuppliers;
import io.activej.csp.file.ChannelFileWriter;
import io.activej.eventloop.Eventloop;
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
import static io.activej.csp.binary.BinaryChannelSupplier.UNEXPECTED_DATA_EXCEPTION;
import static io.activej.csp.binary.BinaryChannelSupplier.UNEXPECTED_END_OF_STREAM_EXCEPTION;
import static io.activej.promise.TestUtils.await;
import static io.activej.promise.TestUtils.awaitException;
import static io.activej.remotefs.FsClient.BAD_PATH;
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
	private RemoteFsServer server;
	private FsClient client;

	@Before
	public void setup() throws IOException {
		Executor executor = newCachedThreadPool();

		storage = temporaryFolder.newFolder("server_storage").toPath();
		server = RemoteFsServer.create(Eventloop.getCurrentEventloop(), executor, storage).withListenAddress(address);
		server.listen();
		client = RemoteFsClient.create(Eventloop.getCurrentEventloop(), address);
	}

	@Test
	public void testUpload() throws IOException {
		String resultFile = "file_uploaded.txt";

		await(upload(resultFile, CONTENT)
				.whenComplete(server::close));

		assertArrayEquals(CONTENT, Files.readAllBytes(storage.resolve(resultFile)));
	}

	@Test
	public void uploadLessThanSpecified() {
		String filename = "incomplete.txt";
		Path path = storage.resolve(filename);
		assertFalse(Files.exists(path));

		Throwable exception = awaitException(ChannelSupplier.of(wrapUtf8("data"))
				.streamTo(client.upload(filename, 10))
				.whenComplete(server::close));

		assertEquals(UNEXPECTED_END_OF_STREAM_EXCEPTION, exception);

		assertFalse(Files.exists(path));
	}

	@Test
	public void uploadMoreThanSpecified() {
		String filename = "incomplete.txt";
		Path path = storage.resolve(filename);
		assertFalse(Files.exists(path));

		Throwable exception = awaitException(ChannelSupplier.of(wrapUtf8("data data data data"))
				.streamTo(client.upload(filename, 10))
				.whenComplete(server::close));

		assertEquals(UNEXPECTED_DATA_EXCEPTION, exception);

		assertFalse(Files.exists(path));
	}

	@Test
	public void testUploadMultiple() throws IOException {
		int files = 10;

		await(Promises.all(IntStream.range(0, 10)
				.mapToObj(i -> ChannelSupplier.of(ByteBuf.wrapForReading(CONTENT))
						.streamTo(ChannelConsumer.ofPromise(client.upload("file" + i, CONTENT.length)))))
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

		assertSame(BAD_PATH, exception);
	}

	@Test
	public void testOnClientExceptionWhileUploading() {
		String resultFile = "upload_with_exceptions.txt";

		ChannelSupplier<ByteBuf> supplier = ChannelSuppliers.concat(
				ChannelSupplier.of(wrapUtf8("Test1"), wrapUtf8(" Test2"), wrapUtf8(" Test3")).async(),
				ChannelSupplier.of(ByteBuf.wrapForReading(BIG_FILE)),
				ChannelSupplier.ofException(new StacklessException(FsIntegrationTest.class, "Test exception")),
				ChannelSupplier.of(wrapUtf8("Test4")));

		Throwable exception = awaitException(supplier.streamTo(ChannelConsumer.ofPromise(client.upload(resultFile, Long.MAX_VALUE)))
				.whenComplete(server::close));

		assertThat(exception, instanceOf(StacklessException.class));
		assertThat(exception.getMessage(), containsString("Test exception"));

		assertFalse(Files.exists(storage.resolve(resultFile)));
	}

	private Promise<ByteBuf> download(String file) {
		return client.download(file)
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
		Throwable exception = awaitException(ChannelSupplier.ofPromise(client.download(file))
				.streamTo(ChannelConsumer.of($ -> Promise.complete()))
				.whenComplete(server::close));

		assertThat(exception, instanceOf(StacklessException.class));
		assertThat(exception.getMessage(), containsString("File not found"));
	}

	@Test
	public void testManySimultaneousDownloads() throws IOException {
		String file = "some_file.txt";
		Files.write(storage.resolve(file), CONTENT);

		List<Promise<Void>> tasks = new ArrayList<>();

		Executor executor = newCachedThreadPool();
		for (int i = 0; i < 10; i++) {
			tasks.add(ChannelSupplier.ofPromise(client.download(file))
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

		await(client.delete(file)
				.whenComplete(server::close));

		assertFalse(Files.exists(storage.resolve(file)));
	}

	@Test
	public void testDeleteMissingFile() {
		String file = "no_file.txt";

		FileMetadata metadata = await(client.delete(file)
				.then(() -> client.info(file))
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

		Map<String, FileMetadata> metadataMap = await(client.list("**")
				.whenComplete(server::close));

		assertEquals(expected, metadataMap.keySet());
	}

	@Test
	public void testSubfolderClient() throws IOException {
		Files.createDirectories(storage.resolve("this/is/not/empty/directory/"));
		Files.createDirectories(storage.resolve("subfolder1/"));
		Files.createDirectories(storage.resolve("subfolder2/subsubfolder"));
		Files.write(storage.resolve("this/is/not/empty/directory/file1.txt"), CONTENT);
		Files.write(storage.resolve("this/is/not/empty/directory/file1.txt"), CONTENT);
		Files.write(storage.resolve("file1.txt"), CONTENT);
		Files.write(storage.resolve("first file.txt"), CONTENT);
		Files.write(storage.resolve("subfolder1/file1.txt"), CONTENT);
		Files.write(storage.resolve("subfolder1/first file.txt"), CONTENT);
		Files.write(storage.resolve("subfolder2/file1.txt"), CONTENT);
		Files.write(storage.resolve("subfolder2/first file.txt"), CONTENT);
		Files.write(storage.resolve("subfolder2/subsubfolder/file1.txt"), CONTENT);
		Files.write(storage.resolve("subfolder2/subsubfolder/first file.txt"), CONTENT);

		Set<String> expected1 = new HashSet<>();
		expected1.add("file1.txt");
		expected1.add("first file.txt");

		Set<String> expected2 = new HashSet<>(expected1);
		expected2.add("subsubfolder/file1.txt");
		expected2.add("subsubfolder/first file.txt");

		Tuple2<Map<String, FileMetadata>, Map<String, FileMetadata>> tuple = await(
				Promises.toTuple(client.subfolder("subfolder1").list("**"), client.subfolder("subfolder2").list("**"))
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
				.streamTo(client.append(filename, offset))
				.then(() -> client.download(filename))
				.then(supplier -> supplier.toCollector(ByteBufQueue.collector())
						.map(byteBuf -> byteBuf.asString(UTF_8)))
				.whenComplete(server::close));

		assertEquals(contentString + toAppend, result);
	}

	private Promise<Void> upload(String resultFile, byte[] bytes) {
		return client.upload(resultFile, bytes.length)
				.then(ChannelSupplier.of(ByteBuf.wrapForReading(bytes))::streamTo);
	}
}
