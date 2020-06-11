package io.activej.remotefs;

import io.activej.async.function.AsyncConsumer;
import io.activej.bytebuf.ByteBuf;
import io.activej.bytebuf.ByteBufQueue;
import io.activej.common.MemSize;
import io.activej.csp.ChannelConsumer;
import io.activej.csp.ChannelSupplier;
import io.activej.eventloop.Eventloop;
import io.activej.promise.Promise;
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
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.function.Function;

import static io.activej.common.Preconditions.checkNotNull;
import static io.activej.promise.TestUtils.await;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.*;

public final class TestCachedFsClient {

	public static final Function<ChannelSupplier<ByteBuf>, Promise<Void>> RECYCLING_FUNCTION = supplier ->
			supplier.streamTo(ChannelConsumer.of(AsyncConsumer.of(ByteBuf::recycle)));

	public static final Function<ChannelSupplier<ByteBuf>, Promise<String>> TO_STRING = supplier ->
			supplier.toCollector(ByteBufQueue.collector()).map(buf -> buf.asString(UTF_8));

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@Rule
	public final TemporaryFolder tempFolder = new TemporaryFolder();

	private Path cacheStorage;
	private Path serverStorage;
	private Path cacheTestFile;
	private Path serverTestFile;
	private CachedFsClient cacheRemote;
	private String testTxtContent;

	private FsClient main;
	private FsClient cache;

	@Before
	public void setUp() throws Exception {
		cacheStorage = tempFolder.newFolder("cacheStorage").toPath();
		serverStorage = tempFolder.newFolder("serverStorage").toPath();

		Files.createDirectories(cacheStorage);
		Files.createDirectories(serverStorage);

		cacheTestFile = cacheStorage.resolve("test.txt");

		serverTestFile = serverStorage.resolve("test.txt");
		testTxtContent = "line1\nline2\nline3\nline4\nline5\nline6\nline7\nline8";
		Files.write(serverTestFile, testTxtContent.getBytes(UTF_8));

		Eventloop eventloop = Eventloop.getCurrentEventloop();
		Executor executor = Executors.newSingleThreadExecutor();

		main = LocalFsClient.create(eventloop, executor, serverStorage);
		cache = LocalFsClient.create(eventloop, executor, cacheStorage);
		cacheRemote = CachedFsClient.create(main, cache, CachedFsClient.lruCompare())
				.with(MemSize.kilobytes(50));
	}

	@Test
	public void testDownloadFileNotInCache() {
		String downloadedString = await(cacheRemote.download("test.txt")
				.then(TO_STRING));

		assertEquals(testTxtContent, downloadedString);

		String cachedString = await(cache.download("test.txt")
				.then(TO_STRING));

		assertEquals(testTxtContent, cachedString);
	}

	@Test
	public void testDownloadFileNotInCacheWithOffsetAndLength() {
		String downloadedString = await(cacheRemote.download("test.txt", 1, 2)
				.then(TO_STRING));

		assertEquals("in", downloadedString);
		assertFalse(Files.exists(cacheStorage.resolve("test.txt")));
	}

	@Test
	public void testDownloadFilePartlyInCacheWithOffsetAndLength() throws IOException {
		Files.write(cacheTestFile, "line1\nline2\nline3".getBytes(UTF_8));

		String downloadedString = await(cacheRemote.download("test.txt", 1, 2)
				.then(TO_STRING));

		assertEquals("in", downloadedString);
	}

	@Test
	public void testDownloadFileFullyInCache() throws IOException {
		Files.copy(serverTestFile, cacheTestFile);
		String downloadedString = await(cacheRemote.download("test.txt")
				.then(TO_STRING));

		assertEquals(testTxtContent, downloadedString);
	}

	@Test
	public void testDownloadFileFullyInCacheWithOffsetAndLength() throws IOException {
		Files.copy(serverTestFile, cacheTestFile);
		String downloadedString = await(cacheRemote.download("test.txt", 1, 2)
				.then(TO_STRING));

		assertEquals("in", downloadedString);
	}

	@Test
	public void testDownloadFileNotOnServer() throws IOException {
		Path filePath = cacheStorage.resolve("cacheOnly.txt");
		String fileContent = "This file is stored only in cache";
		Files.write(filePath, fileContent.getBytes());
		String downloadedString = await(cacheRemote.download("cacheOnly.txt")
				.then(TO_STRING));

		assertEquals(fileContent, downloadedString);
	}

	@Test
	public void testDownloadFileNotOnServerWithOffsetAndLength() throws IOException {
		Path filePath = cacheStorage.resolve("cacheOnly.txt");
		String fileContent = "This file is stored only in cache";
		Files.write(filePath, fileContent.getBytes());
		String downloadedString = await(cacheRemote.download("cacheOnly.txt", 1, 2)
				.then(TO_STRING));

		assertEquals("hi", downloadedString);
	}

	@Test
	public void testList() throws IOException {
		// Creating directories
		Files.createDirectories(cacheStorage.resolve("a"));
		Files.createDirectories(cacheStorage.resolve("b"));
		Files.createDirectories(cacheStorage.resolve("c/d"));
		Files.createDirectories(serverStorage.resolve("a"));
		Files.createDirectories(serverStorage.resolve("b"));

		// Adding 4 NEW files to cache, total is 5
		Files.write(cacheStorage.resolve("test1.txt"), "11".getBytes());
		Files.write(cacheStorage.resolve("a/test2.txt"), "22".getBytes());
		Files.write(cacheStorage.resolve("b/test3.txt"), "33".getBytes());
		Files.write(cacheStorage.resolve("c/d/test4.txt"), "44".getBytes());

		//		// Adding 3 NEW files to server, total is 8
		Files.write(serverStorage.resolve("_test1.txt"), "11new".getBytes());
		Files.write(serverStorage.resolve("a/_test2.txt"), "22new".getBytes());
		Files.write(serverStorage.resolve("b/_test3.txt"), "33new".getBytes());

		// Adding 2 SAME (as in cache) files to server, total is still 8
		Files.write(serverStorage.resolve("test1.txt"), "11server".getBytes());
		Files.write(serverStorage.resolve("a/test2.txt"), "22server".getBytes());

		// Adding 1 new file, total is 9
		Files.write(serverStorage.resolve("newFile.txt"), "New data".getBytes());

		List<FileMetadata> list = await(cacheRemote.list("**"));

		assertEquals(9, list.size());
	}

	@Test
	public void testGetMetadata() throws IOException {
		Files.write(serverStorage.resolve("newFile.txt"), "Initial data\n".getBytes(), StandardOpenOption.CREATE_NEW);

		FileMetadata oldMetadata = await(cacheRemote.getMetadata("newFile.txt"));
		assertNotNull("oldMetadata == null", oldMetadata);

		Files.write(serverStorage.resolve("newFile.txt"), "Appended data\n".getBytes(), StandardOpenOption.APPEND);

		FileMetadata newMetadata = checkNotNull(await(cacheRemote.getMetadata("newFile.txt")));

		assertTrue("New metadata is not greater than old one", newMetadata.getSize() > oldMetadata.getSize());
	}

	@Test
	public void testGetMetadataOfNonExistingFile() {
		FileMetadata metadata = await(cacheRemote.getMetadata("nonExisting.txt"));

		assertNull(metadata);
	}

	@Test
	public void testEnsureCapacity() throws IOException {
		// Current cache size limit is 50 KB
		MemSize sizeLimit = MemSize.kilobytes(50);

		// ~100 KB
		initializeCacheDownloadFiles(20, "testFile_");

		MemSize cacheSize = await(cacheRemote.getTotalCacheSize());

		assertTrue(sizeLimit.toLong() >= cacheSize.toLong());
	}

	@Test
	public void testEnsureCapacityWithOldFiles() throws IOException {
		// Current cache size limit is 50 Kb
		MemSize sizeLimit = MemSize.kilobytes(50);

		// 49 Kb old file in cache
		byte[] data = new byte[(int) (4.9 * 1024)];
		new Random().nextBytes(data);
		Files.write(cacheStorage.resolve("oldFile.txt"), data);

		await(cacheRemote.start());

		// ~10 KB
		initializeCacheDownloadFiles(2, "testFile_");

		MemSize cacheSize = await(cacheRemote.getTotalCacheSize());

		assertTrue(sizeLimit.toLong() >= cacheSize.toLong());
	}

	@Test
	public void testSetCacheSizeLimit() throws IOException {
		// Current cache size limit is 50 Kb
		MemSize sizeLimit = MemSize.kilobytes(50);

		// 100Kb
		initializeCacheDownloadFiles(20, "testFile_");

		MemSize cacheSize = await(cacheRemote.getTotalCacheSize());

		assertTrue(sizeLimit.toLong() >= cacheSize.toLong());

		MemSize newSizeLimit = MemSize.kilobytes(20);

		await(cacheRemote.setCacheSizeLimit(newSizeLimit));
		MemSize newCacheSize = await(cacheRemote.getTotalCacheSize());

		assertTrue(newSizeLimit.toLong() >= newCacheSize.toLong());
	}

	@Test
	public void testStart() throws IOException {
		initializeCacheFolder();

		await(cacheRemote.start());
		MemSize cacheSize = await(cacheRemote.getTotalCacheSize());

		assertTrue(cacheSize.toLong() < cacheRemote.getCacheSizeLimit().toLong());
	}

	@Test(expected = IllegalStateException.class)
	public void testStartWithoutMemSize() {
		cacheRemote = CachedFsClient.create(main, cache, CachedFsClient.lruCompare());
		await(cacheRemote.start());
	}

	@Test
	public void testLRUComparator() throws IOException {
		// LRU is a default cache policy here
		cacheRemote.setCacheSizeLimit(MemSize.kilobytes(100));

		// 25 KB
		initializeCacheDownloadFiles(5, "testFile_");

		// 100 KB
		initializeFiles(20, "newTestFile_");
		downloadFiles(20, 1, "newTestFile_");

		List<FileMetadata> list = await(cache.list("**"));
		list.forEach(val -> assertTrue(val.getName().startsWith("new")));
	}

	@Test
	public void testLFUComparator() throws IOException {
		cacheRemote = CachedFsClient.create(main, cache, CachedFsClient.lfuCompare()).with(MemSize.kilobytes(25));

		// 20 KB
		initializeCacheDownloadFiles(4, "testFile_");

		// increasing cache hit values
		downloadFiles(4, 2, "testFile_");

		// 10 KB
		initializeCacheDownloadFiles(2, "newTestFile_");

		List<FileMetadata> list = await(cache.list("**"));
		assertEquals(1, list.stream().filter(fileMetadata -> fileMetadata.getName().startsWith("new")).count());
	}

	@Test
	public void testSizeComparator() throws IOException {
		// create 49 KB File
		byte[] fileData = new byte[49 * 1024];
		new Random().nextBytes(fileData);
		Files.write(cacheStorage.resolve("bigFile.txt"), fileData);

		await(cacheRemote.start()
				.then(() -> cacheRemote.download("bigFile.txt"))
				.then(RECYCLING_FUNCTION));
		await(cacheRemote.download("bigFile.txt")
				.then(RECYCLING_FUNCTION));

		// 1 file
		initializeCacheDownloadFiles(1, "testFile_");

		List<FileMetadata> list = await(cache.list("**"));

		list.forEach(value -> {
			System.out.println(value);
			assertTrue(value.getName().startsWith("test"));
		});
	}

	@Test
	public void testEnsureSpaceLoadFactor() throws IOException {
		// 50 KB - CacheSizeLimit
		initializeCacheDownloadFiles(10, "test");
		// create 1 byte file - should trigger ensureCapacity() -> resulting size should be ~ 35Kb
		byte[] fileData = new byte[1];
		new Random().nextBytes(fileData);
		Files.write(serverStorage.resolve("tiny.txt"), fileData);

		await(cacheRemote.download("tiny.txt")
				.then(RECYCLING_FUNCTION));

		MemSize cacheSize = await(cacheRemote.getTotalCacheSize());
		assertEquals(35 * 1024 + 1, cacheSize.toLong());
	}

	@Test
	public void testDelete() throws IOException {
		// 10 KB
		initializeCacheDownloadFiles(2, "test");
		// 10 KB
		initializeCacheDownloadFiles(2, "toDelete");

		List<FileMetadata> listBefore = await(cache.list("**"));
		assertEquals(4, listBefore.size());

		await(cacheRemote.delete("toDelete0"));
		await(cacheRemote.delete("toDelete1"));

		List<FileMetadata> listAfter = await(cache.list("**"));

		assertEquals(2, listAfter.size());
		listAfter.forEach(file -> assertTrue(file.getName().startsWith("test")));
	}

	private void initializeCacheFolder() throws IOException {
		Files.createDirectories(cacheStorage.resolve("a"));
		Files.createDirectories(cacheStorage.resolve("b"));
		Files.createDirectories(cacheStorage.resolve("c/d"));

		// 49 bytes
		Files.write(cacheStorage.resolve("test1.txt"), "File".getBytes());
		Files.write(cacheStorage.resolve("a/test2.txt"), "Second File".getBytes());
		Files.write(cacheStorage.resolve("b/test3.txt"), "Yet Another File".getBytes());
		Files.write(cacheStorage.resolve("c/d/test4.txt"), "The other one file".getBytes());
	}

	private void initializeCacheDownloadFiles(int numberOfFiles, String prefix) throws IOException {
		initializeFiles(numberOfFiles, prefix);
		downloadFiles(numberOfFiles, 1, prefix);
	}

	private long initializeFiles(int numberOfFiles, String prefix) throws IOException {
		Random random = new Random();
		String[] files = new String[numberOfFiles];
		long sizeAccum = 0;
		for (int i = 0; i < numberOfFiles; i++) {
			int dataSize = 5 * 1024;
			byte[] data = new byte[dataSize];
			random.nextBytes(data);
			files[i] = prefix + i;
			Files.write(serverStorage.resolve(files[i]), data);
			sizeAccum += data.length;
		}
		return sizeAccum;
	}

	private void downloadFiles(int numberOfFiles, int nTimes, String prefix) {
		for (int j = 0; j < nTimes; j++) {
			for (int i = 0; i < numberOfFiles; i++) {
				await(cacheRemote.download(prefix + i)
						.then(RECYCLING_FUNCTION));
			}
		}
	}
}
