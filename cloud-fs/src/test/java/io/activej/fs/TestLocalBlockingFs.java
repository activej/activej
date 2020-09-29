package io.activej.fs;

import io.activej.common.collection.CollectionUtils;
import io.activej.fs.exception.GlobException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.*;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;

import static io.activej.common.collection.CollectionUtils.set;
import static io.activej.fs.LocalBlockingFs.DEFAULT_TEMP_DIR;
import static io.activej.fs.Utils.*;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;
import static java.util.stream.Collectors.toSet;
import static org.junit.Assert.*;

public final class TestLocalBlockingFs {

	@Rule
	public final TemporaryFolder tmpFolder = new TemporaryFolder();

	private Path storagePath;
	private Path clientPath;

	private LocalBlockingFs client;

	@Before
	public void setup() throws IOException {
		storagePath = tmpFolder.newFolder("storage").toPath();
		clientPath = tmpFolder.newFolder("client").toPath();

		Files.createDirectories(storagePath);
		Files.createDirectories(clientPath);

		Path f = clientPath.resolve("f.txt");
		Files.write(f, "some text1\n\nmore text1\t\n\n\r".getBytes(UTF_8), CREATE, TRUNCATE_EXISTING);

		Path c = clientPath.resolve("c.txt");
		Files.write(c, "some text2\n\nmore text2\t\n\n\r".getBytes(UTF_8), CREATE, TRUNCATE_EXISTING);

		Files.createDirectories(storagePath.resolve("1"));
		Files.createDirectories(storagePath.resolve("2/3"));
		Files.createDirectories(storagePath.resolve("2/b"));

		Path a1 = storagePath.resolve("1/a.txt");
		Files.write(a1, "1\n2\n3\n4\n5\n6\n".getBytes(UTF_8), CREATE, TRUNCATE_EXISTING);

		Path b = storagePath.resolve("1/b.txt");
		Files.write(b, "7\n8\n9\n10\n11\n12\n".getBytes(UTF_8), CREATE, TRUNCATE_EXISTING);

		Path a2 = storagePath.resolve("2/3/a.txt");
		Files.write(a2, "6\n5\n4\n3\n2\n1\n".getBytes(UTF_8), CREATE, TRUNCATE_EXISTING);

		Path d = storagePath.resolve("2/b/d.txt");
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < 1_000_000; i++) {
			sb.append(i).append("\n");
		}
		Files.write(d, sb.toString().getBytes(UTF_8));

		Path e = storagePath.resolve("2/b/e.txt");
		try {
			Files.createFile(e);
		} catch (IOException ignored) {
		}

		client = LocalBlockingFs.create(storagePath);
		initTempDir(storagePath);
	}

	@Test
	public void testDoUpload() throws IOException {
		Path path = clientPath.resolve("c.txt");

		try (InputStream inputStream = new FileInputStream(path.toFile());
				OutputStream outputStream = client.upload("1/c.txt")) {
			LocalFileUtils.copy(inputStream, outputStream);
		}

		assertArrayEquals(Files.readAllBytes(path), Files.readAllBytes(storagePath.resolve("1/c.txt")));
	}

	@Test(expected = DirectoryNotEmptyException.class)
	public void testUploadToDirectory() throws IOException {
		client.upload("1").close();
	}

	@Test(expected = DirectoryNotEmptyException.class)
	public void testAppendToDirectory() throws IOException {
		client.append("1", 0);
	}

	@Test
	public void testAppendToEmptyDirectory() throws IOException {
		byte[] data = "data".getBytes();
		Path empty = CollectionUtils.getLast(createEmptyDirectories(storagePath));
		assertTrue(Files.isDirectory(empty));
		assertEquals(0, Files.list(empty).count());

		try (OutputStream outputStream = client.append(storagePath.relativize(empty).toString(), 0)) {
			outputStream.write(data);
		}

		assertTrue(Files.isRegularFile(empty));
		assertArrayEquals(data, Files.readAllBytes(empty));
	}

	@Test
	public void testAppendOffsetExceedsSize() throws IOException {
		String path = "1/a.txt";
		long size = Files.size(storagePath.resolve(path));
		assertTrue(size > 0);

		try {
			client.append(path, size * 2);
			fail();
		} catch (IOException e) {
			assertEquals("Offset exceeds file size", e.getMessage());
		}
	}

	@Test
	public void uploadLessThanSpecified() {
		String filename = "incomplete.txt";
		Path path = storagePath.resolve(filename);
		assertFalse(Files.exists(path));

		try {
			try (OutputStream outputStream = client.upload(filename, 10)) {
				outputStream.write("Hello".getBytes());
			}
			fail();
		} catch (IOException e) {
			assertEquals("Size mismatch", e.getMessage());
		}
	}

	@Test
	public void uploadMoreThanSpecified() {
		String filename = "incomplete.txt";
		Path path = storagePath.resolve(filename);
		assertFalse(Files.exists(path));

		try {
			try (OutputStream outputStream = client.upload(filename, 10)) {
				outputStream.write("Hello Hello Hello".getBytes());
			}
			fail();
		} catch (IOException e) {
			assertEquals("Size mismatch", e.getMessage());
		}
	}

	@Test
	public void testDownload() throws IOException {
		Path outputFile = clientPath.resolve("d.txt");

		try (InputStream inputStream = client.download("2/b/d.txt");
				OutputStream outputStream = new FileOutputStream(outputFile.toFile())) {
			LocalFileUtils.copy(inputStream, outputStream);
		}

		assertArrayEquals(Files.readAllBytes(storagePath.resolve("2/b/d.txt")), Files.readAllBytes(outputFile));
	}

	@Test
	public void testDownloadWithOffset() throws IOException {
		String filename = "filename";
		try (OutputStream outputStream = client.upload(filename)) {
			outputStream.write("abcdefgh".getBytes());
		}

		try (InputStream inputStream = client.download(filename, 3, Long.MAX_VALUE)) {
			assertEquals("defgh", asString(inputStream));
		}
	}

	@Test
	public void testDownloadWithOffsetExceedingFileSize() throws IOException {
		String filename = "filename";
		try (OutputStream outputStream = client.upload(filename)) {
			outputStream.write("abcdefgh".getBytes());
		}

		//noinspection EmptyTryBlock
		try (InputStream ignored = client.download(filename, 100, Long.MAX_VALUE)) {
		} catch (IOException e) {
			assertEquals("Offset exceeds file size", e.getMessage());
		}
	}

	@Test
	public void testDownloadWithLimit() throws IOException {
		String filename = "filename";
		try (OutputStream outputStream = client.upload(filename)) {
			outputStream.write("abcdefgh".getBytes());
		}

		try (InputStream inputStream = client.download(filename, 3, 2)) {
			assertEquals("de", asString(inputStream));
		}
	}

	@Test(expected = FileNotFoundException.class)
	public void testDownloadNonExistingFile() throws IOException {
		client.download("no_file.txt");
	}

	@Test
	public void testDeleteFile() throws IOException {
		assertTrue(Files.exists(storagePath.resolve("2/3/a.txt")));

		client.delete("2/3/a.txt");

		assertFalse(Files.exists(storagePath.resolve("2/3/a.txt")));
	}

	@Test
	public void testDeleteNonExistingFile() throws IOException {
		client.delete("no_file.txt");
	}

	@Test
	public void testListFiles() throws IOException {
		Set<String> expected = set(
				"1/a.txt",
				"1/b.txt",
				"2/3/a.txt",
				"2/b/d.txt",
				"2/b/e.txt"
		);

		Map<String, FileMetadata> actual = client.list("**");

		assertEquals(expected, actual.keySet());
	}

	@Test
	public void testGlobListFiles() throws IOException {
		Set<String> expected = set(
				"2/3/a.txt",
				"2/b/d.txt",
				"2/b/e.txt"
		);

		Map<String, FileMetadata> actual = client.list("2/*/*.txt");

		assertEquals(expected, actual.keySet());
	}

	@Test
	public void testMove() throws IOException {
		byte[] expected = Files.readAllBytes(storagePath.resolve("1/a.txt"));
		client.move("1/a.txt", "3/new_folder/z.txt");

		assertArrayEquals(expected, Files.readAllBytes(storagePath.resolve("3/new_folder/z.txt")));
		assertFalse(Files.exists(storagePath.resolve("1/a.txt")));
	}

	@Test
	public void testMoveIntoExisting() throws IOException {
		byte[] expected = Files.readAllBytes(storagePath.resolve("1/b.txt"));
		client.move("1/b.txt", "1/a.txt");

		assertArrayEquals(expected, Files.readAllBytes(storagePath.resolve("1/a.txt")));
		assertFalse(Files.exists(storagePath.resolve("1/b.txt")));
	}

	@Test(expected = FileNotFoundException.class)
	public void testMoveNothingIntoNothing() throws IOException {
		client.move("i_do_not_exist.txt", "neither_am_i.txt");
	}

	@Test
	public void testOverwritingDirAsFile() throws IOException {
		try (OutputStream outputStream = client.upload("newdir/a.txt")) {
			outputStream.write("test".getBytes());
		}

		client.delete("newdir/a.txt");

		assertTrue(client.list("**").keySet().stream().noneMatch(name -> name.contains("newdir")));

		try (OutputStream outputStream = client.upload("newdir")) {
			outputStream.write("test".getBytes());
		}

		assertNotNull(client.info("newdir"));
	}

	@Test
	public void testDeleteEmpty() throws IOException {
		client.delete("");
	}

	@Test(expected = GlobException.class)
	public void testListMalformedGlob() throws IOException {
		client.list("[");
	}

	@Test
	public void tempFilesAreNotListed() throws IOException {
		Map<String, FileMetadata> before = client.list("**");

		Path tempDir = storagePath.resolve(DEFAULT_TEMP_DIR);
		Files.createDirectories(tempDir);
		Files.write(tempDir.resolve("systemFile.txt"), "test data".getBytes());
		Path folder = tempDir.resolve("folder");
		Files.createDirectories(folder);
		Files.write(folder.resolve("systemFile2.txt"), "test data".getBytes());

		Map<String, FileMetadata> after = client.list("**");

		assertEquals(before, after);
	}

	@Test
	public void copyCreatesNewFile() throws IOException {
		try (OutputStream outputStream = client.upload("first")) {
			outputStream.write("test".getBytes());
		}

		client.copy("first", "second");

		try (OutputStream outputStream = client.append("first", 4)) {
			outputStream.write("first".getBytes());
		}

		try (InputStream inputStreamFirst = client.download("first");
				InputStream inputStreamSecond = client.download("second")) {
			assertEquals("testfirst", asString(inputStreamFirst));
			assertEquals("test", asString(inputStreamSecond));
		}

		try (OutputStream outputStream = client.append("second", 4)) {
			outputStream.write("second".getBytes());
		}

		try (InputStream inputStreamFirst = client.download("first");
				InputStream inputStreamSecond = client.download("second")) {
			assertEquals("testfirst", asString(inputStreamFirst));
			assertEquals("testsecond", asString(inputStreamSecond));
		}
	}

	@Test
	public void copyWithHardLinksDoesNotCreateNewFile() throws IOException {
		client.withHardLinkOnCopy(true);

		try (OutputStream outputStream = client.upload("first")) {
			outputStream.write("test".getBytes());
		}

		client.copy("first", "second");

		try (OutputStream outputStream = client.append("first", 4)) {
			outputStream.write("first".getBytes());
		}

		try (InputStream inputStreamFirst = client.download("first");
				InputStream inputStreamSecond = client.download("second")) {
			assertEquals("testfirst", asString(inputStreamFirst));
			assertEquals("testfirst", asString(inputStreamSecond));
		}

		try (OutputStream outputStream = client.append("second", 9)) {
			outputStream.write("second".getBytes());
		}

		try (InputStream inputStreamFirst = client.download("first");
				InputStream inputStreamSecond = client.download("second")) {
			assertEquals("testfirstsecond", asString(inputStreamFirst));
			assertEquals("testfirstsecond", asString(inputStreamSecond));
		}
	}

	@Test
	public void testAppendInTheMiddle() throws IOException {
		String filename = "test";

		// Creating file
		try (OutputStream outputStream = client.upload(filename)) {
			outputStream.write("data".getBytes());
		}

		try (OutputStream outputStream = client.append(filename, 2)) {
			outputStream.write('d');
		}

		try (InputStream inputStream = client.download(filename)) {
			assertEquals("dada", asString(inputStream));
		}
	}

	@Test
	public void testEmptyDirectoryCleanupOnUpload() throws IOException {
		List<Path> emptyDirs = createEmptyDirectories(storagePath);
		String data = "test";

		try (OutputStream outputStream = client.upload("empty")) {
			outputStream.write(data.getBytes());
		}

		try (InputStream inputStream = client.download("empty")) {
			assertEquals(data, asString(inputStream));
		}

		for (Path emptyDir : emptyDirs) {
			assertFalse(Files.isDirectory(emptyDir));
		}
	}

	@Test
	public void testEmptyDirectoryCleanupOnAppend() throws IOException {
		List<Path> emptyDirs = createEmptyDirectories(storagePath);
		String data = "test";

		try (OutputStream outputStream = client.append("empty", 0)) {
			outputStream.write(data.getBytes());
		}

		try (InputStream inputStream = client.download("empty")) {
			assertEquals(data, asString(inputStream));
		}

		for (Path emptyDir : emptyDirs) {
			assertFalse(Files.isDirectory(emptyDir));
		}
	}

	@Test
	public void testEmptyDirectoryCleanupOnMove() throws IOException {
		List<Path> emptyDirs = createEmptyDirectories(storagePath);
		String data = "test";

		try (OutputStream outputStream = client.upload("source")) {
			outputStream.write(data.getBytes());
		}

		client.move("source", "empty");

		try (InputStream inputStream = client.download("empty")) {
			assertEquals(data, asString(inputStream));
		}

		for (Path emptyDir : emptyDirs) {
			assertFalse(Files.isDirectory(emptyDir));
		}
	}

	@Test
	public void testEmptyDirectoryCleanupOnCopy() throws IOException {
		List<Path> emptyDirs = createEmptyDirectories(storagePath);
		String data = "test";

		try (OutputStream outputStream = client.upload("source")) {
			outputStream.write(data.getBytes());
		}

		client.copy("source", "empty");

		try (InputStream inputStream = client.download("empty")) {
			assertEquals(data, asString(inputStream));
		}

		for (Path emptyDir : emptyDirs) {
			assertFalse(Files.isDirectory(emptyDir));
		}
	}

	@Test(expected = DirectoryNotEmptyException.class)
	public void testEmptyDirectoryCleanupWithOneFile() throws IOException {
		List<Path> emptyDirs = createEmptyDirectories(storagePath);
		Path randomPath = emptyDirs.get(ThreadLocalRandom.current().nextInt(emptyDirs.size()));
		Files.createFile(randomPath.resolve("file"));

		client.upload("empty").close();
	}

	@Test
	public void testUploadToSameNewDir() throws IOException {
		String dir = "newDir";
		Set<String> filenames = IntStream.range(0, 5)
				.mapToObj(i -> dir + BlockingFs.SEPARATOR + i + ".txt")
				.collect(toSet());

		for (String filename : filenames) {
			client.upload(filename).close();
		}

		Map<String, FileMetadata> files = client.list(dir + BlockingFs.SEPARATOR + '*');
		assertEquals(filenames, files.keySet());
		for (FileMetadata meta : files.values()) {
			assertEquals(0, meta.getSize());
		}
	}

}
