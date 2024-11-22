package io.activej.fs;

import io.activej.fs.exception.FileSystemStructureException;
import io.activej.fs.exception.GlobException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.*;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static io.activej.common.collection.CollectionUtils.last;
import static io.activej.fs.BlockingFileSystem.DEFAULT_TEMP_DIR;
import static io.activej.fs.Utils.asString;
import static io.activej.fs.Utils.createEmptyDirectories;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;
import static java.util.stream.Collectors.toSet;
import static org.junit.Assert.*;
import static org.junit.Assume.assumeTrue;

public final class TestBlockingFileSystem {

	@Rule
	public final TemporaryFolder tmpFolder = new TemporaryFolder();

	private Path storagePath;
	private Path clientPath;

	private BlockingFileSystem client;

	@Before
	public void setup() throws IOException {
		storagePath = tmpFolder.newFolder("storage").toPath();
		clientPath = tmpFolder.newFolder("client").toPath();

		Files.createDirectories(storagePath);
		Files.createDirectories(clientPath);

		Path f = clientPath.resolve("f.txt");
		Files.writeString(f, """
			some text1

			more text1\t

			\r""", CREATE, TRUNCATE_EXISTING);

		Path c = clientPath.resolve("c.txt");
		Files.writeString(c, """
			some text2

			more text2\t

			\r""", CREATE, TRUNCATE_EXISTING);

		Files.createDirectories(storagePath.resolve("1"));
		Files.createDirectories(storagePath.resolve("2/3"));
		Files.createDirectories(storagePath.resolve("2/b"));

		Path a1 = storagePath.resolve("1/a.txt");
		Files.writeString(a1, """
			1
			2
			3
			4
			5
			6
			""", CREATE, TRUNCATE_EXISTING);

		Path b = storagePath.resolve("1/b.txt");
		Files.writeString(b, """
			7
			8
			9
			10
			11
			12
			""", CREATE, TRUNCATE_EXISTING);

		Path a2 = storagePath.resolve("2/3/a.txt");
		Files.writeString(a2, """
			6
			5
			4
			3
			2
			1
			""", CREATE, TRUNCATE_EXISTING);

		Path d = storagePath.resolve("2/b/d.txt");
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < 1_000_000; i++) {
			sb.append(i).append("\n");
		}
		Files.writeString(d, sb.toString());

		Path e = storagePath.resolve("2/b/e.txt");
		try {
			Files.createFile(e);
		} catch (IOException ignored) {
		}

		client = BlockingFileSystem.create(storagePath);
		client.start();
	}

	@Test
	public void upload() throws IOException {
		Path path = clientPath.resolve("c.txt");

		try (
			InputStream inputStream = new FileInputStream(path.toFile());
			OutputStream outputStream = client.upload("1/c.txt")
		) {
			inputStream.transferTo(outputStream);
		}

		assertEquals(-1, Files.mismatch(path, storagePath.resolve("1/c.txt")));
	}

	@Test(expected = DirectoryNotEmptyException.class)
	public void uploadToDirectory() throws IOException {
		client.upload("1").close();
	}

	@Test(expected = DirectoryNotEmptyException.class)
	public void appendToDirectory() throws IOException {
		client.append("1", 0);
	}

	@Test
	public void appendToEmptyDirectory() throws IOException {
		byte[] data = "data".getBytes();
		Path empty = last(createEmptyDirectories(storagePath));
		assertTrue(Files.isDirectory(empty));
		try (Stream<Path> list = Files.list(empty)) {
			assertEquals(0, list.count());
		}

		try (OutputStream outputStream = client.append(storagePath.relativize(empty).toString(), 0)) {
			outputStream.write(data);
		}

		assertTrue(Files.isRegularFile(empty));
		assertArrayEquals(data, Files.readAllBytes(empty));
	}

	@Test
	public void appendOffsetExceedsSize() throws IOException {
		String path = "1/a.txt";
		long size = Files.size(storagePath.resolve(path));
		assertTrue(size > 0);

		IOException e = assertThrows(IOException.class, () -> client.append(path, size * 2));
		assertEquals("Offset exceeds file size", e.getMessage());
	}

	@Test
	public void uploadLessThanSpecified() {
		String filename = "incomplete.txt";
		Path path = storagePath.resolve(filename);
		assertFalse(Files.exists(path));

		IOException e = assertThrows(IOException.class, () -> {
			try (OutputStream outputStream = client.upload(filename, 10)) {
				outputStream.write("Hello".getBytes());
			}
		});
		assertEquals("Size mismatch", e.getMessage());
	}

	@Test
	public void uploadMoreThanSpecified() {
		String filename = "incomplete.txt";
		Path path = storagePath.resolve(filename);
		assertFalse(Files.exists(path));

		IOException e = assertThrows(IOException.class, () -> {
			try (OutputStream outputStream = client.upload(filename, 10)) {
				outputStream.write("Hello Hello Hello".getBytes());
			}
		});
		assertEquals("Size mismatch", e.getMessage());
	}

	@Test
	public void download() throws IOException {
		Path outputFile = clientPath.resolve("d.txt");

		try (
			InputStream inputStream = client.download("2/b/d.txt");
			OutputStream outputStream = new FileOutputStream(outputFile.toFile())
		) {
			inputStream.transferTo(outputStream);
		}

		assertEquals(-1, Files.mismatch(storagePath.resolve("2/b/d.txt"), outputFile));
	}

	@Test
	public void downloadWithOffset() throws IOException {
		String filename = "filename";
		try (OutputStream outputStream = client.upload(filename)) {
			outputStream.write("abcdefgh".getBytes());
		}

		try (InputStream inputStream = client.download(filename, 3, Long.MAX_VALUE)) {
			assertEquals("defgh", asString(inputStream));
		}
	}

	@Test
	public void downloadWithOffsetExceedingFileSize() throws IOException {
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
	public void downloadWithLimit() throws IOException {
		String filename = "filename";
		try (OutputStream outputStream = client.upload(filename)) {
			outputStream.write("abcdefgh".getBytes());
		}

		try (InputStream inputStream = client.download(filename, 3, 2)) {
			assertEquals("de", asString(inputStream));
		}
	}

	@Test(expected = FileNotFoundException.class)
	public void downloadNonExistingFile() throws IOException {
		client.download("no_file.txt");
	}

	@Test
	public void deleteFile() throws IOException {
		assertTrue(Files.exists(storagePath.resolve("2/3/a.txt")));

		client.delete("2/3/a.txt");

		assertFalse(Files.exists(storagePath.resolve("2/3/a.txt")));
	}

	@Test
	public void deleteNonExistingFile() throws IOException {
		client.delete("no_file.txt");
	}

	@Test
	public void listFiles() throws IOException {
		Set<String> expected = Set.of("1/a.txt", "1/b.txt", "2/3/a.txt", "2/b/d.txt", "2/b/e.txt");

		Map<String, FileMetadata> actual = client.list("**");

		assertEquals(expected, actual.keySet());
	}

	@Test
	public void globListFiles() throws IOException {
		Set<String> expected = Set.of("2/3/a.txt", "2/b/d.txt", "2/b/e.txt");

		Map<String, FileMetadata> actual = client.list("2/*/*.txt");

		assertEquals(expected, actual.keySet());
	}

	@Test
	public void move() throws IOException {
		byte[] expected = Files.readAllBytes(storagePath.resolve("1/a.txt"));
		client.move("1/a.txt", "3/new_folder/z.txt");

		assertArrayEquals(expected, Files.readAllBytes(storagePath.resolve("3/new_folder/z.txt")));
		assertFalse(Files.exists(storagePath.resolve("1/a.txt")));
	}

	@Test
	public void moveIntoExisting() throws IOException {
		byte[] expected = Files.readAllBytes(storagePath.resolve("1/b.txt"));
		client.move("1/b.txt", "1/a.txt");

		assertArrayEquals(expected, Files.readAllBytes(storagePath.resolve("1/a.txt")));
		assertFalse(Files.exists(storagePath.resolve("1/b.txt")));
	}

	@Test(expected = FileNotFoundException.class)
	public void moveNothingIntoNothing() throws IOException {
		client.move("i_do_not_exist.txt", "neither_am_i.txt");
	}

	@Test
	public void overwritingDirAsFile() throws IOException {
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
	public void deleteEmpty() throws IOException {
		client.delete("");
	}

	@Test(expected = GlobException.class)
	public void listMalformedGlob() throws IOException {
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

		try (
			InputStream inputStreamFirst = client.download("first");
			InputStream inputStreamSecond = client.download("second")
		) {
			assertEquals("testfirst", asString(inputStreamFirst));
			assertEquals("test", asString(inputStreamSecond));
		}

		try (OutputStream outputStream = client.append("second", 4)) {
			outputStream.write("second".getBytes());
		}

		try (
			InputStream inputStreamFirst = client.download("first");
			InputStream inputStreamSecond = client.download("second")
		) {
			assertEquals("testfirst", asString(inputStreamFirst));
			assertEquals("testsecond", asString(inputStreamSecond));
		}
	}

	@Test
	public void copyWithHardLinksDoesNotCreateNewFile() throws IOException {
		client = BlockingFileSystem.builder(storagePath)
			.withHardLinkOnCopy(true)
			.build();
		client.start();

		try (OutputStream outputStream = client.upload("first")) {
			outputStream.write("test".getBytes());
		}

		client.copy("first", "second");

		try (OutputStream outputStream = client.append("first", 4)) {
			outputStream.write("first".getBytes());
		}

		try (
			InputStream inputStreamFirst = client.download("first");
			InputStream inputStreamSecond = client.download("second")
		) {
			assertEquals("testfirst", asString(inputStreamFirst));
			assertEquals("testfirst", asString(inputStreamSecond));
		}

		try (OutputStream outputStream = client.append("second", 9)) {
			outputStream.write("second".getBytes());
		}

		try (
			InputStream inputStreamFirst = client.download("first");
			InputStream inputStreamSecond = client.download("second")
		) {
			assertEquals("testfirstsecond", asString(inputStreamFirst));
			assertEquals("testfirstsecond", asString(inputStreamSecond));
		}
	}

	@Test
	public void appendInTheMiddle() throws IOException {
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
	public void emptyDirectoryCleanupOnUpload() throws IOException {
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
	public void emptyDirectoryCleanupOnAppend() throws IOException {
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
	public void emptyDirectoryCleanupOnMove() throws IOException {
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
	public void emptyDirectoryCleanupOnCopy() throws IOException {
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
	public void tesemptyDirectoryCleanupWithOneFile() throws IOException {
		List<Path> emptyDirs = createEmptyDirectories(storagePath);
		Path randomPath = emptyDirs.get(ThreadLocalRandom.current().nextInt(emptyDirs.size()));
		Files.createFile(randomPath.resolve("file"));

		client.upload("empty").close();
	}

	@Test
	public void uploadToSameNewDir() throws IOException {
		String dir = "newDir";
		Set<String> filenames = IntStream.range(0, 5)
			.mapToObj(i -> dir + IBlockingFileSystem.SEPARATOR + i + ".txt")
			.collect(toSet());

		for (String filename : filenames) {
			client.upload(filename).close();
		}

		Map<String, FileMetadata> files = client.list(dir + IBlockingFileSystem.SEPARATOR + '*');
		assertEquals(filenames, files.keySet());
		for (FileMetadata meta : files.values()) {
			assertEquals(0, meta.getSize());
		}
	}

	@Test
	public void uploadCloseIdempotence() throws IOException {
		OutputStream outputStream = client.upload("somefile");
		outputStream.close();
		outputStream.close();
	}

	@Test
	public void uploadWithSizeCloseIdempotence() throws IOException {
		OutputStream outputStream = client.upload("somefile", 5);
		outputStream.write(new byte[]{1, 2, 3, 4, 5});
		outputStream.close();
		outputStream.close();
	}

	@Test
	public void appendCloseIdempotence() throws IOException {
		OutputStream outputStream = client.append("somefile", 0);
		outputStream.close();
		outputStream.close();
	}

	@Test
	public void copyWithDeletedTempDir() throws IOException {
		try (
			InputStream inputStream = new ByteArrayInputStream("Test content".getBytes(UTF_8));
			OutputStream outputStream = client.upload("test.txt")
		) {
			inputStream.transferTo(outputStream);
		}

		Path tempDir = storagePath.resolve(FileSystem.DEFAULT_TEMP_DIR);
		Files.delete(tempDir);

		FileSystemStructureException e = assertThrows(FileSystemStructureException.class, () -> client.copy("test.txt", "test.txt.copy"));
		assertEquals(e.getMessage(), "Temporary directory " + tempDir + " not found");
	}

	@Test
	public void uploadWithDeletedTempDir() throws IOException {
		Path tempDir = storagePath.resolve(FileSystem.DEFAULT_TEMP_DIR);
		Files.delete(tempDir);

		try (
			InputStream inputStream = new ByteArrayInputStream("Test content".getBytes(UTF_8));
			OutputStream outputStream = client.upload("test.txt")
		) {
			inputStream.transferTo(outputStream);
			fail();
		} catch (FileSystemStructureException e) {
			assertEquals(e.getMessage(), "Temporary directory " + tempDir + " not found");
		}
	}

	@Test
	public void relativePaths() throws IOException {
		Path current = Paths.get(".").toAbsolutePath();
		assumeTrue("This test is located on a different drive than temporary directory", current.getRoot().equals(storagePath.getRoot()));

		Set<String> expected = Set.of(
			"1/a.txt",
			"1/b.txt",
			"2/3/a.txt",
			"2/b/d.txt",
			"2/b/e.txt"
		);

		Path relativePath = current.relativize(storagePath);
		relativePath = relativePath.getParent().resolve(".").resolve(relativePath.getFileName());

		assertFalse(relativePath.isAbsolute());

		client = BlockingFileSystem.create(relativePath);
		client.start();

		Map<String, FileMetadata> actual = client.list("**");

		assertEquals(expected, actual.keySet());
	}
}
