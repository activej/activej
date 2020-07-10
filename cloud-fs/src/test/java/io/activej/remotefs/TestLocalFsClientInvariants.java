package io.activej.remotefs;

import io.activej.bytebuf.ByteBuf;
import io.activej.csp.ChannelConsumer;
import io.activej.csp.ChannelSupplier;
import io.activej.promise.Promise;
import io.activej.promise.Promises;
import io.activej.test.TestUtils.ThrowingConsumer;
import io.activej.test.rules.ByteBufRule;
import io.activej.test.rules.EventloopRule;
import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static io.activej.common.collection.CollectionUtils.*;
import static io.activej.eventloop.Eventloop.getCurrentEventloop;
import static io.activej.promise.TestUtils.await;
import static io.activej.promise.TestUtils.awaitException;
import static io.activej.remotefs.FsClient.*;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;
import static java.util.Arrays.asList;
import static java.util.Collections.*;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.*;

public final class TestLocalFsClientInvariants {
	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@Rule
	public final TemporaryFolder tmpFolder = new TemporaryFolder();

	private Path firstPath;
	private Path secondPath;

	private FsClient first;
	private FsClient second;

	@Before
	public void setUp() throws Exception {
		/*
		 * Initial files:
		 *
		 * ........file
		 * ........file2
		 * ........directory/subdir/file3.txt
		 * ........directory/file.txt
		 * ........directory2/file2.txt
		 */
		firstPath = tmpFolder.newFolder("first").toPath();
		secondPath = tmpFolder.newFolder("second").toPath();

		first = LocalFsClient.create(getCurrentEventloop(), newSingleThreadExecutor(), firstPath);
		second = new DefaultFsClient(LocalFsClient.create(getCurrentEventloop(), newSingleThreadExecutor(), secondPath));

		initializeDirs(asList(
				"file",
				"file2",
				"directory/subdir/file3.txt",
				"directory/file.txt",
				"directory2/file2.txt"
		));
	}

	private void initializeDirs(List<String> paths) {
		try {
			clearDirectory(firstPath);
			clearDirectory(secondPath);

			for (String path : paths) {
				Path file = firstPath.resolve(path);
				Files.createDirectories(file.getParent());
				Files.write(file, String.format("This is contents of file %s", file).getBytes(UTF_8), CREATE, TRUNCATE_EXISTING);

				Path copyOfFile = secondPath.resolve(path);
				Files.createDirectories(copyOfFile.getParent());
				Files.copy(file, copyOfFile);
			}
		} catch (IOException e) {
			throw new AssertionError(e);
		}
	}

	// region move
	@Test
	public void regularMove() throws IOException {
		String from = "file";
		String to = "newFile";

		byte[] bytesBefore = Files.readAllBytes(firstPath.resolve(from));
		both(client -> await(client.move(from, to)));

		bothPaths(path -> {
			assertThat(listPaths(path), not(contains(from)));
			assertArrayEquals(bytesBefore, Files.readAllBytes(path.resolve(to)));
		});
		assertFilesAreSame();
	}

	@Test
	public void moveToNewDirectory() throws IOException {
		String from = "file";
		String to = "a/b/c/d/newFile";

		byte[] bytesBefore = Files.readAllBytes(firstPath.resolve(from));
		both(client -> await(client.move(from, to)));

		bothPaths(path -> {
			assertThat(listPaths(path), not(contains(from)));
			assertArrayEquals(bytesBefore, Files.readAllBytes(path.resolve(to)));
		});
		assertFilesAreSame();
	}

	@Test
	public void moveDirectory() {
		List<Path> before = listPaths(firstPath);
		both(client -> assertSame(IS_DIRECTORY, awaitException(client.move("directory", "newDirectory"))));

		assertFilesAreSame();
		assertEquals(before, listPaths(firstPath));
	}

	@Test
	public void moveToExistingDirectoryName() {
		List<Path> before = listPaths(firstPath);
		both(client -> assertSame(IS_DIRECTORY, awaitException(client.move("file", "directory"))));

		assertFilesAreSame();
		assertEquals(before, listPaths(firstPath));
	}

	@Test
	public void moveToFileInsideDirectoryAsAFile() {
		List<Path> before = listPaths(firstPath);
		both(client -> assertSame(FILE_EXISTS, awaitException(client.move("file2", "file/newFile"))));

		assertFilesAreSame();
		assertEquals(before, listPaths(firstPath));
	}

	@Test
	public void moveFromFileInsideDirectoryAsAFile() {
		List<Path> before = listPaths(firstPath);
		both(client -> assertSame(FILE_NOT_FOUND, awaitException(client.move("file/someFile", "newFile"))));

		assertFilesAreSame();
		assertEquals(before, listPaths(firstPath));
	}

	@Test
	public void moveNonExistentFile() {
		List<Path> before = listPaths(firstPath);
		both(client -> assertSame(FILE_NOT_FOUND, awaitException(client.move("nonexistent", "nonexistentTarget"))));

		assertFilesAreSame();
		assertEquals(before, listPaths(firstPath));
	}

	@Test
	public void moveNonExistentFileToDirectory() {
		List<Path> before = listPaths(firstPath);
		both(client -> assertSame(FILE_NOT_FOUND, awaitException(client.move("nonexistent", "directory"))));

		assertFilesAreSame();
		assertEquals(before, listPaths(firstPath));
	}

	@Test
	public void moveSelfFiles() {
		both(client -> assertEquals(FILE_EXISTS, awaitException(client.move("file2", "file2"))));

		bothPaths(path -> assertThat(listPaths(path), not(contains(Paths.get("file2")))));
		assertFilesAreSame();
	}

	@Test
	public void moveSelfNonExistent() {
		both(client -> assertEquals(FILE_NOT_FOUND, awaitException(client.move("nonexistent", "nonexistent"))));
		assertFilesAreSame();
	}

	@Test
	public void moveSelfDirectories() {
		both(client -> assertEquals(IS_DIRECTORY, awaitException(client.move("directory", "directory"))));
		assertFilesAreSame();
	}

	@Test
	public void moveFromEmptyFilename() {
		Map<String, FileMetadata> before = await(first.list("**"));
		both(client -> assertEquals(IS_DIRECTORY, awaitException(client.move("", "newFile"))));
		both(client -> assertMetadataEquals(before, await(client.list("**"))));
	}

	@Test
	public void moveToEmptyFilename() {
		Map<String, FileMetadata> before = await(first.list("**"));
		both(client -> assertEquals(IS_DIRECTORY, awaitException(client.move("file", ""))));
		both(client -> assertMetadataEquals(before, await(client.list("**"))));
	}

	@Test
	public void moveUpdatesTimestamp() {
		both(client -> {
			FileMetadata oldMeta = await(client.info("file"));
			await(Promises.delay(10));
			await(client.move("file", "newFile"));
			FileMetadata newMeta = await(client.info("newFile"));

			assertEquals(oldMeta.getSize(), newMeta.getSize());
			assertTrue(newMeta.getTimestamp() > oldMeta.getTimestamp());
		});
		assertFilesAreSame();
	}

	@Test
	public void moveNotIdempotent() {
		both(client -> {
			// first call completes successfully
			await(client.move("file", "newFile"));
			Throwable e = awaitException(client.move("file", "newFile"));
			assertSame(FILE_NOT_FOUND, e);
		});
		assertFilesAreSame();
	}
	// endregion

	// region copy
	@Test
	public void regularCopy() {
		both(client -> await(client.copy("file2", "newFile")));
		assertFileEquals("file2", "newFile");
		assertFilesAreSame();
	}

	@Test
	public void copyToNewDirectory() {
		both(client -> await(client.copy("file", "a/b/c/d/newFile")));
		assertFileEquals("file", "a/b/c/d/newFile");
		assertFilesAreSame();
	}

	@Test
	public void copyDirectory() {
		both(client -> assertEquals(IS_DIRECTORY, awaitException(client.copy("directory", "newDirectory"))));
		assertFilesAreSame();
	}

	@Test
	public void copyToExistingDirectoryName() {
		both(client -> assertEquals(IS_DIRECTORY, awaitException(client.copy("file", "directory"))));
		assertFilesAreSame();
	}

	@Test
	public void copyToFileInsideDirectoryAsAFile() {
		both(client -> {
			Throwable file2 = awaitException(client.copy("file2", "file/newFile"));
			assertEquals(FILE_EXISTS, file2);
		});
		assertFilesAreSame();
	}

	@Test
	public void copyFromFileInsideDirectoryAsAFile() {
		both(client -> assertEquals(FILE_NOT_FOUND, awaitException(client.copy("file/newFile", "newFile"))));
		assertFilesAreSame();
	}

	@Test
	public void copyNonExistentFile() {
		both(client -> assertEquals(FILE_NOT_FOUND, awaitException(client.copy("nonexistent", "nonexistentTarget"))));
		assertFilesAreSame();
	}

	@Test
	public void copyNonExistentFileToDirectory() {
		both(client -> assertEquals(FILE_NOT_FOUND, awaitException(client.copy("nonexistent", "directory"))));
		assertFilesAreSame();
	}

	@Test
	public void copySelf() throws IOException {
		byte[] bytes = Files.readAllBytes(firstPath.resolve("file2"));
		both(client -> assertEquals(FILE_EXISTS, awaitException(client.copy("file2", "file2"))));
		assertFilesAreSame();
		assertArrayEquals(bytes, Files.readAllBytes(firstPath.resolve("file2")));
	}

	@Test
	public void copyFromEmptyFilename() {
		Map<String, FileMetadata> before = await(first.list("**"));
		both(client -> assertEquals(IS_DIRECTORY, awaitException(client.copy("", "newFile"))));
		both(client -> assertMetadataEquals(before, await(client.list("**"))));
	}

	@Test
	public void copyToEmptyFilename() {
		Map<String, FileMetadata> before = await(first.list("**"));
		both(client -> assertEquals(IS_DIRECTORY, awaitException(client.copy("file", ""))));
		both(client -> assertMetadataEquals(before, await(client.list("**"))));
	}

	@Test
	public void copyUpdatesTimestamp() {
		both(client -> {
			FileMetadata oldMeta = await(client.info("file"));
			await(Promises.delay(10));
			await(client.copy("file", "newFile"));
			FileMetadata newMeta = await(client.info("newFile"));

			assertEquals(oldMeta.getSize(), newMeta.getSize());
			assertTrue(newMeta.getTimestamp() > oldMeta.getTimestamp());
		});
		assertFilesAreSame();
	}

	@Test
	public void copyIsNotIdempotent() {
		both(client -> {
			await(client.copy("file", "newFile"));
			assertSame(FILE_EXISTS, awaitException(client.copy("file", "newFile")));
		});
		assertFilesAreSame();
	}
	// endregion

	// region deleteAll
	@Test
	public void deleteAllEmpty() {
		List<Path> before = listPaths(firstPath);
		both(client -> await(client.deleteAll(emptySet())));

		assertEquals(before, listPaths(firstPath));
		assertFilesAreSame();
	}

	@Test
	public void deleteAllSingleFile() {
		both(client -> await(client.deleteAll(singleton("file"))));

		bothPaths(path -> assertThat(listPaths(path), not(contains(Paths.get("file")))));
		assertFilesAreSame();
	}

	@Test
	public void deleteAllMultipleFiles() {
		both(client -> await(client.deleteAll(set("file", "file2"))));

		bothPaths(path -> assertThat(listPaths(path), not(contains(Paths.get("file"), Paths.get("file2")))));
		assertFilesAreSame();
	}

	@Test
	public void deleteAllSingleDirectory() {
		List<Path> before = listPaths(firstPath);
		both(client -> assertEquals(IS_DIRECTORY, awaitException(client.deleteAll(singleton("directory")))));

		assertEquals(before, listPaths(firstPath));
		assertFilesAreSame();
	}

	@Test
	public void deleteAllMultipleDirectories() {
		List<Path> before = listPaths(firstPath);
		both(client -> assertEquals(IS_DIRECTORY, awaitException(client.deleteAll(set("directory", "directory2")))));

		assertEquals(before, listPaths(firstPath));
		assertFilesAreSame();
	}

	@Test
	public void deleteAllFilesAndDirectories() {
		both(client -> assertEquals(IS_DIRECTORY, awaitException(client.deleteAll(set("file", "directory")))));
		assertFilesAreSame();
	}

	@Test
	public void deleteAllWithNonExisting() {
		both(client -> await(client.deleteAll(set("file", "nonexistent"))));

		bothPaths(path -> assertThat(listPaths(path), not(contains(Paths.get("file")))));
		assertFilesAreSame();
	}

	@Test
	public void deleteAllWithRoot() {
		both(client -> await(client.deleteAll(set("file", ""))));
		assertFilesAreSame();
	}

	@Test
	public void deleteAllWithFileOutsideRoot() {
		both(client -> assertEquals(BAD_PATH, awaitException(client.deleteAll(set("file", "..")))));
		assertFilesAreSame();
	}

	@Test
	public void deleteAllIsIdempotent() {
		both(client -> {
			await(client.deleteAll(set("file", "file2")));
			await(client.deleteAll(set("file", "file2")));
			await(client.deleteAll(set("file", "file2")));
			await(client.deleteAll(set("file", "file2")));
		});

		bothPaths(path -> assertThat(listPaths(path), not(contains(Paths.get("file"), Paths.get("file2")))));
		assertFilesAreSame();
	}
	//endregion

	// region copyAll
	@Test
	public void copyAllEmpty() {
		List<Path> before = listPaths(firstPath);
		both(client -> await(client.copyAll(emptyMap())));

		assertEquals(before, listPaths(firstPath));
		assertFilesAreSame();
	}

	@Test
	public void copyAllSingleFile() {
		both(client -> await(client.copyAll((map("file", "newFile")))));

		assertFileEquals("file", "newFile");
		assertFilesAreSame();
	}

	@Test
	public void copyAllMultipleFiles() {
		both(client -> await(client.copyAll(map(
				"file", "newFile",
				"file2", "newFile2"
		))));

		assertFileEquals("file", "newFile");
		assertFileEquals("file2", "newFile2");
		assertFilesAreSame();
	}

	@Test
	public void copyAllFromSingleDirectory() {
		List<Path> before = listPaths(firstPath);
		both(client -> assertEquals(IS_DIRECTORY, awaitException(client.copyAll(map("directory", "newFile")))));

		assertEquals(before, listPaths(firstPath));
		assertFilesAreSame();
	}

	@Test
	public void copyAllToSingleDirectory() {
		List<Path> before = listPaths(firstPath);
		both(client -> assertEquals(IS_DIRECTORY, awaitException(client.copyAll(map("file", "directory")))));

		assertEquals(before, listPaths(firstPath));
		assertFilesAreSame();
	}

	@Test
	public void copyAllMultipleDirectories() {
		List<Path> before = listPaths(firstPath);
		both(client -> assertEquals(IS_DIRECTORY, awaitException(client.copyAll((map(
				"directory", "newDirectory",
				"directory2", "newDirectory2"
		))))));

		assertEquals(before, listPaths(firstPath));
		assertFilesAreSame();
	}

	@Test
	public void copyAllFilesAndDirectories() {
		both(client -> assertEquals(IS_DIRECTORY, awaitException(client.copyAll(map(
				"file", "newFile",
				"directory", "newDirectory"
		)))));
		assertFilesAreSame();
	}

	@Test
	public void copyAllWithFromNonExisting() {
		both(client -> assertEquals(FILE_NOT_FOUND, awaitException(client.copyAll(map(
				"file", "newFile",
				"nonexistent", "newFile2"
		)))));

		assertFilesAreSame();
	}

	@Test
	public void copyAllFromRoot() {
		both(client -> assertEquals(IS_DIRECTORY, awaitException(client.copyAll(map(
				"file", "newFile",
				"", "newRoot"
		)))));

		assertFilesAreSame();
	}

	@Test
	public void copyAllToRoot() {
		both(client -> assertEquals(IS_DIRECTORY, awaitException(client.copyAll(map(
				"file", "newFile",
				"file2", ""
		)))));
		assertFilesAreSame();
	}

	@Test
	public void copyAllToFileOutsideRoot() {
		both(client -> assertEquals(BAD_PATH, awaitException(client.copyAll(map(
				"file", "newFile",
				"file2", "../new"
		)))));
		assertFilesAreSame();
	}

	@Test
	public void copyAllFromFileOutsideRoot() {
		both(client -> assertEquals(BAD_PATH, awaitException(client.copyAll(map(
				"file", "newFile",
				"../new", "newFile2"
		)))));
		assertFilesAreSame();
	}

	@Test
	public void copyAllIsNotIdempotent() {
		both(client -> {
			await(client.copyAll(map("file", "newFile", "file2", "newFile2")));
			assertEquals(FILE_EXISTS, awaitException(client.copyAll(map("file", "newFile", "file2", "newFile2"))));
		});

		assertFileEquals("file", "newFile");
		assertFileEquals("file2", "newFile2");
		assertFilesAreSame();
	}

	@Test
	public void copyAllUpdatesTimestamps() {
		both(client -> {
			FileMetadata oldMeta1 = await(client.info("file"));
			FileMetadata oldMeta2 = await(client.info("file2"));

			await(Promises.delay(10));

			await(client.copyAll(map(
					"file", "newFile",
					"file2", "newFile2"
			)));

			FileMetadata newMeta1 = await(client.info("newFile"));
			FileMetadata newMeta2 = await(client.info("newFile2"));

			assertEquals(oldMeta1.getSize(), newMeta1.getSize());
			assertEquals(oldMeta2.getSize(), newMeta2.getSize());

			assertTrue(newMeta1.getTimestamp() > oldMeta1.getTimestamp());
			assertTrue(newMeta2.getTimestamp() > oldMeta2.getTimestamp());
		});

		assertFilesAreSame();
	}
	//endregion

	// region moveAll
	@Test
	public void moveAllEmpty() {
		List<Path> before = listPaths(firstPath);
		both(client -> await(client.moveAll(emptyMap())));

		assertEquals(before, listPaths(firstPath));
		assertFilesAreSame();
	}

	@Test
	public void moveAllSingleFile() throws IOException {
		byte[] bytesBefore = Files.readAllBytes(firstPath.resolve("file"));

		both(client -> await(client.moveAll((map("file", "newFile")))));

		bothPaths(path -> {
			assertThat(listPaths(path), not(contains(Paths.get("file"))));
			assertArrayEquals(bytesBefore, Files.readAllBytes(path.resolve("newFile")));
		});

		assertFilesAreSame();
	}

	@Test
	public void moveAllMultipleFiles() throws IOException {
		byte[] bytesBefore1 = Files.readAllBytes(firstPath.resolve("file"));
		byte[] bytesBefore2 = Files.readAllBytes(firstPath.resolve("file2"));

		both(client -> await(client.moveAll((map(
				"file", "newFile",
				"file2", "newFile2"
		)))));

		bothPaths(path -> {
			assertThat(listPaths(path), not(contains(Paths.get("file"), Paths.get("file2"))));
			assertArrayEquals(bytesBefore1, Files.readAllBytes(path.resolve("newFile")));
			assertArrayEquals(bytesBefore2, Files.readAllBytes(path.resolve("newFile2")));
		});

		assertFilesAreSame();
	}

	@Test
	public void moveAllFromSingleDirectory() {
		List<Path> before = listPaths(firstPath);
		both(client -> assertEquals(IS_DIRECTORY, awaitException(client.moveAll(map("directory", "newFile")))));

		assertEquals(before, listPaths(firstPath));
		assertFilesAreSame();
	}

	@Test
	public void moveAllToSingleDirectory() {
		List<Path> before = listPaths(firstPath);
		both(client -> assertEquals(IS_DIRECTORY, awaitException(client.moveAll(map("file", "directory")))));

		assertEquals(before, listPaths(firstPath));
		assertFilesAreSame();
	}

	@Test
	public void moveAllMultipleDirectories() {
		List<Path> before = listPaths(firstPath);
		both(client -> assertEquals(IS_DIRECTORY, awaitException(client.moveAll((map(
				"directory", "newDirectory",
				"directory2", "newDirectory2"
		))))));

		assertEquals(before, listPaths(firstPath));
		assertFilesAreSame();
	}

	@Test
	public void moveAllFilesAndDirectories() {
		both(client -> assertEquals(IS_DIRECTORY, awaitException(client.moveAll(map(
				"file", "newFile",
				"directory", "newDirectory"
		)))));
	}

	@Test
	public void moveAllWithFromNonExisting() {
		both(client -> assertEquals(FILE_NOT_FOUND, awaitException(client.moveAll(map(
				"file", "newFile",
				"nonexistent", "newFile2"
		)))));
	}

	@Test
	public void moveAllFromRoot() {
		both(client -> assertEquals(IS_DIRECTORY, awaitException(client.moveAll(map(
				"file", "newFile",
				"", "newRoot"
		)))));
	}

	@Test
	public void moveAllToRoot() {
		both(client -> assertEquals(IS_DIRECTORY, awaitException(client.moveAll(map(
				"file", "newFile",
				"file2", ""
		)))));
	}

	@Test
	public void moveAllToFileOutsideRoot() {
		both(client -> assertEquals(BAD_PATH, awaitException(client.moveAll(map(
				"file", "newFile",
				"file2", "../new"
		)))));
	}

	@Test
	public void moveAllFromFileOutsideRoot() {
		both(client -> assertEquals(BAD_PATH, awaitException(client.moveAll(map(
				"file", "newFile",
				"../new", "newFile2"
		)))));
	}

	@Test
	public void moveAllNotIdempotent() {
		both(client -> {
			await(client.moveAll(map("file", "newFile", "file2", "newFile2")));
			assertEquals(FILE_NOT_FOUND, awaitException(client.moveAll(map("file", "newFile", "file2", "newFile2"))));
		});

		assertFilesAreSame();
	}

	@Test
	public void moveAllUpdatesTimestamps() {
		both(client -> {
			FileMetadata oldMeta1 = await(client.info("file"));
			FileMetadata oldMeta2 = await(client.info("file2"));

			await(Promises.delay(10));

			await(client.moveAll(map(
					"file", "newFile",
					"file2", "newFile2"
			)));

			FileMetadata newMeta1 = await(client.info("newFile"));
			FileMetadata newMeta2 = await(client.info("newFile2"));

			assertEquals(oldMeta1.getSize(), newMeta1.getSize());
			assertEquals(oldMeta2.getSize(), newMeta2.getSize());

			assertTrue(newMeta1.getTimestamp() > oldMeta1.getTimestamp());
			assertTrue(newMeta2.getTimestamp() > oldMeta2.getTimestamp());
		});

		assertFilesAreSame();
	}
	//endregion


	@Test
	public void infoAllEmpty() {
		both(client -> {
			Map<String, FileMetadata> result = await(client.infoAll(emptySet()));
			assertTrue(result.isEmpty());
		});
	}

	@Test
	public void infoAllSingle() {
		both(client -> {
			Map<String, FileMetadata> result = await(client.infoAll(singleton("file")));
			assertEquals(1, result.size());
			assertEquals("file", first(result.keySet()));
		});
	}

	@Test
	public void infoAllMultiple() {
		both(client -> {
			Map<String, FileMetadata> result = await(client.infoAll(set("file", "file2")));
			assertEquals(2, result.size());
			assertEquals(set("file", "file2"), result.keySet());
		});
	}

	@Test
	public void infoAllMultipleWithMissing() {
		both(client -> {
			Map<String, FileMetadata> result = await(client.infoAll(set("file", "nonexistent")));
			assertEquals(1, result.size());
			assertEquals("file", first(result.keySet()));
		});
	}

	@Test
	public void infoAllWithAllMissing() {
		both(client -> {
			Map<String, FileMetadata> result = await(client.infoAll(set("nonexistent", "nonexistent2")));
			assertEquals(0, result.size());
		});
	}

	// region helpers
	private void assertFileEquals(String first, String second) {
		try {
			assertArrayEquals(Files.readAllBytes(firstPath.resolve(first)), Files.readAllBytes(firstPath.resolve(second)));
			assertArrayEquals(Files.readAllBytes(secondPath.resolve(first)), Files.readAllBytes(secondPath.resolve(second)));
		} catch (IOException e) {
			throw new AssertionError(e);
		}
	}

	private void assertFilesAreSame() {
		try {
			List<Path> firstFiles = listFiles(firstPath);
			List<Path> secondFiles = listFiles(secondPath);
			assertEquals(firstFiles.size(), secondFiles.size());

			for (int i = 0; i < firstFiles.size(); i++) {
				Path firstPath = firstFiles.get(i);
				if (Files.isDirectory(firstPath)) continue;
				byte[] firstFileBytes = Files.readAllBytes(firstPath);
				byte[] secondFileBytes = Files.readAllBytes(secondFiles.get(i));
				assertArrayEquals(firstFileBytes, secondFileBytes);
			}
		} catch (IOException e) {
			throw new AssertionError(e);
		}
	}

	private List<Path> listPaths(Path directoryPath) {
		List<Path> paths = new ArrayList<>();
		list(directoryPath, paths, true);
		return paths.stream()
				.map(directoryPath::relativize)
				.collect(toList());
	}


	private List<Path> listFiles(Path directoryPath) {
		List<Path> list = new ArrayList<>();
		list(directoryPath, list, false);
		return list;
	}

	private void list(Path directoryPath, List<Path> paths, boolean includeDirs) {
		try {
			List<Path> subPaths = Files.list(directoryPath).collect(toList());
			for (Path path : subPaths) {
				if (Files.isRegularFile(path)) {
					paths.add(path);
				} else if (Files.isDirectory(path)) {
					if (includeDirs) paths.add(path);
					list(path, paths, includeDirs);
				} else {
					throw new AssertionError();
				}
			}
		} catch (IOException e) {
			throw new AssertionError(e);
		}
	}

	private void clearDirectory(Path dir) throws IOException {
		for (Iterator<Path> iterator = Files.list(dir).iterator(); iterator.hasNext(); ) {
			Path file = iterator.next();
			if (Files.isDirectory(file))
				clearDirectory(file);
			Files.delete(file);
		}
	}

	private void assertMetadataEquals(Map<String, FileMetadata> expected, Map<String, FileMetadata> actual) {
		assertEquals(removeTimestamp(expected), removeTimestamp(actual));
	}

	private Map<String, FileMetadata> removeTimestamp(Map<String, FileMetadata> map) {
		return map.entrySet().stream()
				.collect(toMap(Map.Entry::getKey, e -> FileMetadata.of(e.getValue().getSize(), 0)));
	}

	private void both(Consumer<FsClient> clientConsumer) {
		clientConsumer.accept(first);
		clientConsumer.accept(second);
	}

	private void bothPaths(ThrowingConsumer<Path> pathConsumer) {
		try {
			pathConsumer.accept(firstPath);
			pathConsumer.accept(secondPath);
		} catch (Throwable e) {
			throw new AssertionError(e);
		}
	}
	// endregion

	// Default methods are not overridden
	private static class DefaultFsClient implements FsClient {
		private final FsClient peer;

		private DefaultFsClient(FsClient peer) {
			this.peer = peer;
		}

		@Override
		public Promise<ChannelConsumer<ByteBuf>> upload(@NotNull String name) {
			return peer.upload(name);
		}

		@Override
		public Promise<ChannelConsumer<ByteBuf>> upload(@NotNull String name, long size) {
			return peer.upload(name, size);
		}

		@Override
		public Promise<ChannelConsumer<ByteBuf>> append(@NotNull String name, long offset) {
			return peer.append(name, offset);
		}

		@Override
		public Promise<ChannelSupplier<ByteBuf>> download(@NotNull String name, long offset, long limit) {
			return peer.download(name, offset, limit);
		}

		@Override
		public Promise<Void> delete(@NotNull String name) {
			return peer.delete(name);
		}

		@Override
		public Promise<Map<String, FileMetadata>> list(@NotNull String glob) {
			return peer.list(glob);
		}
	}
}
