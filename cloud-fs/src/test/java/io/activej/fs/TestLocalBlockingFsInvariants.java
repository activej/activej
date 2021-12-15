package io.activej.fs;

import io.activej.common.function.ConsumerEx;
import io.activej.fs.LocalFileUtils.IORunnable;
import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.*;
import java.nio.file.*;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;

import static io.activej.common.Utils.*;
import static io.activej.fs.Utils.*;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;
import static java.util.Arrays.asList;
import static java.util.Collections.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

@SuppressWarnings("ConstantConditions")
@RunWith(Parameterized.class)
public final class TestLocalBlockingFsInvariants {
	@Rule
	public final TemporaryFolder tmpFolder = new TemporaryFolder();

	private Path firstPath;
	private Path secondPath;

	private BlockingFs first;
	private BlockingFs second;

	@Parameterized.Parameter()
	public String testName;

	@Parameterized.Parameter(1)
	public UnaryOperator<LocalBlockingFs> initializer;

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

		LocalBlockingFs firstLocalFs = LocalBlockingFs.create(firstPath);
		firstLocalFs.start();
		first = initializer.apply(firstLocalFs);

		LocalBlockingFs secondLocalFs = initializer.apply(LocalBlockingFs.create(secondPath));
		secondLocalFs.start();
		second = new DefaultBlockingFs(secondLocalFs);

		initializeDirs(asList(
				"file",
				"file2",
				"directory/subdir/file3.txt",
				"directory/file.txt",
				"directory2/file2.txt"
		));
	}

	@Parameterized.Parameters(name = "{0}")
	public static Collection<Object[]> getParameters() {
		return Arrays.asList(
				new Object[]{
						"Regular",
						(UnaryOperator<LocalBlockingFs>) fs -> fs},
				new Object[]{
						"With Hard Link On Copy",
						(UnaryOperator<LocalBlockingFs>) fs -> fs.withHardLinkOnCopy(true)
				}
		);
	}

	private void initializeDirs(List<String> paths) {
		try {
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
		both(client -> client.move(from, to));

		bothPaths(path -> {
			assertThat(listPaths(path), not(contains(from)));
			assertArrayEquals(bytesBefore, Files.readAllBytes(path.resolve(to)));
		});
		assertFilesAreSame(firstPath, secondPath);
	}

	@Test
	public void moveToNewDirectory() throws IOException {
		String from = "file";
		String to = "a/b/c/d/newFile";

		byte[] bytesBefore = Files.readAllBytes(firstPath.resolve(from));
		both(client -> client.move(from, to));

		bothPaths(path -> {
			assertThat(listPaths(path), not(contains(from)));
			assertArrayEquals(bytesBefore, Files.readAllBytes(path.resolve(to)));
		});
		assertFilesAreSame(firstPath, secondPath);
	}

	@Test
	public void moveDirectory() {
		List<Path> before = listPaths(firstPath);
		both(client -> {
			try {
				client.move("directory", "newDirectory");
				fail();
			} catch (java.io.FileNotFoundException ignored) {
			}
		});

		assertFilesAreSame(firstPath, secondPath);
		assertEquals(before, listPaths(firstPath));
	}

	@Test
	public void moveToExistingDirectoryName() {
		List<Path> before = listPaths(firstPath);
		both(client -> {
			try {
				client.move("file", "directory");
				fail();
			} catch (DirectoryNotEmptyException ignored) {
			}
		});

		assertFilesAreSame(firstPath, secondPath);
		assertEquals(before, listPaths(firstPath));
	}

	@Test
	public void moveToFileInsideDirectoryAsAFile() {
		List<Path> before = listPaths(firstPath);
		both(client -> {
			try {
				client.move("file2", "file/newFile");
				fail();
			} catch (FileSystemException ignored) {
			}
		});

		assertFilesAreSame(firstPath, secondPath);
		assertEquals(before, listPaths(firstPath));
	}

	@Test
	public void moveFromFileInsideDirectoryAsAFile() {
		List<Path> before = listPaths(firstPath);
		both(client -> assertException(FileNotFoundException.class, () -> client.move("file/someFile", "newFile")));

		assertFilesAreSame(firstPath, secondPath);
		assertEquals(before, listPaths(firstPath));
	}

	@Test
	public void moveNonExistentFile() {
		List<Path> before = listPaths(firstPath);
		both(client -> assertException(FileNotFoundException.class, () -> client.move("nonexistent", "nonexistentTarget")));

		assertFilesAreSame(firstPath, secondPath);
		assertEquals(before, listPaths(firstPath));
	}

	@Test
	public void moveNonExistentFileToDirectory() {
		List<Path> before = listPaths(firstPath);
		both(client -> assertException(FileNotFoundException.class, () -> client.move("nonexistent", "directory")));

		assertFilesAreSame(firstPath, secondPath);
		assertEquals(before, listPaths(firstPath));
	}

	@Test
	public void moveSelfFiles() {
		both(client -> client.move("file2", "file2"));

		bothPaths(path -> assertThat(listPaths(path), not(contains(Paths.get("file2")))));
		assertFilesAreSame(firstPath, secondPath);
	}

	@Test
	public void moveSelfNonExistent() {
		both(client -> assertException(FileNotFoundException.class, () -> client.move("nonexistent", "nonexistent")));
		assertFilesAreSame(firstPath, secondPath);
	}

	@Test
	public void moveSelfDirectories() {
		both(client -> assertException(FileNotFoundException.class, () -> client.move("directory", "directory")));
		assertFilesAreSame(firstPath, secondPath);
	}

	@Test
	public void moveFromEmptyFilename() throws IOException {
		Map<String, FileMetadata> before = first.list("**");
		both(client -> assertException(FileNotFoundException.class, () -> client.move("", "newFile")));
		both(client -> assertMetadataEquals(before, client.list("**")));
		assertFilesAreSame(firstPath, secondPath);
	}

	@Test
	public void moveToEmptyFilename() throws IOException {
		Map<String, FileMetadata> before = first.list("**");
		both(client -> assertException(DirectoryNotEmptyException.class, () -> client.move("file", "")));
		both(client -> assertMetadataEquals(before, client.list("**")));
		assertFilesAreSame(firstPath, secondPath);
	}

	@Test
	public void moveUpdatesTimestamp() {
		both(client -> {
			FileMetadata oldMeta = client.info("file");
			Thread.sleep(getDelay(oldMeta.getTimestamp()));
			client.move("file", "newFile");
			FileMetadata newMeta = client.info("newFile");

			assertEquals(oldMeta.getSize(), newMeta.getSize());
			assertTrue(newMeta.getTimestamp() > oldMeta.getTimestamp());
		});
		assertFilesAreSame(firstPath, secondPath);
	}

	@Test
	public void moveNotIdempotent() {
		both(client -> {
			// first call completes successfully
			client.move("file", "newFile");
			assertException(FileNotFoundException.class, () -> client.move("file", "newFile"));
		});
		assertFilesAreSame(firstPath, secondPath);
	}
	// endregion

	// region copy
	@Test
	public void regularCopy() {
		both(client -> client.copy("file2", "newFile"));
		assertFileEquals(firstPath, secondPath, "file2", "newFile");
		assertFilesAreSame(firstPath, secondPath);
	}

	@Test
	public void copyToNewDirectory() {
		both(client -> client.copy("file", "a/b/c/d/newFile"));
		assertFileEquals(firstPath, secondPath, "file", "a/b/c/d/newFile");
		assertFilesAreSame(firstPath, secondPath);
	}

	@Test
	public void copyDirectory() {
		both(client -> assertException(FileNotFoundException.class, () -> client.copy("directory", "newDirectory")));
		assertFilesAreSame(firstPath, secondPath);
	}

	@Test
	public void copyToExistingDirectoryName() {
		both(client -> assertException(DirectoryNotEmptyException.class, () -> client.copy("file", "directory")));
		assertFilesAreSame(firstPath, secondPath);
	}

	@Test
	public void copyToFileInsideDirectoryAsAFile() {
		both(client -> {
			try {
				client.copy("file2", "file/newFile");
				fail();
			} catch (FileSystemException ignored) {
			}
		});
		assertFilesAreSame(firstPath, secondPath);
	}

	@Test
	public void copyFromFileInsideDirectoryAsAFile() {
		both(client -> assertException(FileNotFoundException.class, () -> client.copy("file/newFile", "newFile")));
		assertFilesAreSame(firstPath, secondPath);
	}

	@Test
	public void copyNonExistentFile() {
		both(client -> assertException(FileNotFoundException.class, () -> client.copy("nonexistent", "nonexistentTarget")));
		assertFilesAreSame(firstPath, secondPath);
	}

	@Test
	public void copyNonExistentFileToDirectory() {
		both(client -> assertException(FileNotFoundException.class, () -> client.copy("nonexistent", "directory")));
		assertFilesAreSame(firstPath, secondPath);
	}

	@Test
	public void copySelf() throws IOException {
		byte[] bytes = Files.readAllBytes(firstPath.resolve("file2"));
		both(client -> client.copy("file2", "file2"));
		assertFilesAreSame(firstPath, secondPath);
		assertArrayEquals(bytes, Files.readAllBytes(firstPath.resolve("file2")));
	}

	@Test
	public void copyFromEmptyFilename() throws IOException {
		Map<String, FileMetadata> before = first.list("**");
		both(client -> assertException(FileNotFoundException.class, () -> client.copy("", "newFile")));
		both(client -> assertMetadataEquals(before, client.list("**")));
		assertFilesAreSame(firstPath, secondPath);
	}

	@Test
	public void copyToEmptyFilename() throws IOException {
		Map<String, FileMetadata> before = first.list("**");
		both(client -> assertException(DirectoryNotEmptyException.class, () -> client.copy("file", "")));
		both(client -> assertMetadataEquals(before, client.list("**")));
		assertFilesAreSame(firstPath, secondPath);
	}

	@Test
	public void copyUpdatesTimestamp() {
		both(fs -> {
			FileMetadata oldMeta = fs.info("file");
			Thread.sleep(getDelay(oldMeta.getTimestamp()));
			fs.copy("file", "newFile");
			FileMetadata newMeta = fs.info("newFile");

			assertEquals(oldMeta.getSize(), newMeta.getSize());
			assertTrue(newMeta.getTimestamp() > oldMeta.getTimestamp());
		});
		assertFilesAreSame(firstPath, secondPath);
	}

	@Test
	public void copyIsIdempotent() {
		both(client -> {
			client.copy("file", "newFile");
			client.copy("file", "newFile");
		});
		assertFileEquals(firstPath, secondPath, "file", "newFile");
		assertFilesAreSame(firstPath, secondPath);
	}
	// endregion

	// region deleteAll
	@Test
	public void deleteAllEmpty() {
		List<Path> before = listPaths(firstPath);
		both(client -> client.deleteAll(emptySet()));

		assertEquals(before, listPaths(firstPath));
		assertFilesAreSame(firstPath, secondPath);
	}

	@Test
	public void deleteAllSingleFile() {
		both(client -> client.deleteAll(singleton("file")));

		bothPaths(path -> assertThat(listPaths(path), not(contains(Paths.get("file")))));
		assertFilesAreSame(firstPath, secondPath);
	}

	@Test
	public void deleteAllMultipleFiles() {
		both(client -> client.deleteAll(setOf("file", "file2")));

		bothPaths(path -> assertThat(listPaths(path), not(contains(Paths.get("file"), Paths.get("file2")))));
		assertFilesAreSame(firstPath, secondPath);
	}

	@Test
	public void deleteAllSingleDirectory() {
		List<Path> before = listPaths(firstPath);
		both(client -> assertException(DirectoryNotEmptyException.class, () -> client.deleteAll(singleton("directory"))));

		assertEquals(before, listPaths(firstPath));
		assertFilesAreSame(firstPath, secondPath);
	}

	@Test
	public void deleteAllMultipleDirectories() {
		List<Path> before = listPaths(firstPath);
		both(client -> assertException(DirectoryNotEmptyException.class, () -> client.deleteAll(setOf("directory", "directory2"))));

		assertEquals(before, listPaths(firstPath));
		assertFilesAreSame(firstPath, secondPath);
	}

	@Test
	public void deleteAllFilesAndDirectories() {
		both(client -> assertException(DirectoryNotEmptyException.class, () -> client.deleteAll(setOf("file", "directory"))));
		assertFilesAreSame(firstPath, secondPath);
	}

	@Test
	public void deleteAllWithNonExisting() {
		both(client -> client.deleteAll(setOf("file", "nonexistent")));

		bothPaths(path -> assertThat(listPaths(path), not(contains(Paths.get("file")))));
		assertFilesAreSame(firstPath, secondPath);
	}

	@Test
	public void deleteAllWithRoot() {
		both(client -> client.deleteAll(setOf("file", "")));
		assertFilesAreSame(firstPath, secondPath);
	}

	@Test
	public void deleteAllWithFileOutsideRoot() {
		both(client -> {
			try {
				client.deleteAll(setOf("file", ".."));
				fail();
			} catch (FileSystemException e) {
				assertThat(e.getMessage(), containsString("Path '..' is forbidden"));
			}
		});
		assertFilesAreSame(firstPath, secondPath);
	}

	@Test
	public void deleteAllIsIdempotent() {
		both(client -> {
			client.deleteAll(setOf("file", "file2"));
			client.deleteAll(setOf("file", "file2"));
			client.deleteAll(setOf("file", "file2"));
			client.deleteAll(setOf("file", "file2"));
		});

		bothPaths(path -> assertThat(listPaths(path), not(contains(Paths.get("file"), Paths.get("file2")))));
		assertFilesAreSame(firstPath, secondPath);
	}
	//endregion

	// region copyAll
	@Test
	public void copyAllEmpty() {
		List<Path> before = listPaths(firstPath);
		both(client -> client.copyAll(emptyMap()));

		assertEquals(before, listPaths(firstPath));
		assertFilesAreSame(firstPath, secondPath);
	}

	@Test
	public void copyAllSingleFile() {
		both(client -> client.copyAll(mapOf("file", "newFile")));

		assertFileEquals(firstPath, secondPath, "file", "newFile");
		assertFilesAreSame(firstPath, secondPath);
	}

	@Test
	public void copyAllMultipleFiles() {
		both(client -> client.copyAll(mapOf(
				"file", "newFile",
				"file2", "newFile2"
		)));

		assertFileEquals(firstPath, secondPath, "file", "newFile");
		assertFileEquals(firstPath, secondPath, "file2", "newFile2");
		assertFilesAreSame(firstPath, secondPath);
	}

	@Test
	public void copyAllFromSingleDirectory() {
		List<Path> before = listPaths(firstPath);
		both(client -> assertException(FileNotFoundException.class, () -> client.copyAll(mapOf("directory", "newFile"))));

		assertEquals(before, listPaths(firstPath));
		assertFilesAreSame(firstPath, secondPath);
	}

	@Test
	public void copyAllToSingleDirectory() {
		List<Path> before = listPaths(firstPath);
		both(client -> assertException(DirectoryNotEmptyException.class, () -> client.copyAll(mapOf("file", "directory"))));

		assertEquals(before, listPaths(firstPath));
		assertFilesAreSame(firstPath, secondPath);
	}

	@Test
	public void copyAllMultipleDirectories() {
		List<Path> before = listPaths(firstPath);
		both(client -> assertException(FileNotFoundException.class,
				() -> client.copyAll(mapOf(
						"directory", "newDirectory",
						"directory2", "newDirectory2"
				))));

		assertEquals(before, listPaths(firstPath));
		assertFilesAreSame(firstPath, secondPath);
	}

	@Test
	public void copyAllFilesAndDirectories() {
		both(client -> assertException(FileNotFoundException.class,
				() -> client.copyAll(mapOf(
						"file", "newFile",
						"directory", "newDirectory"
				))));
		assertFilesAreSame(firstPath, secondPath);
	}

	@Test
	public void copyAllWithFromNonExisting() {
		both(client -> assertException(FileNotFoundException.class,
				() -> client.copyAll(mapOf(
						"file", "newFile",
						"nonexistent", "newFile2"
				))));

		assertFilesAreSame(firstPath, secondPath);
	}

	@Test
	public void copyAllFromRoot() {
		both(client -> assertException(FileNotFoundException.class,
				() -> client.copyAll(mapOf(
						"file", "newFile",
						"", "newRoot"
				))));

		assertFilesAreSame(firstPath, secondPath);
	}

	@Test
	public void copyAllToRoot() {
		both(client -> assertException(DirectoryNotEmptyException.class,
				() -> client.copyAll(mapOf(
						"file", "newFile",
						"file2", ""
				))));
		assertFilesAreSame(firstPath, secondPath);
	}

	@Test
	public void copyAllToFileOutsideRoot() {
		both(client -> {
			try {
				client.copyAll(mapOf(
						"file", "newFile",
						"file2", "../new"
				));
				fail();
			} catch (FileSystemException e) {
				assertThat(e.getMessage(), containsString("Path '.." + File.separatorChar + "new' is forbidden"));
			}
		});
	}

	@Test
	public void copyAllFromFileOutsideRoot() {
		both(client -> {
			try {
				client.copyAll(mapOf(
						"file", "newFile",
						"../new", "newFile2"
				));
				fail();
			} catch (FileSystemException e) {
				assertThat(e.getMessage(), containsString("Path '.." + File.separatorChar + "new' is forbidden"));
			}
		});
		assertFilesAreSame(firstPath, secondPath);
	}

	@Test
	public void copyAllIsIdempotent() {
		both(client -> {
			client.copyAll(mapOf("file", "newFile", "file2", "newFile2"));
			client.copyAll(mapOf("file", "newFile", "file2", "newFile2"));
		});

		assertFileEquals(firstPath, secondPath, "file", "newFile");
		assertFileEquals(firstPath, secondPath, "file2", "newFile2");
		assertFilesAreSame(firstPath, secondPath);
	}

	@Test
	public void copyAllUpdatesTimestamps() {
		both(client -> {
			FileMetadata oldMeta1 = client.info("file");
			FileMetadata oldMeta2 = client.info("file2");

			Thread.sleep(getDelay(oldMeta1.getTimestamp()));

			client.copyAll(mapOf(
					"file", "newFile",
					"file2", "newFile2"
			));

			FileMetadata newMeta1 = client.info("newFile");
			FileMetadata newMeta2 = client.info("newFile2");

			assertEquals(oldMeta1.getSize(), newMeta1.getSize());
			assertEquals(oldMeta2.getSize(), newMeta2.getSize());

			assertTrue(newMeta1.getTimestamp() > oldMeta1.getTimestamp());
			assertTrue(newMeta2.getTimestamp() > oldMeta2.getTimestamp());
		});

		assertFilesAreSame(firstPath, secondPath);
	}
	//endregion

	// region moveAll
	@Test
	public void moveAllEmpty() {
		List<Path> before = listPaths(firstPath);
		both(client -> client.moveAll(emptyMap()));

		assertEquals(before, listPaths(firstPath));
		assertFilesAreSame(firstPath, secondPath);
	}

	@Test
	public void moveAllSingleFile() throws IOException {
		byte[] bytesBefore = Files.readAllBytes(firstPath.resolve("file"));

		both(client -> client.moveAll(mapOf("file", "newFile")));

		bothPaths(path -> {
			assertThat(listPaths(path), not(contains(Paths.get("file"))));
			assertArrayEquals(bytesBefore, Files.readAllBytes(path.resolve("newFile")));
		});

		assertFilesAreSame(firstPath, secondPath);
	}

	@Test
	public void moveAllMultipleFiles() throws IOException {
		byte[] bytesBefore1 = Files.readAllBytes(firstPath.resolve("file"));
		byte[] bytesBefore2 = Files.readAllBytes(firstPath.resolve("file2"));

		both(client -> client.moveAll(mapOf(
				"file", "newFile",
				"file2", "newFile2"
		)));

		bothPaths(path -> {
			assertThat(listPaths(path), not(contains(Paths.get("file"), Paths.get("file2"))));
			assertArrayEquals(bytesBefore1, Files.readAllBytes(path.resolve("newFile")));
			assertArrayEquals(bytesBefore2, Files.readAllBytes(path.resolve("newFile2")));
		});

		assertFilesAreSame(firstPath, secondPath);
	}

	@Test
	public void moveAllFromSingleDirectory() {
		List<Path> before = listPaths(firstPath);
		both(client -> assertException(FileNotFoundException.class, () -> client.moveAll(mapOf("directory", "newFile"))));

		assertEquals(before, listPaths(firstPath));
		assertFilesAreSame(firstPath, secondPath);
	}

	@Test
	public void moveAllToSingleDirectory() {
		List<Path> before = listPaths(firstPath);
		both(client -> assertException(DirectoryNotEmptyException.class, () -> client.moveAll(mapOf("file", "directory"))));

		assertEquals(before, listPaths(firstPath));
		assertFilesAreSame(firstPath, secondPath);
	}

	@Test
	public void moveAllMultipleDirectories() {
		List<Path> before = listPaths(firstPath);
		both(client -> assertException(FileNotFoundException.class,
				() -> client.moveAll(mapOf(
						"directory", "newDirectory",
						"directory2", "newDirectory2"
				))));

		assertEquals(before, listPaths(firstPath));
		assertFilesAreSame(firstPath, secondPath);
	}

	@Test
	public void moveAllFilesAndDirectories() {
		both(client -> assertException(FileNotFoundException.class,
				() -> client.moveAll(mapOf(
						"file", "newFile",
						"directory", "newDirectory"
				))));
	}

	@Test
	public void moveAllWithFromNonExisting() {
		both(client -> assertException(FileNotFoundException.class,
				() -> client.moveAll(mapOf(
						"file", "newFile",
						"nonexistent", "newFile2"
				))));
	}

	@Test
	public void moveAllFromRoot() {
		both(client -> assertException(FileNotFoundException.class,
				() -> client.moveAll(mapOf(
						"file", "newFile",
						"", "newRoot"
				))));
	}

	@Test
	public void moveAllToRoot() {
		both(client -> assertException(DirectoryNotEmptyException.class,
				() -> client.moveAll(mapOf(
						"file", "newFile",
						"file2", ""
				))));
	}

	@Test
	public void moveAllToFileOutsideRoot() {
		both(client -> {
			try {
				client.moveAll(mapOf(
						"file", "newFile",
						"file2", "../new"
				));
				fail();
			} catch (FileSystemException e) {
				assertThat(e.getMessage(), containsString("Path '.." + File.separatorChar + "new' is forbidden"));
			}
		});
	}

	@Test
	public void moveAllFromFileOutsideRoot() {
		both(client -> {
			try {
				client.moveAll(mapOf(
						"file", "newFile",
						"../new", "newFile2"
				));
				fail();
			} catch (FileSystemException e) {
				assertThat(e.getMessage(), containsString("Path '.." + File.separatorChar + "new' is forbidden"));
			}
		});
	}

	@Test
	public void moveAllNotIdempotent() {
		both(client -> {
			client.moveAll(mapOf("file", "newFile", "file2", "newFile2"));
			assertException(FileNotFoundException.class,
					() -> client.moveAll(mapOf(
							"file", "newFile",
							"file2", "newFile2"
					)));
		});

		assertFilesAreSame(firstPath, secondPath);
	}

	@Test
	public void moveAllUpdatesTimestamps() {
		both(client -> {
			FileMetadata oldMeta1 = client.info("file");
			FileMetadata oldMeta2 = client.info("file2");

			Thread.sleep(getDelay(oldMeta1.getTimestamp()));

			client.moveAll(mapOf(
					"file", "newFile",
					"file2", "newFile2"
			));

			FileMetadata newMeta1 = client.info("newFile");
			FileMetadata newMeta2 = client.info("newFile2");

			assertEquals(oldMeta1.getSize(), newMeta1.getSize());
			assertEquals(oldMeta2.getSize(), newMeta2.getSize());

			assertTrue(newMeta1.getTimestamp() > oldMeta1.getTimestamp());
			assertTrue(newMeta2.getTimestamp() > oldMeta2.getTimestamp());
		});

		assertFilesAreSame(firstPath, secondPath);
	}

	@Test
	public void moveAllWithSelfExistent() throws IOException {
		byte[] bytesBefore1 = Files.readAllBytes(firstPath.resolve("file"));
		byte[] bytesBefore2 = Files.readAllBytes(firstPath.resolve("file2"));

		both(client -> client.moveAll(mapOf(
				"file", "file",
				"file2", "newFile2"
		)));

		bothPaths(path -> {
			assertThat(listPaths(path), not(contains(Paths.get("file2"))));
			assertArrayEquals(bytesBefore1, Files.readAllBytes(path.resolve("file")));
			assertArrayEquals(bytesBefore2, Files.readAllBytes(path.resolve("newFile2")));
		});
		assertFilesAreSame(firstPath, secondPath);
	}

	@Test
	public void moveAllWithSelfNonExistent() {
		both(client -> assertException(FileNotFoundException.class,
				() -> client.moveAll(mapOf(
						"file", "newFile",
						"nonexistent", "nonexistent"
				))));
	}

	@Test
	public void moveAllWithSelfDirectory() {
		both(client -> assertException(FileNotFoundException.class,
				() -> client.moveAll(mapOf(
						"file", "newFile",
						"directory", "directory"
				))));
	}
	//endregion

	// region infoAll
	@Test
	public void infoAllEmpty() {
		both(client -> assertTrue(client.infoAll(emptySet()).isEmpty()));
	}

	@Test
	public void infoAllSingle() {
		both(client -> {
			Map<String, FileMetadata> result = client.infoAll(singleton("file"));
			assertEquals(1, result.size());
			assertEquals("file", first(result.keySet()));
		});
	}

	@Test
	public void infoAllMultiple() {
		both(client -> {
			Map<String, FileMetadata> result = client.infoAll(setOf("file", "file2"));
			assertEquals(2, result.size());
			assertEquals(setOf("file", "file2"), result.keySet());
		});
	}

	@Test
	public void infoAllMultipleWithMissing() {
		both(client -> {
			Map<String, FileMetadata> result = client.infoAll(setOf("file", "nonexistent"));
			assertEquals(1, result.size());
			assertEquals("file", first(result.keySet()));
		});
	}

	@Test
	public void infoAllWithAllMissing() {
		both(client -> {
			Map<String, FileMetadata> result = client.infoAll(setOf("nonexistent", "nonexistent2"));
			assertEquals(0, result.size());
		});
	}
	// endregion

	// Default methods are not overridden
	private static class DefaultBlockingFs implements BlockingFs {
		private final BlockingFs peer;

		private DefaultBlockingFs(BlockingFs peer) {
			this.peer = peer;
		}

		@Override
		public OutputStream upload(@NotNull String name) throws IOException {
			return peer.upload(name);
		}

		@Override
		public OutputStream upload(@NotNull String name, long size) throws IOException {
			return peer.upload(name, size);
		}

		@Override
		public OutputStream append(@NotNull String name, long offset) throws IOException {
			return peer.append(name, offset);
		}

		@Override
		public InputStream download(@NotNull String name, long offset, long limit) throws IOException {
			return peer.download(name, offset, limit);
		}

		@Override
		public void delete(@NotNull String name) throws IOException {
			peer.delete(name);
		}

		@Override
		public Map<String, FileMetadata> list(@NotNull String glob) throws IOException {
			return peer.list(glob);
		}
	}

	private void both(ConsumerEx<BlockingFs> fsConsumer) {
		try {
			fsConsumer.accept(first);
			fsConsumer.accept(second);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	private void bothPaths(ConsumerEx<Path> consumer) {
		Utils.bothPaths(firstPath, secondPath, consumer);
	}

	private static void assertException(Class<? extends IOException> errorClass, IORunnable runnable) {
		try {
			runnable.run();
			fail();
		} catch (IOException ioException) {
			assertThat(ioException, instanceOf(errorClass));
		}
	}
}
