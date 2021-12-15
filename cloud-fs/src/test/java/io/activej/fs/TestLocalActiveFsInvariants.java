package io.activej.fs;

import io.activej.bytebuf.ByteBuf;
import io.activej.common.function.ConsumerEx;
import io.activej.csp.ChannelConsumer;
import io.activej.csp.ChannelSupplier;
import io.activej.fs.exception.FileNotFoundException;
import io.activej.fs.exception.ForbiddenPathException;
import io.activej.fs.exception.IsADirectoryException;
import io.activej.fs.exception.PathContainsFileException;
import io.activej.promise.Promise;
import io.activej.promise.Promises;
import io.activej.test.rules.ByteBufRule;
import io.activej.test.rules.EventloopRule;
import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.UnaryOperator;

import static io.activej.common.Utils.*;
import static io.activej.eventloop.Eventloop.getCurrentEventloop;
import static io.activej.fs.Utils.*;
import static io.activej.promise.TestUtils.await;
import static io.activej.promise.TestUtils.awaitException;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;
import static java.util.Arrays.asList;
import static java.util.Collections.*;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

@SuppressWarnings("ConstantConditions")
@RunWith(Parameterized.class)
public final class TestLocalActiveFsInvariants {
	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@Rule
	public final TemporaryFolder tmpFolder = new TemporaryFolder();

	@Parameter()
	public String testName;

	@Parameter(1)
	public UnaryOperator<LocalActiveFs> initializer;

	private Path firstPath;
	private Path secondPath;

	private ActiveFs first;
	private ActiveFs second;

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

		LocalActiveFs firstLocalFs = initializer.apply(LocalActiveFs.create(getCurrentEventloop(), newSingleThreadExecutor(), firstPath));
		await(firstLocalFs.start());
		first = firstLocalFs;

		LocalActiveFs secondLocalFs = initializer.apply(LocalActiveFs.create(getCurrentEventloop(), newSingleThreadExecutor(), secondPath));
		await(secondLocalFs.start());
		second = new DefaultActiveFs(secondLocalFs);

		initializeDirs(asList(
				"file",
				"file2",
				"directory/subdir/file3.txt",
				"directory/file.txt",
				"directory2/file2.txt"
		));
	}

	@Parameters(name = "{0}")
	public static Collection<Object[]> getParameters() {
		return Arrays.asList(
				new Object[]{
						"Regular",
						(UnaryOperator<LocalActiveFs>) fs -> fs},
				new Object[]{
						"With Hard Link On Copy",
						(UnaryOperator<LocalActiveFs>) fs -> fs.withHardLinkOnCopy(true)
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
		both(client -> await(client.move(from, to)));

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
		both(client -> await(client.move(from, to)));

		bothPaths(path -> {
			assertThat(listPaths(path), not(contains(from)));
			assertArrayEquals(bytesBefore, Files.readAllBytes(path.resolve(to)));
		});
		assertFilesAreSame(firstPath, secondPath);
	}

	@Test
	public void moveDirectory() {
		List<Path> before = listPaths(firstPath);
		both(client -> assertThat(awaitException(client.move("directory", "newDirectory")), instanceOf(IsADirectoryException.class)));

		assertFilesAreSame(firstPath, secondPath);
		assertEquals(before, listPaths(firstPath));
	}

	@Test
	public void moveToExistingDirectoryName() {
		List<Path> before = listPaths(firstPath);
		both(client -> assertThat(awaitException(client.move("file", "directory")), instanceOf(IsADirectoryException.class)));

		assertFilesAreSame(firstPath, secondPath);
		assertEquals(before, listPaths(firstPath));
	}

	@Test
	public void moveToFileInsideDirectoryAsAFile() {
		List<Path> before = listPaths(firstPath);
		both(client -> assertThat(awaitException(client.move("file2", "file/newFile")), instanceOf(PathContainsFileException.class)));

		assertFilesAreSame(firstPath, secondPath);
		assertEquals(before, listPaths(firstPath));
	}

	@Test
	public void moveFromFileInsideDirectoryAsAFile() {
		List<Path> before = listPaths(firstPath);
		both(client -> assertThat(awaitException(client.move("file/someFile", "newFile")), instanceOf(FileNotFoundException.class)));

		assertFilesAreSame(firstPath, secondPath);
		assertEquals(before, listPaths(firstPath));
	}

	@Test
	public void moveNonExistentFile() {
		List<Path> before = listPaths(firstPath);
		both(client -> assertThat(awaitException(client.move("nonexistent", "nonexistentTarget")), instanceOf(FileNotFoundException.class)));

		assertFilesAreSame(firstPath, secondPath);
		assertEquals(before, listPaths(firstPath));
	}

	@Test
	public void moveNonExistentFileToDirectory() {
		List<Path> before = listPaths(firstPath);
		both(client -> assertThat(awaitException(client.move("nonexistent", "directory")), instanceOf(FileNotFoundException.class)));

		assertFilesAreSame(firstPath, secondPath);
		assertEquals(before, listPaths(firstPath));
	}

	@Test
	public void moveSelfFiles() {
		both(client -> await(client.move("file2", "file2")));

		bothPaths(path -> assertThat(listPaths(path), not(contains(Paths.get("file2")))));
		assertFilesAreSame(firstPath, secondPath);
	}

	@Test
	public void moveSelfNonExistent() {
		both(client -> assertThat(awaitException(client.move("nonexistent", "nonexistent")), instanceOf(FileNotFoundException.class)));
		assertFilesAreSame(firstPath, secondPath);
	}

	@Test
	public void moveSelfDirectories() {
		both(client -> assertThat(awaitException(client.move("directory", "directory")), instanceOf(IsADirectoryException.class)));
		assertFilesAreSame(firstPath, secondPath);
	}

	@Test
	public void moveFromEmptyFilename() {
		Map<String, FileMetadata> before = await(first.list("**"));
		both(client -> assertThat(awaitException(client.move("", "newFile")), instanceOf(IsADirectoryException.class)));
		both(client -> assertMetadataEquals(before, await(client.list("**"))));
	}

	@Test
	public void moveToEmptyFilename() {
		Map<String, FileMetadata> before = await(first.list("**"));
		both(client -> assertThat(awaitException(client.move("file", "")), instanceOf(IsADirectoryException.class)));
		both(client -> assertMetadataEquals(before, await(client.list("**"))));
	}

	@Test
	public void moveUpdatesTimestamp() {
		both(client -> {
			FileMetadata oldMeta = await(client.info("file"));
			await(Promises.delay(getDelay(oldMeta.getTimestamp())));
			await(client.move("file", "newFile"));
			FileMetadata newMeta = await(client.info("newFile"));

			assertEquals(oldMeta.getSize(), newMeta.getSize());
			assertTrue(newMeta.getTimestamp() > oldMeta.getTimestamp());
		});
		assertFilesAreSame(firstPath, secondPath);
	}

	@Test
	public void moveNotIdempotent() {
		both(client -> {
			// first call completes successfully
			await(client.move("file", "newFile"));
			Exception e = awaitException(client.move("file", "newFile"));
			assertThat(e, instanceOf(FileNotFoundException.class));
		});
		assertFilesAreSame(firstPath, secondPath);
	}
	// endregion

	// region copy
	@Test
	public void regularCopy() {
		both(client -> await(client.copy("file2", "newFile")));
		assertFileEquals(firstPath, secondPath, "file2", "newFile");
		assertFilesAreSame(firstPath, secondPath);
	}

	@Test
	public void copyToNewDirectory() {
		both(client -> await(client.copy("file", "a/b/c/d/newFile")));
		assertFileEquals(firstPath, secondPath, "file", "a/b/c/d/newFile");
		assertFilesAreSame(firstPath, secondPath);
	}

	@Test
	public void copyDirectory() {
		both(client -> assertThat(awaitException(client.copy("directory", "newDirectory")), instanceOf(IsADirectoryException.class)));
		assertFilesAreSame(firstPath, secondPath);
	}

	@Test
	public void copyToExistingDirectoryName() {
		both(client -> assertThat(awaitException(client.copy("file", "directory")), instanceOf(IsADirectoryException.class)));
		assertFilesAreSame(firstPath, secondPath);
	}

	@Test
	public void copyToFileInsideDirectoryAsAFile() {
		both(client -> assertThat(awaitException(client.copy("file2", "file/newFile")), instanceOf(PathContainsFileException.class)));
		assertFilesAreSame(firstPath, secondPath);
	}

	@Test
	public void copyFromFileInsideDirectoryAsAFile() {
		both(client -> assertThat(awaitException(client.copy("file/newFile", "newFile")), instanceOf(FileNotFoundException.class)));
		assertFilesAreSame(firstPath, secondPath);
	}

	@Test
	public void copyNonExistentFile() {
		both(client -> assertThat(awaitException(client.copy("nonexistent", "nonexistentTarget")), instanceOf(FileNotFoundException.class)));
		assertFilesAreSame(firstPath, secondPath);
	}

	@Test
	public void copyNonExistentFileToDirectory() {
		both(client -> assertThat(awaitException(client.copy("nonexistent", "directory")), instanceOf(FileNotFoundException.class)));
		assertFilesAreSame(firstPath, secondPath);
	}

	@Test
	public void copySelf() throws IOException {
		byte[] bytes = Files.readAllBytes(firstPath.resolve("file2"));
		both(client -> await(client.copy("file2", "file2")));
		assertFilesAreSame(firstPath, secondPath);
		assertArrayEquals(bytes, Files.readAllBytes(firstPath.resolve("file2")));
	}

	@Test
	public void copyFromEmptyFilename() {
		Map<String, FileMetadata> before = await(first.list("**"));
		both(client -> assertThat(awaitException(client.copy("", "newFile")), instanceOf(IsADirectoryException.class)));
		both(client -> assertMetadataEquals(before, await(client.list("**"))));
	}

	@Test
	public void copyToEmptyFilename() {
		Map<String, FileMetadata> before = await(first.list("**"));
		both(client -> assertThat(awaitException(client.copy("file", "")), instanceOf(IsADirectoryException.class)));
		both(client -> assertMetadataEquals(before, await(client.list("**"))));
	}

	@Test
	public void copyUpdatesTimestamp() {
		both(fs -> {
			FileMetadata oldMeta = await(fs.info("file"));
			await(Promises.delay(getDelay(oldMeta.getTimestamp())));
			await(fs.copy("file", "newFile"));
			FileMetadata newMeta = await(fs.info("newFile"));

			assertEquals(oldMeta.getSize(), newMeta.getSize());
			assertTrue(newMeta.getTimestamp() > oldMeta.getTimestamp());
		});
		assertFilesAreSame(firstPath, secondPath);
	}

	@Test
	public void copyIsIdempotent() {
		both(client -> {
			await(client.copy("file", "newFile"));
			await(client.copy("file", "newFile"));
		});
		assertFileEquals(firstPath, secondPath, "file", "newFile");
		assertFilesAreSame(firstPath, secondPath);
	}
	// endregion

	// region deleteAll
	@Test
	public void deleteAllEmpty() {
		List<Path> before = listPaths(firstPath);
		both(client -> await(client.deleteAll(emptySet())));

		assertEquals(before, listPaths(firstPath));
		assertFilesAreSame(firstPath, secondPath);
	}

	@Test
	public void deleteAllSingleFile() {
		both(client -> await(client.deleteAll(singleton("file"))));

		bothPaths(path -> assertThat(listPaths(path), not(contains(Paths.get("file")))));
		assertFilesAreSame(firstPath, secondPath);
	}

	@Test
	public void deleteAllMultipleFiles() {
		both(client -> await(client.deleteAll(setOf("file", "file2"))));

		bothPaths(path -> assertThat(listPaths(path), not(contains(Paths.get("file"), Paths.get("file2")))));
		assertFilesAreSame(firstPath, secondPath);
	}

	@Test
	public void deleteAllSingleDirectory() {
		List<Path> before = listPaths(firstPath);
		both(client -> {
			Exception exception = awaitException(client.deleteAll(singleton("directory")));
			assertBatchException(exception, mapOf("directory", IsADirectoryException.class));
		});

		assertEquals(before, listPaths(firstPath));
		assertFilesAreSame(firstPath, secondPath);
	}

	@Test
	public void deleteAllMultipleDirectories() {
		List<Path> before = listPaths(firstPath);
		both(client -> {
			Exception exception = awaitException(client.deleteAll(setOf("directory", "directory2")));
			assertBatchException(exception, mapOf("directory", IsADirectoryException.class));
		});

		assertEquals(before, listPaths(firstPath));
		assertFilesAreSame(firstPath, secondPath);
	}

	@Test
	public void deleteAllFilesAndDirectories() {
		both(client -> {
			Exception exception = awaitException(client.deleteAll(setOf("file", "directory")));
			assertBatchException(exception, mapOf("directory", IsADirectoryException.class));
		});
		assertFilesAreSame(firstPath, secondPath);
	}

	@Test
	public void deleteAllWithNonExisting() {
		both(client -> await(client.deleteAll(setOf("file", "nonexistent"))));

		bothPaths(path -> assertThat(listPaths(path), not(contains(Paths.get("file")))));
		assertFilesAreSame(firstPath, secondPath);
	}

	@Test
	public void deleteAllWithRoot() {
		both(client -> await(client.deleteAll(setOf("file", ""))));
		assertFilesAreSame(firstPath, secondPath);
	}

	@Test
	public void deleteAllWithFileOutsideRoot() {
		both(client -> {
			Exception exception = awaitException(client.deleteAll(setOf("file", "..")));
			assertBatchException(exception, mapOf("..", ForbiddenPathException.class));
		});
		assertFilesAreSame(firstPath, secondPath);
	}

	@Test
	public void deleteAllIsIdempotent() {
		both(client -> {
			await(client.deleteAll(setOf("file", "file2")));
			await(client.deleteAll(setOf("file", "file2")));
			await(client.deleteAll(setOf("file", "file2")));
			await(client.deleteAll(setOf("file", "file2")));
		});

		bothPaths(path -> assertThat(listPaths(path), not(contains(Paths.get("file"), Paths.get("file2")))));
		assertFilesAreSame(firstPath, secondPath);
	}
	//endregion

	// region copyAll
	@Test
	public void copyAllEmpty() {
		List<Path> before = listPaths(firstPath);
		both(client -> await(client.copyAll(emptyMap())));

		assertEquals(before, listPaths(firstPath));
		assertFilesAreSame(firstPath, secondPath);
	}

	@Test
	public void copyAllSingleFile() {
		both(client -> await(client.copyAll(mapOf("file", "newFile"))));

		assertFileEquals(firstPath, secondPath, "file", "newFile");
		assertFilesAreSame(firstPath, secondPath);
	}

	@Test
	public void copyAllMultipleFiles() {
		both(client -> await(client.copyAll(mapOf(
				"file", "newFile",
				"file2", "newFile2"
		))));

		assertFileEquals(firstPath, secondPath, "file", "newFile");
		assertFileEquals(firstPath, secondPath, "file2", "newFile2");
		assertFilesAreSame(firstPath, secondPath);
	}

	@Test
	public void copyAllFromSingleDirectory() {
		List<Path> before = listPaths(firstPath);
		both(client -> {
			Exception exception = awaitException(client.copyAll(mapOf("directory", "newFile")));
			assertBatchException(exception, mapOf("directory", IsADirectoryException.class));
		});

		assertEquals(before, listPaths(firstPath));
		assertFilesAreSame(firstPath, secondPath);
	}

	@Test
	public void copyAllToSingleDirectory() {
		List<Path> before = listPaths(firstPath);
		both(client -> {
			Exception exception = awaitException(client.copyAll(mapOf("file", "directory")));
			assertBatchException(exception, mapOf("file", IsADirectoryException.class));
		});

		assertEquals(before, listPaths(firstPath));
		assertFilesAreSame(firstPath, secondPath);
	}

	@Test
	public void copyAllMultipleDirectories() {
		List<Path> before = listPaths(firstPath);
		both(client -> {
			Exception exception = awaitException(
					client.copyAll(mapOf(
							"directory", "newDirectory",
							"directory2", "newDirectory2"
					)));
			assertBatchException(exception, mapOf("directory", IsADirectoryException.class));
		});

		assertEquals(before, listPaths(firstPath));
		assertFilesAreSame(firstPath, secondPath);
	}

	@Test
	public void copyAllFilesAndDirectories() {
		both(client -> {
			Exception exception = awaitException(
					client.copyAll(mapOf(
							"file", "newFile",
							"directory", "newDirectory"
					)));
			assertBatchException(exception, mapOf("directory", IsADirectoryException.class));
		});
		assertFilesAreSame(firstPath, secondPath);
	}

	@Test
	public void copyAllWithFromNonExisting() {
		both(client -> {
			Exception exception = awaitException(
					client.copyAll(mapOf(
							"file", "newFile",
							"nonexistent", "newFile2"
					)));
			assertBatchException(exception, mapOf("nonexistent", FileNotFoundException.class));
		});

		assertFilesAreSame(firstPath, secondPath);
	}

	@Test
	public void copyAllFromRoot() {
		both(client -> {
			Exception exception = awaitException(
					client.copyAll(mapOf(
							"file", "newFile",
							"", "newRoot"
					)));
			assertBatchException(exception, mapOf("", IsADirectoryException.class));
		});

		assertFilesAreSame(firstPath, secondPath);
	}

	@Test
	public void copyAllToRoot() {
		both(client -> {
			Exception exception = awaitException(
					client.copyAll(mapOf(
							"file", "newFile",
							"file2", ""
					)));
			assertBatchException(exception, mapOf("file2", IsADirectoryException.class));
		});
		assertFilesAreSame(firstPath, secondPath);
	}

	@Test
	public void copyAllToFileOutsideRoot() {
		both(client -> {
			Exception exception = awaitException(
					client.copyAll(mapOf(
							"file", "newFile",
							"file2", "../new"
					)));
			assertBatchException(exception, mapOf("file2", ForbiddenPathException.class));
		});
		assertFilesAreSame(firstPath, secondPath);
	}

	@Test
	public void copyAllFromFileOutsideRoot() {
		both(client -> {
			Exception exception = awaitException(
					client.copyAll(mapOf(
							"file", "newFile",
							"../new", "newFile2"
					)));
			assertBatchException(exception, mapOf("../new", ForbiddenPathException.class));
		});
		assertFilesAreSame(firstPath, secondPath);
	}

	@Test
	public void copyAllIsIdempotent() {
		both(client -> {
			await(client.copyAll(mapOf("file", "newFile", "file2", "newFile2")));
			await(client.copyAll(mapOf("file", "newFile", "file2", "newFile2")));
		});

		assertFileEquals(firstPath, secondPath, "file", "newFile");
		assertFileEquals(firstPath, secondPath, "file2", "newFile2");
		assertFilesAreSame(firstPath, secondPath);
	}

	@Test
	public void copyAllUpdatesTimestamps() {
		both(client -> {
			FileMetadata oldMeta1 = await(client.info("file"));
			FileMetadata oldMeta2 = await(client.info("file2"));

			await(Promises.delay(getDelay(oldMeta1.getTimestamp())));

			await(client.copyAll(mapOf(
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

		assertFilesAreSame(firstPath, secondPath);
	}
	//endregion

	// region moveAll
	@Test
	public void moveAllEmpty() {
		List<Path> before = listPaths(firstPath);
		both(client -> await(client.moveAll(emptyMap())));

		assertEquals(before, listPaths(firstPath));
		assertFilesAreSame(firstPath, secondPath);
	}

	@Test
	public void moveAllSingleFile() throws IOException {
		byte[] bytesBefore = Files.readAllBytes(firstPath.resolve("file"));

		both(client -> await(client.moveAll(mapOf("file", "newFile"))));

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

		both(client -> await(client.moveAll(mapOf(
				"file", "newFile",
				"file2", "newFile2"
		))));

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
		both(client -> {
			Exception exception = awaitException(client.moveAll(mapOf("directory", "newFile")));
			assertBatchException(exception, mapOf("directory", IsADirectoryException.class));
		});

		assertEquals(before, listPaths(firstPath));
		assertFilesAreSame(firstPath, secondPath);
	}

	@Test
	public void moveAllToSingleDirectory() {
		List<Path> before = listPaths(firstPath);
		both(client -> {
			Exception exception = awaitException(client.moveAll(mapOf("file", "directory")));
			assertBatchException(exception, mapOf("file", IsADirectoryException.class));
		});

		assertEquals(before, listPaths(firstPath));
		assertFilesAreSame(firstPath, secondPath);
	}

	@Test
	public void moveAllMultipleDirectories() {
		List<Path> before = listPaths(firstPath);
		both(client -> {
			Exception exception = awaitException(
					client.moveAll(mapOf(
							"directory", "newDirectory",
							"directory2", "newDirectory2"
					)));
			assertBatchException(exception, mapOf("directory", IsADirectoryException.class));
		});

		assertEquals(before, listPaths(firstPath));
		assertFilesAreSame(firstPath, secondPath);
	}

	@Test
	public void moveAllFilesAndDirectories() {
		both(client -> {
			Exception exception = awaitException(
					client.moveAll(mapOf(
							"file", "newFile",
							"directory", "newDirectory"
					)));
			assertBatchException(exception, mapOf("directory", IsADirectoryException.class));
		});
	}

	@Test
	public void moveAllWithFromNonExisting() {
		both(client -> {
			Exception exception = awaitException(
					client.moveAll(mapOf(
							"file", "newFile",
							"nonexistent", "newFile2"
					)));
			assertBatchException(exception, mapOf("nonexistent", FileNotFoundException.class));
		});
	}

	@Test
	public void moveAllFromRoot() {
		both(client -> {
			Exception exception = awaitException(
					client.moveAll(mapOf(
							"file", "newFile",
							"", "newRoot"
					)));
			assertBatchException(exception, mapOf("", IsADirectoryException.class));
		});
	}

	@Test
	public void moveAllToRoot() {
		both(client -> {
			Exception exception = awaitException(
					client.moveAll(mapOf(
							"file", "newFile",
							"file2", ""
					)));
			assertBatchException(exception, mapOf("file2", IsADirectoryException.class));
		});
	}

	@Test
	public void moveAllToFileOutsideRoot() {
		both(client -> {
			Exception exception = awaitException(
					client.moveAll(mapOf(
							"file", "newFile",
							"file2", "../new"
					)));
			assertBatchException(exception, mapOf("file2", ForbiddenPathException.class));
		});
	}

	@Test
	public void moveAllFromFileOutsideRoot() {
		both(client -> {
			Exception exception = awaitException(
					client.moveAll(mapOf(
							"file", "newFile",
							"../new", "newFile2"
					)));
			assertBatchException(exception, mapOf("../new", ForbiddenPathException.class));
		});
	}

	@Test
	public void moveAllNotIdempotent() {
		both(client -> {
			await(client.moveAll(mapOf("file", "newFile", "file2", "newFile2")));
			Exception exception = awaitException(
					client.moveAll(mapOf(
							"file", "newFile",
							"file2", "newFile2"
					)));
			assertBatchException(exception, mapOf("file", FileNotFoundException.class));
		});
		assertFilesAreSame(firstPath, secondPath);
	}

	@Test
	public void moveAllUpdatesTimestamps() {
		both(client -> {
			FileMetadata oldMeta1 = await(client.info("file"));
			FileMetadata oldMeta2 = await(client.info("file2"));

			await(Promises.delay(getDelay(oldMeta1.getTimestamp())));

			await(client.moveAll(mapOf(
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
		assertFilesAreSame(firstPath, secondPath);
	}

	@Test
	public void moveAllWithSelfExistent() throws IOException {
		byte[] bytesBefore1 = Files.readAllBytes(firstPath.resolve("file"));
		byte[] bytesBefore2 = Files.readAllBytes(firstPath.resolve("file2"));

		both(client -> await(client.moveAll(mapOf(
				"file", "file",
				"file2", "newFile2"
		))));

		bothPaths(path -> {
			assertThat(listPaths(path), not(contains(Paths.get("file2"))));
			assertArrayEquals(bytesBefore1, Files.readAllBytes(path.resolve("file")));
			assertArrayEquals(bytesBefore2, Files.readAllBytes(path.resolve("newFile2")));
		});
		assertFilesAreSame(firstPath, secondPath);
	}

	@Test
	public void moveAllWithSelfNonExistent() {
		both(client -> {
			Exception exception = awaitException(
					client.moveAll(mapOf(
							"file", "newFile",
							"nonexistent", "nonexistent"
					)));
			assertBatchException(exception, mapOf("nonexistent", FileNotFoundException.class));
		});
	}

	@Test
	public void moveAllWithSelfDirectory() {
		both(client -> {
			Exception exception = awaitException(
					client.moveAll(mapOf(
							"file", "newFile",
							"directory", "directory"
					)));
			assertBatchException(exception, mapOf("directory", IsADirectoryException.class));
		});
	}
	//endregion

	// region infoAll
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
			Map<String, FileMetadata> result = await(client.infoAll(setOf("file", "file2")));
			assertEquals(2, result.size());
			assertEquals(setOf("file", "file2"), result.keySet());
		});
	}

	@Test
	public void infoAllMultipleWithMissing() {
		both(client -> {
			Map<String, FileMetadata> result = await(client.infoAll(setOf("file", "nonexistent")));
			assertEquals(1, result.size());
			assertEquals("file", first(result.keySet()));
		});
	}

	@Test
	public void infoAllWithAllMissing() {
		both(client -> {
			Map<String, FileMetadata> result = await(client.infoAll(setOf("nonexistent", "nonexistent2")));
			assertEquals(0, result.size());
		});
	}
	// endregion

	// Default methods are not overridden
	private static class DefaultActiveFs implements ActiveFs {
		private final ActiveFs peer;

		private DefaultActiveFs(ActiveFs peer) {
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

	private void both(Consumer<ActiveFs> fsConsumer) {
		fsConsumer.accept(first);
		fsConsumer.accept(second);
	}

	private void bothPaths(ConsumerEx<Path> consumer) {
		Utils.bothPaths(firstPath, secondPath, consumer);
	}
}
