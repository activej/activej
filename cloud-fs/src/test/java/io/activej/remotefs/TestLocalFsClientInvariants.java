package io.activej.remotefs;

import io.activej.bytebuf.ByteBuf;
import io.activej.csp.ChannelConsumer;
import io.activej.csp.ChannelSupplier;
import io.activej.promise.Promise;
import io.activej.test.rules.ByteBufRule;
import io.activej.test.rules.EventloopRule;
import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import static io.activej.eventloop.Eventloop.getCurrentEventloop;
import static io.activej.promise.TestUtils.await;
import static io.activej.promise.TestUtils.awaitException;
import static io.activej.remotefs.FsClient.*;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static java.util.stream.Collectors.toList;
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
		 * ........directory/file.txt
		 * ........directory2/file2.txt
		 */
		firstPath = tmpFolder.newFolder("first").toPath();
		secondPath = tmpFolder.newFolder("second").toPath();

		Files.createDirectories(firstPath);
		Files.createDirectories(secondPath);

		Path file1 = firstPath.resolve("file");
		Files.write(file1, "This is contents of the first file".getBytes(UTF_8), CREATE, TRUNCATE_EXISTING);
		Files.copy(file1, secondPath.resolve("file"));

		Path file2 = firstPath.resolve("file2");
		Files.write(file2, "This is contents of the first file2".getBytes(UTF_8), CREATE, TRUNCATE_EXISTING);
		Files.copy(file2, secondPath.resolve("file2"));

		Path file3 = firstPath.resolve("directory/file.txt");
		Files.createDirectories(file3.getParent());
		Files.write(file3, "This is contents of file in directory/file.txt".getBytes(UTF_8), CREATE, TRUNCATE_EXISTING);
		Path secondFile3 = secondPath.resolve("directory/file.txt");
		Files.createDirectories(secondFile3.getParent());
		Files.copy(file3, secondFile3);

		Path file4 = firstPath.resolve("directory2/file2.txt");
		Files.createDirectories(file4.getParent());
		Files.write(file4, "This is contents of file in directory2/file2.txt".getBytes(UTF_8), CREATE, TRUNCATE_EXISTING);
		Path secondFile4 = secondPath.resolve("directory2/file2.txt");
		Files.createDirectories(secondFile4.getParent());
		Files.copy(file4, secondFile4);

		first = LocalFsClient.create(getCurrentEventloop(), newSingleThreadExecutor(), firstPath);
		second = new DefaultFsClient(LocalFsClient.create(getCurrentEventloop(), newSingleThreadExecutor(), secondPath));
	}

	// region copy
	@Test
	public void regularCopy() {
		copy("file2", "newFile");
		assertFileEquals("file2", "newFile");
		assertFilesAreSame();
	}

	@Test
	public void copyToNewDirectory() {
		copy("file", "a/b/c/d/newFile");
		assertFileEquals("file", "a/b/c/d/newFile");
		assertFilesAreSame();
	}

	@Test
	public void copyDirectory() {
		copy("directory", "newDirectory", IS_DIRECTORY);
		assertFilesAreSame();
	}

	@Test
	public void copyToExistingDirectoryName() {
		copy("file", "directory", IS_DIRECTORY);
		assertFilesAreSame();
	}

	@Test
	public void copyToFileInsideDirectoryAsAFile() {
		copy("file2", "file/newFile", FILE_EXISTS);
		assertFilesAreSame();
	}

	@Test
	public void copyNonExistentFile() {
		copy("nonexistent", "nonexistentTarget", FILE_NOT_FOUND);
		assertFilesAreSame();
	}

	@Test
	public void copyNonExistentFileToDirectory() {
		copy("nonexistent", "directory", FILE_NOT_FOUND);
		assertFilesAreSame();
	}

	@Test
	public void copySelf() throws IOException {
		byte[] bytes = Files.readAllBytes(firstPath.resolve("file2"));
		copy("file2", "file2");
		assertFilesAreSame();
		assertArrayEquals(bytes, Files.readAllBytes(firstPath.resolve("file2")));
	}
	// endregion

	// region move
	@Test
	public void regularMove() {
		move("file", "newFile");
		assertFilesAreSame();
	}

	@Test
	public void moveToNewDirectory() {
		move("file", "a/b/c/d/newFile");
		assertFilesAreSame();
	}

	@Test
	public void moveDirectory() {
		move("directory", "newDirectory", IS_DIRECTORY);
		assertFilesAreSame();
	}

	@Test
	public void moveToExistingDirectoryName() {
		move("file", "directory", IS_DIRECTORY);
		assertFilesAreSame();
	}

	@Test
	public void moveToFileInsideDirectoryAsAFile() {
		move("file2", "file/newFile", FILE_EXISTS);
		assertFilesAreSame();
	}

	@Test
	public void moveNonExistentFile() {
		move("nonexistent", "nonexistentTarget", FILE_NOT_FOUND);
		assertFilesAreSame();
	}

	@Test
	public void moveNonExistentFileToDirectory() {
		move("nonexistent", "directory", FILE_NOT_FOUND);
		assertFilesAreSame();
	}

	@Test
	public void moveSelf() throws IOException {
		byte[] bytes = Files.readAllBytes(firstPath.resolve("file2"));
		move("file2", "file2");
		assertFilesAreSame();
		assertArrayEquals(bytes, Files.readAllBytes(firstPath.resolve("file2")));
	}
	// endregion

	// region moveDir
	@Test
	public void regularMoveDir() {
		List<FileMetadata> filesBefore = await(first.list("directory/**"));
		assertFalse(filesBefore.isEmpty());

		moveDir("directory", "newDirectory");

		both(client -> assertTrue(await(client.list("**")).stream().noneMatch(meta -> meta.getName().contains("directory/"))));
		both(client -> assertMetadataEquals(filesBefore, await(client.list("newDirectory/**")), 1));

		assertFilesAreSame();
	}

	@Test
	public void moveNonExistingDir() {
		moveDir("nonexistent", "newDirectory");
		assertFilesAreSame();

		both(client -> assertTrue(await(client.list("**")).stream()
				.map(FileMetadata::getName)
				.noneMatch(name -> name.contains("nonexistent") || name.contains("newDirectory"))));
	}

	@Test
	public void moveNonExistingDirToExisting() {
		List<FileMetadata> before = await(first.list("directory2/**"));

		moveDir("nonexistent", "directory2");
		assertFilesAreSame();

		both(client -> assertTrue(await(client.list("**")).stream()
				.map(FileMetadata::getName)
				.noneMatch(name -> name.contains("nonexistent"))));

		both(client -> assertMetadataEquals(before, await(client.list("directory2/**")), 0));
	}

	@Test
	public void moveToNewSubDir() {
		List<FileMetadata> before = await(first.list("directory/**"));

		moveDir("directory", "path/to/new/directory");
		assertFilesAreSame();

		both(client -> assertTrue(await(client.list("**")).stream()
				.map(FileMetadata::getName)
				.noneMatch(name -> name.startsWith("directory" + File.separator))));

		both(client -> assertMetadataEquals(before, await(client.list("path/to/new/directory/**")), 1, 4));
	}

	@Test
	public void moveToExistingDir() {
		List<FileMetadata> before = await(first.list("directory/**"));

		moveDir("directory", "directory2");
		assertFilesAreSame();

		both(client -> assertTrue(await(client.list("**")).stream()
				.map(FileMetadata::getName)
				.noneMatch(name -> name.startsWith("directory" + File.separator))));

		both(client -> {
			List<FileMetadata> after = await(client.list("directory2/**"));
			assertTrue(after.size() > before.size());
			assertTrue(strip(after, 1).containsAll(strip(before, 1)));
		});
	}

	@Test
	public void moveFileAsDir() {
		moveDir("file", "newDirectory");
		assertFilesAreSame();
	}

	@Test
	public void moveDirToFile() {
		moveDir("directory", "file", FILE_EXISTS);
		assertFilesAreSame();
	}

	@Test
	public void moveDirSelf() {
		moveDir("directory", "directory");
		assertFilesAreSame();
	}
	// endregion

	// region helpers
	private void copy(String from, String to) {
		await(first.copy(from, to));
		await(second.copy(from, to));
	}

	private void copy(String from, String to, Exception expected) {
		Throwable firstException = awaitException(first.copy(from, to));
		Throwable secondException = awaitException(second.copy(from, to));

		assertEquals(firstException, secondException);
		assertEquals(expected, firstException);
	}

	private void move(String from, String to) {
		await(first.move(from, to));
		await(second.move(from, to));
	}

	private void move(String from, String to, Exception expected) {
		Throwable firstException = awaitException(first.move(from, to));
		Throwable secondException = awaitException(second.move(from, to));

		assertEquals(firstException, secondException);
		assertEquals(expected, firstException);
	}

	private void moveDir(String from, String to) {
		await(first.moveDir(from, to));
		await(second.moveDir(from, to));
	}

	private void moveDir(String from, String to, Exception expected) {
		Throwable firstException = awaitException(first.moveDir(from, to));
		Throwable secondException = awaitException(second.moveDir(from, to));

		assertEquals(firstException, secondException);
		assertEquals(expected, firstException);
	}

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

	private List<Path> listFiles(Path directoryPath) throws IOException {
		List<Path> list = new ArrayList<>();
		listFiles(directoryPath, list);
		return list;
	}

	private void listFiles(Path directoryPath, List<Path> files) throws IOException {
		List<Path> paths = Files.list(directoryPath).collect(toList());
		for (Path path : paths) {
			if (Files.isRegularFile(path)) {
				files.add(path);
			} else if (Files.isDirectory(path)) {
				listFiles(path, files);
			} else {
				throw new AssertionError();
			}
		}
	}

	private void assertMetadataEquals(List<FileMetadata> expected, List<FileMetadata> actual, int ignoreLevel) {
		assertMetadataEquals(expected, actual, ignoreLevel, ignoreLevel);
	}

	private void assertMetadataEquals(List<FileMetadata> expected, List<FileMetadata> actual, int ignoreFirst, int ignoreSecond) {
		assertEquals(strip(expected, ignoreFirst), strip(actual, ignoreSecond));
	}

	private List<FileMetadata> strip(List<FileMetadata> list, int level) {
		return list.stream()
				.map(meta -> {
					String name = meta.getName();
					for (int i = 0; i < level; i++) {
						int index = name.indexOf(File.separatorChar);
						if (index == -1) throw new AssertionError();
						name = name.substring(index + 1);
					}
					return FileMetadata.of(name, meta.getSize(), 0);
				})
				.collect(toList());
	}

	private void both(Consumer<FsClient> clientConsumer) {
		clientConsumer.accept(first);
		clientConsumer.accept(second);
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
		public Promise<ChannelSupplier<ByteBuf>> download(@NotNull String name, long offset, long length) {
			return peer.download(name, offset, length);
		}

		@Override
		public Promise<Void> delete(@NotNull String name) {
			return peer.delete(name);
		}

		@Override
		public Promise<List<FileMetadata>> list(@NotNull String glob) {
			return peer.list(glob);
		}
	}
}
