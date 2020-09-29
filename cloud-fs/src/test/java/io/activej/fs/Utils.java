package io.activej.fs;

import io.activej.fs.exception.FsBatchException;
import io.activej.fs.exception.FsScalarException;
import io.activej.test.TestUtils.ThrowingConsumer;
import org.jetbrains.annotations.NotNull;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import static io.activej.common.collection.CollectionUtils.map;
import static io.activej.fs.LocalActiveFs.DEFAULT_TEMP_DIR;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.*;

public final class Utils {

	public static void initTempDir(Path storage) {
		try {
			Files.createDirectories(storage.resolve(DEFAULT_TEMP_DIR));
		} catch (IOException e) {
			throw new AssertionError(e);
		}
	}

	public static void assertBatchException(@NotNull Throwable e, String name, Class<? extends FsScalarException> exceptionClass) {
		assertBatchException(e, map(name, exceptionClass));
	}

	public static void assertBatchException(@NotNull Throwable e, Map<String, Class<? extends FsScalarException>> exceptionClasses) {
		assertThat(e, instanceOf(FsBatchException.class));
		FsBatchException batchEx = (FsBatchException) e;

		Map<String, FsScalarException> exceptions = batchEx.getExceptions();

		for (Map.Entry<String, Class<? extends FsScalarException>> entry : exceptionClasses.entrySet()) {
			assertThat(exceptions.get(entry.getKey()), instanceOf(entry.getValue()));
		}
	}

	public static List<Path> createEmptyDirectories(Path storagePath) {
		List<Path> result = new ArrayList<>();
		Path root = storagePath.resolve(Paths.get("empty"));
		result.add(root);
		ThreadLocalRandom random = ThreadLocalRandom.current();
		for (int i1 = 0; i1 < random.nextInt(10); i1++) {
			Path empty1 = root.resolve(Paths.get("empty_" + i1));
			result.add(empty1);
			for (int i2 = 0; i2 < random.nextInt(10); i2++) {
				Path empty2 = empty1.resolve(Paths.get("empty_" + i2));
				result.add(empty2);
				for (int i3 = 0; i3 < random.nextInt(10); i3++) {
					Path empty3 = empty2.resolve(Paths.get("empty_" + i3));
					result.add(empty3);
				}
			}
		}
		for (Path path : result) {
			try {
				Files.createDirectories(path);
			} catch (IOException e) {
				throw new AssertionError(e);
			}
		}
		return result;
	}

	public static String asString(InputStream inputStream) throws IOException {
		try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
			LocalFileUtils.copy(inputStream, outputStream);
			return outputStream.toString();
		}
	}

	// In older JDK versions Files::getLastModifiedTime returns time in SECONDS resolution
	public static long getDelay(long timestamp) {
		return timestamp % 1000 == 0 ? 1000 : 10;
	}

	public static void assertFileEquals(Path firstPath, Path secondPath, String first, String second) {
		try {
			assertArrayEquals(Files.readAllBytes(firstPath.resolve(first)), Files.readAllBytes(firstPath.resolve(second)));
			assertArrayEquals(Files.readAllBytes(secondPath.resolve(first)), Files.readAllBytes(secondPath.resolve(second)));
		} catch (IOException e) {
			throw new AssertionError(e);
		}
	}

	public static void assertFilesAreSame(Path firstPath, Path secondPath) {
		try {
			List<Path> firstFiles = listFiles(firstPath);
			List<Path> secondFiles = listFiles(secondPath);
			assertEquals(firstFiles.size(), secondFiles.size());

			for (int i = 0; i < firstFiles.size(); i++) {
				Path fPath = firstFiles.get(i);
				if (Files.isDirectory(fPath)) continue;
				byte[] firstFileBytes = Files.readAllBytes(fPath);
				byte[] secondFileBytes = Files.readAllBytes(secondFiles.get(i));
				assertArrayEquals(firstFileBytes, secondFileBytes);
			}
		} catch (IOException e) {
			throw new AssertionError(e);
		}
	}

	public static List<Path> listPaths(Path directoryPath) {
		List<Path> paths = new ArrayList<>();
		list(directoryPath, paths, true);
		return paths.stream()
				.map(directoryPath::relativize)
				.collect(toList());
	}

	public static void assertMetadataEquals(Map<String, FileMetadata> expected, Map<String, FileMetadata> actual) {
		assertEquals(removeTimestamp(expected), removeTimestamp(actual));
	}

	public static void bothPaths(Path firstPath, Path secondPath, ThrowingConsumer<Path> pathConsumer) {
		try {
			pathConsumer.accept(firstPath);
			pathConsumer.accept(secondPath);
		} catch (Throwable e) {
			throw new AssertionError(e);
		}
	}

	private static List<Path> listFiles(Path directoryPath) {
		List<Path> list = new ArrayList<>();
		list(directoryPath, list, false);
		return list;
	}

	private static void list(Path directoryPath, List<Path> paths, boolean includeDirs) {
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

	private static Map<String, FileMetadata> removeTimestamp(Map<String, FileMetadata> map) {
		return map.entrySet().stream()
				.collect(toMap(Map.Entry::getKey, e -> FileMetadata.of(e.getValue().getSize(), 0)));
	}

}
