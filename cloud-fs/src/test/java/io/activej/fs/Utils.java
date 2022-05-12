package io.activej.fs;

import io.activej.common.function.ConsumerEx;
import io.activej.fs.exception.FsBatchException;
import io.activej.fs.exception.FsScalarException;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.BiConsumer;
import java.util.stream.Stream;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.*;

public final class Utils {

	public static void assertBatchException(@NotNull Exception e, String name, Class<? extends FsScalarException> exceptionClass) {
		assertBatchException(e, Map.of(name, exceptionClass));
	}

	public static void assertBatchException(@NotNull Exception e, Map<String, Class<? extends FsScalarException>> exceptionClasses) {
		assertThat(e, instanceOf(FsBatchException.class));
		FsBatchException batchEx = (FsBatchException) e;

		Map<String, FsScalarException> exceptions = batchEx.getExceptions();

		for (Map.Entry<String, Class<? extends FsScalarException>> entry : exceptionClasses.entrySet()) {
			assertThat(exceptions.get(entry.getKey()), instanceOf(entry.getValue()));
		}
	}

	public static void assertBatchException(@NotNull Exception e, int minExpected, int maxExpected, BiConsumer<String, FsScalarException> exceptionAssertFn) {
		assertThat(e, instanceOf(FsBatchException.class));
		FsBatchException batchEx = (FsBatchException) e;

		Map<String, FsScalarException> exceptions = batchEx.getExceptions();
		assertTrue(exceptions.size() >= minExpected);
		assertTrue(exceptions.size() <= maxExpected);

		for (Map.Entry<String, FsScalarException> entry : exceptions.entrySet()) {
			exceptionAssertFn.accept(entry.getKey(), entry.getValue());
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
		return new String(inputStream.readAllBytes(), UTF_8);
	}

	public static void assertFileEquals(Path firstPath, Path secondPath, String first, String second) {
		try {
			assertEquals(-1, Files.mismatch(firstPath.resolve(first), firstPath.resolve(second)));
			assertEquals(-1, Files.mismatch(secondPath.resolve(first), secondPath.resolve(second)));
		} catch (IOException e) {
			throw new AssertionError(e);
		}
	}

	public static void assertFilesAreSame(Path firstPath, Path secondPath) {
		assertFilesAreSame(firstPath, secondPath, Set.of());
	}

	public static void assertFilesAreSame(Path firstPath, Path secondPath, Set<String> except) {
		try {
			List<Path> firstFiles = listFiles(firstPath, except);
			List<Path> secondFiles = listFiles(secondPath, except);
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
		return listPaths(directoryPath, Set.of());
	}

	public static List<Path> listPaths(Path directoryPath, Set<String> except) {
		List<Path> paths = new ArrayList<>();
		list(directoryPath, paths, except, true);
		return paths.stream()
				.map(directoryPath::relativize)
				.collect(toList());
	}

	public static void assertMetadataEquals(Map<String, FileMetadata> expected, Map<String, FileMetadata> actual) {
		assertEquals(removeTimestamp(expected), removeTimestamp(actual));
	}

	public static void bothPaths(Path firstPath, Path secondPath, ConsumerEx<Path> pathConsumer) {
		try {
			pathConsumer.accept(firstPath);
			pathConsumer.accept(secondPath);
		} catch (Exception e) {
			throw new AssertionError(e);
		}
	}

	private static List<Path> listFiles(Path directoryPath, Set<String> except) {
		List<Path> list = new ArrayList<>();
		list(directoryPath, list, except, false);
		return list;
	}

	private static void list(Path directoryPath, List<Path> paths, Set<String> except, boolean includeDirs) {
		Set<Path> exceptPaths = except.stream()
				.map(directoryPath::resolve)
				.collect(toSet());

		try {
			List<Path> subPaths;
			try (Stream<Path> list = Files.list(directoryPath)) {
				subPaths = list.sorted().collect(toList());
			}
			for (Path path : subPaths) {
				if (exceptPaths.contains(path)) continue;
				if (Files.isRegularFile(path)) {
					paths.add(path);
				} else if (Files.isDirectory(path)) {
					if (includeDirs) paths.add(path);
					list(path, paths, except, includeDirs);
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
