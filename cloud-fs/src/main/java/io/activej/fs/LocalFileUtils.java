/*
 * Copyright (C) 2020 ActiveJ LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.activej.fs;

import io.activej.common.time.CurrentTimeProvider;
import io.activej.fs.exception.FileSystemStructureException;
import io.activej.fs.exception.ForbiddenPathException;
import io.activej.fs.exception.GlobException;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
import java.util.regex.PatternSyntaxException;

import static io.activej.fs.IFileSystem.SEPARATOR;
import static io.activej.fs.util.RemoteFileSystemUtils.isWildcard;
import static java.nio.file.FileVisitResult.CONTINUE;
import static java.nio.file.FileVisitResult.SKIP_SUBTREE;

public final class LocalFileUtils {
	private static final Logger logger = LoggerFactory.getLogger(LocalFileUtils.class);

	static final char SEPARATOR_CHAR = SEPARATOR.charAt(0);
	static final Function<String, String> TO_LOCAL_NAME = File.separatorChar == SEPARATOR_CHAR ?
			Function.identity() :
			s -> s.replace(SEPARATOR_CHAR, File.separatorChar);

	static final Function<String, String> TO_REMOTE_NAME = File.separatorChar == SEPARATOR_CHAR ?
			Function.identity() :
			s -> s.replace(File.separatorChar, SEPARATOR_CHAR);

	static void init(Path storage, Path tempDir, boolean fsyncDirectories) throws IOException {
		createDirectories(tempDir, fsyncDirectories);
		if (!tempDir.startsWith(storage)) {
			createDirectories(storage, fsyncDirectories);
		}
	}

	static Path resolve(Path storage, Path tempDir, String name) throws ForbiddenPathException {
		Path path = storage.resolve(name).normalize();
		if (!path.startsWith(storage) || path.startsWith(tempDir)) {
			throw new ForbiddenPathException("Path '" + name + "' is forbidden");
		}
		return path;
	}

	static void touch(Path path, CurrentTimeProvider timeProvider) throws IOException {
		Files.setLastModifiedTime(path, FileTime.fromMillis(timeProvider.currentTimeMillis()));
	}

	static void tryDelete(Path target) throws IOException {
		Files.walkFileTree(target, new SimpleFileVisitor<>() {
			@Override
			public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
				if (target.equals(file)) {
					Files.deleteIfExists(file);
					return CONTINUE;
				}
				throw new DirectoryNotEmptyException(target.toString());
			}

			@Override
			public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
				Files.deleteIfExists(dir);
				return CONTINUE;
			}
		});
	}

	static <V> V ensureTarget(@Nullable Path source, Path target, boolean fsyncDirectories, IOCallable<V> afterCreation) throws IOException {
		Path parent = target.getParent();
		while (true) {
			try {
				return afterCreation.call();
			} catch (NoSuchFileException e) {
				if (source != null && !Files.exists(source)) {
					throw e;
				}
				createDirectories(parent, fsyncDirectories);
			} catch (FileSystemException e) {
				if (source != null) {
					if (!Files.exists(source)) throw new NoSuchFileException(null);
					if (Files.isDirectory(source)) throw new DirectoryNotEmptyException(source.toString());
				}
				LocalFileUtils.tryDelete(target);
			}
		}
	}

	static void moveViaHardlink(Path from, Path to, CurrentTimeProvider timeProvider) throws IOException {
		Files.createLink(to, from);
		touch(to, timeProvider);
		Files.deleteIfExists(from);
	}

	static void copyViaHardlink(Path from, Path to, CurrentTimeProvider timeProvider) throws IOException {
		Files.createLink(to, from);
		touch(to, timeProvider);
	}

	static void copyViaTempDir(Path from, Path to, CurrentTimeProvider timeProvider, Path tempDir) throws IOException {
		while (true) {
			Path tempFile = tempDir.resolve("copy" + ThreadLocalRandom.current().nextLong());
			try {
				Files.copy(from, tempFile);
			} catch (NoSuchFileException e) {
				if (Files.exists(tempDir)) {
					throw e;
				}
				throw new FileSystemStructureException("Temporary directory " + tempDir + " not found");
			} catch (FileAlreadyExistsException ignored) {
				continue;
			}
			try {
				moveViaHardlink(tempFile, to, timeProvider);
			} finally {
				Files.deleteIfExists(tempFile);
			}
			return;
		}
	}

	static @Nullable FileMetadata toFileMetadata(Path path) throws IOException {
		if (!Files.isRegularFile(path)) return null;

		long size = Files.size(path);
		long timestamp = Files.getLastModifiedTime(path).toMillis();
		return FileMetadata.of(size, timestamp);
	}

	@SuppressWarnings("StringConcatenationInsideStringBufferAppend")
	static String extractSubDir(String glob) {
		StringBuilder sb = new StringBuilder();
		String[] split = glob.split(SEPARATOR);
		for (int i = 0; i < split.length - 1; i++) {
			String part = split[i];
			if (isWildcard(part)) {
				break;
			}
			sb.append(part + SEPARATOR);
		}
		return glob.substring(0, sb.length());
	}

	static List<Path> findMatching(Path tempDir, String subglob, Path subdirectory) throws IOException {
		// optimization for listing all files
		if ("**".equals(subglob)) {
			List<Path> list = new ArrayList<>();
			walkFiles(tempDir, subdirectory, null, list::add);
			return list;
		}

		// optimization for single-file requests
		if (subglob.isEmpty()) {
			return Files.isRegularFile(subdirectory) ?
					List.of(subdirectory) :
					List.of();
		}

		// common route
		List<Path> list = new ArrayList<>();
		PathMatcher matcher = getPathMatcher(subdirectory.getFileSystem(), subglob);

		walkFiles(tempDir, subdirectory, subglob, path -> {
			if (matcher.matches(subdirectory.relativize(path))) {
				list.add(path);
			}
		});

		return list;
	}

	private static void walkFiles(Path tempDir, Path dir, @Nullable String glob, Walker walker) throws IOException {
		if (!Files.isDirectory(dir) || dir.startsWith(tempDir)) {
			return;
		}
		String[] parts;
		if (glob == null || (parts = glob.split(SEPARATOR))[0].contains("**")) {
			Files.walkFileTree(dir, new SimpleFileVisitor<>() {

				@Override
				public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) {
					if (dir.startsWith(tempDir)) {
						return SKIP_SUBTREE;
					}
					return CONTINUE;
				}

				@Override
				public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
					walker.accept(file);
					return CONTINUE;
				}

				@Override
				public FileVisitResult visitFileFailed(Path file, IOException exc) {
					logger.warn("Failed to visit file {}", file, exc);
					return CONTINUE;
				}
			});
			return;
		}

		java.nio.file.FileSystem fs = dir.getFileSystem();

		PathMatcher[] matchers = new PathMatcher[parts.length];
		matchers[0] = getPathMatcher(fs, parts[0]);

		for (int i = 1; i < parts.length; i++) {
			String part = parts[i];
			if (part.contains("**")) {
				break;
			}
			matchers[i] = getPathMatcher(fs, part);
		}

		Files.walkFileTree(dir, new SimpleFileVisitor<>() {
			@Override
			public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
				walker.accept(file);
				return CONTINUE;
			}

			@Override
			public FileVisitResult preVisitDirectory(Path subdir, BasicFileAttributes attrs) {
				if (subdir.equals(dir)) {
					return CONTINUE;
				}
				Path relative = dir.relativize(subdir);
				for (int i = 0; i < Math.min(relative.getNameCount(), matchers.length); i++) {
					PathMatcher matcher = matchers[i];
					if (matcher == null) {
						return CONTINUE;
					}
					if (!matcher.matches(relative.getName(i))) {
						return SKIP_SUBTREE;
					}
				}
				return CONTINUE;
			}

			@Override
			public FileVisitResult visitFileFailed(Path file, IOException exc) {
				logger.warn("Failed to visit file {}", file, exc);
				return CONTINUE;
			}
		});
	}

	private static PathMatcher getPathMatcher(java.nio.file.FileSystem fileSystem, String glob) throws GlobException {
		try {
			return fileSystem.getPathMatcher("glob:" + glob);
		} catch (PatternSyntaxException | UnsupportedOperationException e) {
			throw new GlobException("Glob: " + glob);
		}
	}

	public static void createDirectories(Path path, boolean fsyncDirectories) throws IOException {
		createDirectories(path, path.getRoot(), fsyncDirectories);
	}

	public static void createDirectories(Path path, Path root, boolean fsyncDirectories) throws IOException {
		Path parent = path;
		while (!parent.equals(root)) {
			if (Files.exists(parent)) break;
			parent = parent.getParent();
		}

		Path child = parent;
		for (Path name : parent.relativize(path)) {
			Path newChild = child.resolve(name);
			if (createDir(newChild) && fsyncDirectories) {
				tryFsync(child);
			}
			child = newChild;
		}
	}

	private static boolean createDir(Path path) throws IOException {
		try {
			Files.createDirectory(path);
			return true;
		} catch (FileAlreadyExistsException e) {
			if (!Files.isDirectory(path))
				throw e;
		}
		return false;
	}

	public static void tryFsync(Path path) {
		try (FileChannel channel = FileChannel.open(path)) {
			channel.force(true);
		} catch (IOException ignored) {
		}
	}

	static Path createTempUploadFile(Path tempDir) throws IOException {
		try {
			return Files.createTempFile(tempDir, "upload", "");
		} catch (NoSuchFileException e) {
			if (Files.exists(tempDir)) {
				throw e;
			}
			throw new FileSystemStructureException("Temporary directory " + tempDir + " not found");
		}
	}

	@FunctionalInterface
	public interface IOCallable<V> {
		V call() throws IOException;
	}

	@FunctionalInterface
	public interface IORunnable {
		void run() throws IOException;
	}

	@FunctionalInterface
	public interface Walker {
		void accept(Path path) throws IOException;
	}

	@FunctionalInterface
	public interface FileTransporter {
		void transport(Path from, Path to) throws IOException;
	}

}
