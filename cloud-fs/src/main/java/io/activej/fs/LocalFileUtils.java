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
import io.activej.fs.exception.FsScalarException;
import io.activej.fs.exception.GlobException;
import io.activej.fs.exception.scalar.ForbiddenPathException;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.PatternSyntaxException;

import static io.activej.fs.ActiveFs.SEPARATOR;
import static io.activej.fs.util.RemoteFsUtils.isWildcard;
import static java.nio.file.FileVisitResult.CONTINUE;
import static java.nio.file.FileVisitResult.SKIP_SUBTREE;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

final class LocalFileUtils {
	private static final Logger logger = LoggerFactory.getLogger(LocalFileUtils.class);

	static void init(Path storage, Path tempDir) throws IOException {
		Files.createDirectories(tempDir);
		if (!tempDir.startsWith(storage)) {
			Files.createDirectories(storage);
		}
	}

	static void copy(InputStream from, OutputStream to) throws IOException {
		byte[] buf = new byte[16384];
		for (int n; (n = from.read(buf)) != -1; ) {
			to.write(buf, 0, n);
		}
	}

	static Path resolve(Path storage, Path tempDir, String name) throws ForbiddenPathException {
		Path path = storage.resolve(name).normalize();
		if (!path.startsWith(storage) || path.startsWith(tempDir)) {
			throw new ForbiddenPathException(LocalActiveFs.class, "Path '" + name + "' is forbidden");
		}
		return path;
	}

	static void touch(Path path, CurrentTimeProvider timeProvider) throws IOException {
		Files.setLastModifiedTime(path, FileTime.fromMillis(timeProvider.currentTimeMillis()));
	}

	static void tryDelete(Path target) throws IOException {
		Files.walkFileTree(target, new SimpleFileVisitor<Path>() {
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

	static <V> V ensureTarget(@Nullable Path source, Path target, FsCallable<V> afterCreation) throws IOException {
		Path parent = target.getParent();
		while (true) {
			try {
				return afterCreation.call();
			} catch (NoSuchFileException e) {
				if (source != null && !Files.exists(source)) {
					throw e;
				}
				Files.createDirectories(parent);
			} catch (FileSystemException e) {
				if (source != null && !Files.isRegularFile(source)) {
					throw new NoSuchFileException(null);
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
		try {
			Files.createLink(to, from);
			touch(to, timeProvider);
		} catch (UnsupportedOperationException | SecurityException | FileAlreadyExistsException e) {
			// if couldn't, then just actually copy it
			Files.copy(from, to, REPLACE_EXISTING);
		}
	}

	@Nullable
	static FileMetadata toFileMetadata(Path path) throws IOException {
		if (!Files.isRegularFile(path)) return null;

		long size = Files.size(path);
		long timestamp = Files.getLastModifiedTime(path).toMillis();
		return FileMetadata.of(size, timestamp);
	}

	static String extractSubDir(String glob) {
		StringBuilder sb = new StringBuilder();
		String[] split = glob.split(SEPARATOR);
		for (int i = 0; i < split.length - 1; i++) {
			String part = split[i];
			if (isWildcard(part)) {
				break;
			}
			sb.append(part).append(SEPARATOR);
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
					singletonList(subdirectory) :
					emptyList();
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
			Files.walkFileTree(dir, new SimpleFileVisitor<Path>() {

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

		FileSystem fs = dir.getFileSystem();

		PathMatcher[] matchers = new PathMatcher[parts.length];
		matchers[0] = getPathMatcher(fs, parts[0]);

		for (int i = 1; i < parts.length; i++) {
			String part = parts[i];
			if (part.contains("**")) {
				break;
			}
			matchers[i] = getPathMatcher(fs, part);
		}

		Files.walkFileTree(dir, new SimpleFileVisitor<Path>() {
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

	private static PathMatcher getPathMatcher(FileSystem fileSystem, String glob) throws GlobException {
		try {
			return fileSystem.getPathMatcher("glob:" + glob);
		} catch (PatternSyntaxException | UnsupportedOperationException e) {
			throw new GlobException();
		}
	}

	@FunctionalInterface
	interface FsCallable<V> {
		V call() throws IOException;
	}

	@FunctionalInterface
	interface FsRunnable {
		void run() throws IOException, FsScalarException;
	}

	@FunctionalInterface
	private interface Walker {
		void accept(Path path) throws IOException;
	}

}
