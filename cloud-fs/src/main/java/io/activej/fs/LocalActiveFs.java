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

import io.activej.async.service.EventloopService;
import io.activej.bytebuf.ByteBuf;
import io.activej.common.CollectorsEx;
import io.activej.common.MemSize;
import io.activej.common.exception.UncheckedException;
import io.activej.common.time.CurrentTimeProvider;
import io.activej.common.tuple.Tuple2;
import io.activej.csp.ChannelConsumer;
import io.activej.csp.ChannelSupplier;
import io.activej.csp.dsl.ChannelConsumerTransformer;
import io.activej.csp.file.ChannelFileReader;
import io.activej.csp.file.ChannelFileWriter;
import io.activej.eventloop.Eventloop;
import io.activej.eventloop.jmx.EventloopJmxBeanEx;
import io.activej.fs.exception.FsBatchException;
import io.activej.fs.exception.FsException;
import io.activej.fs.exception.FsIOException;
import io.activej.fs.exception.FsScalarException;
import io.activej.fs.exception.scalar.*;
import io.activej.jmx.api.attribute.JmxAttribute;
import io.activej.promise.Promise;
import io.activej.promise.Promise.BlockingCallable;
import io.activej.promise.Promise.BlockingRunnable;
import io.activej.promise.jmx.PromiseStats;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.Executor;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.regex.PatternSyntaxException;
import java.util.stream.Collector;
import java.util.stream.Stream;

import static io.activej.async.util.LogUtils.Level.TRACE;
import static io.activej.async.util.LogUtils.toLogger;
import static io.activej.common.Checks.checkArgument;
import static io.activej.common.collection.CollectionUtils.*;
import static io.activej.csp.dsl.ChannelConsumerTransformer.identity;
import static io.activej.fs.util.RemoteFsUtils.*;
import static java.nio.file.FileVisitResult.CONTINUE;
import static java.nio.file.FileVisitResult.SKIP_SUBTREE;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static java.nio.file.StandardOpenOption.*;
import static java.util.Collections.*;
import static java.util.Comparator.reverseOrder;

/**
 * An implementation of {@link ActiveFs} which operates on a real underlying filesystem, no networking involved.
 * <p>
 * Only permits file operations to be made within a specified storage path.
 * <p>
 * This implementation does not define new limitations, other than those defined in {@link ActiveFs} interface.
 */
public final class LocalActiveFs implements ActiveFs, EventloopService, EventloopJmxBeanEx {
	private static final Logger logger = LoggerFactory.getLogger(LocalActiveFs.class);

	public static final String DEFAULT_TEMP_DIR = ".upload";

	private static final char SEPARATOR_CHAR = SEPARATOR.charAt(0);
	private static final Function<String, String> toLocalName = File.separatorChar == SEPARATOR_CHAR ?
			Function.identity() :
			s -> s.replace(SEPARATOR_CHAR, File.separatorChar);

	private static final Function<String, String> toRemoteName = File.separatorChar == SEPARATOR_CHAR ?
			Function.identity() :
			s -> s.replace(File.separatorChar, SEPARATOR_CHAR);

	private final Eventloop eventloop;
	private final Path storage;
	private final Executor executor;

	private MemSize readerBufferSize = MemSize.kilobytes(256);
	private boolean hardlinkOnCopy = false;
	private Path tempDir;

	CurrentTimeProvider now;

	//region JMX
	private final PromiseStats uploadBeginPromise = PromiseStats.create(Duration.ofMinutes(5));
	private final PromiseStats uploadFinishPromise = PromiseStats.create(Duration.ofMinutes(5));
	private final PromiseStats appendBeginPromise = PromiseStats.create(Duration.ofMinutes(5));
	private final PromiseStats appendFinishPromise = PromiseStats.create(Duration.ofMinutes(5));
	private final PromiseStats downloadBeginPromise = PromiseStats.create(Duration.ofMinutes(5));
	private final PromiseStats downloadFinishPromise = PromiseStats.create(Duration.ofMinutes(5));
	private final PromiseStats listPromise = PromiseStats.create(Duration.ofMinutes(5));
	private final PromiseStats infoPromise = PromiseStats.create(Duration.ofMinutes(5));
	private final PromiseStats infoAllPromise = PromiseStats.create(Duration.ofMinutes(5));
	private final PromiseStats copyPromise = PromiseStats.create(Duration.ofMinutes(5));
	private final PromiseStats copyAllPromise = PromiseStats.create(Duration.ofMinutes(5));
	private final PromiseStats movePromise = PromiseStats.create(Duration.ofMinutes(5));
	private final PromiseStats moveAllPromise = PromiseStats.create(Duration.ofMinutes(5));
	private final PromiseStats deletePromise = PromiseStats.create(Duration.ofMinutes(5));
	private final PromiseStats deleteAllPromise = PromiseStats.create(Duration.ofMinutes(5));
	//endregion

	// region creators
	private LocalActiveFs(Eventloop eventloop, Path storage, Executor executor) {
		this.eventloop = eventloop;
		this.executor = executor;
		this.storage = storage;
		this.tempDir = storage.resolve(DEFAULT_TEMP_DIR);

		now = eventloop;
	}

	public static LocalActiveFs create(Eventloop eventloop, Executor executor, Path storageDir) {
		return new LocalActiveFs(eventloop, storageDir, executor);
	}

	/**
	 * Sets the buffer size for reading files from the filesystem.
	 */
	public LocalActiveFs withReaderBufferSize(MemSize size) {
		readerBufferSize = size;
		return this;
	}

	/**
	 * If set to {@code true}, an attempt to create a hard link will be made when copying files
	 */
	@SuppressWarnings("UnusedReturnValue")
	public LocalActiveFs withHardLinkOnCopy(boolean hardLinkOnCopy) {
		this.hardlinkOnCopy = hardLinkOnCopy;
		return this;
	}

	/**
	 * Sets a temporary directory for files to be stored while uploading.
	 */
	public LocalActiveFs withTempDir(Path tempDir) {
		this.tempDir = tempDir;
		return this;
	}
	// endregion

	@Override
	public Promise<ChannelConsumer<ByteBuf>> upload(@NotNull String name) {
		return doUpload(name, identity())
				.whenComplete(toLogger(logger, TRACE, "upload", name, this));
	}

	@Override
	public Promise<ChannelConsumer<ByteBuf>> upload(@NotNull String name, long size) {
		return doUpload(name, ofFixedSize(size))
				.whenComplete(toLogger(logger, TRACE, "upload", name, size, this));
	}

	@Override
	public Promise<ChannelConsumer<ByteBuf>> append(@NotNull String name, long offset) {
		checkArgument(offset >= 0, "Offset cannot be less than 0");
		return execute(
				() -> {
					Path path = resolve(name);
					FileChannel channel;
					if (offset == 0) {
						channel = ensureTarget(null, path, () -> FileChannel.open(path, CREATE, WRITE));
					} else {
						channel = FileChannel.open(path, WRITE);
					}
					if (channel.size() < offset) {
						throw new IllegalOffsetException(LocalActiveFs.class);
					}
					return channel;
				})
				.thenEx(translateScalarErrors(name))
				.whenComplete(appendBeginPromise.recordStats())
				.map(channel -> ChannelFileWriter.create(executor, channel)
						.withOffset(offset)
						.withAcknowledgement(ack -> ack
								.thenEx(translateScalarErrors(name))
								.whenComplete(appendFinishPromise.recordStats())
								.whenComplete(toLogger(logger, TRACE, "appendComplete", name, offset, this))))
				.whenComplete(toLogger(logger, TRACE, "append", name, offset, this));
	}

	@Override
	public Promise<ChannelSupplier<ByteBuf>> download(@NotNull String name, long offset, long limit) {
		checkArgument(offset >= 0, "offset < 0");
		checkArgument(limit >= 0, "limit < 0");

		return resolveAsync(name)
				.then(path -> execute(() -> {
					if (!Files.isRegularFile(path)) {
						throw new FileNotFoundException(LocalActiveFs.class);
					}
					FileChannel channel = FileChannel.open(path, READ);
					if (channel.size() < offset) {
						throw new IllegalOffsetException(LocalActiveFs.class);
					}
					return channel;
				}))
				.map(channel -> ChannelFileReader.create(executor, channel)
						.withBufferSize(readerBufferSize)
						.withOffset(offset)
						.withLimit(limit)
						.withEndOfStream(eos -> eos
								.thenEx(translateScalarErrors(name))
								.whenComplete(downloadFinishPromise.recordStats())
								.whenComplete(toLogger(logger, TRACE, "downloadComplete", name, offset, limit))))
				.thenEx(translateScalarErrors(name))
				.whenComplete(toLogger(logger, TRACE, "download", name, offset, limit, this))
				.whenComplete(downloadBeginPromise.recordStats());
	}

	@Override
	public Promise<Map<String, FileMetadata>> list(@NotNull String glob) {
		return execute(() -> findMatching(glob).stream()
				.collect(Collector.of(
						(Supplier<Map<String, FileMetadata>>) HashMap::new,
						(map, path) -> {
							FileMetadata metadata = toFileMetadata(path);
							if (metadata != null) {
								map.put(toFileName(path), metadata);
							}
						},
						CollectorsEx.throwingMerger())
				))
				.thenEx(translateScalarErrors())
				.whenComplete(toLogger(logger, TRACE, "list", glob, this))
				.whenComplete(listPromise.recordStats());
	}

	@Override
	public Promise<Void> copy(@NotNull String name, @NotNull String target) {
		return execute(() -> doCopy(map(name, target)))
				.thenEx(translateScalarErrors())
				.whenComplete(toLogger(logger, TRACE, "copy", name, target, this))
				.whenComplete(copyPromise.recordStats());
	}

	@Override
	public Promise<Void> copyAll(Map<String, String> sourceToTarget) {
		checkArgument(isBijection(sourceToTarget), "Targets must be unique");
		if (sourceToTarget.isEmpty()) return Promise.complete();

		return execute(() -> doCopy(sourceToTarget))
				.whenComplete(toLogger(logger, TRACE, "copyAll", toLimitedString(sourceToTarget, 50), this))
				.whenComplete(copyAllPromise.recordStats());
	}

	@Override
	public Promise<Void> move(@NotNull String name, @NotNull String target) {
		return execute(() -> doMove(map(name, target)))
				.thenEx(translateScalarErrors())
				.whenComplete(toLogger(logger, TRACE, "move", name, target, this))
				.whenComplete(movePromise.recordStats());
	}

	@Override
	public Promise<Void> moveAll(Map<String, String> sourceToTarget) {
		checkArgument(isBijection(sourceToTarget), "Targets must be unique");
		if (sourceToTarget.isEmpty()) return Promise.complete();

		return execute(() -> doMove(sourceToTarget))
				.whenComplete(toLogger(logger, TRACE, "moveAll", toLimitedString(sourceToTarget, 50), this))
				.whenComplete(moveAllPromise.recordStats());
	}

	@Override
	public Promise<Void> delete(@NotNull String name) {
		return execute(() -> doDelete(singleton(name)))
				.thenEx(translateScalarErrors(name))
				.whenComplete(toLogger(logger, TRACE, "delete", name, this))
				.whenComplete(deletePromise.recordStats());
	}

	@Override
	public Promise<Void> deleteAll(Set<String> toDelete) {
		if (toDelete.isEmpty()) return Promise.complete();

		return execute(() -> doDelete(toDelete))
				.whenComplete(toLogger(logger, TRACE, "deleteAll", toDelete, this))
				.whenComplete(deleteAllPromise.recordStats());
	}

	@Override
	public Promise<Void> ping() {
		return Promise.complete(); // local fs is always available
	}

	@Override
	public Promise<@Nullable FileMetadata> info(@NotNull String name) {
		return execute(() -> toFileMetadata(resolve(name)))
				.whenComplete(toLogger(logger, TRACE, "info", name, this))
				.whenComplete(infoPromise.recordStats());
	}

	@Override
	public Promise<Map<String, @NotNull FileMetadata>> infoAll(@NotNull Set<String> names) {
		if (names.isEmpty()) return Promise.of(emptyMap());

		return execute(
				() -> {
					Map<String, FileMetadata> result = new HashMap<>();
					for (String name : names) {
						FileMetadata metadata = toFileMetadata(resolve(name));
						if (metadata != null) {
							result.put(name, metadata);
						}
					}
					return result;
				})
				.whenComplete(toLogger(logger, TRACE, "infoAll", names, this))
				.whenComplete(infoAllPromise.recordStats());
	}

	@NotNull
	@Override
	public Eventloop getEventloop() {
		return eventloop;
	}

	@NotNull
	@Override
	public Promise<Void> start() {
		return execute(() -> {
			clearTempDir();
			Files.createDirectories(tempDir);
			if (!tempDir.startsWith(storage)) {
				Files.createDirectories(storage);
			}
		});
	}

	@NotNull
	@Override
	public Promise<Void> stop() {
		return Promise.complete();
	}

	@Override
	public String toString() {
		return "LocalActiveFs{storage=" + storage + '}';
	}

	private void tryDelete(Path target) throws IOException, IsADirectoryException {
		try {
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
		} catch (DirectoryNotEmptyException e) {
			throw dirEx(storage.relativize(target).toString());
		}
	}

	private IsADirectoryException dirEx(String name) {
		return new IsADirectoryException(LocalActiveFs.class, "Path '" + name + "' is a directory");
	}

	private void clearTempDir() throws IOException {
		if (!Files.isDirectory(tempDir)) {
			return;
		}
		try (Stream<Path> paths = Files.walk(tempDir)) {
			//noinspection ResultOfMethodCallIgnored
			paths.sorted(reverseOrder())
					.map(Path::toFile)
					.forEach(File::delete);
		}
	}

	private Path resolve(String name) throws ForbiddenPathException {
		Path path = storage.resolve(toLocalName.apply(name)).normalize();
		if (!path.startsWith(storage) || path.startsWith(tempDir)) {
			throw new ForbiddenPathException(LocalActiveFs.class, "Path '" + name + "' is forbidden");
		}
		return path;
	}

	private Promise<Path> resolveAsync(String name) {
		try {
			return Promise.of(resolve(name));
		} catch (FsException e) {
			return Promise.ofException(e);
		}
	}

	private Promise<ChannelConsumer<ByteBuf>> doUpload(String name, ChannelConsumerTransformer<ByteBuf, ChannelConsumer<ByteBuf>> transformer) {
		return execute(
				() -> {
					Path tempPath = Files.createTempFile(tempDir, "", "");
					return new Tuple2<>(tempPath, FileChannel.open(tempPath, CREATE, WRITE));
				})
				.map(pathAndChannel -> ChannelFileWriter.create(executor, pathAndChannel.getValue2())
						.transformWith(transformer)
						.withAcknowledgement(ack -> ack
								.then(() -> execute(() -> doMove(pathAndChannel.getValue1(), resolve(name))))
								.thenEx(translateScalarErrors(name))
								.whenException(() -> execute(() -> Files.deleteIfExists(pathAndChannel.getValue1())))
								.whenComplete(uploadFinishPromise.recordStats())
								.whenComplete(toLogger(logger, TRACE, "uploadComplete", name, this))))
				.thenEx(translateScalarErrors(name))
				.whenComplete(uploadBeginPromise.recordStats());
	}

	private void doCopy(Map<String, String> sourceToTargetMap) throws FsBatchException, FsIOException {
		for (Map.Entry<String, String> entry : sourceToTargetMap.entrySet()) {
			String from = entry.getKey();
			String to = entry.getValue();
			translateBatchErrors(from, to, () -> {
				Path path = resolve(from);
				if (!Files.isRegularFile(path)) {
					throw new FileNotFoundException(LocalActiveFs.class, "File '" + from + "' not found");
				}
				Path targetPath = resolve(to);

				if (path.equals(targetPath)) {
					touch(path);
					return;
				}

				ensureTarget(path, targetPath, () -> {
					if (hardlinkOnCopy) {
						try {
							Files.createLink(targetPath, path);
							touch(targetPath);
						} catch (UnsupportedOperationException | SecurityException | FileAlreadyExistsException e) {
							// if couldn't, then just actually copy it
							Files.copy(path, targetPath, REPLACE_EXISTING);
						}
					} else {
						Files.copy(path, targetPath, REPLACE_EXISTING);
					}
					return null;
				});
			});
		}
	}

	private void doMove(Map<String, String> sourceToTargetMap) throws FsBatchException, FsIOException {
		for (Map.Entry<String, String> entry : sourceToTargetMap.entrySet()) {
			String from = entry.getKey();
			String to = entry.getValue();
			translateBatchErrors(from, to, () -> {
				Path path = resolve(from);
				if (!Files.isRegularFile(path)) {
					throw new FileNotFoundException(LocalActiveFs.class, "File '" + from + "' not found");
				}
				Path targetPath = resolve(to);
				if (path.equals(targetPath)) {
					touch(path);
					return;
				}
				doMove(path, targetPath);
			});
		}
	}

	private void doMove(Path path, Path targetPath) throws IOException, FsScalarException {
		ensureTarget(path, targetPath, () -> {
			Files.createLink(targetPath, path);
			touch(targetPath);
			Files.deleteIfExists(path);
			return null;
		});
	}

	private void doDelete(Set<String> toDelete) throws FsBatchException, FsIOException {
		for (String name : toDelete) {
			translateBatchErrors(name, null, () -> {
				Path path = resolve(name);
				// cannot delete storage
				if (path.equals(storage)) return;

				try {
					Files.deleteIfExists(path);
				} catch (DirectoryNotEmptyException e) {
					throw dirEx(name);
				}
			});
		}
	}

	private <V> V ensureTarget(@Nullable Path source, Path target, FsCallable<V> afterCreation) throws IOException, FsScalarException {
		Path parent = target.getParent();
		while (true) {
			try {
				return afterCreation.call();
			} catch (NoSuchFileException e) {
				if (Files.exists(parent)) {
					throw e;
				}
				Files.createDirectories(parent);
			} catch (FileSystemException e) {
				if (source != null && !Files.isRegularFile(source)) {
					throw new FileNotFoundException(LocalActiveFs.class);
				}
				try {
					tryDelete(target);
				} catch (FileSystemException e2) {
					throw new PathContainsFileException(LocalActiveFs.class);
				}
			}
		}
	}

	private void touch(Path path) throws IOException {
		Files.setLastModifiedTime(path, FileTime.fromMillis(now.currentTimeMillis()));
	}

	private Collection<Path> findMatching(String glob) throws IOException, ForbiddenPathException, MalformedGlobException {
		// optimization for 'ping' empty list requests
		if (glob.isEmpty()) {
			return emptyList();
		}

		// get strict prefix folder from the glob
		StringBuilder sb = new StringBuilder();
		String[] split = glob.split(SEPARATOR);
		for (int i = 0; i < split.length - 1; i++) {
			String part = split[i];
			if (isWildcard(part)) {
				break;
			}
			sb.append(part).append(SEPARATOR);
		}
		String subglob = glob.substring(sb.length());
		Path subdirectory = resolve(sb.toString());

		// optimization for listing all files
		if ("**".equals(subglob)) {
			List<Path> list = new ArrayList<>();
			walkFiles(subdirectory, list::add);
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
		PathMatcher matcher = getPathMatcher(storage.getFileSystem(), subglob);

		walkFiles(subdirectory, subglob, path -> {
			if (matcher.matches(subdirectory.relativize(path))) {
				list.add(path);
			}
		});

		return list;
	}

	private String toFileName(Path path) {
		return toRemoteName.apply(storage.relativize(path).toString());
	}

	@Nullable
	private FileMetadata toFileMetadata(Path path) {
		try {
			if (!Files.isRegularFile(path)) return null;

			long timestamp = Files.getLastModifiedTime(path).toMillis();
			return FileMetadata.of(Files.size(path), timestamp);
		} catch (IOException e) {
			logger.warn("Failed to retrieve metadata for {}", path, e);
			throw new UncheckedException(new FsIOException(LocalActiveFs.class, "Failed to retrieve metadata"));
		}
	}

	@FunctionalInterface
	private interface Walker {

		void accept(Path path) throws IOException;
	}

	private void walkFiles(Path dir, Walker walker) throws IOException, MalformedGlobException {
		walkFiles(dir, null, walker);
	}

	private void walkFiles(Path dir, @Nullable String glob, Walker walker) throws IOException, MalformedGlobException {
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
					logger.warn("Failed to visit file {}", storage.relativize(file), exc);
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
				logger.warn("Failed to visit file {}", storage.relativize(file), exc);
				return CONTINUE;
			}
		});
	}

	private <T> Promise<T> execute(BlockingCallable<T> callable) {
		return Promise.ofBlockingCallable(executor, callable);
	}

	private Promise<Void> execute(BlockingRunnable runnable) {
		return Promise.ofBlockingRunnable(executor, runnable);
	}

	private <T> BiFunction<T, @Nullable Throwable, Promise<? extends T>> translateScalarErrors() {
		return translateScalarErrors(null);
	}

	private <T> BiFunction<T, @Nullable Throwable, Promise<? extends T>> translateScalarErrors(@Nullable String name) {
		return (v, e) -> {
			if (e == null) {
				return Promise.of(v);
			} else if (e instanceof FsBatchException) {
				Map<String, FsScalarException> exceptions = ((FsBatchException) e).getExceptions();
				assert exceptions.size() == 1;
				return Promise.ofException(first(exceptions.values()));
			} else if (e instanceof FsException) {
				return Promise.ofException(e);
			} else if (e instanceof FileAlreadyExistsException) {
				return execute(() -> {
					if (name != null && Files.isDirectory(resolve(name))) throw dirEx(name);
					throw new PathContainsFileException(LocalActiveFs.class);
				});
			} else if (e instanceof NoSuchFileException) {
				return Promise.ofException(new FileNotFoundException(LocalActiveFs.class));
			}
			return execute(() -> {
				if (name != null && Files.isDirectory(resolve(name))) throw dirEx(name);
				logger.warn("Operation failed", e);
				if (e instanceof IOException) {
					throw new FsIOException(LocalActiveFs.class, "IO Error");
				}
				throw new FsIOException(LocalActiveFs.class, "Unknown error");
			});
		};
	}

	private void translateBatchErrors(@NotNull String first, @Nullable String second, FsRunnable runnable) throws FsBatchException, FsIOException {
		try {
			runnable.run();
		} catch (FsScalarException e) {
			throw batchEx(LocalActiveFs.class, first, e);
		} catch (FileAlreadyExistsException e) {
			checkIfDirectories(first, second);
			throw batchEx(LocalActiveFs.class, first, new PathContainsFileException(LocalActiveFs.class));
		} catch (NoSuchFileException e) {
			throw batchEx(LocalActiveFs.class, first, new FileNotFoundException(LocalActiveFs.class));
		} catch (IOException e) {
			checkIfDirectories(first, second);
			logger.warn("Operation failed", e);
			throw new FsIOException(LocalActiveFs.class, "IO Error'");
		} catch (Exception e) {
			logger.warn("Operation failed", e);
			throw new FsIOException(LocalActiveFs.class, "Unknown Error");
		}
	}

	private void checkIfDirectories(@NotNull String first, @Nullable String second) throws FsBatchException {
		try {
			if (Files.isDirectory(resolve(first))) {
				throw batchEx(LocalActiveFs.class, first, dirEx(first));
			}
		} catch (ForbiddenPathException e) {
			throw batchEx(LocalActiveFs.class, first, e);
		}
		try {
			if (Files.isDirectory(resolve(second))) {
				throw batchEx(LocalActiveFs.class, first, dirEx(second));
			}
		} catch (ForbiddenPathException e) {
			throw batchEx(LocalActiveFs.class, first, e);
		}
	}

	private PathMatcher getPathMatcher(FileSystem fileSystem, String glob) throws MalformedGlobException {
		try {
			return fileSystem.getPathMatcher("glob:" + glob);
		} catch (PatternSyntaxException | UnsupportedOperationException e) {
			logger.warn("Malformed glob", e);
			throw new MalformedGlobException(LocalActiveFs.class);
		}
	}

	private interface FsCallable<V> {
		V call() throws IOException;
	}

	private interface FsRunnable {
		void run() throws IOException, FsScalarException;
	}

	//region JMX
	@JmxAttribute
	public PromiseStats getUploadBeginPromise() {
		return uploadBeginPromise;
	}

	@JmxAttribute
	public PromiseStats getUploadFinishPromise() {
		return uploadFinishPromise;
	}

	@JmxAttribute
	public PromiseStats getAppendBeginPromise() {
		return uploadBeginPromise;
	}

	@JmxAttribute
	public PromiseStats getAppendFinishPromise() {
		return uploadFinishPromise;
	}

	@JmxAttribute
	public PromiseStats getDownloadBeginPromise() {
		return downloadBeginPromise;
	}

	@JmxAttribute
	public PromiseStats getDownloadFinishPromise() {
		return downloadFinishPromise;
	}

	@JmxAttribute
	public PromiseStats getListPromise() {
		return listPromise;
	}

	@JmxAttribute
	public PromiseStats getInfoPromise() {
		return infoPromise;
	}

	@JmxAttribute
	public PromiseStats getInfoAllPromise() {
		return infoAllPromise;
	}

	@JmxAttribute
	public PromiseStats getCopyPromise() {
		return copyPromise;
	}

	@JmxAttribute
	public PromiseStats getCopyAllPromise() {
		return copyAllPromise;
	}

	@JmxAttribute
	public PromiseStats getMovePromise() {
		return movePromise;
	}

	@JmxAttribute
	public PromiseStats getMoveAllPromise() {
		return moveAllPromise;
	}

	@JmxAttribute
	public PromiseStats getDeletePromise() {
		return deletePromise;
	}

	@JmxAttribute
	public PromiseStats getDeleteAllPromise() {
		return deleteAllPromise;
	}
	//endregion
}
